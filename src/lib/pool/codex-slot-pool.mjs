/**
 * GPT slot pool. The Claude pool scheduler never sees these VMs (`evaluateSlotGate`
 * returns `codex_vm`). A Codex hop picks here, then failovers on
 * quota/auth before any SSE byte is committed.
 */
import { isCodexVm } from '../vm/vm-kind.mjs'
import { codexQuotaParkFromLimits, extraToCodexSnapshot, normalizeCodexLimits } from '../protocol/codex-usage.mjs'
import { isLeftoverQuotaScheduleOff, isQuotaWindowReason } from './availability.mjs'
import { orderOpenAIAccounts } from './openai-account-selector.mjs'
import { bumpOpenAICursor, openAIRuntimeSignals, readOpenAICursor } from './openai-account-runtime.mjs'
import { modelMatchesAllowlist } from './slot-model-gate.mjs'
import { proxyBlockedReason } from '../vm/proxy-policy.mjs'
import { effectiveOpenAIPolicy, evaluateOpenAIQuotaGate, normalizeOpenAIQuotaPolicy } from './openai-quota-policy.mjs'

// `stopped` is leftover Claude docker lifecycle. Codex kernel is independent.
const HARD_UNAVAILABLE = new Set(['dead', 'error', 'disabled'])

export function isCodexSlotReady(vm) {
  if (!vm || !isCodexVm(vm)) return false
  if (proxyBlockedReason(vm.proxy)) return false
  if (vm.schedulable === false && !isLeftoverQuotaScheduleOff(vm)) return false
  if (!vm.has_token) return false
  const status = String(vm.status || '').toLowerCase()
  if (HARD_UNAVAILABLE.has(status)) return false
  return true
}

export function isCodexSlotParked(vm, now = Date.now(), policy = null) {
  return buildCodexSlotSnapshot(vm, now, normalizeOpenAIQuotaPolicy(policy)).parked
}

function extraFromSummary(vm) {
  if (vm?.codex?.extra && typeof vm.codex.extra === 'object') return vm.codex.extra
  if (vm?.codex_extra && typeof vm.codex_extra === 'object') return vm.codex_extra
  const usage = vm?.codex_usage
  const windows = Array.isArray(usage?.windows) ? usage.windows : []
  const w5 = windows.find((w) => w?.id === '5h') || {}
  const w7 = windows.find((w) => w?.id === '7d') || {}
  const limits = usage?.limits || {}
  return {
    codex_5h_used_percent: w5.used_percent ?? limits.used_5h_percent,
    codex_7d_used_percent: w7.used_percent ?? limits.used_7d_percent,
    codex_5h_reset_at: w5.reset_at ?? limits.reset_5h_at ?? vm?.reset_5h,
    codex_7d_reset_at: w7.reset_at ?? limits.reset_7d_at ?? vm?.reset_7d,
    codex_5h_window_minutes: w5.window_minutes ?? limits.window_5h_minutes ?? 300,
    codex_7d_window_minutes: w7.window_minutes ?? limits.window_7d_minutes ?? 10080,
    codex_limited_until: vm?.codex_limited_until || null,
  }
}

function usageState(vm, now) {
  const extra = extraFromSummary(vm)
  const limits = normalizeCodexLimits(extraToCodexSnapshot(extra))
  const u5 = Number(vm?.utilization_5h)
  const u7 = Number(vm?.utilization_7d)
  const p5 = Number(limits.used_5h_percent)
  const p7 = Number(limits.used_7d_percent)
  const hasVmUsage = Number.isFinite(u5) || Number.isFinite(u7)
  const used = hasVmUsage
    ? Math.max(Number.isFinite(u5) ? u5 : 0, Number.isFinite(u7) ? u7 : 0)
    : Number.isFinite(p5) || Number.isFinite(p7)
      ? Math.max(Number.isFinite(p5) ? p5 / 100 : 0, Number.isFinite(p7) ? p7 / 100 : 0)
      : null
  const reset5At = Date.parse(limits.reset_5h_at || '')
  const reset7At = Date.parse(limits.reset_7d_at || '')
  const park = codexQuotaParkFromLimits(limits, extra.codex_limited_until, now)
  const live5 = Number.isFinite(p5) && p5 >= 100 && (!Number.isFinite(reset5At) || reset5At > now)
  const live7 = Number.isFinite(p7) && p7 >= 100 && (!Number.isFinite(reset7At) || reset7At > now)
  const quotaWindowReason = live5
    ? 'quota_5h_header'
    : live7
      ? 'quota_7d_header'
      : park.limited
        ? 'quota_5h_header'
        : null
  const vmReset5 = Date.parse(vm?.reset_5h || '')
  const vmReset7 = Date.parse(vm?.reset_7d || '')
  let quotaResetAt = null
  for (const at of [reset5At, reset7At, vmReset5, vmReset7]) {
    if (Number.isFinite(at) && (quotaResetAt == null || at < quotaResetAt)) quotaResetAt = at
  }
  const restrictionTimes = [reset5At, reset7At, Date.parse(extra.codex_limited_until || '')].filter(
    (value) => Number.isFinite(value) && value > now,
  )
  return {
    extra,
    limits,
    park,
    quotaWindowReason,
    restrictionUntil: restrictionTimes.length ? Math.min(...restrictionTimes) : now + 5 * 60_000,
    stress: used ?? 0,
    quotaRemainingRank: used == null ? null : Math.round((1 - Math.min(1, Math.max(0, used))) * 10_000),
    quotaResetAt,
  }
}

function statusOf(vm, parked, quotaWindowReason) {
  if (!vm || !isCodexVm(vm)) return 'error'
  if (proxyBlockedReason(vm.proxy)) return 'disabled'
  if (vm.schedulable === false && !isLeftoverQuotaScheduleOff(vm)) return 'disabled'
  const status = String(vm.status || '').toLowerCase()
  if (status === 'disabled') return 'disabled'
  if (status === 'dead' || status === 'error') return 'error'
  if (!vm.has_token) return 'error'
  if (parked || quotaWindowReason) return 'quota_exhausted'
  return 'normal'
}

function buildCodexSlotSnapshot(vm, now, policy) {
  const usage = usageState(vm, now)
  const rawExtra = vm?.codex?.extra || vm?.codex_extra
  const prepared =
    rawExtra === usage.extra && rawExtra && typeof rawExtra === 'object' && Object.keys(rawExtra).length
      ? { extra: rawExtra, limits: usage.limits }
      : null
  const localGate = evaluateOpenAIQuotaGate(vm, policy, now, prepared)
  const explicitLimitAt = Date.parse(vm?.codex_limited_until || '')
  const parked = (Number.isFinite(explicitLimitAt) && explicitLimitAt > now) || usage.park.limited || localGate.limited
  return {
    vm,
    usage,
    localGate,
    parked,
    parkUntil: Number.isFinite(explicitLimitAt) ? explicitLimitAt : usage.park.until || now,
    status: statusOf(vm, parked, usage.quotaWindowReason),
    effective: effectiveOpenAIPolicy(vm, policy),
  }
}

/**
 * Ordered candidate ids. Pinned master hops stay on that slot.
 * Ready slots first (lowest 5h/7d stress), parked slots last so a
 * total-exhaust pool still has somewhere to fail instead of 503.
 */
function sameQuotaPolicy(left, right) {
  return (
    left?.limit_5h === right?.limit_5h &&
    left?.limit_7d === right?.limit_7d &&
    left?.max_concurrency === right?.max_concurrency &&
    left?.max_rpm === right?.max_rpm &&
    left?.max_sessions === right?.max_sessions
  )
}

function reusableSnapshotEntries(snapshotCache, policy, now) {
  if (!(snapshotCache?.entries instanceof Map) || Number(snapshotCache.expiresAt) <= now) return null
  if (!snapshotCache.policy) snapshotCache.policy = policy
  else if (!sameQuotaPolicy(snapshotCache.policy, policy)) {
    snapshotCache.entries.clear()
    snapshotCache.policy = policy
  }
  return snapshotCache.entries
}

function slotSnapshot(vm, now, policy, entries) {
  const cached = entries?.get(vm?.id)
  if (cached?.vm === vm) return cached
  const snapshot = buildCodexSlotSnapshot(vm, now, policy)
  entries?.set(vm.id, snapshot)
  return snapshot
}

function analyzeCodexSlots(vms, { pin = null, now = Date.now(), quotaPolicy = null, snapshotCache = null } = {}) {
  const list = Array.isArray(vms) ? vms : []
  const policy = normalizeOpenAIQuotaPolicy(quotaPolicy)
  const snapshots = reusableSnapshotEntries(snapshotCache, policy, now) || new Map()
  if (pin) {
    const vm = list.find((item) => item?.id === pin) || null
    if (!vm || !isCodexVm(vm)) {
      return { result: { error: 'platform_mismatch', pin, ids: [], ready: [], parked: [] }, snapshots, policy }
    }
    const blocked = proxyBlockedReason(vm.proxy)
    if (blocked) return { result: { error: blocked, pin, ids: [], ready: [], parked: [] }, snapshots, policy }
    slotSnapshot(vm, now, policy, snapshots)
    return { result: { ids: [vm.id], pin, ready: [vm.id], parked: [] }, snapshots, policy }
  }
  const ready = []
  const parked = []
  for (const vm of list) {
    if (!isCodexSlotReady(vm)) continue
    const snapshot = slotSnapshot(vm, now, policy, snapshots)
    if (snapshot.parked) parked.push(snapshot)
    else ready.push(snapshot)
  }
  ready.sort((a, b) => {
    const d = a.usage.stress - b.usage.stress
    return d !== 0 ? d : String(a.vm.id).localeCompare(String(b.vm.id))
  })
  parked.sort((a, b) => {
    const d = a.parkUntil - b.parkUntil
    return d !== 0 ? d : String(a.vm.id).localeCompare(String(b.vm.id))
  })
  const ids = [...ready, ...parked].map((snapshot) => snapshot.vm.id)
  const result = ids.length
    ? {
        ids,
        ready: ready.map((snapshot) => snapshot.vm.id),
        parked: parked.map((snapshot) => snapshot.vm.id),
      }
    : { error: 'no_codex_vm', ids: [], ready: [], parked: [] }
  return { result, snapshots, policy }
}

export function pickCodexSlots(vms, options = {}) {
  return analyzeCodexSlots(vms, options).result
}

export function codexAccountStatus(vm, now = Date.now(), policy = null) {
  return buildCodexSlotSnapshot(vm, now, normalizeOpenAIQuotaPolicy(policy)).status
}

/** Panel allowlist on a GPT slot. No model or no list means allowed. */
export function codexSlotAllowsModel(vm, model) {
  if (!model) return true
  return modelMatchesAllowlist(model, vm?.allowed_models ?? vm?.policy?.allowed_models)
}

export function codexAccountCandidate(vm, now = Date.now(), signals = null, policy = null) {
  const snapshot = buildCodexSlotSnapshot(vm, now, normalizeOpenAIQuotaPolicy(policy))
  return candidateFromSnapshot(snapshot, now, signals)
}

function candidateFromSnapshot(snapshot, now, signals = null) {
  const vm = snapshot.vm
  const runtime = signals || openAIRuntimeSignals(vm?.id, now)
  const effective = snapshot.effective
  const capacityBlockedUntil = Number(runtime.capacityBlockedUntil)
  const recoveryBlocked = !!runtime.recoveryBlocked
  return {
    id: vm.id,
    weight: Number(vm?.policy?.weight ?? vm?.weight ?? 1) || 1,
    concurrency: effective.max_concurrency,
    maxRpm: effective.max_rpm,
    maxSessions: effective.max_sessions,
    rpmCount: runtime.rpmCount || 0,
    rpmResetAt: runtime.rpmResetAt ?? null,
    status:
      recoveryBlocked || (Number.isFinite(capacityBlockedUntil) && capacityBlockedUntil > now)
        ? 'capacity_busy'
        : snapshot.status,
    inFlight: runtime.inFlight || 0,
    lastStartedAt: runtime.lastStartedAt ?? null,
    quotaResetAt: snapshot.usage.quotaResetAt,
    quotaRemainingRank: snapshot.usage.quotaRemainingRank,
    failureRateBps: runtime.failureRateBps || 0,
    firstOutputLatencyMs: runtime.firstOutputLatencyMs ?? null,
    capacityBlockedUntil:
      Number.isFinite(capacityBlockedUntil) && capacityBlockedUntil > now ? capacityBlockedUntil : null,
    recoveryBlocked,
  }
}

/**
 * OpenAI pool order. Session affinity is a preferred account.
 * Remaining ids are the same decision with that account excluded.
 * Quota-exhausted slots stay out of the hop list.
 *
 * `home` is the live bound slot. Only it (or any slot when there is none)
 * owes the conversation a `max_sessions` window; any other slot lends this
 * one request its execution seat. `session_slots` is not a window cap.
 * `boundState` is `gone` only when the bound slot left the pool for good
 * (quota, credential, operator, model); a busy home is still `live`.
 */
export function orderCodexSessionSlots(
  vms,
  {
    pin = null,
    boundVmId = null,
    sessionKey = null,
    sessionLimit = null,
    quotaPolicy = null,
    idleMin = 5,
    now = Date.now(),
    strategy = 'smart',
    requestIntervalMs = 0,
    roundRobinCursor = null,
    model = null,
    excluded = null,
    snapshotCache = null,
  } = {},
) {
  const analysis = analyzeCodexSlots(vms, { pin, now, quotaPolicy, snapshotCache })
  const { snapshots, policy } = analysis
  const found = analysis.result
  const picked = found.error ? found : withModelAllowed(found, snapshots, model)
  if (picked.error) return { ...picked, sticky: false, candidates: [] }
  const pool = picked.ids.filter((id) => !excluded?.has(id))
  if (!pool.length)
    return { error: 'candidates_exhausted', ids: [], ready: [], parked: [], sticky: false, candidates: [] }
  if (pin) {
    const candidate = candidateFromSnapshot(snapshots.get(pin), now)
    if (candidate.status === 'capacity_busy') {
      return {
        error: 'capacity_unavailable',
        retryAt: candidate.capacityBlockedUntil,
        ids: [],
        ready: [],
        parked: picked.parked || [],
        sticky: false,
        candidates: [],
      }
    }
    return {
      ...picked,
      ids: pool,
      sticky: false,
      candidates: [candidate],
    }
  }
  const home = boundVmId && pool.includes(boundVmId) && snapshots.get(boundVmId)?.status === 'normal' ? boundVmId : null
  const boundState = boundVmId ? (home ? 'live' : excluded?.has(boundVmId) ? 'excluded' : 'gone') : null
  const claimsWindow = (id) => !!sessionKey && (!home || id === home)
  const accepts = (id) => {
    if (!claimsWindow(id) || typeof sessionLimit?.canAccept !== 'function') return true
    const cap = snapshots.get(id)?.effective.max_sessions || 0
    if (!cap) return true
    return sessionLimit.canAccept(id, sessionKey, { max: cap, idleMin, now }).ok !== false
  }
  const ids = pool.filter(accepts)
  if (!ids.length) {
    return { error: 'session_window_full', ids: [], ready: [], parked: [], sticky: false, boundState, candidates: [] }
  }
  const cursor = roundRobinCursor == null ? readOpenAICursor() : roundRobinCursor
  const candidates = ids.map((id) => candidateFromSnapshot(snapshots.get(id), now))
  const candidateById = new Map(candidates.map((candidate) => [candidate.id, candidate]))
  const ordered = orderOpenAIAccounts(candidates, {
    strategy,
    now,
    requestIntervalMs,
    preferredAccountId: home && ids.includes(home) ? home : null,
    preferredOverridesWeight: true,
    roundRobinCursor: cursor,
  })
  if (!ordered.ids.length) {
    const quotaExhausted = ids.length > 0 && ids.every((id) => snapshots.get(id)?.status === 'quota_exhausted')
    return {
      error: quotaExhausted ? 'quota_exhausted' : 'capacity_unavailable',
      retryAt: quotaExhausted ? null : capacityRetryAt(candidates, now),
      ids: [],
      ready: [],
      parked: picked.parked || [],
      sticky: false,
      boundState,
      home,
      candidates: [],
    }
  }
  if (roundRobinCursor == null) bumpOpenAICursor()
  const sticky = ordered.preferred === 'hit' && ordered.ids[0] === boundVmId
  const ready = new Set(picked.ready || [])
  const orderedIds = new Set(ordered.ids)
  return {
    ...picked,
    ids: ordered.ids,
    ready: ordered.ids.filter((id) => ready.has(id)),
    parked: (picked.parked || []).filter((id) => orderedIds.has(id)),
    sticky,
    strategy,
    boundState,
    home,
    candidates: ordered.ids.map((id) => candidateById.get(id)),
  }
}

/** Distinct conversation windows on a GPT slot. 0 = off. */
export function codexMaxSessions(vm, policy = null) {
  const n = effectiveOpenAIPolicy(vm, normalizeOpenAIQuotaPolicy(policy)).max_sessions
  return n > 0 ? Math.round(n) : 0
}

/** Earliest RPM window opening among normal slots capped only by RPM. */
function capacityRetryAt(candidates, now) {
  const rpmTimes = candidates
    .filter((c) => c.status === 'normal' && c.maxRpm > 0 && c.rpmCount >= c.maxRpm && c.inFlight < c.concurrency)
    .map((c) => Number(c.rpmResetAt))
    .filter((t) => Number.isFinite(t) && t > now)
  const busyTimes = candidates.map((c) => Number(c.capacityBlockedUntil)).filter((t) => Number.isFinite(t) && t > now)
  const times = [...rpmTimes, ...busyTimes]
  return times.length ? Math.min(...times) : null
}

function withModelAllowed(picked, snapshots, model) {
  if (!model) return picked
  const keep = (id) => codexSlotAllowsModel(snapshots.get(id)?.vm, model)
  const ids = picked.ids.filter(keep)
  if (!ids.length) return { error: 'model_not_allowed', ids: [], ready: [], parked: [] }
  return {
    ...picked,
    ids,
    ready: (picked.ready || []).filter(keep),
    parked: (picked.parked || []).filter(keep),
  }
}

export function isCodexFailoverError(result) {
  if (!result || result.ok === true || result.committed === true) return false
  const status = Number(result.status) || 0
  const code = String(result.body?.error?.code || result.error_code || '')
  if (status === 429 || status === 401 || status === 403) return true
  return /usage_limit_reached|upstream_auth|no_credential|no_capacity|sticky_unavailable/.test(code)
}

export function codexQuotaWindowReason(vm, now = Date.now()) {
  return usageState(vm, now).quotaWindowReason
}

function hasQuotaRestriction(vm) {
  const reason = vm?.claude?.temp_unschedulable_reason || vm?.temp_unschedulable_reason
  return isQuotaWindowReason(reason)
}

/**
 * Extra 5h/7d writes restriction, not 调度关. Leftover quota-off
 * (not schedule_manual) restores the operator switch.
 */
export function evaluateCodexQuotaSchedule(vm, now = Date.now(), policy = null) {
  if (!vm || !isCodexVm(vm)) return { action: 'keep', reason: null }
  const snapshot = buildCodexSlotSnapshot(vm, now, normalizeOpenAIQuotaPolicy(policy))
  const hardReason = snapshot.usage.quotaWindowReason
  const local = snapshot.localGate
  const reason = hardReason || (local.limited ? local.reason : null)
  const leftover = isLeftoverQuotaScheduleOff(vm)
  if (reason) {
    const until = hardReason ? snapshot.usage.restrictionUntil : local.until
    return { action: leftover ? 'restore' : 'restrict', reason, until }
  }
  if (policy == null && /_local$/.test(vm?.claude?.temp_unschedulable_reason || vm?.temp_unschedulable_reason || '')) {
    return { action: 'keep', reason: vm?.claude?.temp_unschedulable_reason || vm?.temp_unschedulable_reason }
  }
  if (leftover) return { action: 'enable', reason: null }
  if (hasQuotaRestriction(vm)) return { action: 'clear', reason: null }
  return { action: 'keep', reason: null }
}
