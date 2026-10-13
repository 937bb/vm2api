/**
 * Codex hop from handle-protocol. Claude convert/pool/CRS never runs here.
 */
import path from 'node:path'
import { getVm, listVms, persistCodexUsage, syncCodexQuotaSchedule } from '../vm/vm-registry.mjs'
import { isValidVmId } from '../vm/vm-file.mjs'
import { isCodexProtocolAllowed, isCodexVm, normalizeCodexRouting } from './codex-route.mjs'
import { keyAllowsVm, keyScopeFromRequest, vmPoolDenial } from '../admin/key-scope.mjs'
import { normalizeOpenAIQuotaPolicy } from '../pool/openai-quota-policy.mjs'
import { restrictCodexClient } from './codex-restriction.mjs'
import {
  responsesSseToChatChunk,
  responsesSseToAnthropicEvents,
  createAnthropicSseState,
  createResponsesSseEventNamer,
  assembleCodexBodyFromSse,
  codexBodyToAnthropicMessage,
  isToolArgumentsError,
  toCodexResponses,
} from './codex-convert.mjs'
import { extraFromCodexHeaders, codexQuotaPark, CODEX_DEFAULT_PARK_MS } from './codex-usage.mjs'
import { extractOpenaiUsage } from './openai-usage.mjs'
import { streamCodexKernel } from '../transport/codex-kernel-client.mjs'
import {
  ensureCodexKernel,
  invalidateCodexKernelHealth,
  restartCodexKernel,
  writeCodexKernelConfig,
} from '../transport/codex-kernel-supervisor.mjs'
import { boundProxyUrl, isLocalEgressProxy } from '../vm/egress.mjs'
import { hostProxyUrlForVm } from '../vm/slot-host.mjs'
import { readCodexAccounts, syncCodexAccountScheduling, upsertCodexAccount } from '../vm/codex-slot.mjs'
import {
  CODEX_APP_VERSION,
  CODEX_CATALOG_ORIGINATOR,
  CODEX_SEARCH_URL,
  CODEX_USER_AGENT,
  makeProxyFetch,
  refreshCodexAccessToken,
} from './codex-models.mjs'
import { orderCodexSessionSlots, codexSlotAllowsModel, isCodexFailoverError } from '../pool/codex-slot-pool.mjs'
import {
  acquireOpenAISlot,
  beginOpenAIAccountRecovery,
  deferOpenAIWaiter,
  endOpenAIAccountRecovery,
  markOpenAICapacityRejected,
  markOpenAIKernelCooldown,
  openAIRuntimeSignals,
  reportOpenAIAttempt,
  waitForOpenAIAccountIdle,
  waitForOpenAICapacity,
} from '../pool/openai-account-runtime.mjs'
import { globalOpenAILeaseEnabled } from '../pool/openai-global-lease.mjs'
import { CLIENT_POOL_BUSY_MESSAGE, retryAfterSeconds } from '../core/errors.mjs'
import { isContentPolicyErrorCode } from '../core/refusal-guard.mjs'
import { streamIdleTimeoutMs } from '../core/config.mjs'
import { applyOpenaiWashLog } from './openai-wash.mjs'
import { sessionIdForLog } from './log-fields.mjs'
import { redactHeaders } from '../admin/request-log.mjs'
import {
  extractCallerSession,
  extractFirstUserIdentity,
  firstUserIdentityText,
  outboundSessionMode,
  resolveOutboundSessionId,
} from '../identity/identity-rewrite.mjs'
import { clientIp } from '../pool/sticky-router.mjs'
import { createDownstreamKeepalive, DEFAULT_STREAM_KEEPALIVE_MS } from './stream-keepalive.mjs'
import { writeKeepaliveIfWritable, writeWithBackpressure } from './stream-backpressure.mjs'

// Official Codex client headers for ChatGPT Responses. Auth is attached by the kernel.
const CODEX_KERNEL_HEADER_ALLOWLIST = Object.freeze([
  'originator',
  'openai-beta',
  'session-id',
  'thread-id',
  'x-client-request-id',
  'x-codex-beta-features',
  'x-codex-installation-id',
  'x-codex-parent-thread-id',
  'x-codex-turn-metadata',
  'x-codex-turn-state',
  'x-codex-window-id',
  'x-openai-internal-codex-responses-lite',
  'x-openai-memgen-request',
  'x-openai-subagent',
])

function headerString(headers, name) {
  const value = headers?.[name]
  return typeof value === 'string' && value.trim() ? value : ''
}

function codexKernelHeaders(reqHeaders = {}, body = {}, session = null) {
  const headers = {}
  for (const name of CODEX_KERNEL_HEADER_ALLOWLIST) {
    const value = headerString(reqHeaders, name)
    if (value) headers[name] = value
  }
  if (typeof session?.session_id === 'string' && session.session_id) {
    headers['session-id'] = session.session_id
  }
  const model = typeof body?.model === 'string' ? body.model.trim() : ''
  if (model) {
    const tier = typeof body?.service_tier === 'string' ? body.service_tier.trim().toLowerCase() : ''
    headers['x-codex-routing-hint'] =
      tier && tier !== 'default' && tier !== 'standard' && tier !== 'auto'
        ? `model=${model};tier=${tier}`
        : `model=${model}`
  }
  return headers
}

function sessionFrom(req, body) {
  const headers = req.headers || {}
  return {
    session_id:
      headers['x-session-id'] ||
      headers['session-id'] ||
      headers['x-conversation-id'] ||
      body?.conversation_id ||
      body?.session_id ||
      null,
    previous_response_id: body?.previous_response_id || null,
  }
}

/** Session seed identity: first non-empty input item, first non-reminder block (firstUserIdentityText). */
function firstUserIdentityFromCodex(body = {}) {
  if (typeof body?.input === 'string') return body.input
  if (Array.isArray(body?.input)) {
    for (const item of body.input) {
      const texts =
        typeof item === 'string'
          ? [item]
          : typeof item?.content === 'string'
            ? [item.content]
            : Array.isArray(item?.content)
              ? item.content.map((part) =>
                  typeof part === 'string' ? part : typeof part?.text === 'string' ? part.text : '',
                )
              : []
      const text = firstUserIdentityText(texts).trim()
      if (text) return text
    }
  }
  return extractFirstUserIdentity(body?.messages) || String(body?.prompt || '')
}

function applyCodexRebuildBody(body, sessionId, mode) {
  const out = { ...body }
  if (mode !== 'rebuild') return out
  out.prompt_cache_key = sessionId
  delete out.conversation_id
  delete out.session_id
  return out
}

function codexTransport(vm, routing) {
  const value = String(vm?.codex?.transport || routing?.codex?.transport || '')
    .trim()
    .toLowerCase()
  return value === 'websocket' || value === 'ws' ? 'websocket' : null
}

function pinnedVmId(req) {
  const pinVmRaw = String(req?.headers?.['x-kin-vm'] || '').trim()
  if (req?.apiKeyKind !== 'master') return null
  return isValidVmId(pinVmRaw) ? pinVmRaw : null
}

const DEFAULT_CODEX_INVENTORY_CACHE_MS = 250
const CODEX_INVENTORY_CACHE_LIMIT = 32
const DEFAULT_CODEX_KERNEL_PERMISSION_COOLDOWN_MS = 30_000
const codexInventoryCache = new Map()

function headerValue(headers, name) {
  const target = String(name).toLowerCase()
  for (const [key, value] of Object.entries(headers || {})) {
    if (String(key).toLowerCase() === target) return Array.isArray(value) ? value[0] : value
  }
  return null
}

function codexKernelCooldownUntil(result, now = Date.now()) {
  if (Number(result?.status) !== 403 || result?.committed) return null
  const code = String(result?.body?.error?.code || result?.error_code || '')
  if (isContentPolicyErrorCode(code)) return null
  const retryAfter = retryAfterSeconds(headerValue(result?.headers, 'retry-after'), now)
  return now + (retryAfter == null ? DEFAULT_CODEX_KERNEL_PERMISSION_COOLDOWN_MS : retryAfter * 1000)
}

function codexInventoryCacheMs() {
  const configured = Number(process.env.KIN_CODEX_INVENTORY_CACHE_MS)
  if (!Number.isFinite(configured)) return DEFAULT_CODEX_INVENTORY_CACHE_MS
  return Math.min(5000, Math.max(0, Math.trunc(configured)))
}

function codexInventoryKey(projectRoot, routingPolicy) {
  return `${projectRoot}\0${JSON.stringify(routingPolicy || {})}`
}

function clearCodexInventory(projectRoot) {
  const prefix = `${projectRoot}\0`
  for (const key of codexInventoryCache.keys()) {
    if (key.startsWith(prefix)) codexInventoryCache.delete(key)
  }
}

function listCodexInventory(projectRoot, routingPolicy, now = Date.now()) {
  const ttl = codexInventoryCacheMs()
  const key = codexInventoryKey(projectRoot, routingPolicy)
  const cached = ttl > 0 ? codexInventoryCache.get(key) : null
  if (cached && now - cached.at < ttl) {
    return { vms: cached.vms.slice(), snapshotCache: cached.snapshotCache }
  }
  const vms = listVms(projectRoot, { codex: { quota: routingPolicy } })
  for (const vm of vms) {
    if (isCodexVm(vm)) syncCodexQuotaSchedule(projectRoot, vm, { now, policy: routingPolicy })
  }
  const snapshotCache = ttl > 0 ? { expiresAt: now + ttl, entries: new Map(), policy: null } : null
  if (ttl > 0) {
    codexInventoryCache.set(key, { at: now, vms, snapshotCache })
    if (codexInventoryCache.size > CODEX_INVENTORY_CACHE_LIMIT) {
      codexInventoryCache.delete(codexInventoryCache.keys().next().value)
    }
  }
  return { vms: vms.slice(), snapshotCache }
}

/**
 * Current OpenAI candidates. Concurrent admissions share a very short
 * read-only inventory snapshot so one burst does not synchronously rescan all
 * VM and credential files for every waiter. A busy home keeps
 * its pin; only a home that left the pool for good releases the session.
 * A request that continues a stored response (`previous_response_id`) can
 * only run where that response lives.
 */
export function pickCodexCandidates(
  projectRoot,
  req,
  {
    stickyRouter = null,
    sessions = null,
    body = null,
    excluded = null,
    routing = {},
    stickyKeys: preparedStickyKeys = null,
    responseKey: preparedResponseKey = null,
    capacityRetryVmId = null,
  } = {},
) {
  const pin = pinnedVmId(req)
  const model = body?.model || null
  const routingPolicy = normalizeCodexRouting(routing.codex || routing).quota
  const scope = keyScopeFromRequest(req)
  const poolDenial = vmPoolDenial(scope)
  if (poolDenial) return { error: 'vm_pool_unavailable', message: poolDenial.message, ids: [], candidates: [] }
  if (scope.group_type === 'anthropic') return { error: 'key_group_mismatch', ids: [], candidates: [] }
  if (pin) {
    const vm = getVm(projectRoot, pin)
    if (!vm || !isCodexVm(vm)) return { error: 'platform_mismatch', pin, ids: [] }
    if (!keyAllowsVm(scope, vm)) return { error: 'key_group_mismatch', pin, ids: [] }
    if (!codexSlotAllowsModel(vm, model)) return { error: 'model_not_allowed', pin, ids: [] }
    const ordered = orderCodexSessionSlots([vm], {
      pin,
      excluded,
      quotaPolicy: routingPolicy,
    })
    return { ...ordered, pin, sticky: false, sessionKey: null, stickyKeys: [] }
  }
  const inventory = listCodexInventory(projectRoot, routingPolicy)
  const vms = inventory.vms
  const previousResponseId = String(body?.previous_response_id || '').trim()
  const responseKey = previousResponseId
    ? preparedResponseKey || stickyRouter?.responseKey?.(req, previousResponseId, 'openai')
    : null
  const responseBound = responseKey ? stickyRouter?.resolve?.(responseKey) : null
  const continuesResponse = !!previousResponseId
  const canCollectStickyKeys = typeof stickyRouter?.collectPoolKeys === 'function'
  const stickyKeys = Array.isArray(preparedStickyKeys)
    ? preparedStickyKeys
    : canCollectStickyKeys
      ? stickyRouter.collectPoolKeys(req, body || {}, { platform: 'openai' }) || []
      : []
  let sessionKey = stickyKeys[0] || null
  let sessionBound = null
  // A Responses continuation is pinned by its response id. Looking up the
  // ordinary conversation aliases cannot change that route and only adds
  // synchronous SQLite work to the request hot path.
  if (!continuesResponse) {
    for (const key of stickyKeys) {
      const hit = stickyRouter?.resolve?.(key)
      if (!hit) continue
      sessionKey = key
      sessionBound = hit
      break
    }
    if (!canCollectStickyKeys) {
      sessionKey = stickyRouter?.extractPoolKey?.(req, body || {}, { platform: 'openai' }) || null
      sessionBound = sessionKey ? stickyRouter?.resolve?.(sessionKey) : null
    }
  }
  const bound = continuesResponse ? responseBound : sessionBound
  const scopedVms = vms.filter((vm) => keyAllowsVm(scope, vm))
  const retryVmId = isValidVmId(capacityRetryVmId) ? capacityRetryVmId : null
  if (continuesResponse && !bound?.vmId) {
    return { error: 'response_not_portable', ids: [], sessionKey, stickyKeys, responseKey }
  }
  if (scope.vm_pool_id && scopedVms.length === 0 && !continuesResponse) {
    return { error: 'vm_pool_unavailable', message: '账号池没有可调度的 GPT 槽位', ids: [], candidates: [] }
  }
  const candidateVms = continuesResponse
    ? scopedVms.filter((vm) => vm.id === bound.vmId)
    : retryVmId
      ? scopedVms.filter((vm) => vm.id === retryVmId)
      : scopedVms
  const ordered = orderCodexSessionSlots(candidateVms, {
    pin: retryVmId,
    boundVmId: bound?.vmId || null,
    sessionKey,
    sessionLimit: sessions,
    model,
    excluded,
    quotaPolicy: routingPolicy,
    snapshotCache: inventory.snapshotCache,
  })
  if (bound?.vmId && sessionKey && ordered.boundState === 'gone') {
    for (const key of stickyKeys.length ? stickyKeys : [sessionKey]) stickyRouter?.unbind?.(key)
    try {
      sessions?.drop?.(bound.vmId, sessionKey)
    } catch {}
  }
  if (continuesResponse && (ordered.boundState === 'gone' || ordered.error === 'candidates_exhausted')) {
    return { error: 'response_not_portable', ids: [], sessionKey, stickyKeys, responseKey, boundVmId: bound.vmId }
  }
  const retryOwnsPin = !!retryVmId && bound?.vmId === retryVmId
  return {
    ...ordered,
    sticky: retryOwnsPin || ordered.sticky,
    boundState: retryOwnsPin ? 'live' : ordered.boundState,
    home: retryOwnsPin ? retryVmId : ordered.home,
    sessionKey,
    stickyKeys,
    responseKey,
    routeBound: bound,
  }
}

function ingestCodexHop(projectRoot, vmId, result, now = Date.now(), policy = null) {
  const headers = result?.headers || {}
  const extra = extraFromCodexHeaders(headers, now)
  let limitedUntil = null
  if (
    isCodexFailoverError(result) &&
    (Number(result?.status) === 429 || /usage_limit_reached/.test(String(result?.body?.error?.code || '')))
  ) {
    const park = extra ? codexQuotaPark(extra, now) : { limited: true, until: now + CODEX_DEFAULT_PARK_MS }
    limitedUntil = park.until || now + CODEX_DEFAULT_PARK_MS
  }
  if (!extra && !limitedUntil) return null
  const saved = persistCodexUsage(projectRoot, vmId, { headers, extra, limitedUntil, now, policy })
  clearCodexInventory(projectRoot)
  return saved
}

function execFor(projectRoot, vm) {
  return {
    vmId: vm.id,
    homeDir: path.join(projectRoot, 'vms', vm.id, 'cli-home'),
    vm,
  }
}

export function isRetryableCodexTransport(result) {
  if (!result || result.ok === true) return false
  if (result.committed === true) return false
  const code = String(result.body?.error?.code || result.error_code || '')
  const msg = String(result.body?.error?.message || result.error || '')
  const status = Number(result.status) || 0
  if (result.transportError === true) return true
  if (status === 0) return true
  if (status === 502 && /upstream_transport|worker_transport|transport/i.test(`${code} ${msg}`)) return true
  return /upstream_transport|worker_transport_error/i.test(code)
}

/** Idle SOCKS / first hop 502 is retryable only before any SSE byte is committed. */
/** `response.service_tier` from a Responses SSE data line, if present. */
export function serviceTierFromSseLine(line, event = null) {
  if (event && typeof event === 'object') {
    const tier = event?.response?.service_tier ?? event?.service_tier
    return typeof tier === 'string' && tier ? tier : null
  }
  if (typeof line !== 'string' || !line.startsWith('data:') || !line.includes('service_tier')) return null
  try {
    const ev = JSON.parse(line.slice(5).trim())
    const tier = ev?.response?.service_tier ?? ev?.service_tier
    return typeof tier === 'string' && tier ? tier : null
  } catch {
    return null
  }
}

/** Responses `usage` object from one SSE data line, if it carries token counts. */
export function usageFromSseLine(line, event = null) {
  if (event && typeof event === 'object') {
    const usage = event?.response?.usage || event?.usage
    return usage && typeof usage === 'object' ? usage : null
  }
  if (typeof line !== 'string' || !line.startsWith('data:') || !line.includes('usage')) return null
  try {
    const ev = JSON.parse(line.slice(5).trim())
    const usage = ev?.response?.usage || ev?.usage
    return usage && typeof usage === 'object' ? usage : null
  } catch {
    return null
  }
}

/** Response id carried by a Responses SSE lifecycle frame. */
export function responseIdFromSseLine(line, event = null) {
  if (event && typeof event === 'object') {
    const id = event?.response?.id
    return typeof id === 'string' && id.trim() ? id.trim() : null
  }
  if (typeof line !== 'string' || !line.startsWith('data:') || !line.includes('"id"')) return null
  try {
    const ev = JSON.parse(line.slice(5).trim())
    const id = ev?.response?.id
    return typeof id === 'string' && id.trim() ? id.trim() : null
  } catch {
    return null
  }
}

function usageTokens(usage) {
  const extracted = extractOpenaiUsage(usage)
  if (!extracted) return 0
  return (
    (extracted.input_tokens || 0) +
    (extracted.output_tokens || 0) +
    (extracted.cached_tokens || 0) +
    (extracted.cache_write_tokens || 0)
  )
}

function preferUsage(left, right) {
  if (usageTokens(right) > usageTokens(left)) return right
  return left || right || null
}

const WAIT_TIMEOUT_MIN_MS = 1000
const WAIT_TIMEOUT_MAX_MS = 120000
const DEFAULT_WAIT_TIMEOUT_MS = 30000
const DEFAULT_MAX_TOTAL_ATTEMPTS = 12
const DEFAULT_CODEX_STREAM_DEADLINE_MS = 300_000
const MAX_CODEX_STREAM_DEADLINE_MS = 600_000
const codexAuthRecoveries = new Map()

/** Codex can carry long reasoning turns, so allow an independent queue budget. */
function codexWaitTimeoutMs(routing) {
  const env = Number(process.env.KIN_CODEX_WAIT_TIMEOUT_MS)
  const n = Number.isFinite(env) ? env : Number(routing?.pool?.fallback_wait_timeout_ms)
  if (!Number.isFinite(n)) return DEFAULT_WAIT_TIMEOUT_MS
  return Math.min(WAIT_TIMEOUT_MAX_MS, Math.max(WAIT_TIMEOUT_MIN_MS, n))
}

function codexMaxTotalAttempts(routing) {
  const n = Number(routing?.failover?.max_total_attempts)
  if (!Number.isFinite(n)) return DEFAULT_MAX_TOTAL_ATTEMPTS
  return Math.min(32, Math.max(1, Math.trunc(n)))
}

function codexStreamDeadlineMs(upstreamTimeoutMs) {
  const configured = Number(process.env.KIN_CODEX_STREAM_DEADLINE_MS)
  const fallback = Math.min(
    DEFAULT_CODEX_STREAM_DEADLINE_MS,
    Math.max(1_000, Number(upstreamTimeoutMs) || DEFAULT_CODEX_STREAM_DEADLINE_MS),
  )
  const deadline = Number.isFinite(configured) && configured > 0 ? configured : fallback
  return Math.min(MAX_CODEX_STREAM_DEADLINE_MS, Math.max(1_000, Math.round(deadline)))
}

function isInvalidatedCodexAccess(result) {
  if (result?.committed) return false
  const status = Number(result?.status) || 0
  if (status !== 401 && status !== 403) return false
  const error = result?.body?.error || result?.body || {}
  return /invalidated oauth token|oauth token (?:has )?(?:expired|been invalidated)|access token has been revoked/i.test(
    `${error?.code || ''} ${error?.message || ''}`,
  )
}

async function recoverInvalidatedCodexAccess({ projectRoot, vm, usedAccess, refresh, restart }) {
  const key = String(vm?.id || '')
  if (!key || !usedAccess) return { ok: false, error: 'missing_access_token' }
  const existing = codexAuthRecoveries.get(key)
  if (existing) return { ok: false, deferred: true, inFlight: openAIRuntimeSignals(key).inFlight }

  const entry = { fromAccess: usedAccess, promise: null }
  beginOpenAIAccountRecovery(key)
  entry.promise = (async () => {
    // Never restart a shared kernel while another accepted stream is active.
    // This recovery runs in the background; the triggering request releases
    // its lease before the idle check can complete.
    const idle = await waitForOpenAIAccountIdle(key, {
      timeoutMs: Math.min(300_000, Math.max(10_000, Number(process.env.KIN_CODEX_AUTH_RECOVERY_TIMEOUT_MS) || 120_000)),
    })
    if (!idle) return { ok: false, error: 'kernel_drain_timeout' }
    let current = searchCredentials(projectRoot, vm)
    if (current.access === usedAccess) {
      if (!current.refresh) return { ok: false, error: 'missing_refresh_token' }
      const token = await refresh({
        refreshToken: current.refresh,
        proxyUrl: hostProxyUrlForVm(vm),
      })
      if (!token?.ok || !token.access_token) {
        return { ok: false, error: token?.error || 'refresh_failed' }
      }
      upsertCodexAccount(projectRoot, key, {
        access_token: token.access_token,
        refresh_token: token.refresh_token || current.refresh,
        id_token: token.id_token || current.first.id_token,
        expires_at: token.expires_at || current.first.expires_at,
      })
      current = searchCredentials(projectRoot, vm)
    }
    const ready = await restart(execFor(projectRoot, vm), { timeoutMs: 8000 })
    return ready?.ok
      ? { ok: true, access: current.access }
      : { ok: false, error: ready?.reason || 'kernel_restart_failed' }
  })().catch((error) => ({ ok: false, error: String(error?.message || error || 'auth_recovery_failed') }))
  codexAuthRecoveries.set(key, entry)
  entry.promise.finally(() => {
    try {
      // Token persistence and the following kernel restart can both rewrite
      // codex-credentials.json after the request-level scheduling guard ran.
      syncCodexAccountScheduling(projectRoot, vm)
    } catch {
      // Recovery completion must always release the runtime drain guard. A
      // later startup/hop reconciliation will retry a transient file error.
    } finally {
      endOpenAIAccountRecovery(key)
      if (codexAuthRecoveries.get(key) === entry) codexAuthRecoveries.delete(key)
    }
  })
  return { ok: false, deferred: true, inFlight: openAIRuntimeSignals(key).inFlight }
}

function clientGoneSignal(req, res) {
  const controller = new AbortController()
  const onGone = () => controller.abort()
  req?.once?.('aborted', onGone)
  res?.once?.('close', onGone)
  return {
    signal: controller.signal,
    settle() {
      req?.off?.('aborted', onGone)
      res?.off?.('close', onGone)
    },
  }
}

/**
 * Admission = fresh candidates + one synchronous seat claim. Candidates are
 * re-read every round, so a seat taken while another request awaited kernel
 * init is never double-sold. All full: FIFO wait for a release or the RPM
 * window, up to the pool wait deadline.
 */
async function admitCodexCandidate(projectRoot, req, opts, { deadline, signal }) {
  let woken = false
  for (;;) {
    const picked = pickCodexCandidates(projectRoot, req, opts)
    const livePolicy = normalizeCodexRouting(opts.routing?.codex || opts.routing).quota
    if (picked.error && picked.error !== 'capacity_unavailable') return { picked }
    for (const candidate of picked.candidates || []) {
      let lease
      try {
        lease = await acquireOpenAISlot(candidate.id, {
          concurrency: candidate.concurrency,
          maxRpm: candidate.maxRpm,
          quotaPolicy: livePolicy,
        })
      } catch (error) {
        if (error?.code === 'openai_lease_backend_unavailable') {
          return { picked: { error: 'openai_lease_backend_unavailable', ids: [] } }
        }
        throw error
      }
      if (lease) return { picked, vmId: candidate.id, lease }
    }
    const now = Date.now()
    if (now >= deadline) return { picked: { ...picked, error: 'capacity_unavailable', ids: [] } }
    const retryAt = Number(picked.retryAt)
    // Redis capacity uses the bounded poller. Local-only mode passes a missed
    // handoff slowly so an incompatible FIFO waiter cannot strand a free seat.
    if (woken === 'capacity' && !globalOpenAILeaseEnabled() && !(Number.isFinite(retryAt) && retryAt > now)) {
      deferOpenAIWaiter()
    }
    let sliceDeadline = Number.isFinite(retryAt) && retryAt > now ? Math.min(deadline, retryAt) : deadline
    let waited
    try {
      waited = await waitForOpenAICapacity({
        deadline: sliceDeadline,
        signal,
        globalPoll: globalOpenAILeaseEnabled() && sliceDeadline === deadline,
      })
    } catch (error) {
      if (error?.code === 'pool_wait_queue_full') return { picked: { error: 'pool_wait_queue_full', ids: [] } }
      throw error
    }
    if (waited.aborted) return { picked: { error: 'client_cancelled', ids: [] } }
    woken = waited.woken ? waited.reason || 'capacity' : false
  }
}

// Observed with gpt-6-astra + Responses Lite, not a documented upstream limit.
// Keep the model gate tight until another Lite model shows the same 128 KiB failure.
// Retry only a failed full-history attempt, keeping the newest reasoning suffix.
export function reasoningHistoryFallback(args, result) {
  if (process.env.KIN_CODEX_REASONING_HISTORY_FALLBACK === '0') return null
  const body = args.body
  const error = result?.body?.error || result?.body?.response?.error
  if (result?.ok || result?.committed || result?.transportError || args.signal?.aborted) return null
  if (![500, 502].includes(Number(result?.status)) || error?.code !== 'server_error') return null
  if (body?.model !== 'gpt-6-astra' || body?.reasoning?.context !== 'all_turns' || !Array.isArray(body.input))
    return null
  if (String(args.envelope?.headers?.['x-openai-internal-codex-responses-lite']).toLowerCase() !== 'true') return null
  if (body.previous_response_id || args.envelope?.session?.previous_response_id) return null
  const encrypted = (item) =>
    item?.type === 'reasoning' && typeof item.encrypted_content === 'string' && item.encrypted_content.length > 0
  const history = body.input.filter(encrypted)
  const originalBytes = Buffer.byteLength(JSON.stringify(history.map((item) => item.encrypted_content)))
  if (originalBytes <= 128 * 1024) return null
  let retainedBytes = 2
  const keep = new Set()
  for (let i = body.input.length - 1; i >= 0; i--) {
    const item = body.input[i]
    if (!encrypted(item)) continue
    const bytes = Buffer.byteLength(JSON.stringify(item.encrypted_content)) + (keep.size ? 1 : 0)
    if (retainedBytes + bytes > 120 * 1024) break
    retainedBytes += bytes
    keep.add(i)
  }
  if (!keep.size || keep.size === history.length) return null
  const nextBody = { ...body, input: body.input.filter((item, i) => !encrypted(item) || keep.has(i)) }
  return {
    args: { ...args, body: nextBody, envelope: { ...args.envelope, body: nextBody } },
    info: {
      original_array_bytes: originalBytes,
      retained_array_bytes: retainedBytes,
      original_items: history.length,
      retained_items: keep.size,
      omitted_items: history.length - keep.size,
    },
  }
}

export async function runCodexKernelHop({
  hop,
  args = {},
  onEvent,
  onEventBatch,
  onReasoningFallback,
  retryTransport = process.env.KIN_CODEX_UNSAFE_TRANSPORT_RETRY === '1',
  retrySignal = args.signal,
  allowReasoningFallback = true,
} = {}) {
  let emitted = false
  const wrapped = async (line, event = null) => {
    emitted = true
    if (onEvent) await onEvent(line, event)
  }
  const wrappedBatch = async (entries) => {
    if (!Array.isArray(entries) || entries.length === 0) return
    emitted = true
    if (onEventBatch) await onEventBatch(entries)
    else if (onEvent) {
      for (const entry of entries) await onEvent(entry?.line, entry?.event ?? null)
    }
  }
  const hopArgs = onEventBatch
    ? { ...args, onEvent: wrapped, onEventBatch: wrappedBatch }
    : { ...args, onEvent: wrapped }
  let result = await hop(hopArgs)
  // A kernel transport error can happen after OpenAI accepted the POST. Without
  // an upstream idempotency key, replaying here can execute and bill it twice.
  if (retryTransport && !retrySignal?.aborted && !result?.ok && !emitted && isRetryableCodexTransport(result)) {
    result = { ...(await hop(hopArgs)), transport_retried: true }
  }
  const fallback =
    allowReasoningFallback && !emitted && !result?.transport_retried
      ? reasoningHistoryFallback({ ...args, signal: retrySignal }, result)
      : null
  if (fallback) {
    if (onReasoningFallback)
      await onReasoningFallback({ originalResult: result, body: fallback.args.body, ...fallback.info })
    if (retrySignal?.aborted) return result
    result = {
      ...(await hop({
        ...fallback.args,
        signal: args.signal,
        onEvent: wrapped,
        ...(onEventBatch ? { onEventBatch: wrappedBatch } : {}),
      })),
      reasoning_history_retried: true,
      reasoning_history_fallback: fallback.info,
    }
  }
  return result
}

async function writeCommittedCodexError(res, protocol, result, signal) {
  if (res?.destroyed || res?.writableEnded) return false
  const source = result?.body?.error || result?.body || {}
  const code = String(source?.code || result?.error_code || 'codex_upstream')
  const message = String(source?.message || result?.message || 'Upstream stream ended before completion')
  try {
    if (protocol === 'openai.responses') {
      return await writeWithBackpressure(
        res,
        `event: error\ndata: ${JSON.stringify({ type: 'error', code, message, param: null })}\n\n`,
        signal,
      )
    } else if (protocol === 'anthropic.messages') {
      return await writeWithBackpressure(
        res,
        `event: error\ndata: ${JSON.stringify({
          type: 'error',
          error: { type: source?.type || 'api_error', message },
        })}\n\n`,
        signal,
      )
    } else {
      return await writeWithBackpressure(
        res,
        `data: ${JSON.stringify({
          error: { type: source?.type || 'api_error', code, message },
        })}\n\ndata: [DONE]\n\n`,
        signal,
      )
    }
  } catch {
    return false
  }
}

export async function handleCodexProtocol({
  req,
  res,
  protocol,
  ctx,
  inbound,
  logBag,
  stats,
  json,
  writeSSEHeaders,
  routing = {},
  projectRoot,
  requestId = null,
  attemptsRepo = null,
  ops = {},
  stickyRouter = null,
  sessions = null,
  body = null,
  captureOutbound = false,
  requestStartedAt = Date.now(),
  upstreamTimeoutMs = Number(process.env.KIN_UPSTREAM_TIMEOUT) || 600_000,
}) {
  const codex = normalizeCodexRouting(routing.codex)
  const allowed = isCodexProtocolAllowed(protocol, { codex })
  if (!allowed.ok) {
    stats.errors++
    logBag.via = 'codex-kernel'
    logBag.error_code = allowed.code
    return json(res, 400, {
      error: {
        type: 'invalid_request_error',
        code: allowed.code,
        message: `protocol '${protocol}' is not allowed on the Codex hop`,
      },
    })
  }
  const restriction = restrictCodexClient(req.headers, ctx.body || inbound, { codex }, protocol)
  if (!restriction.ok) {
    stats.errors++
    logBag.via = 'codex-kernel'
    logBag.error_code = restriction.code
    return json(res, 403, {
      error: { type: 'permission_error', code: restriction.code, message: restriction.message },
    })
  }
  const converted = toCodexResponses(protocol, ctx.body, codex.convert)
  if (!converted.ok) {
    stats.errors++
    logBag.via = 'codex-kernel'
    applyOpenaiWashLog(logBag, {
      inboundPath: ctx.path,
      inboundProtocol: protocol,
      converted: false,
    })
    logBag.error_code = converted.code
    return json(res, 400, {
      error: {
        type: 'invalid_request_error',
        code: converted.code,
        message: 'request could not be converted to Codex Responses',
      },
    })
  }
  applyOpenaiWashLog(logBag, {
    inboundPath: ctx.path,
    inboundProtocol: protocol,
    converted: converted.converted,
    outboundBody: converted.body,
  })
  const stream = inbound?.stream !== false && ctx.body?.stream !== false
  const inboundSession = sessionFrom(req, converted.body)
  const sessionMode = outboundSessionMode(routing)
  const callerSession = extractCallerSession({
    inbound: converted.body,
    body: converted.body,
    headers: req.headers,
  })
  // Chat→Responses conversion can surface a session key the raw inbound lacked.
  if (!logBag.session_id) logBag.session_id = sessionIdForLog(callerSession)
  const firstUserIdentity = firstUserIdentityFromCodex(converted.body)
  const hop = ops.streamCodexKernel || streamCodexKernel
  const writeCfg = ops.writeCodexKernelConfig || writeCodexKernelConfig
  const ensure = ops.ensureCodexKernel || ensureCodexKernel
  const anthropicSse = protocol === 'anthropic.messages' ? createAnthropicSseState() : null
  const chatSse =
    protocol === 'openai.chat' || protocol === 'openai.completions'
      ? { id: 'codex', seq: 0, tools: new Map(), sawTool: false }
      : null
  const pickOpts = {
    stickyRouter,
    sessions,
    body: body || converted.body || inbound,
    excluded: new Set(),
    routing,
  }
  const previousResponseId = String((body || converted.body || inbound)?.previous_response_id || '').trim()
  pickOpts.responseKey = previousResponseId
    ? stickyRouter?.responseKey?.(req, previousResponseId, 'openai') || null
    : null
  pickOpts.stickyKeys = previousResponseId
    ? []
    : stickyRouter?.collectPoolKeys?.(req, pickOpts.body || {}, { platform: 'openai' }) || []
  const deadline = Date.now() + codexWaitTimeoutMs(routing)
  const admissionStartedAt = Date.now()
  const maxAttempts = codexMaxTotalAttempts(routing)
  const gone = clientGoneSignal(req, res)
  logBag.via = 'codex-kernel'
  logBag.attempt_count = 0
  let last = null
  let hops = 0
  let attempts = 0
  let counted = false
  try {
    for (;;) {
      if (attempts >= maxAttempts) {
        const lastCode = String(last?.body?.error?.code || last?.error_code || '')
        if (lastCode === 'no_capacity') {
          return rejectCodexAdmission({
            res,
            json,
            stats,
            logBag,
            picked: { error: 'capacity_unavailable', ids: [] },
            model: converted.body?.model,
          })
        }
        break
      }
      const admitted = await admitCodexCandidate(projectRoot, req, pickOpts, { deadline, signal: gone.signal })
      const picked = admitted.picked
      if (!admitted.lease) {
        if (picked.error === 'client_cancelled') {
          logBag.final_state = 'cancelled'
          return
        }
        // no_capacity is the kernel's local credential scheduler, not an
        // OpenAI 503. If its bounded retry budget expires, report pool
        // backpressure so callers do not amplify it as an upstream outage.
        const lastCode = String(last?.body?.error?.code || last?.error_code || '')
        if (lastCode === 'no_capacity') {
          return rejectCodexAdmission({ res, json, stats, logBag, picked, model: converted.body?.model })
        }
        // Real upstream failures already happened: report the last one, not the pool.
        if (last) break
        return rejectCodexAdmission({ res, json, stats, logBag, picked, model: converted.body?.model })
      }
      const vm = getVm(projectRoot, admitted.vmId)
      const lease = admitted.lease
      if (!vm || !isCodexVm(vm)) {
        await lease.release()
        pickOpts.excluded.add(admitted.vmId)
        continue
      }
      if (!counted) {
        counted = true
        stats.requests++
        stats.by_route[protocol] = (stats.by_route[protocol] || 0) + 1
      }
      const stickyKeys = picked.stickyKeys?.length ? picked.stickyKeys : picked.sessionKey ? [picked.sessionKey] : []
      const stickyBound = picked.routeBound || (picked.sessionKey ? stickyRouter?.resolve?.(picked.sessionKey) : null)
      // The home (or first placement) owns the conversation window; a borrowed
      // seat elsewhere takes only this request's execution lease.
      const claimsWindow = !!picked.sessionKey && (!picked.home || picked.home === vm.id)
      let windowGen = null
      if (claimsWindow) {
        try {
          windowGen = sessions?.touch?.(vm.id, picked.sessionKey) ?? null
        } catch {}
      }
      const ownsPin = !!stickyBound?.vmId && stickyBound.vmId === vm.id
      const bindSticky = (sessionId = null) => {
        if (!picked.sessionKey || !stickyRouter?.bind) return
        const payload = { accountId: vm.id, vmId: vm.id }
        if (sessionId) payload.sessionId = sessionId
        for (const key of stickyKeys) stickyRouter.bind(key, payload)
      }
      const bindResponse = (responseId, sessionId = null) => {
        const key = stickyRouter?.responseKey?.(req, responseId, 'openai')
        if (!key || !stickyRouter?.bind) return
        stickyRouter.bind(key, { accountId: vm.id, vmId: vm.id, sessionId: sessionId || null }, { countHit: false })
      }
      const leaveSticky = () => {
        // A stored response cannot fail over. Preserve its conversation pin so
        // a later full-context request does not silently migrate after this hop.
        if (picked.responseKey) return
        if (!ownsPin || !picked.sessionKey) return
        try {
          sessions?.drop?.(vm.id, picked.sessionKey)
        } catch {}
        for (const key of stickyKeys) stickyRouter?.unbind?.(key)
      }
      let attemptKind = 'failed'
      try {
        writeCfg(projectRoot, vm, {
          proxyUrl: boundProxyUrl(vm.proxy),
          proxyRequired: true,
        })
        const ready = await ensure(execFor(projectRoot, vm))
        if (!ready?.ok) {
          // Kernel init failed: this seat goes back and the next candidate is re-admitted.
          last = {
            ok: false,
            status: 503,
            committed: false,
            body: {
              error: {
                type: 'api_error',
                code: 'codex_kernel_unavailable',
                message: `Codex kernel 未就绪（${ready?.reason || 'not_ready'}）。GPT 槽走独立 kernel，不是 wrap cli-hop。`,
              },
            },
          }
          pickOpts.excluded.add(vm.id)
          if (!res.headersSent) continue
          stats.errors++
          logBag.error_code = 'codex_kernel_unavailable'
          return json(res, 503, last.body)
        }
        // The bundled Codex kernel has no cancel route. Once a POST is handed
        // to it, aborting this Unix request only detaches Node while the
        // upstream job keeps running and retains the kernel permit. Do not
        // submit if the caller already left; after submission, drain to a
        // terminal result and release the local lease.
        if (gone.signal.aborted) {
          logBag.final_state = 'cancelled'
          return
        }
        hops += 1
        logBag.vm_id = vm.id
        logBag.account_id = vm.id
        logBag.final_account_id = vm.id
        attempts += 1
        logBag.attempt_count = attempts
        const chunks = []
        const nameSseEvent = createResponsesSseEventNamer()
        let responseServiceTier = null
        let responseId = null
        let streamedUsage = null
        let conversionError = null
        const attemptStartedAt = Date.now()
        const attemptNo = attempts
        const selectionReason = picked.sticky
          ? 'sticky'
          : picked.home && picked.home !== vm.id
            ? 'sticky-spill'
            : 'balanced'
        try {
          attemptsRepo?.begin?.({
            requestId,
            attemptNo,
            vmId: vm.id,
            accountId: vm.id,
            model: converted.body?.model,
            selectionReason,
            waitMs: attemptStartedAt - admissionStartedAt,
          })
        } catch {}
        const sessionOptions = {
          boundSessionId: stickyBound?.sessionId || '',
          boundVmId: stickyBound?.vmId || '',
          vmId: vm.id,
          accountId: vm.id,
          firstUserIdentity,
          clientIp: clientIp(req),
          userAgent: req.headers?.['user-agent'] || '',
          // Prompt-cache affinity is per upstream account. A conversation that
          // borrows another slot and later returns must recover its original
          // key instead of minting a new one for every failover generation.
          epoch: vm.id,
        }
        // passthrough forwards an explicit inbound session verbatim; without
        // one it still derives the deterministic seed instead of sending null.
        const outboundSessionId =
          sessionMode === 'passthrough'
            ? inboundSession.session_id ||
              resolveOutboundSessionId(callerSession, { ...sessionOptions, mode: 'passthrough', officialClient: true })
            : resolveOutboundSessionId(callerSession, { ...sessionOptions, mode: sessionMode })
        const session = {
          session_id: outboundSessionId,
          previous_response_id: inboundSession.previous_response_id,
        }
        logBag.outbound_session_id = sessionIdForLog(outboundSessionId)
        const outboundBody = applyCodexRebuildBody({ ...converted.body, stream: true }, outboundSessionId, sessionMode)
        const outboundHeaders = codexKernelHeaders(req.headers, outboundBody, session)
        const transport = codexTransport(vm, routing)
        const accessAtStart = searchCredentials(projectRoot, vm).access
        if (captureOutbound) logBag.outbound_body = outboundBody
        logBag.outbound_headers = redactHeaders(outboundHeaders)
        const streamKeepalive = stream
          ? createDownstreamKeepalive({
              intervalMs: Number(routing?.failover?.stream_keepalive_ms ?? DEFAULT_STREAM_KEEPALIVE_MS),
              userAgent: req.headers?.['user-agent'] || '',
              protocol,
              write: (chunk) => writeKeepaliveIfWritable(res, chunk, writeSSEHeaders),
            })
          : null
        const observeResponseMetadata = (line, event = null) => {
          const tier = serviceTierFromSseLine(line, event)
          if (tier) responseServiceTier = tier
          const seenResponseId = responseIdFromSseLine(line, event)
          if (seenResponseId) responseId = seenResponseId
          const seen = usageFromSseLine(line, event)
          if (seen) streamedUsage = preferUsage(streamedUsage, seen)
        }
        const writeResponsesBatch = async (entries) => {
          if (conversionError || !Array.isArray(entries) || entries.length === 0) return
          const output = []
          for (const entry of entries) {
            const line = String(entry?.line ?? '')
            const event = entry?.event ?? null
            observeResponseMetadata(line, event)
            if (gone.signal.aborted) continue
            if (!stream) {
              chunks.push(line)
              continue
            }
            const named = nameSseEvent(line, event)
            if (named) output.push(named)
          }
          if (!stream || gone.signal.aborted || output.length === 0) return
          streamKeepalive?.observeActivity()
          if (!res.headersSent) writeSSEHeaders(res)
          await writeWithBackpressure(res, output.join(''), gone.signal)
        }
        let result
        try {
          const run = () =>
            runCodexKernelHop({
              hop,
              args: {
                exec: execFor(projectRoot, vm),
                body: outboundBody,
                reqHeaders: req.headers,
                envelope: {
                  body: outboundBody,
                  headers: outboundHeaders,
                  stream: true,
                  session,
                  ...(transport ? { transport } : {}),
                },
                onResponse: ({ status }) => {
                  logBag.kernel_http_status = Number(status) || null
                },
                timeoutMs: Math.max(1_000, Number(upstreamTimeoutMs) || 600_000),
                idleTimeoutMs: streamIdleTimeoutMs(routing),
                overallTimeoutMs: codexStreamDeadlineMs(upstreamTimeoutMs),
                responsesPassthrough: protocol === 'openai.responses',
                // Kernel HTTP 200 and response.created only acknowledge the
                // local/upstream lifecycle. Start downstream keepalives after
                // real output commits, so a following SSE error can retain its
                // actual HTTP status instead of becoming a false 200 stream.
                onCommit: () => streamKeepalive?.arm(),
              },
              retrySignal: gone.signal,
              allowReasoningFallback: attempts < maxAttempts,
              retryTransport: process.env.KIN_CODEX_UNSAFE_TRANSPORT_RETRY === '1' && attempts < maxAttempts,
              onReasoningFallback: ({ body: retryBody }) => {
                if (captureOutbound) logBag.outbound_body = retryBody
              },
              onEventBatch: protocol === 'openai.responses' ? writeResponsesBatch : null,
              onEvent: async (line, event = null) => {
                if (conversionError) return
                observeResponseMetadata(line, event)
                // Keep consuming the kernel stream after a downstream disconnect.
                // This is lifecycle cleanup, not delivery: no more bytes are
                // written or buffered for the departed client.
                if (gone.signal.aborted) return
                if (!stream) {
                  chunks.push(line)
                  return
                }
                if (protocol === 'openai.responses') streamKeepalive?.observeActivity()
                else streamKeepalive?.observeLine(line)
                if (!res.headersSent) writeSSEHeaders(res)
                if (protocol !== 'openai.responses') {
                  try {
                    const mapped =
                      protocol === 'anthropic.messages'
                        ? responsesSseToAnthropicEvents(line, anthropicSse)
                        : responsesSseToChatChunk(line, 'codex', chatSse)
                    if (mapped) await writeWithBackpressure(res, mapped, gone.signal)
                  } catch (error) {
                    if (error.code !== 'tool_arguments_mismatch') throw error
                    conversionError = { type: 'upstream_protocol_error', code: error.code, message: error.message }
                    await writeWithBackpressure(
                      res,
                      protocol === 'anthropic.messages'
                        ? `event: error\ndata: ${JSON.stringify({ type: 'error', error: conversionError })}\n\n`
                        : `data: ${JSON.stringify({ error: conversionError })}\n\ndata: [DONE]\n\n`,
                      gone.signal,
                    )
                  }
                  return
                }
                const named = nameSseEvent(line, event)
                if (named) await writeWithBackpressure(res, named, gone.signal)
              },
            })
          result = await run()
          if (result?.transportError) invalidateCodexKernelHealth(vm.id)
          if (isInvalidatedCodexAccess(result) && !gone.signal.aborted) {
            const recovered = await recoverInvalidatedCodexAccess({
              projectRoot,
              vm,
              usedAccess: accessAtStart,
              refresh: ops.refreshCodexAccessToken || refreshCodexAccessToken,
              restart: ops.restartCodexKernel || restartCodexKernel,
            })
            if (recovered.deferred) logBag.codex_oauth_recovery_deferred = true
          }
        } finally {
          streamKeepalive?.stop()
          // The bundled kernel rewrites its credential document after token
          // persistence and drops panel-owned scheduling fields back to zero.
          // Restore those fields after every terminal hop so a later kernel
          // start cannot silently fall back to single-account capacity.
          syncCodexAccountScheduling(projectRoot, vm)
        }
        // The log and total duration share ingress time; pool scoring keeps hop TTFT.
        logBag.first_token_ms = Number.isFinite(result?.firstTokenAt)
          ? Math.max(0, result.firstTokenAt - requestStartedAt)
          : (result?.ttftMs ?? null)
        if (conversionError) {
          result.ok = false
          result.status = 502
          result.terminalState = 'incomplete'
          result.body = { error: conversionError }
        }
        ingestCodexHop(projectRoot, vm.id, result, Date.now(), normalizeCodexRouting(routing.codex).quota)
        if (result?.transport_retried) {
          attempts += 1
          logBag.attempt_count = attempts
          logBag.transport_retried = true
        }
        if (result?.reasoning_history_retried) {
          attempts += 1
          logBag.attempt_count = attempts
          logBag.reasoning_history_retried = true
        }
        last = result
        const hopUsage = result.usage || result.body?.usage || result.body?.response?.usage || null
        responseId =
          responseId ||
          (typeof result?.body?.response?.id === 'string' ? result.body.response.id : null) ||
          (typeof result?.body?.id === 'string' ? result.body.id : null)
        const usage = preferUsage(hopUsage, streamedUsage)
        const errorCode = String(result?.body?.error?.code || result?.error_code || '')
        // A Responses stream may carry a completed usage frame even when the
        // generic stream assembler cannot label the body as an assistant message.
        const delivered = !conversionError && (result?.ok || (Number(result?.status) === 200 && usageTokens(usage) > 0))
        if (errorCode === 'no_capacity') {
          attemptKind = 'capacity_rejected'
          markOpenAICapacityRejected(vm.id)
        }
        const kernelCooldownUntil = delivered ? null : codexKernelCooldownUntil(result)
        if (kernelCooldownUntil != null) markOpenAIKernelCooldown(vm.id, kernelCooldownUntil)
        const runtimeAfterAttempt = openAIRuntimeSignals(vm.id)
        try {
          attemptsRepo?.complete?.(requestId, attemptNo, {
            upstreamStatus: result?.status ?? null,
            errorScope: delivered
              ? null
              : errorCode === 'no_capacity'
                ? 'account'
                : result?.transportError
                  ? 'transport'
                  : Number(result?.status) === 401 || Number(result?.status) === 403
                    ? 'credential'
                    : Number(result?.status) === 429
                      ? 'quota'
                      : 'upstream',
            action: delivered
              ? 'return'
              : errorCode === 'no_capacity'
                ? 'wait'
                : isCodexFailoverError(result)
                  ? 'failover'
                  : 'stop',
            cooldownUntil: runtimeAfterAttempt.capacityBlockedUntil,
            downstreamCommitted: !!(result?.committed || res.headersSent),
            terminalState: delivered ? 'verified' : result?.terminalState || 'error',
            usage,
            ttftMs: result?.ttftMs ?? null,
            latencyMs: Date.now() - attemptStartedAt,
          })
        } catch {}
        // Responses SSE is not a Claude assistant message, so the stream client
        // reports ok:false / incomplete. A 200 hop that carried tokens still billed.
        if (delivered) {
          attemptKind = 'succeeded'
          logBag.error_code = null
          logBag.error_message = null
          logBag.upstream_status = null
          const extracted = extractOpenaiUsage(usage)
          const serviceTier = responseServiceTier || converted.body?.service_tier || usage?.service_tier || null
          logBag.usage = usage && serviceTier ? { ...usage, service_tier: serviceTier } : usage
          logBag.input_tokens = extracted?.input_tokens ?? usage?.input_tokens ?? usage?.prompt_tokens ?? null
          logBag.output_tokens = extracted?.output_tokens ?? usage?.output_tokens ?? usage?.completion_tokens ?? null
          logBag.cache_read_tokens =
            extracted?.cached_tokens ?? usage?.input_tokens_details?.cached_tokens ?? usage?.cache_read_tokens ?? null
          logBag.cache_creation_tokens =
            extracted?.cache_write_tokens ??
            usage?.input_tokens_details?.cache_write_tokens ??
            usage?.cache_creation_tokens ??
            null
          reportOpenAIAttempt(vm.id, 'succeeded', result.ttftMs ?? null)
          if (gone.signal.aborted) {
            logBag.final_state = 'client_cancelled_after_submit'
            return
          }
          bindSticky(outboundSessionId)
          if (responseId) bindResponse(responseId, outboundSessionId)
          logBag.final_state = result?.ok ? result.terminalState || 'verified' : 'verified'
          logBag.upstream_model = converted.body.model
          if (hops > 1) logBag.codex_failed_over = true
          if (!stream) {
            let body
            try {
              const assembled = assembleCodexBodyFromSse(chunks, result.body || {})
              body =
                protocol === 'anthropic.messages'
                  ? codexBodyToAnthropicMessage(assembled, converted.body.model)
                  : assembled
            } catch (error) {
              if (!isToolArgumentsError(error)) throw error
              stats.errors++
              logBag.error_code = error.code
              logBag.final_state = 'incomplete'
              return json(res, 502, {
                error: { type: 'upstream_protocol_error', code: error.code, message: error.message },
              })
            }
            return json(res, 200, body)
          }
          if (!res.headersSent) writeSSEHeaders(res)
          return res.end()
        }
        logBag.error_message = result?.body?.error?.message || result?.body?.message || null
        reportOpenAIAttempt(vm.id, attemptKind, result?.ttftMs ?? null)
        if (gone.signal.aborted) {
          logBag.final_state = 'client_cancelled_after_submit'
          return
        }
        if (res.headersSent) {
          stats.errors++
          logBag.error_code = result?.body?.error?.code || 'codex_upstream'
          logBag.upstream_status = result?.status || 0
          logBag.final_state = result?.terminalState || 'incomplete'
          if (!gone.signal.aborted) await writeCommittedCodexError(res, protocol, result, gone.signal)
          return res.end()
        }
        if (isCodexFailoverError(result)) {
          // no_capacity is a transient admission mismatch: park it in the
          // runtime gate and let a pinned conversation wait on its home slot.
          // New conversations may still spill because they have no warm cache.
          if (errorCode === 'no_capacity') {
            if (ownsPin) pickOpts.capacityRetryVmId = vm.id
          } else {
            pickOpts.capacityRetryVmId = null
            pickOpts.excluded.add(vm.id)
            leaveSticky()
          }
          continue
        }
        break
      } finally {
        await lease.release()
        if (claimsWindow) {
          try {
            sessions?.release?.(vm.id, picked.sessionKey, { gen: windowGen })
          } catch {}
        }
      }
    }
  } finally {
    gone.settle()
  }
  stats.errors++
  logBag.error_code = last?.body?.error?.code || 'codex_upstream'
  logBag.error_message = last?.body?.error?.message || last?.body?.message || null
  logBag.upstream_status = last?.status || 0
  return json(res, last?.status || 502, last?.body || { error: { type: 'api_error', code: 'codex_upstream' } })
}

/** No seat was admitted and nothing ran: say which gate stopped the request. */
function rejectCodexAdmission({ res, json, stats, logBag, picked, model }) {
  stats.errors++
  logBag.error_code = picked.error || 'no_codex_vm'
  if (picked.error === 'platform_mismatch') {
    return json(res, 400, {
      error: {
        type: 'invalid_request_error',
        code: 'platform_mismatch',
        message: `vm '${picked.pin}' is not a GPT slot`,
      },
    })
  }
  if (picked.error === 'key_group_mismatch') {
    return json(res, 403, {
      error: {
        type: 'permission_error',
        code: 'key_group_mismatch',
        message: '此密钥不能调用 openai',
      },
    })
  }
  if (picked.error === 'vm_pool_unavailable') {
    return json(res, 403, {
      error: {
        type: 'permission_error',
        code: 'vm_pool_unavailable',
        message: picked.message || '账号池不可用',
      },
    })
  }
  if (picked.error === 'model_not_allowed') {
    return json(res, 400, {
      error: {
        type: 'invalid_request_error',
        code: 'model_not_allowed',
        message: `model '${model || ''}' is not allowed on any GPT slot`,
        param: 'model',
      },
    })
  }
  if (picked.error === 'response_not_portable') {
    return json(res, 409, {
      error: {
        type: 'invalid_request_error',
        code: 'response_not_portable',
        message: 'previous_response_id 所在的 GPT 账号已不可用；该响应无法在其他账号继续，请携带完整上下文重新发起',
      },
    })
  }
  if (
    picked.error === 'capacity_unavailable' ||
    picked.error === 'session_window_full' ||
    picked.error === 'pool_wait_queue_full'
  ) {
    // Queue-full has no retryAt. Every Codex pool_overloaded 429 still advertises >= 1s.
    const waitMs = Number(picked.retryAt) - Date.now()
    const retryAfterSec = Math.max(1, waitMs > 0 ? Math.ceil(waitMs / 1000) : 0)
    res.setHeader?.('retry-after', String(retryAfterSec))
    return json(res, 429, {
      error: {
        type: 'rate_limit_error',
        code: 'pool_overloaded',
        message: CLIENT_POOL_BUSY_MESSAGE,
        details: { reason: picked.error },
      },
    })
  }
  if (picked.error === 'quota_exhausted') {
    return json(res, 503, {
      error: { type: 'api_error', code: 'quota_exhausted', message: 'OpenAI 号池额度已耗尽' },
    })
  }
  if (picked.error === 'openai_lease_backend_unavailable') {
    return json(res, 503, {
      error: {
        type: 'api_error',
        code: 'gateway_capacity_backend_unavailable',
        message: 'Gateway concurrency coordinator is unavailable. Retry later.',
      },
    })
  }
  return json(res, 503, {
    error: { type: 'api_error', code: picked.error || 'no_codex_vm', message: 'no Codex kernel VM is configured' },
  })
}

// Codex web search (`web.run`) is a plain JSON POST to `{base_url}/alpha/search`. Responses-Lite
// models (gpt-6.1-sol, gpt-6-astra) have no hosted web_search tool, and the CLI fails the whole
// turn on a non-2xx answer, so a missing route breaks every turn that searches.
const CODEX_SEARCH_TIMEOUT_MS = 120000
// The backend answers "Unknown parameter" for these.
const CODEX_SEARCH_DROP_FIELDS = Object.freeze(['prompt_cache_key', 'prompt_cache_retention', 'store'])
const searchRefreshes = new Map()

function searchCredentials(projectRoot, vm) {
  const first = readCodexAccounts(projectRoot, vm.id)[0] || {}
  return {
    first,
    access: String(first.access_token || vm.codex?.access_token || '').trim(),
    refresh: String(first.refresh_token || vm.codex?.refresh_token || '').trim(),
    accountId: String(first.chatgpt_account_id || vm.codex?.chatgpt_account_id || '').trim(),
  }
}

/**
 * Access token to retry with after a 401/403. The kernel refreshes the same rotating
 * refresh token on its own, so first adopt a token it wrote meanwhile; otherwise refresh
 * once per slot, shared by concurrent callers.
 */
function refreshSearchAccess(projectRoot, vm, usedAccess, { proxyUrl, fetchImpl, refresh }) {
  const now = searchCredentials(projectRoot, vm)
  if (now.access && now.access !== usedAccess) return now.access
  if (!now.refresh) return ''
  let pending = searchRefreshes.get(vm.id)
  if (!pending) {
    pending = (async () => {
      const tok = await refresh({ refreshToken: now.refresh, proxyUrl, fetchImpl })
      if (!tok?.ok || !tok.access_token) return ''
      upsertCodexAccount(projectRoot, vm.id, {
        access_token: tok.access_token,
        refresh_token: tok.refresh_token || now.refresh,
        id_token: tok.id_token || now.first.id_token,
        expires_at: tok.expires_at || now.first.expires_at,
      })
      return tok.access_token
    })().finally(() => searchRefreshes.delete(vm.id))
    searchRefreshes.set(vm.id, pending)
  }
  return pending
}

function plainHeaders(headers) {
  if (typeof headers?.entries === 'function') return Object.fromEntries(headers.entries())
  return { ...(headers || {}) }
}

/** One search call on one slot. Resolves `{ status, text, headers }`, or `{ status: 0, error }`. */
async function postCodexSearch({ projectRoot, vm, body, turnMetadata, fetchImpl, refresh = refreshCodexAccessToken }) {
  const proxyUrl = hostProxyUrlForVm(vm)
  if (!fetchImpl && !proxyUrl && !isLocalEgressProxy(vm.proxy)) return { status: 0, error: 'proxy_required' }
  const doFetch = fetchImpl || makeProxyFetch(proxyUrl, CODEX_SEARCH_TIMEOUT_MS, { maxMs: CODEX_SEARCH_TIMEOUT_MS })
  const creds = searchCredentials(projectRoot, vm)
  const send = async (access) => {
    // Same identity as the slot's kernel; the backend wants originator, User-Agent and version to agree.
    const headers = {
      authorization: `Bearer ${access}`,
      'content-type': 'application/json',
      accept: 'application/json',
      originator: CODEX_CATALOG_ORIGINATOR,
      'user-agent': CODEX_USER_AGENT,
      version: CODEX_APP_VERSION,
    }
    if (creds.accountId) headers['chatgpt-account-id'] = creds.accountId
    if (turnMetadata) headers['x-codex-turn-metadata'] = turnMetadata
    try {
      const res = await doFetch(CODEX_SEARCH_URL, { method: 'POST', headers, body: JSON.stringify(body) })
      return { status: Number(res?.status) || 0, text: await res.text(), headers: plainHeaders(res?.headers) }
    } catch (e) {
      return { status: 0, error: /abort/i.test(`${e?.name} ${e?.message}`) ? 'upstream_timeout' : 'upstream_transport' }
    }
  }
  if (!creds.access && !creds.refresh) return { status: 0, error: 'no_oauth_token' }
  let out = creds.access ? await send(creds.access) : { status: 401 }
  if (out.status === 401 || out.status === 403) {
    const access = await refreshSearchAccess(projectRoot, vm, creds.access, { proxyUrl, fetchImpl, refresh })
    if (access) out = await send(access)
  }
  return out
}

function isSearchFailover(out) {
  const status = Number(out?.status) || 0
  return status === 0 || status === 401 || status === 402 || status === 403 || status === 429 || status >= 500
}

function parseJson(text) {
  try {
    return JSON.parse(text)
  } catch {
    return undefined
  }
}

/**
 * POST /v1/alpha/search for Codex web search. Not inference: no session window, sticky bind
 * or latency report, but the call holds a seat on the slot it runs on. Auth/quota/rate-limit
 * failures move to the next GPT slot; anything else is the backend's answer, passed through.
 */
export async function handleCodexSearch({
  req,
  res,
  body,
  logBag,
  stats,
  json,
  routing = {},
  projectRoot,
  stickyRouter = null,
  ops = {},
}) {
  logBag.via = 'codex-search'
  const reject = (status, type, code, message) => {
    stats.errors++
    logBag.error_code = code
    return json(res, status, { error: { type, code, message } })
  }
  const codex = normalizeCodexRouting(routing.codex)
  if (!codex.enabled) return reject(400, 'invalid_request_error', 'codex_disabled', 'Codex routing is disabled')
  const restriction = restrictCodexClient(req.headers, body, { codex }, 'openai.search')
  if (!restriction.ok) return reject(403, 'permission_error', restriction.code, restriction.message)
  if (!body || typeof body !== 'object' || typeof body.model !== 'string' || !body.model.trim()) {
    return reject(400, 'invalid_request_error', 'model_required', 'model is required')
  }
  const outbound = { ...body }
  for (const key of CODEX_SEARCH_DROP_FIELDS) delete outbound[key]
  const turnMetadata = headerString(req.headers, 'x-codex-turn-metadata')
  const post = ops.postCodexSearch || postCodexSearch
  const pickOpts = { stickyRouter, sessions: null, body: outbound, excluded: new Set() }
  const deadline = Date.now() + codexWaitTimeoutMs(routing)
  const gone = clientGoneSignal(req, res)
  let last = null
  let attempts = 0
  try {
    for (;;) {
      const admitted = await admitCodexCandidate(projectRoot, req, pickOpts, { deadline, signal: gone.signal })
      if (!admitted.lease) {
        if (admitted.picked.error === 'client_cancelled') {
          logBag.final_state = 'cancelled'
          return
        }
        if (last) break
        return rejectCodexAdmission({ res, json, stats, logBag, picked: admitted.picked, model: outbound.model })
      }
      try {
        const vm = getVm(projectRoot, admitted.vmId)
        pickOpts.excluded.add(admitted.vmId)
        if (!vm || !isCodexVm(vm)) continue
        if (!attempts) {
          stats.requests++
          stats.by_route['openai.search'] = (stats.by_route['openai.search'] || 0) + 1
        }
        attempts += 1
        logBag.vm_id = vm.id
        logBag.account_id = vm.id
        logBag.attempt_count = attempts
        last = await post({
          projectRoot,
          vm,
          body: outbound,
          turnMetadata,
          fetchImpl: ops.fetchImpl,
          refresh: ops.refresh,
        })
        logBag.upstream_status = last.status || null
        ingestCodexHop(projectRoot, vm.id, {
          ok: last.status >= 200 && last.status < 300,
          status: last.status,
          headers: last.headers || {},
          body: parseJson(last.text),
        })
        if (!isSearchFailover(last)) break
      } finally {
        await admitted.lease.release()
      }
    }
  } finally {
    gone.settle()
  }
  if (!last.status) {
    logBag.final_state = 'upstream_error'
    return reject(502, 'api_error', last.error || 'upstream_transport', 'Codex search upstream request failed')
  }
  const payload = parseJson(last.text)
  if (payload === undefined) {
    logBag.final_state = 'upstream_error'
    return reject(
      502,
      'api_error',
      'upstream_invalid_body',
      `Codex search upstream answered ${last.status} without JSON`,
    )
  }
  logBag.final_state = last.status < 300 ? 'verified' : 'upstream_error'
  if (last.status >= 400) {
    stats.errors++
    logBag.error_code = payload?.error?.code || `upstream_${last.status}`
  }
  return json(res, last.status, payload)
}
