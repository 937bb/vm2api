/**
 * Process-local OpenAI scheduling signals.
 * Failure EWMA matches codex-proxy-rs AccountFeedbackStats:
 * success/failure α=0.2, capacity reject α=0.4, 15 minute half-life.
 */

import {
  acquireOpenAIGlobalLease,
  globalOpenAILeaseEnabled,
  globalOpenAILeasePollMs,
  globalOpenAILeaseRequired,
} from './openai-global-lease.mjs'

const FAILURE_ALPHA = 0.2
const CAPACITY_ALPHA = 0.4
const FAILURE_HALF_LIFE_MS = 15 * 60 * 1000
const RPM_WINDOW_MS = 60_000
const CAPACITY_REJECT_BASE_MS = 500
const CAPACITY_REJECT_MAX_MS = 30_000

export const OPENAI_MAX_WAITERS = 256
const DEFAULT_GLOBAL_POLL_BATCH = 32

function maxWaiters() {
  const configured = Number(process.env.KIN_OPENAI_MAX_WAITERS)
  if (!Number.isFinite(configured)) return OPENAI_MAX_WAITERS
  return Math.min(2048, Math.max(1, Math.trunc(configured)))
}

function globalPollBatch() {
  const configured = Number(process.env.KIN_OPENAI_REDIS_POLL_BATCH)
  if (!Number.isFinite(configured)) return DEFAULT_GLOBAL_POLL_BATCH
  return Math.min(256, Math.max(1, Math.trunc(configured)))
}

const slots = new Map()
const waiters = []
const feedback = new Map()
let roundRobinCursor = 0
let globalPollTimer = null
let deferredWakeTimer = null

function scheduleGlobalPoll() {
  if (globalPollTimer || !waiters.length) return
  globalPollTimer = setTimeout(() => {
    globalPollTimer = null
    const count = Math.min(globalPollBatch(), waiters.length)
    for (let i = 0; i < count; i += 1) {
      const next = waiters.shift()
      next?.wake('global_poll')
    }
    if (waiters.length) scheduleGlobalPoll()
  }, globalOpenAILeasePollMs())
  globalPollTimer.unref?.()
}

export function deferOpenAIWaiter(reason = 'capacity') {
  if (deferredWakeTimer || !waiters.length) return
  deferredWakeTimer = setTimeout(() => {
    deferredWakeTimer = null
    wakeOpenAIWaiter(reason)
  }, 10)
  deferredWakeTimer.unref?.()
}

function slotOf(id) {
  const key = String(id || '')
  let slot = slots.get(key)
  if (!slot) {
    slot = {
      inFlight: 0,
      lastStartedAt: null,
      starts: [],
      startsHead: 0,
      capacityRejects: 0,
      capacityBlockedUntil: null,
      capacityProbeInFlight: false,
      recoveryBlocked: false,
      retirementBlocked: false,
    }
    slots.set(key, slot)
  }
  return slot
}

function decayedFailure(entry, now) {
  if (!entry?.updatedAt) return entry?.value || 0
  const elapsed = Math.max(0, now - entry.updatedAt)
  return entry.value * 0.5 ** (elapsed / FAILURE_HALF_LIFE_MS)
}

function recentStarts(slot, now) {
  if (!slot?.starts?.length) return 0
  let head = Number(slot.startsHead) || 0
  while (head < slot.starts.length && now - slot.starts[head] >= RPM_WINDOW_MS) head += 1
  if (head >= slot.starts.length) {
    slot.starts.length = 0
    slot.startsHead = 0
    return 0
  }
  // Compact infrequently; normal reads only advance an index and allocate nothing.
  if (head >= 1024 && head * 2 >= slot.starts.length) {
    slot.starts = slot.starts.slice(head)
    head = 0
  }
  slot.startsHead = head
  return slot.starts.length - head
}

export function openAIRuntimeSignals(id, now = Date.now()) {
  const key = String(id || '')
  const slot = slots.get(key)
  const entry = feedback.get(key)
  const failure = entry ? decayedFailure(entry, now) : 0
  const latency = entry?.latency
  const rpmCount = recentStarts(slot, now)
  return {
    inFlight: slot?.inFlight || 0,
    lastStartedAt: slot?.lastStartedAt ?? null,
    rpmCount,
    rpmResetAt: rpmCount ? slot.starts[slot.startsHead] + RPM_WINDOW_MS : null,
    capacityBlockedUntil: Number(slot?.capacityBlockedUntil) > now ? Number(slot.capacityBlockedUntil) : null,
    recoveryBlocked: !!(slot?.recoveryBlocked || slot?.retirementBlocked),
    retirementBlocked: !!slot?.retirementBlocked,
    failureRateBps: Math.round(Math.min(1, Math.max(0, failure)) * 10_000),
    firstOutputLatencyMs: Number.isFinite(latency) && latency > 0 ? Math.round(latency) : null,
  }
}

export function openAIRuntimeSnapshot(id, now = Date.now()) {
  return openAIRuntimeSignals(id, now)
}

export function readOpenAICursor() {
  return roundRobinCursor
}

export function bumpOpenAICursor() {
  roundRobinCursor += 1
  return roundRobinCursor
}

/**
 * One synchronous check-and-claim: concurrency and RPM are read and taken in
 * the same tick, so two admissions can never both see the last free seat.
 * Returns a lease whose release() is idempotent, or null when full.
 */
export function tryAcquireOpenAISlot(id, { concurrency = 1, maxRpm = 0, now = Date.now(), quotaPolicy = null } = {}) {
  // Candidate selection resolves pins against the same live policy snapshot.
  // Never replace a resolved legacy/pinned candidate cap with the global cap
  // here: doing so can sell more seats than the candidate was selected for.
  const effectiveConcurrency = Number.isFinite(Number(concurrency))
    ? Number(concurrency)
    : Number(quotaPolicy?.max_concurrency)
  const effectiveRpm = Number.isFinite(Number(maxRpm)) ? Number(maxRpm) : Number(quotaPolicy?.max_rpm)
  const cap = Number(effectiveConcurrency) > 0 ? Number(effectiveConcurrency) : 1
  const slot = slotOf(id)
  const recoveringCapacity = Number(slot.capacityRejects || 0) > 0
  if (
    slot.recoveryBlocked ||
    slot.retirementBlocked ||
    Number(slot.capacityBlockedUntil) > now ||
    (recoveringCapacity && slot.capacityProbeInFlight)
  )
    return null
  if (slot.inFlight >= cap) return null
  const rpm = Number(effectiveRpm) || 0
  const rpmCount = recentStarts(slot, now)
  if (rpm > 0 && rpmCount >= rpm) return null
  slot.inFlight += 1
  if (recoveringCapacity) slot.capacityProbeInFlight = true
  slot.lastStartedAt = now
  slot.starts.push(now)
  let released = false
  return {
    id: String(id || ''),
    release({ wake = true } = {}) {
      if (released) return
      released = true
      slot.inFlight = Math.max(0, slot.inFlight - 1)
      if (recoveringCapacity) slot.capacityProbeInFlight = false
      if (wake) wakeOpenAIWaiter()
    },
  }
}

/**
 * Take the process-local seat first, then atomically claim the same account in
 * Redis when multi-gateway coordination is enabled.
 */
export async function acquireOpenAISlot(id, options = {}) {
  const local = tryAcquireOpenAISlot(id, options)
  if (!local || !globalOpenAILeaseEnabled()) return local
  const concurrency = Number.isFinite(Number(options.concurrency))
    ? Number(options.concurrency)
    : Number(options.quotaPolicy?.max_concurrency)
  let global
  try {
    global = await acquireOpenAIGlobalLease(id, { concurrency })
  } catch (cause) {
    if (!globalOpenAILeaseRequired()) return local
    local.release({ wake: false })
    throw Object.assign(new Error('OpenAI global concurrency backend is unavailable'), {
      code: 'openai_lease_backend_unavailable',
      cause,
    })
  }
  if (!global) {
    // This was only a provisional local claim. Waking another waiter here
    // turns a full Redis account into an unbounded O(queue) retry chain.
    local.release({ wake: false })
    return null
  }
  let released = false
  return {
    id: local.id,
    global: true,
    async release() {
      if (released) return
      released = true
      // Publish the Redis release before waking a local waiter. Otherwise the
      // woken request races the still-live global lease and starts another
      // failed-probe cascade.
      local.release({ wake: false })
      const removed = await global.release()
      wakeOpenAIWaiter()
      return removed
    },
  }
}

export function openAIWaiterCount() {
  return waiters.length
}

/** FIFO: one released seat wakes the oldest waiter. A woken waiter that still misses passes it on. */
export function wakeOpenAIWaiter(reason = 'capacity') {
  const next = waiters.shift()
  next?.wake(reason)
}

/**
 * The kernel can be healthy while its only credential is temporarily busy.
 * Back pressure subsequent admissions instead of leaking a local 503 to every caller.
 */
export function markOpenAICapacityRejected(id, now = Date.now()) {
  const slot = slotOf(id)
  slot.capacityRejects = Math.min(16, Number(slot.capacityRejects || 0) + 1)
  const delay = Math.min(CAPACITY_REJECT_MAX_MS, CAPACITY_REJECT_BASE_MS * 2 ** (slot.capacityRejects - 1))
  slot.capacityBlockedUntil = Math.max(Number(slot.capacityBlockedUntil) || 0, now + delay)
  return slot.capacityBlockedUntil
}

export function clearOpenAICapacityRejected(id) {
  const slot = slots.get(String(id || ''))
  if (!slot) return
  slot.capacityRejects = 0
  slot.capacityBlockedUntil = null
  slot.capacityProbeInFlight = false
}

export function beginOpenAIAccountRecovery(id) {
  const slot = slotOf(id)
  slot.recoveryBlocked = true
}

export function endOpenAIAccountRecovery(id) {
  const slot = slots.get(String(id || ''))
  if (!slot) return
  slot.recoveryBlocked = false
  wakeOpenAIWaiter()
}

export function beginOpenAIAccountRetirement(id) {
  const slot = slotOf(id)
  slot.retirementBlocked = true
}

export function endOpenAIAccountRetirement(id) {
  const slot = slots.get(String(id || ''))
  if (!slot) return
  slot.retirementBlocked = false
  wakeOpenAIWaiter()
}

export async function waitForOpenAIAccountIdle(id, { timeoutMs = 120_000, intervalMs = 25 } = {}) {
  const deadline = Date.now() + Math.max(0, Number(timeoutMs) || 0)
  for (;;) {
    if ((slotOf(id).inFlight || 0) === 0) return true
    if (Date.now() >= deadline) return false
    await new Promise((resolve) => setTimeout(resolve, Math.max(5, Number(intervalMs) || 25)))
  }
}

/**
 * Park until a seat frees, `deadline` passes, or `signal` aborts.
 * Resolves `{ woken }`; throws `pool_wait_queue_full` past OPENAI_MAX_WAITERS.
 */
export function waitForOpenAICapacity({ deadline, signal = null, globalPoll = false } = {}) {
  if (waiters.length >= maxWaiters()) {
    return Promise.reject(Object.assign(new Error('OpenAI pool wait queue is full'), { code: 'pool_wait_queue_full' }))
  }
  return new Promise((resolve) => {
    const entry = {}
    const cleanup = () => {
      clearTimeout(timer)
      signal?.removeEventListener?.('abort', onAbort)
      const at = waiters.indexOf(entry)
      if (at >= 0) waiters.splice(at, 1)
    }
    const onAbort = () => {
      cleanup()
      resolve({ woken: false, aborted: true })
    }
    entry.wake = (reason = 'capacity') => {
      cleanup()
      resolve({ woken: true, reason })
    }
    const timer = setTimeout(
      () => {
        cleanup()
        resolve({ woken: false })
      },
      Math.max(1, Number(deadline) - Date.now()),
    )
    timer.unref?.()
    waiters.push(entry)
    if (globalPoll) scheduleGlobalPoll()
    if (signal?.aborted) onAbort()
    else signal?.addEventListener?.('abort', onAbort, { once: true })
  })
}

export function reportOpenAIAttempt(id, kind, firstOutputMs = null, now = Date.now()) {
  const key = String(id || '')
  if (!key) return
  const entry = feedback.get(key) || { value: 0, updatedAt: null, latency: null }
  const sample = kind === 'succeeded' ? 0 : 1
  const alpha = kind === 'capacity_rejected' ? CAPACITY_ALPHA : FAILURE_ALPHA
  const decayed = decayedFailure(entry, now)
  entry.value = alpha * sample + (1 - alpha) * decayed
  entry.updatedAt = now
  const latency = Number(firstOutputMs)
  if (Number.isFinite(latency) && latency > 0) {
    entry.latency = entry.latency == null ? latency : FAILURE_ALPHA * latency + (1 - FAILURE_ALPHA) * entry.latency
  }
  feedback.set(key, entry)
  if (kind === 'succeeded') clearOpenAICapacityRejected(key)
}

export function resetOpenAIAccountRuntime() {
  if (globalPollTimer) clearTimeout(globalPollTimer)
  globalPollTimer = null
  if (deferredWakeTimer) clearTimeout(deferredWakeTimer)
  deferredWakeTimer = null
  slots.clear()
  feedback.clear()
  for (const entry of waiters.splice(0)) entry.wake()
  roundRobinCursor = 0
}
