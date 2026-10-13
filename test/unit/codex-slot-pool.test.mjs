import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  pickCodexSlots,
  orderCodexSessionSlots,
  isCodexSlotParked,
  isCodexFailoverError,
  evaluateCodexQuotaSchedule,
} from '../../src/lib/pool/codex-slot-pool.mjs'
import { SessionLimitRegistry } from '../../src/lib/pool/session-limit.mjs'

function gpt(id, patch = {}) {
  return {
    id,
    platform: 'openai',
    family: 'codex',
    has_token: true,
    schedulable: true,
    status: 'running',
    ...patch,
  }
}

test('ready slots sort by remaining 5h/7d stress', () => {
  const picked = pickCodexSlots([
    gpt('vm-b', { utilization_5h: 0.9, utilization_7d: 0.1 }),
    gpt('vm-a', { utilization_5h: 0.1, utilization_7d: 0.2 }),
    { id: 'vm-claude', platform: 'anthropic', family: 'claude', has_token: true, schedulable: true },
  ])
  assert.deepEqual(picked.ids, ['vm-a', 'vm-b'])
  assert.equal(picked.error, undefined)
})

test('parked slots come after ready ones', () => {
  const now = Date.parse('2026-09-17T00:00:00.000Z')
  const picked = pickCodexSlots(
    [
      gpt('vm-spent', {
        utilization_5h: 1,
        reset_5h: new Date(now + 60_000).toISOString(),
        codex_limited_until: new Date(now + 60_000).toISOString(),
      }),
      gpt('vm-ok', { utilization_5h: 0.2 }),
    ],
    { now },
  )
  assert.deepEqual(picked.ready, ['vm-ok'])
  assert.deepEqual(picked.parked, ['vm-spent'])
  assert.deepEqual(picked.ids, ['vm-ok', 'vm-spent'])
})

test('master pin stays on that GPT slot', () => {
  const picked = pickCodexSlots([gpt('vm-a'), gpt('vm-b')], { pin: 'vm-b' })
  assert.deepEqual(picked.ids, ['vm-b'])
  assert.equal(picked.pin, 'vm-b')
})

test('pin on a Claude slot is platform_mismatch', () => {
  const picked = pickCodexSlots([{ id: 'vm-claude', platform: 'anthropic', family: 'claude', has_token: true }], {
    pin: 'vm-claude',
  })
  assert.equal(picked.error, 'platform_mismatch')
  assert.deepEqual(picked.ids, [])
})

test('empty GPT inventory is no_codex_vm', () => {
  const picked = pickCodexSlots([{ id: 'vm-claude', platform: 'anthropic', family: 'claude', has_token: true }])
  assert.equal(picked.error, 'no_codex_vm')
})

test('capped 5h window parks the slot', () => {
  const now = Date.parse('2026-09-17T00:00:00.000Z')
  assert.equal(
    isCodexSlotParked(
      gpt('vm-x', {
        utilization_5h: 1,
        reset_5h: new Date(now + 10_000).toISOString(),
        codex_usage: {
          windows: [{ id: '5h', used_percent: 100, reset_at: new Date(now + 10_000).toISOString() }],
        },
      }),
      now,
    ),
    true,
  )
})

test('account-scoped errors fail over; generic 5xx and committed hops do not', () => {
  assert.equal(isCodexFailoverError({ ok: false, status: 429, committed: false }), true)
  assert.equal(isCodexFailoverError({ ok: false, status: 502, body: { error: { code: 'usage_limit_reached' } } }), true)
  assert.equal(isCodexFailoverError({ ok: false, status: 503, body: { error: { code: 'no_capacity' } } }), true)
  assert.equal(isCodexFailoverError({ ok: false, status: 503, body: { error: { code: 'server_error' } } }), false)
  assert.equal(
    isCodexFailoverError({ ok: false, status: 502, body: { error: { code: 'server_is_overloaded' } } }),
    false,
  )
  assert.equal(isCodexFailoverError({ ok: false, status: 429, committed: true }), false)
  assert.equal(
    isCodexFailoverError({ ok: false, status: 503, committed: true, body: { error: { code: 'no_capacity' } } }),
    false,
  )
  assert.equal(isCodexFailoverError({ ok: false, status: 502, body: { error: { code: 'upstream_transport' } } }), false)
})

test('spent 5h window evaluates to restriction, not 调度关', () => {
  const now = Date.parse('2026-09-17T00:00:00.000Z')
  const ev = evaluateCodexQuotaSchedule(
    gpt('vm-x', {
      utilization_5h: 1,
      reset_5h: new Date(now + 60_000).toISOString(),
      codex_usage: {
        windows: [{ id: '5h', used_percent: 100, reset_at: new Date(now + 60_000).toISOString(), window_minutes: 300 }],
      },
    }),
    now,
  )
  assert.equal(ev.action, 'restrict')
  assert.equal(ev.reason, 'quota_5h_header')
  assert.equal(ev.until, now + 60_000)
})

test('open window restores leftover quota-off and keeps operator 调度关', () => {
  const now = Date.parse('2026-09-17T00:00:00.000Z')
  const restore = evaluateCodexQuotaSchedule(
    gpt('vm-x', {
      schedulable: false,
      schedule_disabled_reason: 'quota_5h_header',
      utilization_5h: 0.2,
      reset_5h: new Date(now + 60_000).toISOString(),
      codex_usage: {
        windows: [{ id: '5h', used_percent: 20, reset_at: new Date(now + 60_000).toISOString(), window_minutes: 300 }],
      },
    }),
    now,
  )
  assert.equal(restore.action, 'enable')
  const keep = evaluateCodexQuotaSchedule(
    gpt('vm-x', {
      schedulable: false,
      schedule_disabled_reason: 'disabled',
      utilization_5h: 0.2,
    }),
    now,
  )
  assert.equal(keep.action, 'keep')
})

test('elapsed 5h reset is not a live park', () => {
  const now = Date.parse('2026-09-17T00:00:00.000Z')
  assert.equal(
    isCodexSlotParked(
      gpt('vm-x', {
        utilization_5h: 1,
        reset_5h: new Date(now - 1000).toISOString(),
        codex_usage: {
          windows: [{ id: '5h', used_percent: 100, reset_at: new Date(now - 1000).toISOString(), window_minutes: 300 }],
        },
      }),
      now,
    ),
    false,
  )
})

test('a bound OpenAI session stays on its VM and a full window is skipped', () => {
  const sessions = new SessionLimitRegistry()
  const vms = [
    gpt('vm-a', { utilization_5h: 0.1, policy: { maxSessions: 1 } }),
    gpt('vm-b', { utilization_5h: 0.2, policy: { maxSessions: 1 } }),
  ]
  sessions.touch('vm-a', 'conv-a')
  const again = orderCodexSessionSlots(vms, {
    boundVmId: 'vm-a',
    sessionKey: 'conv-a',
    sessionLimit: sessions,
  })
  assert.equal(again.sticky, true)
  assert.equal(again.ids[0], 'vm-a')
  const fresh = orderCodexSessionSlots(vms, { sessionKey: 'conv-b', sessionLimit: sessions })
  assert.deepEqual(fresh.ids, ['vm-b'])
  sessions.touch('vm-b', 'conv-b')
  const full = orderCodexSessionSlots(vms, { sessionKey: 'conv-c', sessionLimit: sessions })
  assert.equal(full.error, 'session_window_full')
  // session_slots is an execution seat count, never a conversation window cap.
  const seatsOnly = [gpt('vm-a', { policy: { sessionSlots: 1 } })]
  assert.deepEqual(orderCodexSessionSlots(seatsOnly, { sessionKey: 'conv-z', sessionLimit: sessions }).ids, ['vm-a'])
})

test('panel max_concurrency on the listVms summary is the candidate cap (#150)', async () => {
  const { codexAccountCandidate } = await import('../../src/lib/pool/codex-slot-pool.mjs')
  const signals = { inFlight: 0 }
  assert.equal(codexAccountCandidate(gpt('vm-a', { max_concurrency: 4 }), Date.now(), signals).concurrency, 4)
  assert.equal(
    codexAccountCandidate(gpt('vm-a', { policy: { maxConcurrency: 3 } }), Date.now(), signals).concurrency,
    3,
  )
  const vms = [gpt('vm-a', { max_concurrency: 4 })]
  const { tryAcquireOpenAISlot, resetOpenAIAccountRuntime } = await import(
    '../../src/lib/pool/openai-account-runtime.mjs'
  )
  resetOpenAIAccountRuntime()
  tryAcquireOpenAISlot('vm-a', { concurrency: 4 })
  tryAcquireOpenAISlot('vm-a', { concurrency: 4 })
  assert.deepEqual(orderCodexSessionSlots(vms).ids, ['vm-a'])
  tryAcquireOpenAISlot('vm-a', { concurrency: 4 })
  tryAcquireOpenAISlot('vm-a', { concurrency: 4 })
  assert.equal(orderCodexSessionSlots(vms).error, 'capacity_unavailable')
  assert.equal(tryAcquireOpenAISlot('vm-a', { concurrency: 4 }), null)
  resetOpenAIAccountRuntime()
})

test('panel max_rpm caps starts per minute on a GPT slot', async () => {
  const { tryAcquireOpenAISlot, resetOpenAIAccountRuntime } = await import(
    '../../src/lib/pool/openai-account-runtime.mjs'
  )
  resetOpenAIAccountRuntime()
  const now = Date.parse('2026-09-27T00:00:00.000Z')
  const vms = [gpt('vm-a', { max_rpm: 2 }), gpt('vm-b', { max_rpm: 0 })]
  for (let i = 0; i < 2; i++) tryAcquireOpenAISlot('vm-a', { concurrency: 2, maxRpm: 2, now }).release()
  assert.equal(tryAcquireOpenAISlot('vm-a', { concurrency: 2, maxRpm: 2, now: now + 1000 }), null)
  assert.deepEqual(orderCodexSessionSlots(vms, { now: now + 1000 }).ids, ['vm-b'])
  assert.ok(orderCodexSessionSlots(vms, { now: now + 61_000 }).ids.includes('vm-a'))
  resetOpenAIAccountRuntime()
})

test('panel allowed_models keeps a GPT slot out of other models', () => {
  const vms = [gpt('vm-a', { allowed_models: ['gpt-5.5'] }), gpt('vm-b')]
  assert.deepEqual(orderCodexSessionSlots(vms, { model: 'gpt-5.4' }).ids, ['vm-b'])
  assert.ok(orderCodexSessionSlots(vms, { model: 'gpt-5.5' }).ids.includes('vm-a'))
  const only = orderCodexSessionSlots([vms[0]], { model: 'gpt-5.4' })
  assert.equal(only.error, 'model_not_allowed')
  assert.equal(orderCodexSessionSlots([vms[0]], { pin: 'vm-a', model: 'gpt-5.4' }).error, 'model_not_allowed')
})

test('GPT wait queue wakes FIFO on release and caps at the configured default', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  assert.equal(rt.OPENAI_MAX_WAITERS, 256)
  const deadline = Date.now() + 5000
  const order = []
  const first = rt.waitForOpenAICapacity({ deadline }).then((r) => order.push(['a', r.woken]))
  const second = rt.waitForOpenAICapacity({ deadline }).then((r) => order.push(['b', r.woken]))
  const lease = rt.tryAcquireOpenAISlot('vm-a')
  lease.release()
  await first
  assert.deepEqual(order, [['a', true]])
  lease.release()
  rt.tryAcquireOpenAISlot('vm-a').release()
  await second
  assert.deepEqual(order, [
    ['a', true],
    ['b', true],
  ])
  const parked = Array.from({ length: rt.OPENAI_MAX_WAITERS }, () => rt.waitForOpenAICapacity({ deadline }))
  assert.equal(rt.openAIWaiterCount(), rt.OPENAI_MAX_WAITERS)
  await assert.rejects(rt.waitForOpenAICapacity({ deadline }), { code: 'pool_wait_queue_full' })
  const ac = new AbortController()
  rt.resetOpenAIAccountRuntime()
  await Promise.all(parked)
  const aborted = rt.waitForOpenAICapacity({ deadline, signal: ac.signal })
  ac.abort()
  assert.deepEqual(await aborted, { woken: false, aborted: true })
  assert.equal(rt.openAIWaiterCount(), 0)
})

test('RPM-only capacity miss reports when the window reopens', async () => {
  const { tryAcquireOpenAISlot, resetOpenAIAccountRuntime } = await import(
    '../../src/lib/pool/openai-account-runtime.mjs'
  )
  resetOpenAIAccountRuntime()
  const now = Date.parse('2026-09-27T00:00:00.000Z')
  tryAcquireOpenAISlot('vm-a', { now }).release()
  const full = orderCodexSessionSlots([gpt('vm-a', { max_rpm: 1 })], { now: now + 1000 })
  assert.equal(full.error, 'capacity_unavailable')
  assert.equal(full.retryAt, now + 60_000)
  resetOpenAIAccountRuntime()
})

test('high-volume RPM history prunes exactly without rescanning retained starts', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const now = Date.parse('2026-10-12T00:00:00.000Z')
  for (let i = 0; i < 4096; i += 1) {
    const lease = rt.tryAcquireOpenAISlot('vm-rpm', { concurrency: 40, now: now + i })
    assert.ok(lease)
    lease.release()
  }
  const partiallyExpired = rt.openAIRuntimeSignals('vm-rpm', now + 60_000 + 2048)
  assert.equal(partiallyExpired.rpmCount, 2047)
  assert.equal(partiallyExpired.rpmResetAt, now + 60_000 + 2049)
  const empty = rt.openAIRuntimeSignals('vm-rpm', now + 65_000)
  assert.equal(empty.rpmCount, 0)
  assert.equal(empty.rpmResetAt, null)
  rt.resetOpenAIAccountRuntime()
})

test('kernel capacity rejection temporarily blocks admission and reports its retry time', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const now = Date.parse('2026-10-12T00:00:00.000Z')
  const retryAt = rt.markOpenAICapacityRejected('vm-a', now)
  const busy = orderCodexSessionSlots([gpt('vm-a')], { now })
  assert.equal(busy.error, 'capacity_unavailable')
  assert.equal(busy.retryAt, retryAt)
  const pinned = orderCodexSessionSlots([gpt('vm-a')], { now, pin: 'vm-a' })
  assert.equal(pinned.error, 'capacity_unavailable')
  assert.equal(pinned.retryAt, retryAt)
  assert.deepEqual(orderCodexSessionSlots([gpt('vm-a')], { now: retryAt }).ids, ['vm-a'])
  const probe = rt.tryAcquireOpenAISlot('vm-a', { concurrency: 40, now: retryAt })
  assert.ok(probe)
  assert.equal(rt.tryAcquireOpenAISlot('vm-a', { concurrency: 40, now: retryAt }), null)
  probe.release()
  assert.ok(rt.tryAcquireOpenAISlot('vm-a', { concurrency: 40, now: retryAt }))
  rt.resetOpenAIAccountRuntime()
})

test('kernel cooldown survives completions from work admitted before the rejection', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const now = Date.parse('2026-10-13T00:00:00.000Z')
  const until = now + 30_000
  assert.equal(rt.markOpenAIKernelCooldown('vm-a', until), until)
  rt.reportOpenAIAttempt('vm-a', 'succeeded', 100, now + 1_000)
  assert.equal(rt.openAIRuntimeSignals('vm-a', now + 1_000).kernelCooldownUntil, until)
  assert.equal(rt.tryAcquireOpenAISlot('vm-a', { concurrency: 40, now: now + 1_000 }), null)
  const lease = rt.tryAcquireOpenAISlot('vm-a', { concurrency: 40, now: until })
  assert.ok(lease)
  lease.release()
  rt.resetOpenAIAccountRuntime()
})

test('account recovery gate blocks new leases without changing configured concurrency', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  rt.beginOpenAIAccountRecovery('vm-gpt-a')
  assert.equal(rt.openAIRuntimeSignals('vm-gpt-a').recoveryBlocked, true)
  assert.equal(rt.tryAcquireOpenAISlot('vm-gpt-a', { concurrency: 40 }), null)
  rt.endOpenAIAccountRecovery('vm-gpt-a')
  const lease = rt.tryAcquireOpenAISlot('vm-gpt-a', { concurrency: 40 })
  assert.ok(lease)
  lease.release()
  rt.resetOpenAIAccountRuntime()
})

test('shared static snapshots keep runtime capacity and recovery signals live', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const now = Date.parse('2026-10-13T00:00:00.000Z')
  const vm = gpt('vm-gpt-a', { max_concurrency: 1 })
  const snapshotCache = { expiresAt: now + 250, entries: new Map(), policy: null }

  const available = orderCodexSessionSlots([vm], { now, snapshotCache })
  assert.equal(available.candidates[0].concurrency, 1)
  assert.equal(snapshotCache.entries.size, 1)

  const lease = rt.tryAcquireOpenAISlot('vm-gpt-a', { concurrency: 1, now })
  assert.ok(lease)
  assert.equal(orderCodexSessionSlots([vm], { now: now + 1, snapshotCache }).error, 'capacity_unavailable')
  lease.release()
  assert.deepEqual(orderCodexSessionSlots([vm], { now: now + 2, snapshotCache }).ids, ['vm-gpt-a'])

  rt.beginOpenAIAccountRecovery('vm-gpt-a')
  assert.equal(orderCodexSessionSlots([vm], { now: now + 3, snapshotCache }).error, 'capacity_unavailable')
  rt.endOpenAIAccountRecovery('vm-gpt-a')
  assert.deepEqual(orderCodexSessionSlots([vm], { now: now + 4, snapshotCache }).ids, ['vm-gpt-a'])
  rt.resetOpenAIAccountRuntime()
})

test('static snapshot expiry refreshes quota and policy while request filters stay live', () => {
  const now = Date.parse('2026-10-13T00:00:00.000Z')
  const vm = gpt('vm-gpt-a', {
    utilization_5h: 1,
    reset_5h: new Date(now + 200).toISOString(),
    max_concurrency: 2,
    allowed_models: ['gpt-5.4'],
  })
  const snapshotCache = { expiresAt: now + 250, entries: new Map(), policy: null }

  const parked = orderCodexSessionSlots([vm], { now, snapshotCache, model: 'gpt-5.4' })
  assert.deepEqual(parked.parked, ['vm-gpt-a'])
  assert.equal(parked.candidates.length, 0)
  vm.max_concurrency = 40

  const stillCached = orderCodexSessionSlots([vm], { now: now + 100, snapshotCache, model: 'gpt-5.4' })
  assert.deepEqual(stillCached.parked, ['vm-gpt-a'])
  assert.equal(stillCached.candidates.length, 0)
  assert.equal(
    orderCodexSessionSlots([vm], { now: now + 100, snapshotCache, model: 'gpt-6' }).error,
    'model_not_allowed',
  )
  assert.equal(
    orderCodexSessionSlots([vm], {
      now: now + 100,
      snapshotCache,
      model: 'gpt-5.4',
      excluded: new Set(['vm-gpt-a']),
    }).error,
    'candidates_exhausted',
  )

  const refreshed = orderCodexSessionSlots([vm], { now: now + 300, snapshotCache, model: 'gpt-5.4' })
  assert.deepEqual(refreshed.ids, ['vm-gpt-a'])
  assert.deepEqual(refreshed.parked, [])
  assert.equal(refreshed.candidates[0].concurrency, 40)
})
