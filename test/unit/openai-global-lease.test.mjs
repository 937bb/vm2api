import test from 'node:test'
import assert from 'node:assert/strict'
import { spawn, spawnSync } from 'node:child_process'
import fs from 'node:fs'
import net from 'node:net'
import os from 'node:os'
import path from 'node:path'
import {
  acquireOpenAIGlobalLease,
  clearOpenAIAccountRetiring,
  closeOpenAIGlobalLeaseClient,
  markOpenAIAccountRetiring,
  openAIGlobalActiveCount,
  setOpenAIGlobalLeaseClientForTest,
} from '../../src/lib/pool/openai-global-lease.mjs'
import {
  acquireOpenAISlot,
  deferOpenAIWaiter,
  openAIWaiterCount,
  openAIRuntimeSignals,
  resetOpenAIAccountRuntime,
  waitForOpenAICapacity,
} from '../../src/lib/pool/openai-account-runtime.mjs'

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

async function freePort() {
  const server = net.createServer()
  await new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', resolve)
  })
  const port = server.address().port
  await new Promise((resolve) => server.close(resolve))
  return port
}

async function waitForPort(port, child) {
  const deadline = Date.now() + 5_000
  while (Date.now() < deadline) {
    if (child.exitCode != null) throw new Error(`redis-server exited with ${child.exitCode}`)
    const ready = await new Promise((resolve) => {
      const socket = net.createConnection({ host: '127.0.0.1', port })
      socket.once('connect', () => {
        socket.destroy()
        resolve(true)
      })
      socket.once('error', () => resolve(false))
    })
    if (ready) return
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
  throw new Error('redis-server did not become ready')
}

test('Redis leases enforce one cross-gateway cap, survive heartbeat, and block retirement', {
  timeout: 20_000,
}, async (t) => {
  if (spawnSync('redis-server', ['--version'], { stdio: 'ignore' }).status !== 0) {
    t.skip('redis-server is not installed')
    return
  }

  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-openai-lease-'))
  const port = await freePort()
  const child = spawn(
    'redis-server',
    ['--port', String(port), '--bind', '127.0.0.1', '--save', '', '--appendonly', 'no', '--dir', dir],
    { stdio: 'ignore' },
  )
  const envNames = [
    'KIN_OPENAI_REDIS_URL',
    'KIN_OPENAI_REDIS_PREFIX',
    'KIN_OPENAI_REDIS_REQUIRED',
    'KIN_OPENAI_REDIS_LEASE_TTL_MS',
    'KIN_OPENAI_REDIS_HEARTBEAT_MS',
  ]
  const previous = Object.fromEntries(envNames.map((name) => [name, process.env[name]]))
  t.after(async () => {
    resetOpenAIAccountRuntime()
    await closeOpenAIGlobalLeaseClient()
    if (child.exitCode == null) {
      child.kill('SIGTERM')
      await new Promise((resolve) => child.once('exit', resolve))
    }
    for (const name of envNames) {
      if (previous[name] == null) delete process.env[name]
      else process.env[name] = previous[name]
    }
    fs.rmSync(dir, { recursive: true, force: true })
  })

  await waitForPort(port, child)
  process.env.KIN_OPENAI_REDIS_URL = `redis://127.0.0.1:${port}`
  process.env.KIN_OPENAI_REDIS_PREFIX = `vm2api:test:${process.pid}:${Date.now()}`
  process.env.KIN_OPENAI_REDIS_REQUIRED = '1'
  process.env.KIN_OPENAI_REDIS_LEASE_TTL_MS = '5000'
  process.env.KIN_OPENAI_REDIS_HEARTBEAT_MS = '1000'

  const attempts = await Promise.all(
    Array.from({ length: 80 }, () => acquireOpenAIGlobalLease('vm-gpt-a', { concurrency: 40 })),
  )
  const admitted = attempts.filter(Boolean)
  assert.equal(admitted.length, 40)
  assert.equal(await openAIGlobalActiveCount('vm-gpt-a'), 40)

  resetOpenAIAccountRuntime()
  assert.equal(await acquireOpenAISlot('vm-gpt-a', { concurrency: 40 }), null)
  assert.equal(openAIRuntimeSignals('vm-gpt-a').inFlight, 0)
  await Promise.all(admitted.map((lease) => lease.release()))
  assert.equal(await openAIGlobalActiveCount('vm-gpt-a'), 0)

  const heartbeat = await acquireOpenAIGlobalLease('vm-gpt-a', { concurrency: 40 })
  assert.ok(heartbeat)
  await new Promise((resolve) => setTimeout(resolve, 5_500))
  assert.equal(await openAIGlobalActiveCount('vm-gpt-a'), 1)
  await heartbeat.release()

  const retirement = await markOpenAIAccountRetiring('vm-gpt-a')
  assert.equal(retirement.active, 0)
  assert.equal(await acquireOpenAIGlobalLease('vm-gpt-a', { concurrency: 40 }), null)
  assert.equal(await clearOpenAIAccountRetiring('vm-gpt-a'), true)
  const resumed = await acquireOpenAIGlobalLease('vm-gpt-a', { concurrency: 40 })
  assert.ok(resumed)
  await resumed.release()
})

test('required Redis failure rejects admission and releases the local seat', async (t) => {
  const previousRequired = process.env.KIN_OPENAI_REDIS_REQUIRED
  process.env.KIN_OPENAI_REDIS_REQUIRED = '1'
  resetOpenAIAccountRuntime()
  await setOpenAIGlobalLeaseClientForTest({
    eval: async () => {
      throw Object.assign(new Error('redis down'), { code: 'ECONNREFUSED' })
    },
  })
  t.after(async () => {
    resetOpenAIAccountRuntime()
    await setOpenAIGlobalLeaseClientForTest(null)
    if (previousRequired == null) delete process.env.KIN_OPENAI_REDIS_REQUIRED
    else process.env.KIN_OPENAI_REDIS_REQUIRED = previousRequired
  })

  await assert.rejects(acquireOpenAISlot('vm-gpt-a', { concurrency: 40 }), {
    code: 'openai_lease_backend_unavailable',
  })
  assert.equal(openAIRuntimeSignals('vm-gpt-a').inFlight, 0)
})

test('a denied provisional Redis claim does not wake another local waiter', async (t) => {
  resetOpenAIAccountRuntime()
  await setOpenAIGlobalLeaseClientForTest({ eval: async () => [0, 40, 0] })
  t.after(async () => {
    resetOpenAIAccountRuntime()
    await setOpenAIGlobalLeaseClientForTest(null)
  })

  let settled = false
  const parked = waitForOpenAICapacity({ deadline: Date.now() + 1000 }).then((result) => {
    settled = true
    return result
  })
  assert.equal(await acquireOpenAISlot('vm-gpt-full', { concurrency: 40 }), null)
  await delay(25)
  assert.equal(settled, false)
  assert.equal(openAIWaiterCount(), 1)

  resetOpenAIAccountRuntime()
  await parked
})

test('a real global release is removed before the next waiter wakes', async (t) => {
  let finishRelease
  const releaseGate = new Promise((resolve) => {
    finishRelease = resolve
  })
  resetOpenAIAccountRuntime()
  await setOpenAIGlobalLeaseClientForTest({
    eval: async (script) => {
      if (script.includes("local removed = redis.call('ZREM'")) return releaseGate
      return [1, 1, 0]
    },
  })
  t.after(async () => {
    resetOpenAIAccountRuntime()
    await setOpenAIGlobalLeaseClientForTest(null)
  })

  const lease = await acquireOpenAISlot('vm-gpt-release', { concurrency: 40 })
  assert.ok(lease)
  let settled = false
  const parked = waitForOpenAICapacity({ deadline: Date.now() + 1000 }).then((result) => {
    settled = true
    return result
  })
  const releasing = lease.release()
  await delay(25)
  assert.equal(settled, false)

  finishRelease(1)
  assert.equal(await releasing, true)
  assert.deepEqual(await parked, { woken: true, reason: 'capacity' })
})

test('global capacity polling wakes a bounded batch instead of the whole queue', async (t) => {
  const previousPoll = process.env.KIN_OPENAI_REDIS_POLL_MS
  const previousBatch = process.env.KIN_OPENAI_REDIS_POLL_BATCH
  process.env.KIN_OPENAI_REDIS_POLL_MS = '25'
  process.env.KIN_OPENAI_REDIS_POLL_BATCH = '3'
  resetOpenAIAccountRuntime()
  t.after(() => {
    resetOpenAIAccountRuntime()
    if (previousPoll == null) delete process.env.KIN_OPENAI_REDIS_POLL_MS
    else process.env.KIN_OPENAI_REDIS_POLL_MS = previousPoll
    if (previousBatch == null) delete process.env.KIN_OPENAI_REDIS_POLL_BATCH
    else process.env.KIN_OPENAI_REDIS_POLL_BATCH = previousBatch
  })

  const settled = []
  const parked = Array.from({ length: 10 }, (_, index) =>
    waitForOpenAICapacity({ deadline: Date.now() + 1000, globalPoll: true }).then((result) => {
      settled.push([index, result])
      return result
    }),
  )
  while (!settled.length) await delay(5)
  await new Promise((resolve) => setImmediate(resolve))
  assert.equal(settled.length, 3)
  assert.equal(openAIWaiterCount(), 7)
  assert.ok(settled.every(([, result]) => result.reason === 'global_poll'))

  resetOpenAIAccountRuntime()
  await Promise.all(parked)
})

test('missed capacity handoffs coalesce into one deferred FIFO wake', async (t) => {
  resetOpenAIAccountRuntime()
  t.after(() => resetOpenAIAccountRuntime())

  const settled = []
  const parked = Array.from({ length: 20 }, (_, index) =>
    waitForOpenAICapacity({ deadline: Date.now() + 1000 }).then((result) => {
      settled.push([index, result])
      return result
    }),
  )
  for (let i = 0; i < 100; i += 1) deferOpenAIWaiter()
  await delay(5)
  assert.equal(settled.length, 0)
  await delay(15)
  assert.equal(settled.length, 1)
  assert.equal(openAIWaiterCount(), 19)
  assert.deepEqual(settled[0], [0, { woken: true, reason: 'capacity' }])

  resetOpenAIAccountRuntime()
  await Promise.all(parked)
})
