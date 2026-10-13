import crypto from 'node:crypto'
import { createClient } from '@redis/client'

const DEFAULT_LEASE_TTL_MS = 90_000
const DEFAULT_HEARTBEAT_MS = 20_000
const DEFAULT_RETIRE_TTL_MS = 10 * 60_000
const DEFAULT_POLL_MS = 200
const DEFAULT_CONNECT_TIMEOUT_MS = 800
const DEFAULT_COMMAND_TIMEOUT_MS = 1_500

const ACQUIRE_SCRIPT = `
local clock = redis.call('TIME')
local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
local active = redis.call('ZCARD', KEYS[1])
if redis.call('EXISTS', KEYS[2]) == 1 then
  return {0, active, 1}
end
local cap = tonumber(ARGV[1])
if active >= cap then
  return {0, active, 0}
end
redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), ARGV[3])
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]) * 2)
return {1, active + 1, 0}
`

const RENEW_SCRIPT = `
local clock = redis.call('TIME')
local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
if redis.call('ZSCORE', KEYS[1], ARGV[2]) == false then
  return 0
end
redis.call('ZADD', KEYS[1], now + tonumber(ARGV[1]), ARGV[2])
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[1]) * 2)
return 1
`

const RELEASE_SCRIPT = `
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if redis.call('ZCARD', KEYS[1]) == 0 then
  redis.call('DEL', KEYS[1])
end
return removed
`

const COUNT_SCRIPT = `
local clock = redis.call('TIME')
local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
return redis.call('ZCARD', KEYS[1])
`

let redisClient = null
let redisConnect = null
let injectedClient = null
let lastWarningAt = 0

function boundedEnv(name, fallback, min, max) {
  const value = Number(process.env[name])
  if (!Number.isFinite(value)) return fallback
  return Math.min(max, Math.max(min, Math.trunc(value)))
}

function leaseTtlMs() {
  return boundedEnv('KIN_OPENAI_REDIS_LEASE_TTL_MS', DEFAULT_LEASE_TTL_MS, 5_000, 15 * 60_000)
}

function heartbeatMs(ttlMs) {
  return Math.min(
    Math.max(1_000, Math.floor(ttlMs / 3)),
    boundedEnv('KIN_OPENAI_REDIS_HEARTBEAT_MS', DEFAULT_HEARTBEAT_MS, 1_000, 5 * 60_000),
  )
}

function commandTimeoutMs() {
  return boundedEnv('KIN_OPENAI_REDIS_COMMAND_TIMEOUT_MS', DEFAULT_COMMAND_TIMEOUT_MS, 100, 10_000)
}

function namespace() {
  return String(process.env.KIN_OPENAI_REDIS_PREFIX || 'vm2api:openai').trim() || 'vm2api:openai'
}

function keysFor(id) {
  const account = encodeURIComponent(String(id || '').trim())
  const stem = `${namespace()}:{${account}}`
  return { leases: `${stem}:leases`, retiring: `${stem}:retiring` }
}

function warn(error) {
  const now = Date.now()
  if (now - lastWarningAt < 10_000) return
  lastWarningAt = now
  console.warn(`[openai-redis-lease] ${error?.code || error?.message || error}`)
}

function withTimeout(promise, label) {
  const ms = commandTimeoutMs()
  let timer
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => {
        reject(Object.assign(new Error(`${label} timed out after ${ms}ms`), { code: 'redis_command_timeout' }))
      }, ms)
      timer.unref?.()
    }),
  ]).finally(() => clearTimeout(timer))
}

async function clientForLease() {
  if (injectedClient) return injectedClient
  if (redisClient?.isReady) return redisClient
  if (redisConnect) return redisConnect
  if (redisClient) {
    try {
      redisClient.destroy()
    } catch {}
    redisClient = null
  }
  const url = String(process.env.KIN_OPENAI_REDIS_URL || '').trim()
  if (!url) return null
  const next = createClient({
    url,
    socket: {
      connectTimeout: boundedEnv('KIN_OPENAI_REDIS_CONNECT_TIMEOUT_MS', DEFAULT_CONNECT_TIMEOUT_MS, 100, 10_000),
      reconnectStrategy: false,
    },
  })
  redisClient = next
  next.on('error', warn)
  next.on('end', () => {
    if (redisClient === next) redisClient = null
  })
  redisConnect = next
    .connect()
    .then(() => next)
    .catch((error) => {
      if (redisClient === next) redisClient = null
      try {
        next.destroy()
      } catch {}
      throw error
    })
    .finally(() => {
      redisConnect = null
    })
  return redisConnect
}

async function evalScript(script, keys, args, label) {
  const client = await clientForLease()
  if (!client) return null
  return withTimeout(
    client.eval(script, {
      keys,
      arguments: args.map((value) => String(value)),
    }),
    label,
  )
}

export function globalOpenAILeaseEnabled() {
  return !!(injectedClient || String(process.env.KIN_OPENAI_REDIS_URL || '').trim())
}

export function globalOpenAILeaseRequired() {
  if (!globalOpenAILeaseEnabled()) return false
  return String(process.env.KIN_OPENAI_REDIS_REQUIRED || '1').trim() !== '0'
}

export function globalOpenAILeasePollMs() {
  return boundedEnv('KIN_OPENAI_REDIS_POLL_MS', DEFAULT_POLL_MS, 25, 2_000)
}

export async function acquireOpenAIGlobalLease(id, { concurrency = 1 } = {}) {
  if (!globalOpenAILeaseEnabled()) return null
  const account = String(id || '').trim()
  if (!account) throw Object.assign(new Error('OpenAI lease account is required'), { code: 'redis_lease_id_missing' })
  const cap = Math.max(1, Math.trunc(Number(concurrency) || 1))
  const ttlMs = leaseTtlMs()
  const leaseId = `${process.env.KIN_CODEX_KERNEL_INSTANCE || 'gateway'}:${process.pid}:${crypto.randomUUID()}`
  const key = keysFor(account)
  let reply
  try {
    reply = await evalScript(
      ACQUIRE_SCRIPT,
      [key.leases, key.retiring],
      [cap, ttlMs, leaseId],
      'Redis OpenAI lease acquire',
    )
  } catch (error) {
    warn(error)
    throw error
  }
  if (!Array.isArray(reply) || Number(reply[0]) !== 1) return null

  let released = false
  let renewing = false
  const renew = async () => {
    if (released || renewing) return
    renewing = true
    try {
      const updated = await evalScript(RENEW_SCRIPT, [key.leases], [ttlMs, leaseId], 'Redis OpenAI lease renew')
      if (!released && Number(updated) !== 1) warn({ code: `lease_lost:${account}` })
    } catch (error) {
      warn(error)
    } finally {
      renewing = false
    }
  }
  const timer = setInterval(renew, heartbeatMs(ttlMs))
  timer.unref?.()
  return {
    id: account,
    leaseId,
    active: Number(reply[1]) || 1,
    async release() {
      if (released) return false
      released = true
      clearInterval(timer)
      try {
        return Number(await evalScript(RELEASE_SCRIPT, [key.leases], [leaseId], 'Redis OpenAI lease release')) === 1
      } catch (error) {
        warn(error)
        return false
      }
    },
  }
}

export async function openAIGlobalActiveCount(id) {
  if (!globalOpenAILeaseEnabled()) return 0
  const key = keysFor(id)
  return Number(await evalScript(COUNT_SCRIPT, [key.leases], [], 'Redis OpenAI lease count')) || 0
}

export async function markOpenAIAccountRetiring(id, { ttlMs = null } = {}) {
  if (!globalOpenAILeaseEnabled()) return { enabled: false, active: 0 }
  const client = await clientForLease()
  const key = keysFor(id)
  const retirementTtl = Number.isFinite(Number(ttlMs))
    ? Math.max(5_000, Math.trunc(Number(ttlMs)))
    : boundedEnv('KIN_OPENAI_REDIS_RETIRE_TTL_MS', DEFAULT_RETIRE_TTL_MS, 5_000, 24 * 60 * 60_000)
  await withTimeout(
    client.set(key.retiring, `${process.env.KIN_CODEX_KERNEL_INSTANCE || 'gateway'}:${process.pid}`, {
      PX: retirementTtl,
    }),
    'Redis OpenAI retirement mark',
  )
  return { enabled: true, active: await openAIGlobalActiveCount(id), ttlMs: retirementTtl }
}

export async function clearOpenAIAccountRetiring(id) {
  if (!globalOpenAILeaseEnabled()) return false
  const client = await clientForLease()
  const key = keysFor(id)
  return Number(await withTimeout(client.del(key.retiring), 'Redis OpenAI retirement clear')) > 0
}

export async function closeOpenAIGlobalLeaseClient() {
  const client = redisClient
  redisClient = null
  redisConnect = null
  if (!client) return
  try {
    client.destroy()
  } catch {}
}

export async function setOpenAIGlobalLeaseClientForTest(client = null) {
  await closeOpenAIGlobalLeaseClient()
  injectedClient = client
}
