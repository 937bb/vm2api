import { performance } from 'node:perf_hooks'

const baseUrl = String(process.argv[2] || 'http://127.0.0.1:8787').replace(/\/$/, '')
const concurrency = Math.max(1, Math.trunc(Number(process.argv[3]) || 500))
const holdMs = Math.max(1000, Math.trunc(Number(process.argv[4]) || 15_000))
const apiKey = String(process.env.VM2API_API_KEY || '').trim()
const model = String(process.env.VM2API_STRESS_MODEL || 'gpt-5.4').trim()

if (!apiKey) {
  console.error('VM2API_API_KEY is required')
  process.exit(2)
}

const requestController = new AbortController()
const requestTimer = setTimeout(() => requestController.abort(), holdMs)
const statuses = new Map()
const errors = new Map()
const healthMs = []
let healthFailures = 0
let probing = true

function increment(map, key) {
  map.set(key, (map.get(key) || 0) + 1)
}

async function probeHealth() {
  while (probing) {
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), 2000)
    const started = performance.now()
    try {
      const response = await fetch(`${baseUrl}/health`, { signal: controller.signal })
      await response.arrayBuffer()
      if (response.ok) healthMs.push(performance.now() - started)
      else healthFailures += 1
    } catch {
      healthFailures += 1
    } finally {
      clearTimeout(timer)
    }
    await new Promise((resolve) => setTimeout(resolve, 250))
  }
}

async function request() {
  try {
    const response = await fetch(`${baseUrl}/v1/responses`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${apiKey}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({ model, input: 'admission load test', stream: true }),
      signal: requestController.signal,
    })
    increment(statuses, String(response.status))
    await response.arrayBuffer()
  } catch (error) {
    increment(errors, error?.name || error?.code || 'error')
  }
}

const started = performance.now()
const healthTask = probeHealth()
await Promise.all(Array.from({ length: concurrency }, request))
clearTimeout(requestTimer)
probing = false
await healthTask

healthMs.sort((a, b) => a - b)
const percentile = (p) =>
  healthMs.length ? healthMs[Math.min(healthMs.length - 1, Math.floor(healthMs.length * p))] : null
console.log(
  JSON.stringify(
    {
      base_url: baseUrl,
      concurrency,
      hold_ms: holdMs,
      elapsed_ms: Math.round(performance.now() - started),
      statuses: Object.fromEntries(statuses),
      errors: Object.fromEntries(errors),
      health: {
        ok: healthMs.length,
        failed: healthFailures,
        p50_ms: percentile(0.5) == null ? null : Math.round(percentile(0.5)),
        p95_ms: percentile(0.95) == null ? null : Math.round(percentile(0.95)),
        max_ms: healthMs.length ? Math.round(healthMs.at(-1)) : null,
      },
    },
    null,
    2,
  ),
)
