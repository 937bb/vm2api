import { monitorEventLoopDelay, performance } from 'node:perf_hooks'

const INFERENCE_PATHS = new Set([
  '/v1/chat/completions',
  '/chat/completions',
  '/v1/completions',
  '/completions',
  '/v1/responses',
  '/responses',
  '/v1/messages',
  '/messages',
  '/v1/alpha/search',
  '/alpha/search',
])

function milliseconds(nanoseconds) {
  const value = Number(nanoseconds)
  if (!Number.isFinite(value)) return null
  return Math.round((value / 1e6) * 1000) / 1000
}

export function isInferenceRequest(method, pathname) {
  return String(method || '').toUpperCase() === 'POST' && INFERENCE_PATHS.has(String(pathname || ''))
}

export function createGatewayRuntime({ eventLoopResolutionMs = 20 } = {}) {
  const eventLoop = monitorEventLoopDelay({ resolution: Math.max(10, Math.trunc(eventLoopResolutionMs) || 20) })
  const eventLoopStart = performance.eventLoopUtilization()
  const idleWaiters = new Set()
  let activeHttp = 0
  let activeInference = 0
  let peakHttp = 0
  let peakInference = 0
  let draining = false
  let stopped = false
  eventLoop.enable()

  function settleIdle() {
    if (activeHttp !== 0 || activeInference !== 0) return
    for (const resolve of idleWaiters) resolve()
    idleWaiters.clear()
  }

  function beginRequest() {
    activeHttp += 1
    peakHttp = Math.max(peakHttp, activeHttp)
    let inference = false
    let released = false
    return {
      markInference() {
        if (released || inference) return
        inference = true
        activeInference += 1
        peakInference = Math.max(peakInference, activeInference)
      },
      release() {
        if (released) return
        released = true
        activeHttp = Math.max(0, activeHttp - 1)
        if (inference) activeInference = Math.max(0, activeInference - 1)
        settleIdle()
      },
    }
  }

  function snapshot() {
    const utilization = performance.eventLoopUtilization(eventLoopStart)
    return {
      draining,
      active_http: activeHttp,
      active_inference: activeInference,
      peak_http: peakHttp,
      peak_inference: peakInference,
      event_loop: {
        utilization: Math.round(utilization.utilization * 1_000_000) / 1_000_000,
        delay_ms: {
          p50: milliseconds(eventLoop.percentile(50)),
          p95: milliseconds(eventLoop.percentile(95)),
          p99: milliseconds(eventLoop.percentile(99)),
          max: milliseconds(eventLoop.max),
        },
      },
    }
  }

  return {
    beginRequest,
    snapshot,
    startDraining() {
      draining = true
      settleIdle()
    },
    whenIdle() {
      if (activeHttp === 0 && activeInference === 0) return Promise.resolve()
      return new Promise((resolve) => idleWaiters.add(resolve))
    },
    stop() {
      if (stopped) return
      stopped = true
      eventLoop.disable()
    },
  }
}
