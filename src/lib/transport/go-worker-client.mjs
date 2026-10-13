import http from 'node:http'
import { randomUUID } from 'node:crypto'
import fs from 'node:fs'
import path from 'node:path'
import { prepareOutboundHeaders } from '../protocol/outbound-attempt.mjs'
import { sanitizeAnthropicBodyForBetaTokens } from '../protocol/anthropic-policy.mjs'
import { withRequestProtocolBetas } from '../protocol/claude-code-betas.mjs'
import { sealClaudeCodeCch } from '../identity/cch.mjs'
import { credentialModeFromOauth } from '../oauth/credential-mode.mjs'
import { isCrsMock, writeCrsTrace, mockCrsPayload, emitMockSse } from './crs-mock.mjs'
import {
  classifyCredentialRefresh,
  hasAccessPresence,
  hasCredentialPresence,
  hasRefreshPresence,
  needsRefresh,
  readWorkerCredentialFile,
  REFRESH_SKEW_MS,
  writeWorkerCredentialFile,
} from '../oauth/oauth-credentials.mjs'
import { isApiKeyMode } from '../oauth/credential-mode.mjs'
import { runSlotOauth } from './slot-oauth.mjs'
import { slotHost } from '../vm/slot-host.mjs'
import { applyClaudeSSELineToMessage, createClaudeMessageAssembler } from '../protocol/convert.mjs'
import {
  clientCancelledResult,
  isClientCancelledResult,
  isCompleteAssistantMessage,
  isWrapConnectionError,
} from '../core/errors.mjs'
import { isContentPolicyErrorCode } from '../core/refusal-guard.mjs'
import { extraHeadersFromLimitError, isPlanLimitMessage } from '../pool/quota-window.mjs'

const MAX_BODY = 64 * 1024 * 1024
const credentialRefreshTail = new Map()
const internalTokenCache = new Map()
const INTERNAL_TOKEN_CACHE_MS = 1000

function withCredentialRefreshLock(key, fn) {
  const prev = credentialRefreshTail.get(key) || Promise.resolve()
  let release
  const gate = new Promise((resolve) => {
    release = resolve
  })
  const tail = prev.catch(() => {}).then(() => gate)
  credentialRefreshTail.set(key, tail)
  return prev
    .catch(() => {})
    .then(async () => {
      try {
        return await fn()
      } finally {
        release()
        if (credentialRefreshTail.get(key) === tail) credentialRefreshTail.delete(key)
      }
    })
}

export function workerPaths(exec = {}) {
  const slotRoot = exec.homeDir ? path.dirname(exec.homeDir) : null
  const runDir = exec.vm?.runtime?.worker_run_dir || (slotRoot ? path.join(slotRoot, 'run') : null)
  return {
    runDir,
    socketPath: exec.vm?.runtime?.worker_socket || (runDir ? path.join(runDir, 'worker.sock') : null),
    tokenPath: exec.vm?.runtime?.worker_token_file || (runDir ? path.join(runDir, 'internal.token') : null),
  }
}

function readInternalToken(exec) {
  const { tokenPath } = workerPaths(exec)
  if (!tokenPath) return ''
  const now = Date.now()
  const cached = internalTokenCache.get(tokenPath)
  if (cached && now - cached.at < INTERNAL_TOKEN_CACHE_MS) return cached.value
  try {
    const value = fs.readFileSync(tokenPath, 'utf8').trim()
    internalTokenCache.set(tokenPath, { at: now, value })
    return value
  } catch {
    internalTokenCache.delete(tokenPath)
    return ''
  }
}

function workerRequest(
  exec,
  {
    method = 'GET',
    requestPath,
    body = null,
    signal,
    timeoutMs = 180000,
    timeoutMode = 'overall',
    headers = {},
    onPayloadBytes,
  } = {},
) {
  return new Promise((resolve, reject) => {
    const { socketPath } = workerPaths(exec)
    if (!socketPath) {
      reject(Object.assign(new Error('slot worker socket is not configured'), { code: 'worker_socket_missing' }))
      return
    }
    const payload = body == null ? null : Buffer.from(typeof body === 'string' ? body : JSON.stringify(body))
    if (typeof onPayloadBytes === 'function') onPayloadBytes(payload?.length || 0)
    const internalToken = readInternalToken(exec)
    const requestHeaders = { ...headers }
    if (payload) {
      requestHeaders['content-type'] = 'application/json'
      requestHeaders['content-length'] = String(payload.length)
    }
    if (internalToken) {
      // Rust/Codex kernels use the namespaced header; retain the legacy worker header.
      requestHeaders['x-kin-internal-token'] = internalToken
      requestHeaders['x-internal-token'] = internalToken
    }
    let timer = null
    const clearTimer = () => {
      if (timer) {
        clearTimeout(timer)
        timer = null
      }
    }
    const arm = (ms, message) => {
      clearTimer()
      const wait = Math.max(1, Number(ms) || 0)
      timer = setTimeout(() => {
        req.destroy(Object.assign(new Error(message), { code: 'worker_timeout' }))
      }, wait)
      timer.unref?.()
    }
    const req = http.request(
      {
        socketPath,
        path: requestPath,
        method,
        headers: requestHeaders,
        signal,
      },
      (res) => {
        if (timeoutMode === 'first-byte') clearTimer()
        resolve(res)
      },
    )
    arm(timeoutMs, `slot worker timeout after ${timeoutMs}ms`)
    req.once('close', clearTimer)
    req.once('error', reject)
    if (payload) req.write(payload)
    req.end()
  })
}

/**
 * Client went away: name the hop to the kernel so it cancels that job on the
 * CLI right now. The dropped socket alone left the kernel guessing whether
 * the CLI still owned the job. Best effort: the kernel also sees the drop,
 * and an old worker without the route just answers 404.
 */
function requestKernelCancel(exec, requestId) {
  workerRequest(exec, {
    method: 'POST',
    requestPath: '/internal/v1/cancel',
    body: { request_id: requestId },
    timeoutMs: 3000,
  })
    .then((res) => res.resume())
    .catch((error) => {
      console.warn(`[kernel-cancel] ${exec?.vmId || 'slot'} ${requestId}: ${error?.code || error?.message || error}`)
    })
}

function cancelOnAbort(exec, signal, requestId) {
  if (!signal || signal.aborted) return () => {}
  const onAbort = () => requestKernelCancel(exec, requestId)
  signal.addEventListener('abort', onAbort, { once: true })
  return () => signal.removeEventListener('abort', onAbort)
}

async function readAll(stream, limit = MAX_BODY) {
  const chunks = []
  let size = 0
  for await (const chunk of stream) {
    const value = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
    size += value.length
    if (size > limit) {
      stream.destroy?.()
      throw Object.assign(new Error(`worker response exceeds ${limit} bytes`), { code: 'worker_response_too_large' })
    }
    chunks.push(value)
  }
  return Buffer.concat(chunks)
}

function parseJson(buffer) {
  try {
    return JSON.parse(String(buffer || ''))
  } catch {
    return {
      type: 'error',
      error: { type: 'worker_error', code: 'worker_invalid_json', message: String(buffer || '').slice(0, 400) },
    }
  }
}

function publicHeaders(headers = {}) {
  const result = {}
  for (const [key, value] of Object.entries(headers || {})) {
    if (value == null) continue
    const lower = String(key).toLowerCase()
    if (lower === 'set-cookie' || lower === 'authorization' || lower === 'x-api-key') continue
    result[lower] = Array.isArray(value) ? value.join(',') : String(value)
  }
  return result
}

/** Flatten wrap `X-Rate-Limit-Headers` JSON into Anthropic Extra keys. */
function mergeRateLimitHeaders(headers = {}) {
  const out = { ...headers }
  const packed = headers['x-rate-limit-headers']
  if (!packed) return out
  try {
    const parsed = typeof packed === 'string' ? JSON.parse(packed) : packed
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return out
    for (const [key, value] of Object.entries(parsed)) {
      if (value == null || value === '') continue
      const lower = String(key).toLowerCase()
      if (lower.startsWith('anthropic-ratelimit-') || lower === 'retry-after' || lower === 'request-id') {
        out[lower] = String(value)
      }
    }
  } catch {}
  return out
}

/** Parse the X-Usage / X-Model / X-Stop-Reason worker metadata. */
function streamMetaFromHeaders(headers = {}) {
  let usage = null
  if (headers['x-usage']) {
    try {
      usage = JSON.parse(headers['x-usage'])
    } catch {}
  }
  return {
    usage,
    model: headers['x-model'] || null,
    stopReason: headers['x-stop-reason'] || null,
  }
}

function mergeUsage(current, next) {
  if (!next || typeof next !== 'object') return current
  const out = { ...(current || {}) }
  for (const [key, value] of Object.entries(next)) {
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      out[key] = { ...(typeof out[key] === 'object' && out[key] ? out[key] : {}), ...value }
    } else {
      out[key] = value
    }
  }
  return out
}

function responsesEventType(raw) {
  const text = String(raw || '').trim()
  if (!text.startsWith('{') || !text.endsWith('}')) return ''
  const match = text.match(/^\s*\{\s*"type"\s*:\s*"([^"\\]+)"/) || text.match(/"type"\s*:\s*"([^"\\]+)"/)
  return match?.[1] || ''
}

function responsesEventNeedsFullParse(type, raw) {
  if (!type) return true
  if (raw.includes('"usage"') || raw.includes('"service_tier"')) return true
  return (
    type === 'error' ||
    type === 'response.created' ||
    type === 'response.failed' ||
    type === 'response.incomplete' ||
    type === 'response.completed' ||
    type === 'response.done' ||
    type === 'response_headers' ||
    type === 'response.output_item.added' ||
    type === 'response.output_item.done' ||
    type === 'response.content_part.added' ||
    type === 'response.content_part.done'
  )
}

function parseWorkerSseEvent(raw, mode) {
  if (mode !== 'responses-passthrough') return JSON.parse(raw)
  const type = responsesEventType(raw)
  if (responsesEventNeedsFullParse(type, raw)) return JSON.parse(raw)
  return type.endsWith('.delta') ? { type, delta: '1' } : { type }
}

/** First user-visible assistant output — never transport/terminal metadata alone. */
export function isDownstreamCommitEvent(event) {
  if (!event || typeof event !== 'object') return false
  const t = String(event.type || '')
  if (t === 'error' || t === 'message_start' || t === 'response_headers') return false
  if (t === 'message_stop' || t === 'response.completed' || t === 'response.done' || t === 'message_delta') return false

  if (t === 'content_block_delta') {
    const d = event.delta || {}
    return !!(d.text || d.thinking || d.partial_json || d.refusal || d.signature)
  }
  if (t === 'content_block_start') {
    const b = event.content_block || {}
    const kind = String(b.type || '')
    return (
      kind === 'text' ||
      kind === 'thinking' ||
      kind === 'redacted_thinking' ||
      kind === 'refusal' ||
      kind === 'tool_use' ||
      kind === 'server_tool_use' ||
      kind === 'mcp_tool_use' ||
      kind.endsWith('_tool_use') ||
      !!(b.text || b.thinking)
    )
  }
  return t.startsWith('response.output_')
}

/**
 * Timing starts at generated content, independently of downstream commitment.
 * A generated tool name is first output: name-only tools often never send a
 * parameter delta. Connection metadata, empty blocks, and trailers are not TTFT.
 * Shared Claude/Codex kernel stream path only; API transport still stamps the
 * first `data:` line in api-protocol.mjs.
 */
function hasGeneratedContent(block) {
  if (!block || typeof block !== 'object') return false
  const nonempty = (value) => typeof value === 'string' && value.length > 0
  if ([block.text, block.thinking, block.refusal].some(nonempty)) return true
  if (block.type === 'redacted_thinking') return nonempty(block.data)
  return (
    (block.type === 'function_call' ||
      block.type === 'custom_tool_call' ||
      String(block.type || '').endsWith('tool_use')) &&
    (nonempty(block.name) || nonempty(block.arguments) || nonempty(block.input))
  )
}

function isFirstTokenEvent(event) {
  if (!event || typeof event !== 'object') return false
  const type = String(event.type || '')
  if (type === 'content_block_delta') {
    const delta = event.delta || {}
    return [delta.text, delta.thinking, delta.partial_json, delta.refusal].some(
      (value) => typeof value === 'string' && value.length > 0,
    )
  }
  if (type === 'content_block_start') return hasGeneratedContent(event.content_block)
  if (type.startsWith('response.') && type.endsWith('.delta')) {
    return typeof event.delta === 'string' && event.delta.length > 0
  }
  if (type === 'response.content_part.added' || type === 'response.content_part.done')
    return hasGeneratedContent(event.part)
  if (type === 'response.output_item.added' || type === 'response.output_item.done') {
    return (
      hasGeneratedContent(event.item) ||
      (Array.isArray(event.item?.content) && event.item.content.some(hasGeneratedContent))
    )
  }
  return false
}

/**
 * The upstream entered a real response lifecycle. This switches the worker
 * watchdog from the first-event deadline to the stream-idle deadline, but it
 * does not mean downstream HTTP 200 is safe to commit: Responses can still
 * send a structured error immediately after response.created/in_progress.
 */
export function isUpstreamAcceptedEvent(event) {
  if (!event || typeof event !== 'object') return false
  const type = String(event.type || '')
  if (type === 'error' || type === 'response_headers') return false
  return type === 'response.created' || type === 'response.in_progress' || isDownstreamCommitEvent(event)
}

/** Frames that prove the model/tool turn advanced, excluding transport/control chatter. */
export function isStreamBusinessProgressEvent(event) {
  if (!event || typeof event !== 'object') return false
  const type = String(event.type || '')
  if (
    type === 'error' ||
    type === 'message_stop' ||
    type === 'response.completed' ||
    type === 'response.done' ||
    type === 'response.failed' ||
    type === 'response.incomplete'
  ) {
    return true
  }
  if (type === 'content_block_start' || type === 'content_block_stop' || type === 'message_delta') return true
  if (type === 'content_block_delta') return isDownstreamCommitEvent(event)
  if (type.startsWith('response.output_') || type.startsWith('response.content_part.')) return true
  if (
    type.startsWith('response.reasoning_') ||
    type.startsWith('response.function_call_arguments.') ||
    type.startsWith('response.custom_tool_call_input.')
  ) {
    return true
  }
  return /^response\.(?:web_search_call|file_search_call|computer_tool_call|code_interpreter_call|mcp_call)\./.test(
    type,
  )
}

/** Anthropic SSE: message_start.message.usage + message_delta.usage. OpenAI Responses: response.usage. */
export function usageFromSseEvent(event) {
  if (!event || typeof event !== 'object') return null
  if (event.usage && typeof event.usage === 'object') return event.usage
  if (event.response?.usage && typeof event.response.usage === 'object') return event.response.usage
  if (event.message?.usage && typeof event.message.usage === 'object') return event.message.usage
  return null
}

const AUTH_ERROR_TEXT =
  /authentication_error|token has been revoked|oauth_revoked|invalid_grant|invalid (?:bearer|x-api-key)|OAuth token has expired/i
// cli-hop can flatten an organization permission denial into generic api_error.
// Preserve the denial instead of replacing it with an empty-hop error.
const PERMISSION_ERROR_TEXT = /organization does not have access to claude/i

/**
 * The kernel cli-hop answers 200 and then streams `event: error` (sub2api
 * sseStreamErrorEventError). Before any downstream byte, restore the HTTP
 * status the upstream meant so the pool classifies it like a real response.
 */
export function semanticStatusForStreamError(errorBody) {
  const type = String(errorBody?.error?.type || errorBody?.type || '')
  const code = String(errorBody?.error?.code || errorBody?.code || '')
  const message = String(errorBody?.error?.message || errorBody?.message || '')
  if (isPlanLimitMessage(message)) return 429
  if (isContentPolicyErrorCode(code)) return 403
  // The kernel names the status the job failed with; older kernels did not.
  // Codex Responses Lite error events put it on the outer object (`{type,status,error}`).
  const explicit = Number(errorBody?.error?.status ?? errorBody?.status)
  if (explicit >= 400 && explicit < 600) return explicit
  if (type === 'rate_limit_error') return 429
  if (type === 'overloaded_error') return 529
  if (type === 'authentication_error' || AUTH_ERROR_TEXT.test(message)) return 401
  if (type === 'permission_error' || PERMISSION_ERROR_TEXT.test(message)) return 403
  if (type === 'invalid_request_error') return 400
  return 502
}

/**
 * Kernel `map_kernel` folds every provider failure into 502 `provider_error`.
 * Restore recognized permission denials and plan-limit / overload statuses.
 */
export function restoreKernelErrorStatus(result = {}, { now = Date.now() } = {}) {
  if (!result || result.ok || Number(result.status) !== 502) return result
  const body = result.body
  if (!(body?.type === 'error' || body?.error)) return result
  const status = semanticStatusForStreamError(body)
  if (status !== 403 && status !== 429 && status !== 529) return result
  const headers =
    status === 429
      ? extraHeadersFromLimitError(String(body?.error?.message || ''), result.headers || {}, now)
      : result.headers
  return { ...result, status, headers, terminalState: 'rejected' }
}

/**
 * Uncommitted hop that streamed a real provider error gets that status.
 * A hop that ended with no visible output stays on its 2xx status and is an
 * empty hop. Promoting it to 502 makes a healthy VM look overloaded.
 */
function unfinishedEmptyHop(result) {
  return {
    ...result,
    ok: false,
    terminalState: 'incomplete',
    body: {
      type: 'error',
      error: {
        type: 'api_error',
        code: 'empty_response',
        message: 'Upstream stream ended without visible output',
      },
    },
  }
}

export function restoreUncommittedHop(result = {}, { now = Date.now() } = {}) {
  if (!result || result.committed || result.ok || isClientCancelledResult(result)) return result
  if (Number(result.status) < 200 || Number(result.status) >= 300) return result
  const body = result.body
  if (body?.type === 'error' || body?.error) {
    const status = semanticStatusForStreamError(body)
    const message = String(body?.error?.message || body?.message || '')
    const code = String(body?.error?.code || '')
    if (code && code !== 'empty_response') {
      const retryAfter = body?.error?.retry_after
      const coded = retryAfter ? { ...(result.headers || {}), 'retry-after': String(retryAfter) } : result.headers
      const headers = status === 429 ? extraHeadersFromLimitError(message, coded || {}, now) : coded
      return {
        ...result,
        status,
        headers,
        terminalState: status === 502 ? result.terminalState || 'incomplete' : 'rejected',
        streamError: status !== 502,
      }
    }
    if (status === 502 && !/overload|usage policy/i.test(message)) {
      if (isWrapConnectionError(message)) return { ...result, ok: false, terminalState: 'incomplete' }
      return unfinishedEmptyHop(result)
    }
    const headers =
      status === 429 ? extraHeadersFromLimitError(String(message), result.headers || {}, now) : result.headers
    const terminalState = status === 502 ? result.terminalState : 'rejected'
    return { ...result, status, headers, terminalState, streamError: status !== 502 }
  }
  if (Array.isArray(body?.content) && body.content.length) return result
  return unfinishedEmptyHop(result)
}

function dumpSessionEnvelope(envelope) {
  const dir = process.env.KIN_SESSION_DUMP
  if (!dir) return
  try {
    fs.mkdirSync(dir, { recursive: true })
    const headers = {}
    for (const [key, value] of Object.entries(envelope.headers || {})) {
      headers[key] = /authorization|api-key|cookie|token/i.test(key) ? '***REDACTED***' : value
    }
    const rec = {
      ts: new Date().toISOString(),
      hop: 'go-worker-envelope',
      note: 'This is the JSON body+headers the slot worker POSTs to api.anthropic.com/v1/messages. Authorization is attached by the worker from OAuth and is not in this envelope.',
      stream: envelope.stream,
      delivery_mode: envelope.delivery_mode,
      headers,
      body: envelope.body,
    }
    const name = `${Date.now()}-${Math.random().toString(16).slice(2, 8)}-envelope.json`
    fs.writeFileSync(path.join(dir, name), JSON.stringify(rec, null, 2))
  } catch {}
}

/**
 * `cliHop`: the slot cli-node builds the wire and always sends
 * mid-conversation-system; the kernel drops these envelope headers. Gating the
 * body on the VM's HTTP betas would lift every role=system turn into system[],
 * so system grows each turn and no cached prefix is ever read.
 */
export function finalizeWorkerPayload({ body, reqHeaders, exec, identity, want1m = false, cliHop = false }) {
  const model = body?.model || ''
  const credMode = credentialModeFromOauth(exec?.vm?.claude || {})
  const headers = prepareOutboundHeaders(reqHeaders, exec?.homeDir, identity, model, {
    credentialMode: credMode,
    want1m: want1m === true,
  })
  const betas = withRequestProtocolBetas(String(headers?.['anthropic-beta'] || '').split(','), body)
  headers['anthropic-beta'] = betas.join(',')
  // Envelope headers are discarded; the existing MessageRequest.betas channel
  // carries request gates to the native CLI, which builds the actual HTTP wire.
  const gated = cliHop ? { ...body, betas } : sanitizeAnthropicBodyForBetaTokens(body, headers['anthropic-beta'])
  return { headers, body: sealClaudeCodeCch(gated) }
}

function workerEnvelope({
  requestContext = null,
  body,
  reqHeaders,
  exec,
  identity,
  stream,
  deliveryMode,
  want1m = false,
  cacheTtl = null,
  preserveCacheBreakpoints = null,
  cliHop = false,
} = {}) {
  const finalized = finalizeWorkerPayload({ body, reqHeaders, exec, identity, want1m, cliHop })
  const envelope = {
    ...(requestContext ? { request_context: requestContext } : {}),
    body: finalized.body,
    headers: finalized.headers,
    stream: !!stream,
    delivery_mode: deliveryMode || 'realtime',
    preserve_cache_breakpoints: preserveCacheBreakpoints == null ? cacheTtl == null : preserveCacheBreakpoints === true,
    cache_ttl: cacheTtl == null ? null : String(cacheTtl),
  }
  dumpSessionEnvelope(envelope)
  return envelope
}

function mockScenario(exec) {
  try {
    const configured = JSON.parse(process.env.KIN_MOCK_ACCOUNT_SCENARIOS || '{}')
    return configured?.[exec?.vmId] || null
  } catch {
    return null
  }
}

export async function callGoWorker({
  requestContext = null,
  exec,
  body,
  reqHeaders = {},
  timeoutMs = 180000,
  identity = null,
  signal,
  want1m = false,
  cacheTtl = null,
  preserveCacheBreakpoints = null,
  requestPath = '/internal/v1/messages',
  envelope = null,
  cliHop = false,
} = {}) {
  if (isCrsMock()) {
    const { body: outboundBody, headers } = finalizeWorkerPayload({ body, reqHeaders, exec, identity, want1m, cliHop })
    writeCrsTrace({ body: outboundBody, headers, stream: false })
    const mock = mockCrsPayload({ scenario: mockScenario(exec) })
    return {
      ...mock,
      via: 'go-worker-mock',
      terminalState: mock.ok ? 'verified' : 'error',
      usage: mock.body?.usage || null,
      model: mock.body?.model || null,
      stopReason: mock.body?.stop_reason || null,
    }
  }
  const requestId = randomUUID()
  const stopCancel = cancelOnAbort(exec, signal, requestId)
  try {
    const response = await workerRequest(exec, {
      method: 'POST',
      requestPath,
      body: {
        ...(envelope ||
          workerEnvelope({
            body,
            reqHeaders,
            exec,
            identity,
            stream: false,
            want1m,
            cacheTtl,
            preserveCacheBreakpoints,
            cliHop,
            requestContext,
          })),
        request_id: requestId,
      },
      signal,
      timeoutMs,
    })
    const data = await readAll(response)
    const parsed = parseJson(data)
    const headers = mergeRateLimitHeaders(publicHeaders(response.headers))
    return restoreKernelErrorStatus({
      ok: response.statusCode >= 200 && response.statusCode < 300 && parsed?.type !== 'error',
      status: response.statusCode || 0,
      via: 'go-worker',
      body: parsed,
      headers,
      usage: parsed?.usage || null,
      model: parsed?.model || null,
      stopReason: parsed?.stop_reason || null,
      terminalState: headers['x-terminal-state'] || null,
      transportError: false,
    })
  } catch (error) {
    return {
      ok: false,
      status: 0,
      via: 'go-worker',
      body: {
        type: 'error',
        error: {
          type: 'worker_error',
          code: error.code || 'worker_transport_error',
          message: String(error.message || error).slice(0, 300),
        },
      },
      headers: {},
      terminalState: 'transport_error',
      transportError: true,
    }
  } finally {
    stopCancel()
  }
}

export async function streamGoWorker({
  requestContext = null,
  exec,
  body,
  reqHeaders = {},
  timeoutMs = 180000,
  idleTimeoutMs = 0,
  overallTimeoutMs = 0,
  identity = null,
  signal,
  deliveryMode = 'realtime',
  onEvent,
  onEventBatch,
  onCommit,
  onResponse,
  onAccepted,
  want1m = false,
  cacheTtl = null,
  preserveCacheBreakpoints = null,
  requestPath = '/internal/v1/messages',
  envelope = null,
  cliHop = false,
  sseEventMode = null,
} = {}) {
  if (isCrsMock()) {
    const { body: outboundBody, headers } = finalizeWorkerPayload({ body, reqHeaders, exec, identity, want1m, cliHop })
    writeCrsTrace({ body: outboundBody, headers, stream: true })
    const scenario = mockScenario(exec)
    const mockStartedAt = Date.now()
    if (scenario === 'incomplete_stream') {
      if (deliveryMode === 'verified') {
        return {
          ok: false,
          status: 502,
          via: 'go-worker-mock-stream',
          body: { type: 'error', error: { type: 'api_error', message: 'stream closed before message_stop' } },
          headers: {},
          terminalState: 'incomplete',
          committed: false,
        }
      }
      if (typeof onCommit === 'function') onCommit()
      if (onEvent) {
        await onEvent('event: message_start')
        await onEvent('data: {"type":"message_start","message":{"content":[]}}')
        await onEvent('')
        await onEvent('event: content_block_delta')
        await onEvent('data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"partial"}}')
        await onEvent('')
        await onEvent('event: error')
        await onEvent('data: {"type":"error","error":{"type":"api_error","message":"stream incomplete"}}')
        await onEvent('')
      }
      return {
        ok: false,
        status: 200,
        via: 'go-worker-mock-stream',
        body: { type: 'error', error: { type: 'api_error', message: 'stream incomplete' } },
        headers: {},
        terminalState: 'incomplete',
        committed: true,
      }
    }
    const payload = mockCrsPayload({ scenario })
    if (!payload.ok) {
      return {
        ...payload,
        via: 'go-worker-mock-stream',
        terminalState: 'rejected',
        committed: false,
      }
    }
    let mockTtftMs = null
    await emitMockSse(async (line) => {
      if (line.startsWith('data:')) {
        try {
          if (mockTtftMs == null && isFirstTokenEvent(JSON.parse(line.slice(5))))
            mockTtftMs = Date.now() - mockStartedAt
        } catch {}
        if (typeof onCommit === 'function') onCommit()
      }
      if (onEvent) await onEvent(line)
    }, payload)
    return {
      ...payload,
      via: 'go-worker-mock-stream',
      terminalState: payload.ok ? 'verified' : 'error',
      committed: !!payload.ok,
      usage: payload.body?.usage || null,
      model: payload.body?.model || null,
      stopReason: payload.body?.stop_reason || null,
      ttftMs: mockTtftMs,
      firstTokenAt: mockTtftMs == null ? null : mockStartedAt + mockTtftMs,
    }
  }
  let committed = false
  // Hop-relative TTFT. firstTokenAt is epoch ms so request logs subtract ingress t0.
  const startedAt = Date.now()
  let ttftMs = null
  const requestId = randomUUID()
  const outbound = {
    ...(envelope ||
      workerEnvelope({
        body,
        reqHeaders,
        exec,
        identity,
        stream: true,
        deliveryMode,
        want1m,
        cacheTtl,
        preserveCacheBreakpoints,
        cliHop,
        requestContext,
      })),
    request_id: requestId,
  }
  const outboundBody = outbound?.body
  const diagnostic = {
    request_body_bytes: 0,
    input_items: Array.isArray(outboundBody?.input)
      ? outboundBody.input.length
      : Array.isArray(outboundBody?.messages)
        ? outboundBody.messages.length
        : outboundBody?.input == null
          ? 0
          : 1,
    tool_count: Array.isArray(outboundBody?.tools) ? outboundBody.tools.length : 0,
    requested_transport: typeof outbound?.transport === 'string' ? outbound.transport : null,
    chunk_count: 0,
    bytes_read: 0,
    event_count: 0,
    event_types: Object.create(null),
    parse_errors: 0,
    first_event: null,
    last_event: null,
    first_event_ms: null,
    last_event_ms: null,
    accepted_ms: null,
    first_business_progress_ms: null,
    last_business_progress_ms: null,
  }
  let lastBusinessProgressAt = startedAt
  const markBusinessProgress = (now = Date.now()) => {
    const elapsed = Math.max(0, now - startedAt)
    if (diagnostic.first_business_progress_ms == null) diagnostic.first_business_progress_ms = elapsed
    diagnostic.last_business_progress_ms = elapsed
    lastBusinessProgressAt = now
  }
  const streamDiagnostic = (terminalState, termination, extra = {}) => ({
    ...diagnostic,
    event_types: { ...diagnostic.event_types },
    elapsed_ms: Math.max(0, Date.now() - startedAt),
    idle_for_ms: Math.max(0, Date.now() - lastBusinessProgressAt),
    terminal_state: terminalState,
    termination,
    ...extra,
  })
  let upstreamAccepted = false
  let watchdogExpired = false
  const overallMs = Math.max(0, Number(overallTimeoutMs) || 0)
  const deadlineController = overallMs > 0 ? new AbortController() : null
  const workerSignal = deadlineController
    ? signal
      ? AbortSignal.any([signal, deadlineController.signal])
      : deadlineController.signal
    : signal
  let deadlineExpired = false
  let deadlineTimer = null
  if (deadlineController) {
    deadlineTimer = setTimeout(() => {
      deadlineExpired = true
      requestKernelCancel(exec, requestId)
      deadlineController.abort()
    }, overallMs)
    deadlineTimer.unref?.()
  }
  const stopCancel = cancelOnAbort(exec, signal, requestId)
  try {
    const response = await workerRequest(exec, {
      method: 'POST',
      requestPath,
      body: outbound,
      signal: workerSignal,
      timeoutMs,
      timeoutMode: 'first-byte',
      headers: { te: 'trailers' },
      onPayloadBytes: (bytes) => {
        diagnostic.request_body_bytes = bytes
      },
    })
    const headers = mergeRateLimitHeaders(publicHeaders(response.headers))
    if (typeof onResponse === 'function') {
      await onResponse({ status: response.statusCode || 0, headers })
    }
    if ((response.statusCode || 0) < 200 || (response.statusCode || 0) >= 300) {
      const data = await readAll(response, 1024 * 1024)
      const terminalState = headers['x-terminal-state'] || 'error'
      return restoreKernelErrorStatus({
        ok: false,
        status: response.statusCode || 0,
        via: 'go-worker-stream',
        body: parseJson(data),
        headers,
        committed: false,
        terminalState,
        transportError: false,
        kernelStream: streamDiagnostic(terminalState, 'http_error', {
          response_status: response.statusCode || 0,
        }),
      })
    }
    let buffer = ''
    let lastError = null
    let dataBuf = ''
    let sseUsage = null
    let sseModel = null
    let sseStop = null
    let sseRateHeaders = {}
    const responsesPassthrough = sseEventMode === 'responses-passthrough'
    const assembler = responsesPassthrough ? null : createClaudeMessageAssembler()
    const batchEvents = responsesPassthrough && typeof onEventBatch === 'function'
    const pendingLines = []
    const takeSseEvent = () => {
      try {
        const event = parseWorkerSseEvent(dataBuf, sseEventMode)
        dataBuf = ''
        return event && typeof event === 'object' ? event : null
      } catch {
        return null
      }
    }
    const dispatchEntries = async (entries) => {
      if (!entries.length) return
      if (batchEvents) {
        await onEventBatch(entries)
        return
      }
      if (onEvent) {
        for (const entry of entries) await onEvent(entry.line, entry.event)
      }
    }
    const flushCommit = (batch = null) => {
      if (committed) return null
      committed = true
      if (typeof onCommit === 'function') onCommit()
      const pending = pendingLines.splice(0)
      if (batch) batch.push(...pending)
      const delivery = batch ? null : dispatchEntries(pending)
      return delivery
    }
    const emitLine = (line, event = null, batch = null) => {
      const entry = { line, event }
      if (!committed) {
        pendingLines.push(entry)
        return null
      }
      if (batch) {
        batch.push(entry)
        return null
      }
      return dispatchEntries([entry])
    }
    const observeSseEvent = (event) => {
      if (!event) return event
      const now = Date.now()
      const type = String(event.type || 'unknown').slice(0, 120) || 'unknown'
      const elapsed = Math.max(0, now - startedAt)
      diagnostic.event_count += 1
      diagnostic.event_types[type] = (diagnostic.event_types[type] || 0) + 1
      if (diagnostic.first_event == null) {
        diagnostic.first_event = type
        diagnostic.first_event_ms = elapsed
      }
      diagnostic.last_event = type
      diagnostic.last_event_ms = elapsed
      if (ttftMs == null && isFirstTokenEvent(event)) ttftMs = elapsed
      if (event.type === 'response_headers' && event.headers && typeof event.headers === 'object') {
        sseRateHeaders = { ...sseRateHeaders, ...event.headers }
      }
      if (event.type === 'error') lastError = event
      if (event.type === 'message_stop') sawMessageStop = true
      if (event.type === 'response.completed' || event.type === 'response.done') sawResponseTerminal = true
      if (isUpstreamAcceptedEvent(event)) {
        if (!upstreamAccepted) {
          upstreamAccepted = true
          diagnostic.accepted_ms = elapsed
          markBusinessProgress(now)
        }
        if (typeof onAccepted === 'function') onAccepted(event)
      }
      if (isStreamBusinessProgressEvent(event)) markBusinessProgress(now)
      const evUsage = usageFromSseEvent(event)
      if (evUsage) sseUsage = mergeUsage(sseUsage, evUsage)
      if (event.message?.model) sseModel = event.message.model
      const stop = event.message?.stop_reason || event.delta?.stop_reason
      if (stop) sseStop = stop
      return event
    }
    const firstByteMs = Math.max(0, Number(timeoutMs) || 0)
    const idleMs = Math.max(0, Number(idleTimeoutMs) || 0)
    let sawMessageStop = false
    let sawResponseTerminal = false
    let idleTimer = null
    if (firstByteMs > 0 || idleMs > 0) {
      const limits = [firstByteMs, idleMs].filter((value) => value > 0)
      const checkEveryMs = Math.min(1000, Math.max(25, Math.floor(Math.min(...limits) / 4)))
      idleTimer = setInterval(() => {
        // Kernel metadata (rate limits / response headers) proves the local
        // socket is alive, but it does not mean OpenAI accepted the turn. Keep
        // the longer first-event deadline until a real lifecycle frame arrives.
        const limit = upstreamAccepted ? idleMs : firstByteMs
        if (limit <= 0) return
        const activeAt = upstreamAccepted ? lastBusinessProgressAt : startedAt
        if (Date.now() - activeAt < limit) return
        if (watchdogExpired) return
        watchdogExpired = true
        const which = upstreamAccepted ? 'idle' : 'first-event'
        requestKernelCancel(exec, requestId)
        response.destroy(
          Object.assign(new Error(`slot worker ${which} timeout after ${limit}ms`), { code: 'worker_timeout' }),
        )
      }, checkEveryMs)
      idleTimer.unref?.()
    }
    try {
      // message_stop is the protocol terminal event, but the kernel still sends
      // job_done and its trailers afterward. Keep reading until the worker
      // closes the response so a normal completion is not mistaken for cancel.
      for await (const chunk of response) {
        const eventBatch = batchEvents ? [] : null
        const value = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk))
        diagnostic.chunk_count += 1
        diagnostic.bytes_read += value.length
        buffer += value.toString('utf8')
        let newline
        while ((newline = buffer.indexOf('\n')) >= 0) {
          const line = buffer.slice(0, newline).replace(/\r$/, '')
          buffer = buffer.slice(newline + 1)
          if (assembler) applyClaudeSSELineToMessage(line, assembler)
          let parsedEvent = null
          if (line.startsWith('data:')) {
            const piece = line.slice(5).trim()
            if (piece && piece !== '[DONE]') {
              dataBuf = dataBuf ? `${dataBuf}\n${piece}` : piece
              parsedEvent = observeSseEvent(takeSseEvent())
              if (!parsedEvent) diagnostic.parse_errors += 1
              if (isDownstreamCommitEvent(parsedEvent)) {
                const delivery = flushCommit(eventBatch)
                if (delivery) await delivery
              }
            }
          } else if (line === '') {
            if (dataBuf) {
              const event = takeSseEvent()
              dataBuf = ''
              observeSseEvent(event)
              if (!event) diagnostic.parse_errors += 1
              if (isDownstreamCommitEvent(event)) {
                const delivery = flushCommit(eventBatch)
                if (delivery) await delivery
              }
            }
          } else if (dataBuf && !line.startsWith('event:') && !line.startsWith(':')) {
            dataBuf = `${dataBuf}\n${line}`
            const event = observeSseEvent(takeSseEvent())
            if (!event) diagnostic.parse_errors += 1
            if (isDownstreamCommitEvent(event)) {
              const delivery = flushCommit(eventBatch)
              if (delivery) await delivery
            }
          }
          const delivery = emitLine(line, parsedEvent, eventBatch)
          if (delivery) await delivery
        }
        if (eventBatch?.length) await dispatchEntries(eventBatch)
      }
      const finalBatch = batchEvents ? [] : null
      if (buffer) {
        if (assembler) applyClaudeSSELineToMessage(buffer, assembler)
        const delivery = emitLine(buffer, null, finalBatch)
        if (delivery) await delivery
      }
      if (dataBuf) {
        const event = observeSseEvent(takeSseEvent())
        if (!event) diagnostic.parse_errors += 1
        if (isDownstreamCommitEvent(event)) {
          const delivery = flushCommit(finalBatch)
          if (delivery) await delivery
        }
      }
      const trailers = mergeRateLimitHeaders(publicHeaders(response.trailers))
      const meta = streamMetaFromHeaders({ ...headers, ...trailers })
      const assembled = assembler?.message || null
      const stopReason = meta.stopReason || sseStop || assembled?.stop_reason || null
      const complete =
        !lastError &&
        (sawResponseTerminal || isCompleteAssistantMessage({ body: assembled, stopReason, sawMessageStop }))
      if (!committed && complete) {
        const delivery = flushCommit(finalBatch)
        if (delivery) await delivery
      }
      if (finalBatch?.length) await dispatchEntries(finalBatch)
      const terminalState = complete ? 'verified' : 'incomplete'
      const rateHeaders = mergeRateLimitHeaders({ ...sseRateHeaders, ...headers, ...trailers })
      const termination = complete ? 'verified' : lastError ? 'error_event' : 'eof_incomplete'
      return restoreUncommittedHop({
        ok: response.statusCode === 200 && !lastError && complete,
        status: response.statusCode || 0,
        via: 'go-worker-stream',

        body:
          lastError ||
          assembled ||
          (responsesPassthrough ? { type: 'response' } : { type: 'message', role: 'assistant', content: [] }),
        headers: rateHeaders,
        // Trailer stays authoritative, but it may carry totals only (Codex/Responses hops).
        // Merge so SSE `input_tokens_details` / cache breakdown survives instead of being short-circuited.
        usage: mergeUsage(mergeUsage(assembled?.usage || null, sseUsage), meta.usage),
        model: meta.model || sseModel || assembled?.model || null,
        stopReason,
        sawMessageStop,
        ttftMs,
        firstTokenAt: ttftMs == null ? null : startedAt + ttftMs,
        committed,
        terminalState,
        transportError: false,
        kernelStream: streamDiagnostic(terminalState, termination, {
          response_status: response.statusCode || 0,
        }),
      })
    } finally {
      if (idleTimer) clearInterval(idleTimer)
    }
  } catch (error) {
    if (signal?.aborted) {
      return {
        ...clientCancelledResult({
          via: 'go-worker-stream',
          ttftMs,
          firstTokenAt: ttftMs == null ? null : startedAt + ttftMs,
          committed,
        }),
        kernelStream: streamDiagnostic(committed ? 'incomplete' : 'cancelled', 'client_cancelled'),
      }
    }
    if (deadlineExpired) {
      return {
        ok: false,
        status: 504,
        via: 'go-worker-stream',
        body: {
          type: 'error',
          error: {
            type: 'timeout_error',
            code: 'kernel_retry_deadline',
            message: `kernel stream exceeded the ${overallMs}ms overall deadline`,
          },
        },
        headers: {},
        ttftMs,
        firstTokenAt: ttftMs == null ? null : startedAt + ttftMs,
        committed,
        terminalState: committed ? 'incomplete' : 'transport_error',
        transportError: true,
        kernelStream: streamDiagnostic(committed ? 'incomplete' : 'transport_error', 'overall_timeout', {
          timeout_ms: overallMs,
        }),
      }
    }
    if (error.code === 'worker_timeout' && !watchdogExpired) requestKernelCancel(exec, requestId)
    return {
      ok: false,
      status: 0,
      via: 'go-worker-stream',
      body: {
        type: 'error',
        error: {
          type: 'worker_error',
          code: error.code || 'worker_transport_error',
          message: String(error.message || error).slice(0, 300),
        },
      },
      headers: {},
      ttftMs,
      firstTokenAt: ttftMs == null ? null : startedAt + ttftMs,
      committed,
      terminalState: committed ? 'incomplete' : 'transport_error',
      transportError: true,
      kernelStream: streamDiagnostic(
        committed ? 'incomplete' : 'transport_error',
        error.code === 'worker_timeout'
          ? upstreamAccepted
            ? 'idle_timeout'
            : 'first_event_timeout'
          : 'transport_error',
        error.code === 'worker_timeout' ? { timeout_ms: upstreamAccepted ? idleTimeoutMs : timeoutMs } : {},
      ),
    }
  } finally {
    if (deadlineTimer) clearTimeout(deadlineTimer)
    stopCancel()
  }
}

export async function workerHealth(exec, { timeoutMs = 3000, signal } = {}) {
  if (isCrsMock()) {
    const claude = exec?.vm?.claude || {}
    const hasCredential = hasCredentialPresence(claude)
    return {
      ok: hasCredential,
      status: hasCredential ? 'ready' : 'degraded',
      vm_id: exec?.vmId || null,
      proxy_configured: true,
      credential: {
        has_access: hasAccessPresence(claude),
        has_refresh: hasRefreshPresence(claude),
        generation: 1,
        needs_refresh: !hasAccessPresence(claude),
      },
      source: 'go-worker-mock',
    }
  }
  try {
    const response = await workerRequest(exec, {
      requestPath: '/internal/health',
      timeoutMs,
      signal,
    })
    const body = parseJson(await readAll(response, 1024 * 1024))
    return { ok: response.statusCode === 200 && body?.ok === true, status: response.statusCode || 0, ...body }
  } catch (error) {
    return {
      ok: false,
      status: 0,
      error: String(error.message || error).slice(0, 300),
      code: error.code || 'worker_unavailable',
    }
  }
}

export async function ensureWorkerCredential(exec, { force = false } = {}) {
  if (isCrsMock()) {
    return {
      ok: true,
      status: 200,
      refreshed: !!force,
      credential: {
        has_access: hasAccessPresence(exec?.vm?.claude),
        has_refresh: hasRefreshPresence(exec?.vm?.claude),
        generation: exec?.vm?.claude?._token_version || 1,
      },
    }
  }

  const credential = readWorkerCredentialFile(exec.homeDir)
  if (!credential) {
    return {
      ok: false,
      status: 400,
      refreshed: false,
      error: { code: 'credential_required', message: 'slot has no credential file' },
    }
  }

  const now = Date.now()
  const apiKey = isApiKeyMode(credential.type || credential.mode)
  const alreadyFresh = apiKey || (!force && !needsRefresh(credential.expires_at, now, REFRESH_SKEW_MS))
  if (alreadyFresh) {
    return {
      ok: true,
      status: 200,
      refreshed: false,
      refresh_class: 'already_fresh',
      credential: credentialSummary(credential, now),
    }
  }

  const snapshotRefresh = String(credential.refresh_token || '')
  const lockKey = String(exec.homeDir || exec.vm?.id || 'credential')
  return withCredentialRefreshLock(lockKey, async () => {
    const live = readWorkerCredentialFile(exec.homeDir) || credential
    const liveRefresh = String(live?.refresh_token || '')
    if (snapshotRefresh && liveRefresh && snapshotRefresh !== liveRefresh) {
      return {
        ok: true,
        status: 200,
        refreshed: false,
        refresh_class: 'already_fresh',
        credential: credentialSummary(live, Date.now()),
      }
    }
    const liveNow = Date.now()
    if (!force && !needsRefresh(live.expires_at, liveNow, REFRESH_SKEW_MS) && !isApiKeyMode(live.type || live.mode)) {
      return {
        ok: true,
        status: 200,
        refreshed: false,
        refresh_class: 'already_fresh',
        credential: credentialSummary(live, liveNow),
      }
    }
    const result = await runSlotOauth(exec, 'refresh', { force: !!force })
    const body = result?.body || {}
    const refreshedCredential = readWorkerCredentialFile(exec.homeDir)
    const mapped = {
      ok: !!result?.ok,
      status: result?.status || 0,
      refreshed: !!body.refreshed,
      refresh_class: result?.ok ? (body.refreshed ? 'rotated' : 'already_fresh') : undefined,
      credential: result?.ok ? credentialSummary({ ...(refreshedCredential || {}), ...body }, Date.now()) : undefined,
      error: result?.ok ? undefined : body.error,
    }
    if (!mapped.ok) mapped.refresh_class = classifyCredentialRefresh(mapped)
    return mapped
  })
}

function credentialSummary(credential, now = Date.now()) {
  if (!credential) return undefined
  const expiresAt = credential.expires_at ?? null
  const expiresAtMs =
    Number(expiresAt) && Number(expiresAt) < 10_000_000_000 ? Number(expiresAt) * 1000 : Number(expiresAt)
  return {
    type: credential.type || credential.mode || null,
    mode: credential.mode || credential.type || null,
    account_uuid: credential.account_uuid || null,
    org_uuid: credential.org_uuid || null,
    email: credential.email || null,
    scope: credential.scope || null,
    auth_scheme: credential.auth_scheme || null,
    has_access: credential.has_access ?? !!(credential.access_token || credential.api_key),
    has_refresh: credential.has_refresh ?? !!credential.refresh_token,
    needs_refresh: isApiKeyMode(credential.type || credential.mode)
      ? false
      : needsRefresh(expiresAt, now, REFRESH_SKEW_MS),
    expires_at: expiresAt,
    ttl_seconds:
      Number.isFinite(expiresAtMs) && expiresAtMs > 0 ? Math.max(0, Math.ceil((expiresAtMs - now) / 1000)) : null,
    generation: credential._token_version || null,
  }
}

export async function importWorkerCredential(exec, credential, { timeoutMs = 60000, signal } = {}) {
  try {
    writeWorkerCredentialFile(exec.homeDir, credential)
    // Import is the one case where the control plane's credential replaces the slot's copy.
    await slotHost(exec.vm).onImport(exec.vm, path.dirname(exec.homeDir))
  } catch (error) {
    return {
      ok: false,
      status: 500,
      error: { code: 'credential_import_failed', message: String(error.message || error).slice(0, 300) },
    }
  }
  return {
    ok: true,
    status: 200,
    credential: {
      has_access: !!credential?.access_token || !!credential?.api_key,
      has_refresh: !!credential?.refresh_token,
      expires_at: credential?.expires_at || null,
      generation: Date.now(),
    },
  }
}

export async function callWorkerGet(exec, requestPath, { timeoutMs = 30000, signal } = {}) {
  if (isCrsMock()) {
    if (requestPath === '/internal/v1/models') {
      return {
        ok: true,
        status: 200,
        body: {
          object: 'list',
          data: [
            {
              id: process.env.KIN_MOCK_MODEL || 'claude-haiku-4-5-20251001',
              object: 'model',
              owned_by: 'anthropic',
            },
          ],
        },
        headers: {},
        via: 'go-worker-mock',
      }
    }
    if (requestPath === '/internal/identity') {
      return {
        ok: true,
        status: 200,
        body: {
          ok: true,
          identity: {
            schema_version: '1',
            runtime_kind: exec?.vm?.runtime?.type || 'docker',
            hostname: exec?.vmId || 'mock',
            os_pretty: 'Mock OS',
            arch: 'x64',
            goos: 'linux',
            worker_version: 'mock',
            collected_at: new Date().toISOString(),
          },
        },
        headers: {},
        via: 'go-worker-mock',
      }
    }
    if (requestPath === '/internal/oauth/usage') {
      return {
        ok: true,
        status: 200,
        body: {
          five_hour: { utilization: 0.12, resets_at: '2026-08-19T20:00:00Z' },
          seven_day: { utilization: 0.34, resets_at: '2026-08-25T00:00:00Z' },
          limits: [
            {
              kind: 'weekly_scoped',
              percent: 21,
              resets_at: '2026-08-25T00:00:00Z',
              scope: { model: { display_name: 'Fable' } },
            },
          ],
        },
        headers: {},
        via: 'go-worker-mock',
      }
    }
  }
  if (requestPath === '/internal/v1/models') {
    return runSlotOauth(exec, 'models', { timeoutMs })
  }
  if (requestPath === '/internal/oauth/usage') {
    return runSlotOauth(exec, 'usage', { timeoutMs })
  }
  try {
    const response = await workerRequest(exec, { requestPath, timeoutMs, signal })
    const data = await readAll(response)
    return {
      ok: response.statusCode >= 200 && response.statusCode < 300,
      status: response.statusCode || 0,
      body: parseJson(data),
      headers: mergeRateLimitHeaders(publicHeaders(response.headers)),
      via: 'go-worker',
    }
  } catch (error) {
    return {
      ok: false,
      status: 0,
      body: {
        error: { code: error.code || 'worker_unavailable', message: String(error.message || error).slice(0, 300) },
      },
      headers: {},
      via: 'go-worker',
      transportError: true,
    }
  }
}

export async function countTokensViaWorker(exec, { body, headers = {}, timeoutMs = 45000 } = {}) {
  if (isCrsMock()) {
    return { ok: true, status: 200, body: { input_tokens: 8 }, headers: {}, via: 'go-worker-mock' }
  }
  return runSlotOauth(exec, 'count-tokens', { body, headers, timeoutMs })
}
