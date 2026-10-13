/**
 * Request logging — sub2api UsageLog + access-log inspired (SQLite-backed).
 *
 * Modes:
 *   off    — no writes
 *   normal — summary row (tokens, status, duration, key/vm ids; no bodies)
 *   debug  — normal + full redacted request record (headers/body/hop_meta)
 *
 * Storage: `request_logs` (summaries) + `request_log_debug` (full records).
 * Optional JSONL mirror for external log shippers: KIN_REQUEST_LOG_JSONL=1
 * (writes the previous data/request-logs/YYYY-MM-DD.jsonl format alongside).
 *
 * Env: KIN_REQUEST_LOG_MODE=off|normal|debug (default normal)
 * Per-request override: X-Kin-Debug: 1 or X-Kin-Log: debug|normal|off
 */

import fs from 'node:fs'
import path from 'node:path'
import crypto from 'node:crypto'
import { redactSecrets } from '../core/security.mjs'
import { resolveStoreDb, withTransaction } from '../db/database.mjs'
import { BufferedWriteQueue } from '../db/buffered-write-queue.mjs'
import { UsageLogsRepo } from '../db/repos/usage-logs-repo.mjs'
import { classifyRequestError, resolveMutedErrorClasses } from './error-class.mjs'
import { costColumnsFromUsage, normalizeUsage } from './pricing.mjs'
import { normalizeCacheTtl } from '../protocol/cache-ttl.mjs'
import { REASONING_EFFORTS, sessionIdForLog } from '../protocol/log-fields.mjs'

/** Persist the token a client actually sent — only for failed ingress auth. */
export function presentedApiKeyForLog(token) {
  const t = String(token || '').trim()
  if (!t || t.startsWith('kin-panel-')) return ''
  return t.slice(0, 240)
}

const MODES = new Set(['off', 'normal', 'debug'])

const DEFAULT_RETAIN_DAYS = 7
const DEFAULT_DEBUG_RETAIN_DAYS = 3
const DEFAULT_MAX_MB = 2048
const DEFAULT_DEBUG_BODY_CHARS = 32 * 1024
const CLEANUP_INTERVAL_MS = 10 * 60 * 1000
// Billing scans grow with retained usage. One minute is fresh enough for
// the panel while preventing dashboard/list/usage polling from monopolizing
// the inference event loop.
const BILLING_CACHE_MS = 60_000
const TOTALS_CACHE_MS = 60_000
const WINDOW_STATS_CACHE_MS = 5_000

function clampInt(value, min, max, fallback) {
  if (value == null || value === '') return fallback
  const n = Number(value)
  if (!Number.isFinite(n)) return fallback
  return Math.min(max, Math.max(min, Math.trunc(n)))
}

/** Keep summary retain_days, a shorter debug window, and a hard size cap. */
export function normalizeLoggingConfig(raw = {}) {
  const modeRaw = String(raw.mode || 'normal')
    .trim()
    .toLowerCase()
  const mode = MODES.has(modeRaw) ? modeRaw : 'normal'
  const retainDays = clampInt(raw.retain_days ?? raw.retainDays, 1, 90, DEFAULT_RETAIN_DAYS)
  let debugRetainDays = clampInt(
    raw.debug_retain_days ?? raw.debugRetainDays,
    1,
    90,
    Math.min(DEFAULT_DEBUG_RETAIN_DAYS, retainDays),
  )
  if (debugRetainDays > retainDays) debugRetainDays = retainDays
  return {
    ...raw,
    mode,
    retain_days: retainDays,
    debug_retain_days: debugRetainDays,
    max_mb: clampInt(raw.max_mb ?? raw.maxMb, 0, 102400, DEFAULT_MAX_MB),
  }
}

function dayKey(d = new Date()) {
  return d.toISOString().slice(0, 10)
}

function safeIp(req) {
  const xf = String(req.headers?.['x-forwarded-for'] || '')
    .split(',')[0]
    .trim()
  const raw = xf || req.socket?.remoteAddress || ''
  return String(raw).slice(0, 45)
}

function summarizeBody(body) {
  if (!body || typeof body !== 'object') return { empty: true }
  const messages = Array.isArray(body.messages) ? body.messages : null
  const roles = messages ? messages.map((m) => m?.role).filter(Boolean) : []
  let systemLen = 0
  if (typeof body.system === 'string') systemLen = body.system.length
  else if (Array.isArray(body.system)) {
    systemLen = body.system.reduce((n, b) => n + (typeof b === 'string' ? b.length : (b?.text || '').length), 0)
  }
  return {
    model: body.model || null,
    stream: !!body.stream,
    max_tokens: body.max_tokens ?? body.max_output_tokens ?? null,
    messages_count: messages ? messages.length : body.input != null ? 1 : 0,
    roles,
    tools_count: Array.isArray(body.tools) ? body.tools.length : 0,
    has_thinking: body.thinking != null,
    system_len: systemLen,
    top_level_keys: Object.keys(body).sort(),
  }
}

/**
 * Cache-creation TTL breakdown, normalized like Sub2API:
 * prefer usage.cache_creation.ephemeral_5m/1h; when the breakdown is absent
 * but cache_creation_input_tokens > 0, attribute everything to the resolved TTL
 * (default 1h).
 */
function cacheCreationBreakdown(usage) {
  if (!usage || typeof usage !== 'object') {
    return { cache_creation_5m_tokens: null, cache_creation_1h_tokens: null }
  }
  const nested = usage.cache_creation
  let five = Number(nested?.ephemeral_5m_input_tokens) || 0
  let hour = Number(nested?.ephemeral_1h_input_tokens) || 0
  const total = Number(usage.cache_creation_input_tokens ?? usage.cache_creation_tokens) || 0
  if (five === 0 && hour === 0 && total > 0) {
    if (normalizeCacheTtl(usage.cache_ttl) === '1h') hour = total
    else five = total
  }
  if (five === 0 && hour === 0) {
    return { cache_creation_5m_tokens: null, cache_creation_1h_tokens: null }
  }
  return { cache_creation_5m_tokens: five, cache_creation_1h_tokens: hour }
}

/** Tri-state model mismatch: null = upstream did not declare a model. */
function modelMismatch(requested, upstream) {
  if (!requested || !upstream) return null
  const norm = (m) =>
    String(m)
      .split('/')
      .filter(Boolean)
      .pop()
      .replace(/\[1m\]$/i, '')
      .toLowerCase()
  return norm(requested) === norm(upstream) ? 0 : 1
}

function redactHeaders(headers = {}) {
  const out = {}
  for (const [k, v] of Object.entries(headers)) {
    const key = String(k).toLowerCase()
    if (key === 'authorization' || key === 'x-api-key' || key === 'cookie' || key === 'set-cookie') {
      out[key] = '***REDACTED***'
    } else if (key === 'x-panel-token') {
      out[key] = '***REDACTED***'
    } else {
      out[key] = typeof v === 'string' ? v.slice(0, 512) : v
    }
  }
  return out
}

const SENSITIVE_KEY = /authorization|token|password|secret|cookie|api[_-]?key/i
const SNAP_MAX_STRING = 80
const SNAP_MAX_ARRAY = 24
const SNAP_MAX_DEPTH = 6
const SNAP_MAX_TOTAL = 12000
const IMAGE_DATA_KEYS = new Set(['data', 'base64', 'b64_json'])

function imagePayloadSummary(value) {
  if (typeof value === 'string') return `…[${value.length} bytes]`
  return '[omitted-image]'
}

function redactImagePayloads(value, seen = new WeakSet()) {
  if (!value || typeof value !== 'object') return value
  if (seen.has(value)) return '[Circular]'
  seen.add(value)
  if (Array.isArray(value)) return value.map((item) => redactImagePayloads(item, seen))
  const type = value.type
  const isImage = type === 'image' || type === 'input_image' || type === 'image_url' || value.source || value.image_url
  const out = {}
  for (const [key, nested] of Object.entries(value)) {
    if (IMAGE_DATA_KEYS.has(key) && typeof nested === 'string' && nested.length > 32) {
      out[key] = imagePayloadSummary(nested)
      continue
    }
    if (isImage && key === 'source' && nested && typeof nested === 'object') {
      out[key] = {
        type: nested.type || 'base64',
        media_type: nested.media_type || null,
        url: Boolean(nested.url),
        bytes: typeof nested.data === 'string' ? nested.data.length : 0,
      }
      continue
    }
    if (isImage && key === 'image_url' && nested && typeof nested === 'object') {
      out[key] = { url: Boolean(nested.url), detail: nested.detail || null }
      continue
    }
    out[key] = redactImagePayloads(nested, seen)
  }
  return out
}

export function sanitizeRequestBodySnapshot(value, opts = {}, seen = new WeakSet(), depth = 0) {
  const maxString = opts.maxStringChars ?? SNAP_MAX_STRING
  const maxArray = opts.maxArrayItems ?? SNAP_MAX_ARRAY
  const maxDepth = opts.maxDepth ?? SNAP_MAX_DEPTH
  if (value == null) return value
  if (typeof value === 'string') {
    return value.length > maxString ? value.slice(0, maxString) + `…[${value.length}]` : value
  }
  if (typeof value !== 'object') return value
  if (seen.has(value)) return '[Circular]'
  if (depth >= maxDepth) return '[MaxDepth]'
  seen.add(value)
  if (Array.isArray(value)) {
    const items = value.slice(0, maxArray).map((v) => sanitizeRequestBodySnapshot(v, opts, seen, depth + 1))
    if (value.length > maxArray) items.push(`…[${value.length - maxArray} more]`)
    return items
  }
  const out = {}
  const isImage =
    value.type === 'image' ||
    value.type === 'input_image' ||
    value.type === 'image_url' ||
    value.source ||
    value.image_url
  for (const [k, v] of Object.entries(value)) {
    if (SENSITIVE_KEY.test(k)) {
      out[k] = '[REDACTED]'
      continue
    }
    if (k === 'encrypted_content') {
      out[k] = `…[${String(v || '').length} chars]`
      continue
    }
    if (k === 'tools' && Array.isArray(v)) {
      out[k] = v
        .slice(0, maxArray)
        .map((t) => ({ type: t?.type || 'function', name: t?.name || t?.function?.name || null }))
      continue
    }
    if (IMAGE_DATA_KEYS.has(k) && typeof v === 'string' && v.length > 32) {
      out[k] = imagePayloadSummary(v)
      continue
    }
    if (isImage && k === 'source' && v && typeof v === 'object') {
      out[k] = {
        type: v.type || 'base64',
        media_type: v.media_type || null,
        url: Boolean(v.url),
        bytes: typeof v.data === 'string' ? v.data.length : 0,
      }
      continue
    }
    out[k] = sanitizeRequestBodySnapshot(v, opts, seen, depth + 1)
  }
  let raw = ''
  try {
    raw = JSON.stringify(out)
  } catch {
    return out
  }
  if (raw.length > (opts.maxTotalChars ?? SNAP_MAX_TOTAL)) {
    return { _truncated: true, _chars: raw.length, preview: raw.slice(0, opts.maxTotalChars ?? SNAP_MAX_TOTAL) }
  }
  return out
}

function clampBody(obj, maxChars = 200_000) {
  let raw
  try {
    raw = redactSecrets(redactImagePayloads(obj))
  } catch {
    raw = '{"error":"redact_failed"}'
  }
  if (raw.length <= maxChars) {
    try {
      return JSON.parse(raw)
    } catch {
      return { _raw: raw.slice(0, maxChars) }
    }
  }
  return { _truncated: true, _chars: raw.length, preview: raw.slice(0, maxChars) }
}

/**
 * Client-facing response as stored in a debug record. The body stays raw text
 * (SSE frames or JSON) so the panel can show exactly what went out; only
 * credential-shaped substrings are masked.
 */
function debugResponse(captured) {
  let body = captured.body || ''
  try {
    body = JSON.parse(redactSecrets(body))
  } catch {}
  return {
    status: captured.status ?? null,
    headers: captured.headers || null,
    body,
    bytes: captured.bytes || 0,
    truncated: !!captured.truncated,
  }
}

export function resolveLogMode(envMode, req) {
  const hdr = String(req?.headers?.['x-kin-log'] || '')
    .trim()
    .toLowerCase()
  if (MODES.has(hdr)) return hdr
  if (String(req?.headers?.['x-kin-debug'] || '') === '1') return 'debug'
  const base = String(envMode || 'normal')
    .trim()
    .toLowerCase()
  return MODES.has(base) ? base : 'normal'
}

export function newRequestId(req) {
  const incoming = String(req?.headers?.['x-request-id'] || '').trim()
  if (incoming && incoming.length <= 128) return incoming
  return crypto.randomUUID()
}

export class RequestLogStore {
  constructor({
    dataDir,
    db,
    mode = process.env.KIN_REQUEST_LOG_MODE || 'normal',
    maxDebugBodyChars = Number(process.env.KIN_REQUEST_LOG_DEBUG_CHARS || DEFAULT_DEBUG_BODY_CHARS),
    retainDays = Number(process.env.KIN_REQUEST_LOG_RETAIN_DAYS || DEFAULT_RETAIN_DAYS),
    debugRetainDays = Number(process.env.KIN_REQUEST_LOG_DEBUG_RETAIN_DAYS || DEFAULT_DEBUG_RETAIN_DAYS),
    maxMb = Number(process.env.KIN_REQUEST_LOG_MAX_MB || DEFAULT_MAX_MB),
    jsonlMirror = process.env.KIN_REQUEST_LOG_JSONL === '1',
    bufferedWrites = false,
    writeBatchSize = Number(process.env.KIN_DB_WRITE_BATCH_SIZE || 256),
    writeQueueSize = Number(process.env.KIN_DB_WRITE_QUEUE_SIZE || 16_384),
    writeFlushIntervalMs = Number(process.env.KIN_DB_WRITE_FLUSH_MS || 10),
  } = {}) {
    this.dataDir = dataDir || path.join(process.cwd(), 'data')
    this.db = resolveStoreDb({ db, dataDir: this.dataDir })
    this.repo = new UsageLogsRepo(this.db)
    this.root = path.join(this.dataDir, 'request-logs')
    this.mode = MODES.has(String(mode).toLowerCase()) ? String(mode).toLowerCase() : 'normal'
    this.maxDebugBodyChars = Number.isFinite(maxDebugBodyChars) ? maxDebugBodyChars : DEFAULT_DEBUG_BODY_CHARS
    this.retainDays = Number.isFinite(retainDays) && retainDays > 0 ? retainDays : DEFAULT_RETAIN_DAYS
    this.debugRetainDays =
      Number.isFinite(debugRetainDays) && debugRetainDays > 0 ? debugRetainDays : DEFAULT_DEBUG_RETAIN_DAYS
    if (this.debugRetainDays > this.retainDays) this.debugRetainDays = this.retainDays
    this.maxMb = Number.isFinite(maxMb) && maxMb >= 0 ? maxMb : DEFAULT_MAX_MB
    this.jsonlMirror = !!jsonlMirror
    this.mutedErrorClasses = resolveMutedErrorClasses(null)
    this._mem = [] // recent normal summaries for panel hot path
    this._memMax = 200
    this._cleanupTimer = null
    this._billingCache = new Map()
    this._totalsCache = null
    this._windowStatsCache = new Map()
    this._writeQueue = bufferedWrites
      ? new BufferedWriteQueue({
          batchSize: writeBatchSize,
          maxPending: writeQueueSize,
          flushIntervalMs: writeFlushIntervalMs,
          flush: (entries) => this._persistWriteBatch(entries),
          onError: (error, count) => {
            console.error(
              JSON.stringify({
                event: 'request_log_write_failed',
                count,
                code: error?.code || null,
                message: String(error?.message || error).slice(0, 200),
              }),
            )
          },
        })
      : null
  }

  setConfig({ mode, retainDays, debugRetainDays, maxMb, mutedErrorClasses } = {}) {
    if (mode != null) {
      const m = String(mode).trim().toLowerCase()
      if (MODES.has(m)) this.mode = m
    }
    if (retainDays != null) {
      const n = Number(retainDays)
      if (Number.isFinite(n) && n > 0) this.retainDays = n
    }
    if (debugRetainDays != null) {
      const n = Number(debugRetainDays)
      if (Number.isFinite(n) && n > 0) this.debugRetainDays = n
    }
    if (maxMb != null) {
      const n = Number(maxMb)
      if (Number.isFinite(n) && n >= 0) this.maxMb = n
    }
    if (this.debugRetainDays > this.retainDays) this.debugRetainDays = this.retainDays
    if (mutedErrorClasses !== undefined) {
      this.mutedErrorClasses = resolveMutedErrorClasses(mutedErrorClasses)
    }
    return this.snapshot()
  }

  /** Kept for API compat + post-restore hook (state lives in DB). */
  reload() {
    this.flushPending()
    this._mem = []
    this._billingCache.clear()
    this._totalsCache = null
    this._windowStatsCache.clear()
  }

  /** Re-bind to a fresh DB connection (after backup restore). */
  rebind(db) {
    // Queued writes belong to the currently bound database. Drain them before
    // swapping repositories so a restore cannot replay old rows into the new DB.
    this.flushPending()
    this.db = db
    this.repo = new UsageLogsRepo(db)
    this._mem = []
    this._billingCache.clear()
    this._totalsCache = null
    this._windowStatsCache.clear()
  }

  _persistWriteBatch(entries) {
    withTransaction(this.db, () => {
      for (const entry of entries) {
        this.repo.insertSummaryIfAbsent(entry.summary)
        if (entry.debug) {
          this.repo.insertDebug(entry.summary.request_id || entry.summary.id, entry.summary.ts, entry.debug)
        }
      }
    })
  }

  _persistWrite(entry) {
    if (this._writeQueue) {
      this._writeQueue.enqueue(entry)
      return
    }
    this._persistWriteBatch([entry])
  }

  flushPending() {
    return this._writeQueue?.flush() || 0
  }

  stop() {
    this.stopCleanupScheduler()
    return this._writeQueue?.close() || 0
  }

  start(req, { protocol = null, pathName = null } = {}) {
    const requestId = newRequestId(req)
    try {
      req.headers = req.headers || {}
    } catch {}
    const mode = resolveLogMode(this.mode, req)
    return {
      request_id: requestId,
      mode,
      t0: Date.now(),
      protocol,
      path: pathName || req.url || '',
      method: req.method || 'POST',
      ip: safeIp(req),
      user_agent: String(req.headers?.['user-agent'] || '').slice(0, 512),
      headers: mode === 'debug' ? redactHeaders(req.headers) : null,
      // Outbound bodies carry the gateway's rewrite (persona / overlay) and are
      // readable by the owning tenant; only the operator's configured mode may
      // enable them, never the caller's x-kin-debug / x-kin-log header.
      capture_outbound: this.mode === 'debug',
    }
  }

  /**
   * Debug mode only: mirror what the client actually received (status,
   * headers, body bytes up to maxDebugBodyChars) onto `ctx.response` so
   * finish() can store it next to the inbound/outbound bodies. Normal mode
   * never wraps the response.
   */
  tapResponse(ctx, res) {
    if (!ctx || ctx.mode !== 'debug' || !res || ctx.response) return
    const limit = Math.max(0, this.maxDebugBodyChars)
    const captured = { status: null, headers: null, body: '', bytes: 0, truncated: false }
    ctx.response = captured
    const decoder = new TextDecoder()
    const take = (chunk, encoding) => {
      if (chunk == null || typeof chunk === 'function') return
      const buf =
        typeof chunk === 'string' ? Buffer.from(chunk, typeof encoding === 'string' ? encoding : 'utf8') : chunk
      if (!(buf instanceof Uint8Array)) return
      captured.bytes += buf.byteLength
      if (captured.truncated) return
      const room = limit - captured.body.length
      const text = decoder.decode(buf, { stream: true })
      if (text.length <= room) {
        captured.body += text
        return
      }
      captured.body += text.slice(0, Math.max(0, room))
      captured.truncated = true
    }
    const write = res.write
    const end = res.end
    res.write = function tappedWrite(chunk, encoding, cb) {
      take(chunk, encoding)
      return write.call(this, chunk, encoding, cb)
    }
    res.end = function tappedEnd(chunk, encoding, cb) {
      take(chunk, encoding)
      return end.call(this, chunk, encoding, cb)
    }
    res.once('finish', () => {
      captured.status = res.statusCode || null
      try {
        captured.headers = redactHeaders(res.getHeaders?.() || {})
      } catch {}
    })
  }

  /**
   * Write normal summary always (unless off).
   * Write debug full record when mode===debug.
   */
  finish(ctx, extra = {}) {
    if (!ctx || ctx.mode === 'off') return null
    const duration_ms = Math.max(0, Date.now() - (ctx.t0 || Date.now()))
    const usage = normalizeUsage(extra.usage || {})
    const summary = {
      id: 'log_' + crypto.randomBytes(6).toString('hex'),
      request_id: ctx.request_id,
      ts: new Date().toISOString(),
      log_mode: ctx.mode,
      method: ctx.method,
      path: extra.path || ctx.path,
      protocol: extra.protocol || ctx.protocol || null,
      model: extra.model || null,
      stream: !!extra.stream,
      status: extra.status ?? null,
      duration_ms,
      api_key_kind: extra.api_key_kind || null,
      api_key_id: extra.api_key_id || null,
      vm_id: extra.vm_id || null,
      account_id: extra.account_id || null,
      workspace: extra.workspace || null,
      input_tokens: extra.input_tokens ?? usage.input_tokens ?? null,
      output_tokens: extra.output_tokens ?? usage.output_tokens ?? null,
      error_code: extra.error_code || null,
      error_message: extra.error_message ? String(extra.error_message).slice(0, 300) : null,
      user_agent: ctx.user_agent,
      ip: ctx.ip,
      api_key_presented: extra.api_key_presented ? presentedApiKeyForLog(extra.api_key_presented) : null,
      has_tools: extra.has_tools ?? null,
      via: extra.via || extra.hop_meta?.via || null,
      cache_read_tokens: extra.cache_read_tokens ?? usage.cache_read_tokens ?? usage.cache_read_input_tokens ?? null,
      cache_creation_tokens:
        extra.cache_creation_tokens ?? usage.cache_creation_tokens ?? usage.cache_creation_input_tokens ?? null,
      ...cacheCreationBreakdown(usage),
      requested_model: extra.requested_model || extra.model || null,
      upstream_model: extra.upstream_model || null,
      model_mismatch: modelMismatch(extra.requested_model || extra.model, extra.upstream_model),
      first_token_ms: extra.first_token_ms ?? null,
      stop_reason: extra.stop_reason || usage.stop_reason || null,
      attempt_count: extra.attempt_count ?? null,
      final_state: extra.final_state || null,
      final_account_id: extra.final_account_id || extra.account_id || null,
      session_id: sessionIdForLog(extra.session_id),
      outbound_session_id: sessionIdForLog(extra.outbound_session_id),
      reasoning_effort: REASONING_EFFORTS.has(extra.reasoning_effort) ? extra.reasoning_effort : null,
      // Gate verdict JSON from intercept-gate; the risk audit groups on it.
      intercept: extra.intercept ? String(extra.intercept).slice(0, 400) : null,
      // sub2api ownership + billing columns
      user_id: extra.user_id ?? null,
      vm_owner_user_id: extra.vm_id ? this.repo.vmOwnerOf(String(extra.vm_id)) : null,
      group_id: extra.group_id ?? 1,
      rate_multiplier: Number.isFinite(Number(extra.rate_multiplier)) ? Number(extra.rate_multiplier) : 1,
      ...costColumnsFromUsage(
        {
          ...usage,
          input_tokens: extra.input_tokens ?? usage.input_tokens,
          output_tokens: extra.output_tokens ?? usage.output_tokens,
          cache_read_tokens: extra.cache_read_tokens ?? usage.cache_read_input_tokens ?? usage.cache_read_tokens,
          cache_creation_tokens:
            extra.cache_creation_tokens ?? usage.cache_creation_input_tokens ?? usage.cache_creation_tokens,
          cache_creation_5m_tokens: extra.cache_creation_5m_tokens ?? usage.cache_creation_5m_tokens,
          cache_creation: usage.cache_creation,
        },
        extra.upstream_model || extra.model || extra.requested_model,
      ),
    }
    // sub2api actual_cost: official cost × group rate multiplier (0 is a valid rate)
    if (summary.total_cost != null) {
      summary.actual_cost = summary.total_cost * summary.rate_multiplier
    }
    const classified = classifyRequestError(summary)
    if (classified?.error_class) summary.error_class = classified.error_class

    if (this.jsonlMirror) this._appendJsonl(summary)
    this._mem.push(summary)
    if (this._mem.length > this._memMax) this._mem = this._mem.slice(-this._memMax)

    let debugRec = null
    if (ctx.mode === 'debug') {
      debugRec = {
        ...summary,
        headers: ctx.headers,
        inbound_summary: extra.inbound_summary || summarizeBody(extra.inbound_body),
        request_body_snapshot:
          !extra.classifier && extra.inbound_body != null ? sanitizeRequestBodySnapshot(extra.inbound_body) : null,
        inbound_body:
          !extra.classifier && extra.inbound_body != null
            ? clampBody(extra.inbound_body, this.maxDebugBodyChars)
            : null,
        hop_meta: extra.hop_meta || null,
        via: extra.via || extra.hop_meta?.via || null,
        upstream_status: extra.upstream_status ?? null,
        outbound_summary: extra.outbound_summary || null,
        cache_prefix: extra.cache_prefix || null,
        cache_continuity: extra.cache_continuity || null,
        classifier: extra.classifier || null,
        outbound_headers:
          ctx.capture_outbound && extra.outbound_headers != null ? redactHeaders(extra.outbound_headers) : null,
        outbound_body:
          ctx.capture_outbound && !extra.classifier && extra.outbound_body != null
            ? clampBody(extra.outbound_body, this.maxDebugBodyChars)
            : null,
        response: ctx.response ? debugResponse(ctx.response) : null,
      }
    }
    try {
      this._persistWrite({ summary, debug: debugRec })
    } catch {}
    return summary
  }

  /** Optional legacy JSONL mirror for external log collectors. */
  _appendJsonl(summary) {
    try {
      fs.mkdirSync(this.root, { recursive: true })
      const file = path.join(this.root, `${dayKey()}.jsonl`)
      fs.appendFileSync(file, JSON.stringify(summary) + '\n', { mode: 0o600 })
    } catch {}
  }

  _mutedExclude(opts = {}) {
    if (opts.error_class || opts.include_muted) return null
    if (opts.exclude_error_class != null && String(opts.exclude_error_class).trim() !== '') {
      return opts.exclude_error_class
    }
    return this.mutedErrorClasses.length ? this.mutedErrorClasses.join(',') : null
  }

  _dropMuted(items, exclude) {
    if (!exclude) return items
    const blocked = new Set(
      String(exclude)
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean),
    )
    if (!blocked.size) return items
    return (items || []).filter((row) => !row.error_class || !blocked.has(row.error_class))
  }

  listNormal({ limit = 50, ...filters } = {}) {
    return this.queryNormal({ limit, ...filters }).items
  }

  /** Filtered + paginated query with total (panel). Default hides muted classes. */
  queryNormal(opts = {}) {
    const exclude = this._mutedExclude(opts)
    const result = this.repo.query({ ...opts, exclude_error_class: exclude })
    result.items = this._dropMuted(result.items, exclude)
    return result
  }

  exportRows(opts = {}) {
    const limit = Math.max(1, Math.min(5000, Number(opts.limit) || 2000))
    return this.queryNormal({ ...opts, limit, offset: 0, maxLimit: 5000 })
  }

  /** Aggregated stats for charts. */
  aggregate(opts = {}) {
    return this.repo.aggregate(opts)
  }

  totals() {
    const now = Date.now()
    if (this._totalsCache && now - this._totalsCache.at < TOTALS_CACHE_MS) return this._totalsCache.value
    const value = this.repo.totals()
    this._totalsCache = { at: now, value }
    return value
  }

  hasSuccessfulRequest(opts = {}) {
    return this.repo.hasSuccessfulRequest(opts)
  }

  billingStats(opts = {}) {
    const now = Date.now()
    const windows = Array.isArray(opts.accountWindows)
      ? opts.accountWindows.map((item) => [
          item?.account_id || '',
          item?.vm_id || '',
          item?.reset_5h || '',
          item?.reset_7d || '',
        ])
      : null
    const key = JSON.stringify([windows, opts.now == null ? null : Math.floor(Number(opts.now) / BILLING_CACHE_MS)])
    const cached = this._billingCache.get(key)
    if (cached && now - cached.at < BILLING_CACHE_MS) return cached.value
    const value = this.repo.billingStats(opts)
    this._billingCache.set(key, { at: now, value })
    if (this._billingCache.size > 8) this._billingCache.delete(this._billingCache.keys().next().value)
    return value
  }

  ownerBilling(opts = {}) {
    return this.repo.ownerBilling(opts)
  }

  costByModel(opts = {}) {
    return this.repo.costByModel(opts)
  }

  vmUsageStats(opts = {}) {
    return this.repo.vmUsageStats(opts)
  }

  keyUsageStats(opts = {}) {
    return this.repo.keyUsageStats(opts)
  }

  /** Windowed SLA / QPS / TTFT snapshot for overview + log analysis. */
  windowStats(opts = {}) {
    const now = Date.now()
    const key = JSON.stringify([opts.since || null, opts.until || null, opts.owner_user_id || null])
    const cached = this._windowStatsCache.get(key)
    if (cached && now - cached.at < WINDOW_STATS_CACHE_MS) return cached.value
    const value = this.repo.windowStats(opts)
    this._windowStatsCache.set(key, { at: now, value })
    if (this._windowStatsCache.size > 16) this._windowStatsCache.delete(this._windowStatsCache.keys().next().value)
    return value
  }

  protocolEntryStats(opts = {}) {
    return this.repo.protocolEntryStats(opts)
  }

  listDebug({
    limit = 20,
    exclude_error_class = null,
    include_muted = false,
    error_class = null,
    owner_user_id = null,
  } = {}) {
    const exclude = this._mutedExclude({ exclude_error_class, include_muted, error_class })
    const n = Math.max(1, Math.min(100, Number(limit) || 20))
    const pool = this.repo.listDebug({ limit: exclude ? Math.min(200, n * 15) : n, owner_user_id })
    const items = error_class ? pool.filter((row) => row.error_class === error_class) : this._dropMuted(pool, exclude)
    return items.slice(0, n)
  }

  getDebug(requestId, { owner_user_id = null } = {}) {
    if (!requestId) return null
    const rec = this.repo.getDebug(requestId) || this.repo.getByRequestId(requestId)
    if (!rec) return null
    if (owner_user_id && !this.repo.belongsToOwner(requestId, owner_user_id)) return null
    return rec
  }

  snapshot() {
    return {
      mode: this.mode,
      retain_days: this.retainDays,
      debug_retain_days: this.debugRetainDays,
      max_mb: this.maxMb,
      muted_error_classes: this.mutedErrorClasses.slice(),
      recent_normal: this._mem.length,
      backend: 'sqlite',
      jsonl_mirror: this.jsonlMirror,
      write_queue: this._writeQueue?.snapshot() || null,
      total_rows: (() => {
        try {
          return this.repo.count()
        } catch {
          return null
        }
      })(),
    }
  }

  /** Retention: delete rows older than retain windows, then enforce max_mb. */
  cleanup() {
    this.flushPending()
    try {
      this.repo.cleanup({
        retainDays: this.retainDays,
        debugRetainDays: this.debugRetainDays,
        // SUM(LENGTH(record_json)) walks every overflow page. A multi-GB debug
        // table can block the single Node event loop for seconds even after
        // logging returns to normal. The table cannot grow outside debug mode,
        // so enforce the byte cap only while full records are being written.
        maxBytes: this.mode === 'debug' && this.maxMb > 0 ? this.maxMb * 1024 * 1024 : 0,
      })
    } catch {}
    this._cleanupLegacyFiles(this.retainDays, this.root)
    this._cleanupLegacyFiles(this.debugRetainDays, path.join(this.root, 'debug'))
  }

  startCleanupScheduler({ intervalMs = CLEANUP_INTERVAL_MS } = {}) {
    this.stopCleanupScheduler()
    const ms = Math.max(60_000, Number(intervalMs) || CLEANUP_INTERVAL_MS)
    const run = () => {
      try {
        this.cleanup()
      } catch {}
    }
    this._cleanupTimer = setInterval(run, ms)
    if (typeof this._cleanupTimer.unref === 'function') this._cleanupTimer.unref()
  }

  stopCleanupScheduler() {
    clearInterval(this._cleanupTimer)
    this._cleanupTimer = null
  }

  _cleanupLegacyFiles(days, dir) {
    const cutoff = Date.now() - days * 86400_000
    try {
      if (!fs.existsSync(dir)) return
      for (const name of fs.readdirSync(dir)) {
        if (!/^\d{4}-\d{2}-\d{2}/.test(name)) continue
        const dayMs = Date.parse(name.slice(0, 10) + 'T00:00:00Z')
        if (!Number.isFinite(dayMs) || dayMs >= cutoff) continue
        fs.rmSync(path.join(dir, name), { recursive: true, force: true })
      }
    } catch {}
  }
}

const EXPORT_COLUMNS = [
  'ts',
  'request_id',
  'method',
  'path',
  'protocol',
  'status',
  'error_class',
  'error_code',
  'error_message',
  'error_label',
  'model',
  'vm_id',
  'ip',
  'api_key_presented',
  'duration_ms',
  'first_token_ms',
  'input_tokens',
  'output_tokens',
  'total_cost',
  'api_key_id',
  'api_key_kind',
]

function csvCell(v) {
  if (v == null || v === '') return ''
  const s = String(v)
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s
}

export function logsToCsv(items = []) {
  const header = EXPORT_COLUMNS.join(',')
  const rows = items.map((row) => EXPORT_COLUMNS.map((k) => csvCell(row[k])).join(','))
  return [header, ...rows].join('\n') + (items.length ? '\n' : '')
}

export function logsToJsonl(items = []) {
  return (
    items
      .map((row) => {
        const out = {}
        for (const k of EXPORT_COLUMNS) out[k] = row[k] ?? null
        return JSON.stringify(out)
      })
      .join('\n') + (items.length ? '\n' : '')
  )
}

export { summarizeBody, redactHeaders, clampBody, DEFAULT_RETAIN_DAYS, DEFAULT_DEBUG_RETAIN_DAYS, DEFAULT_MAX_MB }
