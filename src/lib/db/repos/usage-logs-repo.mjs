/**
 * usage_logs + request_log_debug repository — sub2api UsageLog shape.
 * Normal summaries (always, unless mode=off) and full redacted debug records.
 *
 * DB columns follow sub2api naming (created_at, ip_address, user_id/group_id,
 * actual_cost, rate_multiplier). Records returned to callers keep the
 * historical `ts` / `ip` aliases so the panel API shape is unchanged; writers
 * may pass either name.
 */

import { getDb } from '../database.mjs'
import { collectErrors, enrichLogRow, excludeErrorClassSql, ingressAuthSql } from '../../admin/error-class.mjs'
import {
  calculateCost,
  emptyCostBucket,
  shanghaiDayStartIso,
  sumCostBuckets as sumCostColumns,
  UNPRICED_MODEL,
} from '../../admin/pricing.mjs'
import { cacheHitStats } from '../../admin/cache-metrics.mjs'
import { extraWindowSince, WINDOW_5H_MS, WINDOW_7D_MS } from '../../pool/quota-window.mjs'
import {
  ERROR_PRED,
  SUCCESS_PRED,
  SLA_ERROR_PRED,
  SLA_SUCCESS_PRED,
  ownerPred,
  PROMPT_TOKENS_SQL,
} from './usage-log-preds.mjs'
import { collapseBlockKeywords, foldInterceptRows } from '../../protocol/intercept-stats.mjs'

// Preserve per-row prompt denominators when aligning account quota windows.
// Do not re-apply a single Anthropic formula on the mixed SUM (homepage/usage
// billing). Database-page PromptCacheMetricsPanel still uses input+read+write.
function sumCostBuckets(rows) {
  return {
    ...sumCostColumns(rows),
    prompt_tokens: rows.reduce((sum, row) => sum + (Number(row.prompt_tokens) || 0), 0),
  }
}

const SUMMARY_COLUMNS = [
  'id',
  'request_id',
  'created_at',
  'log_mode',
  'method',
  'path',
  'protocol',
  'model',
  'stream',
  'status',
  'duration_ms',
  'api_key_kind',
  'api_key_id',
  'vm_id',
  'account_id',
  'workspace',
  'input_tokens',
  'output_tokens',
  'error_code',
  'error_message',
  'user_agent',
  'ip_address',
  'api_key_presented',
  'has_tools',
  'via',
  'cache_read_tokens',
  'cache_creation_tokens',
  'cache_creation_5m_tokens',
  'cache_creation_1h_tokens',
  'requested_model',
  'upstream_model',
  'model_mismatch',
  'first_token_ms',
  'stop_reason',
  'attempt_count',
  'final_state',
  'final_account_id',
  'input_cost',
  'output_cost',
  'cache_read_cost',
  'cache_creation_cost',
  'total_cost',
  'pricing_model',
  'user_id',
  'group_id',
  'actual_cost',
  'rate_multiplier',
  'service_tier',
  'speed',
  'long_context',
  'session_id',
  'outbound_session_id',
  'reasoning_effort',
  'intercept',
  'vm_owner_user_id',
]

function toRow(rec) {
  return SUMMARY_COLUMNS.map((c) => {
    if (c === 'created_at') return rec.created_at ?? rec.ts ?? null
    if (c === 'ip_address') return rec.ip_address ?? rec.ip ?? null
    if (c === 'stream') return rec.stream ? 1 : 0
    if (c === 'has_tools') return rec.has_tools == null ? null : rec.has_tools ? 1 : 0
    if (
      c === 'input_tokens' ||
      c === 'output_tokens' ||
      c === 'cache_read_tokens' ||
      c === 'cache_creation_tokens' ||
      c === 'cache_creation_5m_tokens' ||
      c === 'cache_creation_1h_tokens'
    )
      return Number(rec[c]) || 0
    return rec[c] ?? null
  })
}

function fromRow(row) {
  if (!row) return null
  return {
    ...row,
    ts: row.created_at,
    ip: row.ip_address,
    stream: !!row.stream,
    has_tools: row.has_tools == null ? null : !!row.has_tools,
  }
}

function timeCond(since, until) {
  const where = []
  const params = []
  if (since) {
    where.push('created_at >= ?')
    params.push(new Date(since).toISOString())
  }
  if (until) {
    where.push('created_at <= ?')
    params.push(new Date(until).toISOString())
  }
  return { cond: where.length ? `WHERE ${where.join(' AND ')}` : '', params }
}

function accountEventKey(row) {
  return `${row?.account_id || ''}\0${row?.vm_id || ''}`
}

function resolveAccountWindow(windows, row) {
  const list = Array.isArray(windows) ? windows : []
  return (
    list.find((w) => w.account_id === row.account_id && (w.vm_id || null) === (row.vm_id || null)) ||
    list.find((w) => w.account_id && w.account_id === row.account_id) ||
    list.find((w) => w.vm_id && w.vm_id === row.vm_id) ||
    null
  )
}

function groupCostEvents(rows = []) {
  const map = new Map()
  for (const row of rows) {
    const key = accountEventKey(row)
    const list = map.get(key) || []
    list.push(row)
    map.set(key, list)
  }
  return map
}

function bucketEventsSince(rows = [], sinceMs) {
  const since = Number(sinceMs)
  const picked = rows.filter((row) => {
    const at = Date.parse(row.created_at)
    return Number.isFinite(at) && (!Number.isFinite(since) || at >= since)
  })
  if (!picked.length) return emptyCostBucket()
  return sumCostBuckets(
    picked.map((row) => ({
      requests: 1,
      success: Number(row.success || 0),
      errors: Number(row.errors || 0),
      input_tokens: Number(row.input_tokens || 0),
      output_tokens: Number(row.output_tokens || 0),
      cache_read_tokens: Number(row.cache_read_tokens || 0),
      cache_creation_tokens: Number(row.cache_creation_tokens || 0),
      prompt_tokens: Number(row.prompt_tokens || 0),
      input_cost: Number(row.input_cost || 0),
      output_cost: Number(row.output_cost || 0),
      cache_read_cost: Number(row.cache_read_cost || 0),
      cache_creation_cost: Number(row.cache_creation_cost || 0),
      total_cost: Number(row.total_cost || 0),
    })),
  )
}

function cacheHitStatsFromPrompt(row = {}) {
  const base = cacheHitStats(row)
  if (row.prompt_tokens == null) return base
  const prompt = Number(row.prompt_tokens) || 0
  const read = Number(row.cache_read_tokens) || 0
  return {
    ...base,
    prompt_tokens: prompt,
    cache_hit_rate: prompt > 0 ? Math.min(1, read / prompt) : null,
  }
}

function costBucketFromPrefixedRow(row, prefix) {
  const out = emptyCostBucket()
  for (const key of Object.keys(out)) out[key] = Number(row?.[`${prefix}_${key}`] || 0)
  return out
}

function filterCond({ since = null, until = null, vmId = null, accountId = null, apiKeyId = null } = {}) {
  const { cond, params } = timeCond(since, until)
  const parts = cond ? [cond.replace(/^WHERE /, '')] : []
  if (vmId) {
    parts.push('vm_id = ?')
    params.push(vmId)
  }
  if (apiKeyId) {
    parts.push('api_key_id = ?')
    params.push(apiKeyId)
  }
  if (accountId) {
    parts.push('(account_id = ? OR final_account_id = ?)')
    params.push(accountId, accountId)
  }
  return { cond: parts.length ? `WHERE ${parts.join(' AND ')}` : '', params }
}

/** Same percentile pick as sub2api ops cards. */
export function percentile(sorted, p) {
  if (!sorted.length) return null
  const i = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * p) - 1))
  return sorted[i]
}

export function summarizeLatency(values) {
  const s = (values || [])
    .map((v) => Number(v))
    .filter((v) => Number.isFinite(v))
    .sort((a, b) => a - b)
  if (!s.length) {
    return { samples: 0, p50_ms: null, p90_ms: null, p95_ms: null, p99_ms: null, avg_ms: null, max_ms: null }
  }
  return {
    samples: s.length,
    p50_ms: percentile(s, 0.5),
    p90_ms: percentile(s, 0.9),
    p95_ms: percentile(s, 0.95),
    p99_ms: percentile(s, 0.99),
    avg_ms: Math.round(s.reduce((a, b) => a + b, 0) / s.length),
    max_ms: s[s.length - 1],
  }
}

export class UsageLogsRepo {
  constructor(db = getDb()) {
    this.db = db
    this._insert = db.prepare(`
      INSERT INTO usage_logs (${SUMMARY_COLUMNS.join(', ')})
      VALUES (${SUMMARY_COLUMNS.map(() => '?').join(', ')})
    `)
    this._insertIfAbsent = db.prepare(`
      INSERT OR IGNORE INTO usage_logs (${SUMMARY_COLUMNS.join(', ')})
      VALUES (${SUMMARY_COLUMNS.map(() => '?').join(', ')})
    `)
    this._insertDebug = db.prepare(`
      INSERT INTO request_log_debug (request_id, ts, record_json) VALUES (?, ?, ?)
      ON CONFLICT(request_id) DO UPDATE SET ts = excluded.ts, record_json = excluded.record_json
    `)
    this._insertDebugIfAbsent = db.prepare(`
      INSERT OR IGNORE INTO request_log_debug (request_id, ts, record_json) VALUES (?, ?, ?)
    `)
    this._getDebug = db.prepare('SELECT record_json FROM request_log_debug WHERE request_id = ?')
    this._vmOwner = db.prepare('SELECT owner_user_id FROM vms WHERE id = ?')
    this._listDebug = db.prepare('SELECT record_json FROM request_log_debug ORDER BY ts DESC LIMIT ?')
    this._getByRequestId = db.prepare('SELECT * FROM usage_logs WHERE request_id = ?')
    this._debugBytes = db.prepare('SELECT COALESCE(SUM(LENGTH(record_json)), 0) AS n FROM request_log_debug')
    this._debugCount = db.prepare('SELECT COUNT(*) AS n FROM request_log_debug')
    this._deleteOldUsage = db.prepare('DELETE FROM usage_logs WHERE created_at < ?')
    this._deleteOldDebug = db.prepare('DELETE FROM request_log_debug WHERE ts < ?')
    this._deleteOldestDebug = db.prepare(`
      DELETE FROM request_log_debug WHERE request_id IN (
        SELECT request_id FROM request_log_debug ORDER BY ts ASC LIMIT ?
      )
    `)
  }

  /** Cheap EXISTS: ≥1 2xx row visible to the caller (same ownerPred as request-log stats). */
  hasSuccessfulRequest({ owner_user_id = null } = {}) {
    const own = ownerPred(owner_user_id)
    const where = own.sql ? `AND ${own.sql}` : ''
    const row = this.db
      .prepare(`SELECT 1 AS ok FROM usage_logs WHERE status >= 200 AND status < 300 ${where} LIMIT 1`)
      .get(...own.params)
    return !!row
  }

  belongsToOwner(requestId, ownerUserId) {
    if (!requestId) return false
    if (!ownerUserId) return true
    const own = ownerPred(ownerUserId)
    const row = this.db
      .prepare(`SELECT 1 AS ok FROM usage_logs WHERE request_id = ? AND ${own.sql} LIMIT 1`)
      .get(requestId, ...own.params)
    return !!row
  }

  getByRequestId(requestId) {
    if (!requestId) return null
    const row = this._getByRequestId.get(requestId)
    return row ? enrichLogRow(fromRow(row)) : null
  }

  insertSummary(rec) {
    this._insert.run(...toRow(rec))
    return rec
  }

  vmOwnerOf(vmId) {
    if (!vmId) return null
    return this._vmOwner.get(String(vmId))?.owner_user_id || null
  }

  /** INSERT OR IGNORE (legacy import path). @returns true when inserted */
  insertSummaryIfAbsent(rec) {
    return this._insertIfAbsent.run(...toRow(rec)).changes > 0
  }

  insertDebug(requestId, ts, record) {
    this._insertDebug.run(requestId, ts || new Date().toISOString(), JSON.stringify(record))
  }

  insertDebugIfAbsent(requestId, ts, record) {
    return this._insertDebugIfAbsent.run(requestId, ts || new Date().toISOString(), JSON.stringify(record)).changes > 0
  }

  getDebug(requestId) {
    const row = this._getDebug.get(requestId)
    if (!row) return null
    try {
      return enrichLogRow(JSON.parse(row.record_json))
    } catch {
      return null
    }
  }

  listDebug({ limit = 20, owner_user_id = null } = {}) {
    const n = Math.max(1, Math.min(100, Number(limit) || 20))
    const own = ownerPred(owner_user_id)
    if (!own.sql) {
      return this._listDebug
        .all(n)
        .map((r) => {
          try {
            return enrichLogRow(JSON.parse(r.record_json))
          } catch {
            return null
          }
        })
        .filter(Boolean)
    }
    return this.db
      .prepare(`
      SELECT d.record_json
      FROM request_log_debug d
      JOIN usage_logs u ON u.request_id = d.request_id
      WHERE ${own.sql}
      ORDER BY d.ts DESC
      LIMIT ?
    `)
      .all(...own.params, n)
      .map((r) => {
        try {
          return enrichLogRow(JSON.parse(r.record_json))
        } catch {
          return null
        }
      })
      .filter(Boolean)
  }

  /**
   * Filtered, paginated query over summaries (newest first).
   * @returns {{ items: object[], total: number }}
   */
  query({
    limit = 50,
    offset = 0,
    api_key_id = null,
    vm_id = null,
    account_id = null,
    model = null,
    status = null,
    protocol = null,
    since = null,
    until = null,
    q = null,
    error_class = null,
    exclude_error_class = null,
    maxLimit = 500,
    owner_user_id = null,
  } = {}) {
    const where = []
    const params = []
    const own = ownerPred(owner_user_id)
    if (own.sql) {
      where.push(own.sql)
      params.push(...own.params)
    }
    if (api_key_id) {
      where.push('api_key_id = ?')
      params.push(api_key_id)
    }
    if (vm_id) {
      where.push('vm_id = ?')
      params.push(vm_id)
    }
    if (account_id) {
      where.push('account_id = ?')
      params.push(account_id)
    }
    if (model) {
      where.push('model = ?')
      params.push(model)
    }
    if (protocol) {
      where.push('protocol = ?')
      params.push(protocol)
    }
    if (error_class && (status == null || status === '')) status = 'error'
    if (status != null && status !== '') {
      if (String(status) === 'error') where.push(ERROR_PRED)
      else if (String(status) === 'ok') where.push('(status < 400 AND status IS NOT NULL)')
      else {
        where.push('status = ?')
        params.push(Number(status))
      }
    }
    if (!error_class && exclude_error_class) {
      const excludeSql = excludeErrorClassSql(exclude_error_class)
      if (excludeSql) where.push(excludeSql)
    }
    if (since) {
      where.push('created_at >= ?')
      params.push(new Date(since).toISOString())
    }
    if (until) {
      where.push('created_at <= ?')
      params.push(new Date(until).toISOString())
    }
    if (q) {
      where.push('(path LIKE ? OR error_code LIKE ? OR error_message LIKE ? OR request_id LIKE ?)')
      const like = `%${String(q).slice(0, 100)}%`
      params.push(like, like, like, like)
    }
    const cond = where.length ? `WHERE ${where.join(' AND ')}` : ''
    const cap = Math.max(1, Math.min(5000, Number(maxLimit) || 500))
    const n = Math.max(1, Math.min(cap, Number(limit) || 50))
    const off = Math.max(0, Number(offset) || 0)
    if (error_class) {
      const scan = Math.min(5000, Math.max(n + off, 2000))
      const rows = this.db
        .prepare(`SELECT * FROM usage_logs ${cond} ORDER BY created_at DESC, id DESC LIMIT ?`)
        .all(...params, scan)
        .map(fromRow)
        .map(enrichLogRow)
        .filter((row) => row.error_class === error_class)
      return { items: rows.slice(off, off + n), total: rows.length }
    }
    const total = this.db.prepare(`SELECT COUNT(*) c FROM usage_logs ${cond}`).get(...params).c
    const items = this.db
      .prepare(`SELECT * FROM usage_logs ${cond} ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?`)
      .all(...params, n, off)
      .map(fromRow)
      .map(enrichLogRow)
    return { items, total }
  }

  /**
   * Aggregate stats bucketed by day or hour (dashboard/usage charts).
   * @returns {Array<{bucket, requests, errors, input_tokens, output_tokens, avg_duration_ms}>}
   */
  aggregate({ since = null, until = null, bucket = 'day', owner_user_id = null } = {}) {
    const fmt = bucket === 'hour' ? '%Y-%m-%dT%H:00' : '%Y-%m-%d'
    const { cond, params } = timeCond(since, until)
    const own = ownerPred(owner_user_id)
    let where = cond
    const allParams = [...params]
    if (own.sql) {
      where = where ? `${where} AND ${own.sql}` : `WHERE ${own.sql}`
      allParams.push(...own.params)
    }
    return this.db
      .prepare(`
      SELECT strftime('${fmt}', created_at) AS bucket,
             COUNT(*) AS requests,
             SUM(CASE WHEN ${ERROR_PRED} THEN 1 ELSE 0 END) AS errors,
             COALESCE(SUM(input_tokens), 0) AS input_tokens,
             COALESCE(SUM(output_tokens), 0) AS output_tokens,
             COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
             COALESCE(SUM(cache_creation_tokens), 0) AS cache_creation_tokens,
             COALESCE(SUM(${PROMPT_TOKENS_SQL}), 0) AS prompt_tokens,
             COALESCE(SUM(total_cost), 0) AS total_cost,
             CAST(AVG(duration_ms) AS INTEGER) AS avg_duration_ms,
             CAST(AVG(first_token_ms) AS INTEGER) AS avg_first_token_ms
      FROM usage_logs ${where}
      GROUP BY bucket ORDER BY bucket
    `)
      .all(...allParams)
  }

  /** Grand totals (dashboard db_totals). */
  totals() {
    const row = this.db
      .prepare(`
      SELECT COUNT(*) AS requests,
             SUM(CASE WHEN ${ERROR_PRED} THEN 1 ELSE 0 END) AS errors,
             COALESCE(SUM(input_tokens), 0) AS input_tokens,
             COALESCE(SUM(output_tokens), 0) AS output_tokens,
             COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
             COALESCE(SUM(cache_creation_tokens), 0) AS cache_creation_tokens,
             COALESCE(SUM(${PROMPT_TOKENS_SQL}), 0) AS prompt_tokens,
             COALESCE(SUM(input_cost), 0) AS input_cost,
             COALESCE(SUM(output_cost), 0) AS output_cost,
             COALESCE(SUM(cache_read_cost), 0) AS cache_read_cost,
             COALESCE(SUM(cache_creation_cost), 0) AS cache_creation_cost,
             COALESCE(SUM(total_cost), 0) AS total_cost
      FROM usage_logs
    `)
      .get()
    return { ...row, ...cacheHitStatsFromPrompt(row) }
  }

  /**
   * Fill official cost on rows that predate the billing columns, re-pricing with the
   * stored service_tier / speed. Rows the pricer cannot price are marked 'unpriced'
   * (cost stays NULL) so they are never guessed at standard rates. Idempotent.
   */
  backfillMissingCosts({ limit = 4000 } = {}) {
    const rows = this.db
      .prepare(`
      SELECT id, model, upstream_model, requested_model,
             input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens,
             cache_creation_5m_tokens, cache_creation_1h_tokens, rate_multiplier,
             service_tier, speed
      FROM usage_logs
      WHERE total_cost IS NULL AND pricing_model IS NULL
      LIMIT ?
    `)
      .all(Math.max(1, Math.min(20000, Number(limit) || 4000)))
    if (!rows.length) return 0
    const upd = this.db.prepare(`
      UPDATE usage_logs
         SET input_cost = ?, output_cost = ?, cache_read_cost = ?,
             cache_creation_cost = ?, total_cost = ?, actual_cost = ?, pricing_model = ?,
             long_context = ?
       WHERE id = ?
    `)
    this.db.exec('BEGIN')
    try {
      for (const r of rows) {
        const model = r.upstream_model || r.model || r.requested_model
        const c = calculateCost(r, model)
        if (!c.known) {
          upd.run(null, null, null, null, null, null, UNPRICED_MODEL, null, r.id)
          continue
        }
        const parsedRate = r.rate_multiplier == null ? 1 : Number(r.rate_multiplier)
        const rate = Number.isFinite(parsedRate) ? parsedRate : 1
        upd.run(
          c.input_cost,
          c.output_cost,
          c.cache_read_cost,
          c.cache_creation_cost,
          c.total_cost,
          c.total_cost * rate,
          c.pricing_key,
          c.long_context ? 1 : 0,
          r.id,
        )
      }
      this.db.exec('COMMIT')
    } catch (e) {
      try {
        this.db.exec('ROLLBACK')
      } catch {}
      throw e
    }
    return rows.length
  }

  _costSelect(cond, params) {
    const row = this.db
      .prepare(`
      SELECT
        COUNT(*) AS requests,
        COALESCE(SUM(input_tokens), 0) AS input_tokens,
        COALESCE(SUM(output_tokens), 0) AS output_tokens,
        COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
        COALESCE(SUM(cache_creation_tokens), 0) AS cache_creation_tokens,
        COALESCE(SUM(${PROMPT_TOKENS_SQL}), 0) AS prompt_tokens,
        COALESCE(SUM(input_cost), 0) AS input_cost,
        COALESCE(SUM(output_cost), 0) AS output_cost,
        COALESCE(SUM(cache_read_cost), 0) AS cache_read_cost,
        COALESCE(SUM(cache_creation_cost), 0) AS cache_creation_cost,
        COALESCE(SUM(total_cost), 0) AS total_cost
      FROM usage_logs ${cond}
    `)
      .get(...params)
    const out = {
      requests: Number(row?.requests || 0),
      input_tokens: Number(row?.input_tokens || 0),
      output_tokens: Number(row?.output_tokens || 0),
      cache_read_tokens: Number(row?.cache_read_tokens || 0),
      cache_creation_tokens: Number(row?.cache_creation_tokens || 0),
      prompt_tokens: Number(row?.prompt_tokens || 0),
      input_cost: Number(row?.input_cost || 0),
      output_cost: Number(row?.output_cost || 0),
      cache_read_cost: Number(row?.cache_read_cost || 0),
      cache_creation_cost: Number(row?.cache_creation_cost || 0),
      total_cost: Number(row?.total_cost || 0),
    }
    return { ...out, ...cacheHitStatsFromPrompt(out) }
  }

  costByAccount({ since = null, until = null } = {}) {
    const { cond, params } = timeCond(since, until)
    const rows = this.db
      .prepare(`
      SELECT
        COALESCE(NULLIF(final_account_id, ''), NULLIF(account_id, ''), vm_id, '—') AS account_id,
        vm_id,
        COUNT(*) AS requests,
        SUM(CASE WHEN ${SLA_SUCCESS_PRED} THEN 1 ELSE 0 END) AS success,
        SUM(CASE WHEN ${SLA_ERROR_PRED} THEN 1 ELSE 0 END) AS errors,
        COALESCE(SUM(input_tokens), 0) AS input_tokens,
        COALESCE(SUM(output_tokens), 0) AS output_tokens,
        COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
        COALESCE(SUM(cache_creation_tokens), 0) AS cache_creation_tokens,
        COALESCE(SUM(${PROMPT_TOKENS_SQL}), 0) AS prompt_tokens,
        COALESCE(SUM(input_cost), 0) AS input_cost,
        COALESCE(SUM(output_cost), 0) AS output_cost,
        COALESCE(SUM(cache_read_cost), 0) AS cache_read_cost,
        COALESCE(SUM(cache_creation_cost), 0) AS cache_creation_cost,
        COALESCE(SUM(total_cost), 0) AS total_cost
      FROM usage_logs ${cond}
      GROUP BY 1, vm_id
      ORDER BY total_cost DESC
    `)
      .all(...params)
    return rows.map((r) => ({
      account_id: r.account_id,
      vm_id: r.vm_id || null,
      requests: Number(r.requests || 0),
      success: Number(r.success || 0),
      errors: Number(r.errors || 0),
      input_tokens: Number(r.input_tokens || 0),
      output_tokens: Number(r.output_tokens || 0),
      cache_read_tokens: Number(r.cache_read_tokens || 0),
      cache_creation_tokens: Number(r.cache_creation_tokens || 0),
      prompt_tokens: Number(r.prompt_tokens || 0),
      input_cost: Number(r.input_cost || 0),
      output_cost: Number(r.output_cost || 0),
      cache_read_cost: Number(r.cache_read_cost || 0),
      cache_creation_cost: Number(r.cache_creation_cost || 0),
      total_cost: Number(r.total_cost || 0),
    }))
  }

  /**
   * Official cost grouped by upstream model and billing band for one credential / slot.
   * service_tier is folded to fast / flex / other-raw (standard aliases → NULL);
   * speed is 'fast' or NULL. unpriced_requests counts rows left without an estimate.
   */
  costByModel({ vmId = null, accountId = null, since = null, until = null } = {}) {
    const { cond, params } = filterCond({ since, until, vmId, accountId })
    const rows = this.db
      .prepare(`
      SELECT COALESCE(NULLIF(upstream_model, ''), NULLIF(model, ''), NULLIF(requested_model, ''), '—') AS model,
             CASE
               WHEN service_tier IS NULL OR service_tier IN ('', 'default', 'auto', 'standard') THEN NULL
               WHEN service_tier IN ('priority', 'fast') THEN 'fast'
               ELSE service_tier
             END AS service_tier,
             CASE WHEN speed = 'fast' THEN 'fast' ELSE NULL END AS speed,
             COALESCE(long_context, 0) AS long_context,
             COUNT(*) AS requests,
             SUM(CASE WHEN pricing_model = '${UNPRICED_MODEL}' THEN 1 ELSE 0 END) AS unpriced_requests,
             COALESCE(SUM(input_tokens), 0) AS input_tokens,
             COALESCE(SUM(output_tokens), 0) AS output_tokens,
             COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
             COALESCE(SUM(cache_creation_tokens), 0) AS cache_creation_tokens,
             COALESCE(SUM(${PROMPT_TOKENS_SQL}), 0) AS prompt_tokens,
             COALESCE(SUM(input_cost), 0) AS input_cost,
             COALESCE(SUM(output_cost), 0) AS output_cost,
             COALESCE(SUM(cache_read_cost), 0) AS cache_read_cost,
             COALESCE(SUM(cache_creation_cost), 0) AS cache_creation_cost,
             COALESCE(SUM(total_cost), 0) AS total_cost
      FROM usage_logs ${cond}
      GROUP BY 1, 2, 3, 4
      ORDER BY total_cost DESC, requests DESC
      LIMIT 24
    `)
      .all(...params)
    return rows.map((r) => ({
      model: r.model,
      service_tier: r.service_tier || null,
      speed: r.speed || null,
      long_context: Number(r.long_context || 0),
      requests: Number(r.requests || 0),
      unpriced_requests: Number(r.unpriced_requests || 0),
      input_tokens: Number(r.input_tokens || 0),
      output_tokens: Number(r.output_tokens || 0),
      cache_read_tokens: Number(r.cache_read_tokens || 0),
      cache_creation_tokens: Number(r.cache_creation_tokens || 0),
      prompt_tokens: Number(r.prompt_tokens || 0),
      input_cost: Number(r.input_cost || 0),
      output_cost: Number(r.output_cost || 0),
      cache_read_cost: Number(r.cache_read_cost || 0),
      cache_creation_cost: Number(r.cache_creation_cost || 0),
      total_cost: Number(r.total_cost || 0),
    }))
  }

  /**
   * Per-slot usage over the last `days` Shanghai calendar days (stats dialog).
   * Buckets are Shanghai days so the last bucket lines up with billingStats().today.
   * `models` / `endpoints` are ranked by request count and capped; `endpoints`
   * is the inbound request path (usage_logs keeps no separate upstream endpoint).
   */
  vmUsageStats({ vmId, days = 30 } = {}) {
    return this._usageWindow({
      vmId,
      days,
      rankExpr: "COALESCE(NULLIF(path, ''), '—')",
      rankKey: 'endpoints',
      rankLimit: 8,
    })
  }

  /** Same window as vmUsageStats, isolated to one managed key. Third rank is the VM. */
  keyUsageStats({ apiKeyId, days = 30 } = {}) {
    return this._usageWindow({
      apiKeyId,
      days,
      rankExpr: "COALESCE(NULLIF(vm_id, ''), '—')",
      rankKey: 'vms',
      rankLimit: 12,
    })
  }

  _usageWindow({ vmId = null, apiKeyId = null, days = 30, rankExpr, rankKey, rankLimit = 8 } = {}) {
    const empty = { days: 0, since: null, history: [], models: [], [rankKey]: [] }
    if (!vmId && !apiKeyId) return empty
    const span = Math.max(1, Math.min(90, Math.floor(Number(days)) || 30))
    const since = new Date(Date.parse(shanghaiDayStartIso()) - (span - 1) * 86400_000).toISOString()
    const { cond, params } = filterCond({ since, vmId, apiKeyId })
    const history = this.db
      .prepare(`
      SELECT strftime('%Y-%m-%d', created_at, '+8 hours') AS day,
             COUNT(*) AS requests,
             SUM(CASE WHEN ${SLA_ERROR_PRED} THEN 1 ELSE 0 END) AS errors,
             COALESCE(SUM(input_tokens), 0) AS input_tokens,
             COALESCE(SUM(output_tokens), 0) AS output_tokens,
             COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
             COALESCE(SUM(cache_creation_tokens), 0) AS cache_creation_tokens,
             COALESCE(SUM(${PROMPT_TOKENS_SQL}), 0) AS prompt_tokens,
             COALESCE(SUM(total_cost), 0) AS total_cost,
             COALESCE(SUM(duration_ms), 0) AS duration_ms_sum,
             SUM(CASE WHEN duration_ms IS NOT NULL THEN 1 ELSE 0 END) AS duration_n
      FROM usage_logs ${cond}
      GROUP BY day ORDER BY day
    `)
      .all(...params)
      .map((r) => ({
        day: r.day,
        requests: Number(r.requests || 0),
        errors: Number(r.errors || 0),
        input_tokens: Number(r.input_tokens || 0),
        output_tokens: Number(r.output_tokens || 0),
        cache_read_tokens: Number(r.cache_read_tokens || 0),
        cache_creation_tokens: Number(r.cache_creation_tokens || 0),
        prompt_tokens: Number(r.prompt_tokens || 0),
        total_cost: Number(r.total_cost || 0),
        duration_ms_sum: Number(r.duration_ms_sum || 0),
        duration_n: Number(r.duration_n || 0),
      }))
    const group = (expr, limit) =>
      this.db
        .prepare(`
      SELECT ${expr} AS name,
             COUNT(*) AS requests,
             COALESCE(SUM(input_tokens), 0) + COALESCE(SUM(output_tokens), 0) AS tokens,
             COALESCE(SUM(total_cost), 0) AS total_cost
      FROM usage_logs ${cond}
      GROUP BY name ORDER BY requests DESC, total_cost DESC LIMIT ${limit}
    `)
        .all(...params)
        .map((r) => ({
          name: r.name,
          requests: Number(r.requests || 0),
          tokens: Number(r.tokens || 0),
          total_cost: Number(r.total_cost || 0),
        }))
    return {
      days: span,
      since,
      history,
      models: group("COALESCE(NULLIF(upstream_model, ''), NULLIF(model, ''), NULLIF(requested_model, ''), '—')", 12),
      [rankKey]: group(rankExpr, rankLimit),
    }
  }

  /**
   * Shanghai-day protocol gate: blocks, passes, and the words that blocked.
   * `total` is every inbound inference request (warmup answers included) plus
   * gate blocks on other paths, so each stage's share uses one denominator.
   */
  protocolEntryStats({ since = shanghaiDayStartIso() } = {}) {
    // COALESCE: a bare `col = x` on NULL yields NULL, and `NOT NULL` drops the row.
    const block = `(
      COALESCE(error_code, '') = 'policy_blocked'
      OR COALESCE(via, '') IN ('hard-regex', 'jev', 'distill-detect', 'refusal-guard')
      OR COALESCE(final_state, '') IN ('policy_blocked', 'distill_blocked', 'refusal_guard', 'refusal_similar', 'refusal_device')
    )`
    const inference = `path IN ('/v1/messages', '/v1/responses', '/v1/chat/completions')`
    const row = this.db
      .prepare(
        `SELECT
           SUM(CASE WHEN ${inference} OR ${block} THEN 1 ELSE 0 END) AS total,
           SUM(CASE WHEN ${block} THEN 1 ELSE 0 END) AS blocked,
           SUM(CASE WHEN ${inference}
                     AND NOT ${block}
                     AND COALESCE(via, '') != 'warmup-intercept'
                THEN 1 ELSE 0 END) AS passed
         FROM usage_logs
         WHERE created_at >= ?`,
      )
      .get(since)
    const keywords = this.db
      .prepare(
        `SELECT error_message AS message, COUNT(*) AS count
         FROM usage_logs
         WHERE created_at >= ?
           AND ${block}
           AND error_message IS NOT NULL
           AND error_message != ''
         GROUP BY error_message
         ORDER BY count DESC
         LIMIT 50`,
      )
      .all(since)
    const grouped = this.db
      .prepare(
        `SELECT intercept, via, error_message AS message,
                CASE WHEN ${block} THEN 1 ELSE 0 END AS blocked,
                COUNT(*) AS count
         FROM usage_logs
         WHERE created_at >= ?
           AND (
             ${block}
             OR (${inference} AND COALESCE(via, '') != 'warmup-intercept')
           )
         GROUP BY intercept, via, error_message, blocked`,
      )
      .all(since)
    const folded = foldInterceptRows(grouped)
    return {
      since,
      total: Number(row?.total || 0),
      blocked: Number(row?.blocked || 0),
      passed: Number(row?.passed || 0),
      keywords: collapseBlockKeywords(keywords),
      blocks: folded.blocks,
      passes: folded.passes,
    }
  }

  /** Official-standard totals: all-time + Shanghai calendar today + per account. */
  billingStats({ accountWindows = null, now = Date.now() } = {}) {
    try {
      this.backfillMissingCosts()
    } catch {}
    const todayStart = shanghaiDayStartIso()
    const total = this._costSelect('', [])
    const today = this._costSelect('WHERE created_at >= ?', [todayStart])
    const fiveHStart = new Date(now - WINDOW_5H_MS).toISOString()
    const sevenDStart = new Date(now - WINDOW_7D_MS).toISOString()
    const accountsTotal = this.costByAccount()
    const accountsToday = this.costByAccount({ since: todayStart })
    const keyOf = (a) => a.account_id + '\0' + (a.vm_id || '')
    const todayById = new Map(accountsToday.map((a) => [keyOf(a), a]))
    const aligned = Array.isArray(accountWindows) && accountWindows.length > 0
    const fiveById = aligned ? null : new Map(this.costByAccount({ since: fiveHStart }).map((a) => [keyOf(a), a]))
    const sevenById = aligned ? null : new Map(this.costByAccount({ since: sevenDStart }).map((a) => [keyOf(a), a]))
    const alignedById = aligned
      ? this._costByAlignedWindows(accountsTotal, accountWindows, { now, fiveHStart, sevenDStart })
      : null
    const accounts = accountsTotal.map((a) => {
      const t = todayById.get(keyOf(a)) || emptyCostBucket()
      const alignedWindows = aligned ? alignedById.get(keyOf(a)) : null
      const since5 = alignedWindows?.since5 ?? null
      const since7 = alignedWindows?.since7 ?? null
      const w = aligned ? alignedWindows?.five || emptyCostBucket() : fiveById.get(keyOf(a)) || emptyCostBucket()
      const w7 = aligned ? alignedWindows?.seven || emptyCostBucket() : sevenById.get(keyOf(a)) || emptyCostBucket()
      return {
        ...a,
        today: { ...t },
        window_5h: { ...w },
        window_7d: { ...w7 },
        today_cost: Number(t.total_cost || 0),
        today_requests: Number(t.requests || 0),
        today_input_tokens: Number(t.input_tokens || 0),
        today_output_tokens: Number(t.output_tokens || 0),
        today_cache_read_tokens: Number(t.cache_read_tokens || 0),
        today_cache_creation_tokens: Number(t.cache_creation_tokens || 0),
        today_input_cost: Number(t.input_cost || 0),
        today_output_cost: Number(t.output_cost || 0),
        today_cache_cost: Number(t.cache_read_cost || 0) + Number(t.cache_creation_cost || 0),
        window_5h_cost: Number(w.total_cost || 0),
        window_5h_requests: Number(w.requests || 0),
        window_5h_tokens: Number(w.input_tokens || 0) + Number(w.output_tokens || 0),
        window_5h_input_tokens: Number(w.input_tokens || 0),
        window_5h_output_tokens: Number(w.output_tokens || 0),
        window_5h_cache_read_tokens: Number(w.cache_read_tokens || 0),
        window_5h_cache_creation_tokens: Number(w.cache_creation_tokens || 0),
        window_5h_input_cost: Number(w.input_cost || 0),
        window_5h_output_cost: Number(w.output_cost || 0),
        window_5h_cache_cost: Number(w.cache_read_cost || 0) + Number(w.cache_creation_cost || 0),
        window_5h_start: aligned ? new Date(since5).toISOString() : fiveHStart,
        window_7d_cost: Number(w7.total_cost || 0),
        window_7d_requests: Number(w7.requests || 0),
        window_7d_success: Number(w7.success || 0),
        window_7d_errors: Number(w7.errors || 0),
        window_7d_tokens: Number(w7.input_tokens || 0) + Number(w7.output_tokens || 0),
        window_7d_start: aligned ? new Date(since7).toISOString() : sevenDStart,
      }
    })
    const window5h = aligned
      ? {
          ...sumCostBuckets(accounts.map((a) => a.window_5h)),
          ...cacheHitStatsFromPrompt(sumCostBuckets(accounts.map((a) => a.window_5h))),
        }
      : this._costSelect('WHERE created_at >= ?', [fiveHStart])
    const window7d = aligned
      ? {
          ...sumCostBuckets(accounts.map((a) => a.window_7d)),
          ...cacheHitStatsFromPrompt(sumCostBuckets(accounts.map((a) => a.window_7d))),
        }
      : this._costSelect('WHERE created_at >= ?', [sevenDStart])
    return {
      source: 'anthropic-official',
      currency: 'USD',
      today_start: todayStart,
      window_5h_start: aligned
        ? accounts.reduce((min, a) => (!min || a.window_5h_start < min ? a.window_5h_start : min), fiveHStart)
        : fiveHStart,
      window_7d_start: aligned
        ? accounts.reduce((min, a) => (!min || a.window_7d_start < min ? a.window_7d_start : min), sevenDStart)
        : sevenDStart,
      today,
      window_5h: window5h,
      window_7d: window7d,
      total,
      accounts,
    }
  }

  /**
   * Aggregate account-specific reset windows in SQLite. The previous path
   * materialized every usage row from the last seven days in JS on each panel
   * refresh, blocking the event loop and amplifying GC under inference load.
   */
  _costByAlignedWindows(accounts, accountWindows, { now, fiveHStart, sevenDStart }) {
    const stmt = this.db.prepare(`
      WITH cutoff(five_since, seven_since, scan_since) AS (VALUES (?, ?, ?))
      SELECT
        SUM(CASE WHEN created_at >= cutoff.five_since THEN 1 ELSE 0 END) AS five_requests,
        SUM(CASE WHEN created_at >= cutoff.five_since AND ${SLA_SUCCESS_PRED} THEN 1 ELSE 0 END) AS five_success,
        SUM(CASE WHEN created_at >= cutoff.five_since AND ${SLA_ERROR_PRED} THEN 1 ELSE 0 END) AS five_errors,
        COALESCE(SUM(CASE WHEN created_at >= cutoff.five_since THEN input_tokens ELSE 0 END), 0) AS five_input_tokens,
        COALESCE(SUM(CASE WHEN created_at >= cutoff.five_since THEN output_tokens ELSE 0 END), 0) AS five_output_tokens,
        COALESCE(SUM(CASE WHEN created_at >= cutoff.five_since THEN cache_read_tokens ELSE 0 END), 0) AS five_cache_read_tokens,
        COALESCE(SUM(CASE WHEN created_at >= cutoff.five_since THEN cache_creation_tokens ELSE 0 END), 0) AS five_cache_creation_tokens,
        COALESCE(SUM(CASE WHEN created_at >= cutoff.five_since THEN (${PROMPT_TOKENS_SQL}) ELSE 0 END), 0) AS five_prompt_tokens,
        COALESCE(SUM(CASE WHEN created_at >= cutoff.five_since THEN input_cost ELSE 0 END), 0) AS five_input_cost,
        COALESCE(SUM(CASE WHEN created_at >= cutoff.five_since THEN output_cost ELSE 0 END), 0) AS five_output_cost,
        COALESCE(SUM(CASE WHEN created_at >= cutoff.five_since THEN cache_read_cost ELSE 0 END), 0) AS five_cache_read_cost,
        COALESCE(SUM(CASE WHEN created_at >= cutoff.five_since THEN cache_creation_cost ELSE 0 END), 0) AS five_cache_creation_cost,
        COALESCE(SUM(CASE WHEN created_at >= cutoff.five_since THEN total_cost ELSE 0 END), 0) AS five_total_cost,
        SUM(CASE WHEN created_at >= cutoff.seven_since THEN 1 ELSE 0 END) AS seven_requests,
        SUM(CASE WHEN created_at >= cutoff.seven_since AND ${SLA_SUCCESS_PRED} THEN 1 ELSE 0 END) AS seven_success,
        SUM(CASE WHEN created_at >= cutoff.seven_since AND ${SLA_ERROR_PRED} THEN 1 ELSE 0 END) AS seven_errors,
        COALESCE(SUM(CASE WHEN created_at >= cutoff.seven_since THEN input_tokens ELSE 0 END), 0) AS seven_input_tokens,
        COALESCE(SUM(CASE WHEN created_at >= cutoff.seven_since THEN output_tokens ELSE 0 END), 0) AS seven_output_tokens,
        COALESCE(SUM(CASE WHEN created_at >= cutoff.seven_since THEN cache_read_tokens ELSE 0 END), 0) AS seven_cache_read_tokens,
        COALESCE(SUM(CASE WHEN created_at >= cutoff.seven_since THEN cache_creation_tokens ELSE 0 END), 0) AS seven_cache_creation_tokens,
        COALESCE(SUM(CASE WHEN created_at >= cutoff.seven_since THEN (${PROMPT_TOKENS_SQL}) ELSE 0 END), 0) AS seven_prompt_tokens,
        COALESCE(SUM(CASE WHEN created_at >= cutoff.seven_since THEN input_cost ELSE 0 END), 0) AS seven_input_cost,
        COALESCE(SUM(CASE WHEN created_at >= cutoff.seven_since THEN output_cost ELSE 0 END), 0) AS seven_output_cost,
        COALESCE(SUM(CASE WHEN created_at >= cutoff.seven_since THEN cache_read_cost ELSE 0 END), 0) AS seven_cache_read_cost,
        COALESCE(SUM(CASE WHEN created_at >= cutoff.seven_since THEN cache_creation_cost ELSE 0 END), 0) AS seven_cache_creation_cost,
        COALESCE(SUM(CASE WHEN created_at >= cutoff.seven_since THEN total_cost ELSE 0 END), 0) AS seven_total_cost
      FROM usage_logs, cutoff
      WHERE created_at >= cutoff.scan_since
        AND vm_id IS ?
        AND COALESCE(NULLIF(final_account_id, ''), NULLIF(account_id, ''), vm_id, '—') = ?
    `)
    const out = new Map()
    for (const account of accounts || []) {
      const win = resolveAccountWindow(accountWindows, account)
      const since5 = extraWindowSince(win?.reset_5h, WINDOW_5H_MS, now) ?? Date.parse(fiveHStart)
      const since7 = extraWindowSince(win?.reset_7d, WINDOW_7D_MS, now) ?? Date.parse(sevenDStart)
      const row = stmt.get(
        new Date(since5).toISOString(),
        new Date(since7).toISOString(),
        new Date(Math.min(since5, since7)).toISOString(),
        account.vm_id,
        account.account_id,
      )
      out.set(account.account_id + '\0' + (account.vm_id || ''), {
        since5,
        since7,
        five: costBucketFromPrefixedRow(row, 'five'),
        seven: costBucketFromPrefixedRow(row, 'seven'),
      })
    }
    return out
  }

  _costEventsSince(since) {
    return this.db
      .prepare(
        `
      SELECT
        COALESCE(NULLIF(final_account_id, ''), NULLIF(account_id, ''), vm_id, '—') AS account_id,
        vm_id,
        created_at,
        CASE WHEN ${SLA_SUCCESS_PRED} THEN 1 ELSE 0 END AS success,
        CASE WHEN ${SLA_ERROR_PRED} THEN 1 ELSE 0 END AS errors,
        COALESCE(input_tokens, 0) AS input_tokens,
        COALESCE(output_tokens, 0) AS output_tokens,
        COALESCE(cache_read_tokens, 0) AS cache_read_tokens,
        COALESCE(cache_creation_tokens, 0) AS cache_creation_tokens,
        (${PROMPT_TOKENS_SQL}) AS prompt_tokens,
        COALESCE(input_cost, 0) AS input_cost,
        COALESCE(output_cost, 0) AS output_cost,
        COALESCE(cache_read_cost, 0) AS cache_read_cost,
        COALESCE(cache_creation_cost, 0) AS cache_creation_cost,
        COALESCE(total_cost, 0) AS total_cost
      FROM usage_logs
      WHERE created_at >= ?
    `,
      )
      .all(since)
  }

  ownerBilling({ ownerUserId = null, since = null, until = null, groupBy = 'vm' } = {}) {
    const where = []
    const params = []
    const own = ownerPred(ownerUserId)
    if (own.sql) {
      where.push(own.sql)
      params.push(...own.params)
    }
    if (since) {
      where.push('created_at >= ?')
      params.push(new Date(since).toISOString())
    }
    if (until) {
      where.push('created_at <= ?')
      params.push(new Date(until).toISOString())
    }
    const cond = where.length ? `WHERE ${where.join(' AND ')}` : ''
    const grouped = groupBy === 'key' ? 'key' : 'vm'
    const groupCol = grouped === 'key' ? 'api_key_id' : 'vm_id'
    const totals = this.db
      .prepare(`
      SELECT COUNT(*) AS requests,
             SUM(CASE WHEN ${SUCCESS_PRED} THEN 1 ELSE 0 END) AS ok,
             SUM(CASE WHEN ${ERROR_PRED} THEN 1 ELSE 0 END) AS fail,
             COALESCE(SUM(input_tokens), 0) AS tokens_in,
             COALESCE(SUM(output_tokens), 0) AS tokens_out,
             COALESCE(SUM(COALESCE(actual_cost, total_cost, 0)), 0) AS cost_usd,
             COALESCE(SUM(COALESCE(total_cost, 0)), 0) AS official_cost_usd
      FROM usage_logs ${cond}
    `)
      .get(...params)
    const items = this.db
      .prepare(`
      SELECT ${groupCol} AS id,
             COUNT(*) AS requests,
             SUM(CASE WHEN ${SUCCESS_PRED} THEN 1 ELSE 0 END) AS ok,
             SUM(CASE WHEN ${ERROR_PRED} THEN 1 ELSE 0 END) AS fail,
             COALESCE(SUM(input_tokens), 0) AS tokens_in,
             COALESCE(SUM(output_tokens), 0) AS tokens_out,
             COALESCE(SUM(COALESCE(actual_cost, total_cost, 0)), 0) AS cost_usd,
             COALESCE(SUM(COALESCE(total_cost, 0)), 0) AS official_cost_usd
      FROM usage_logs ${cond}
      GROUP BY ${groupCol}
      ORDER BY cost_usd DESC
    `)
      .all(...params)
    const num = (row, key) => Number(row?.[key] || 0)
    return {
      group_by: grouped,
      since: since ? new Date(since).toISOString() : null,
      until: until ? new Date(until).toISOString() : null,
      totals: {
        requests: num(totals, 'requests'),
        ok: num(totals, 'ok'),
        fail: num(totals, 'fail'),
        tokens_in: num(totals, 'tokens_in'),
        tokens_out: num(totals, 'tokens_out'),
        cost_usd: num(totals, 'cost_usd'),
        official_cost_usd: num(totals, 'official_cost_usd'),
      },
      items: items.map((row) => ({
        vm_id: grouped === 'vm' ? row.id || null : undefined,
        api_key_id: grouped === 'key' ? row.id || null : undefined,
        requests: num(row, 'requests'),
        ok: num(row, 'ok'),
        fail: num(row, 'fail'),
        tokens_in: num(row, 'tokens_in'),
        tokens_out: num(row, 'tokens_out'),
        cost_usd: num(row, 'cost_usd'),
        official_cost_usd: num(row, 'official_cost_usd'),
      })),
    }
  }

  /**
   * Windowed ops snapshot aligned with sub2api overview cards:
   * SLA / error / 429 / 503, QPS·TPS, duration + first_token percentiles, per-model.
   */
  windowStats({ since = null, until = null, owner_user_id = null } = {}) {
    const { cond, params } = timeCond(since, until)
    const own = ownerPred(owner_user_id)
    let where = cond
    const allParams = [...params]
    if (own.sql) {
      where = where ? `${where} AND ${own.sql}` : `WHERE ${own.sql}`
      allParams.push(...own.params)
    }
    const counts = this.db
      .prepare(`
      SELECT
        COUNT(*) AS requests,
        SUM(CASE WHEN ${SLA_SUCCESS_PRED} THEN 1 ELSE 0 END) AS success,
        SUM(CASE WHEN ${SLA_ERROR_PRED} THEN 1 ELSE 0 END) AS errors,
        SUM(CASE WHEN status = 429 THEN 1 ELSE 0 END) AS status_429,
        SUM(CASE WHEN status = 529 THEN 1 ELSE 0 END) AS status_529,
        SUM(CASE WHEN status = 503 THEN 1 ELSE 0 END) AS status_503,
        SUM(CASE WHEN stream = 1 THEN 1 ELSE 0 END) AS stream_requests,
        SUM(CASE WHEN first_token_ms IS NOT NULL THEN 1 ELSE 0 END) AS ttft_samples,
        COALESCE(SUM(input_tokens), 0) AS input_tokens,
        COALESCE(SUM(output_tokens), 0) AS output_tokens,
        COALESCE(SUM(cache_read_tokens), 0) AS cache_read_tokens,
        COALESCE(SUM(cache_creation_tokens), 0) AS cache_creation_tokens,
        COALESCE(SUM(${PROMPT_TOKENS_SQL}), 0) AS prompt_tokens,
        COALESCE(SUM(total_cost), 0) AS total_cost
      FROM usage_logs ${where}
    `)
      .get(...allParams)
    const requests = Number(counts?.requests || 0)
    const success = Number(counts?.success || 0)
    const errors = Number(counts?.errors || 0)
    const inputTokens = Number(counts?.input_tokens || 0)
    const outputTokens = Number(counts?.output_tokens || 0)
    const cacheRead = Number(counts?.cache_read_tokens || 0)
    const cacheWrite = Number(counts?.cache_creation_tokens || 0)
    const cache = cacheHitStatsFromPrompt({
      input_tokens: inputTokens,
      cache_read_tokens: cacheRead,
      cache_creation_tokens: cacheWrite,
      prompt_tokens: Number(counts?.prompt_tokens || 0),
    })
    const tokens = inputTokens + outputTokens

    const startMs = since ? Date.parse(since) : null
    const endMs = until ? Date.parse(until) : Date.now()
    const windowSeconds =
      Number.isFinite(startMs) && Number.isFinite(endMs) ? Math.max(1, (endMs - startMs) / 1000) : Math.max(1, requests)

    const minuteRows = this.db
      .prepare(`
      SELECT strftime('%Y-%m-%dT%H:%M', created_at) AS minute,
             COUNT(*) AS requests,
             COALESCE(SUM(COALESCE(input_tokens,0) + COALESCE(output_tokens,0)), 0) AS tokens
      FROM usage_logs ${where}
      GROUP BY minute
    `)
      .all(...allParams)
    const peakReq = minuteRows.reduce((m, r) => Math.max(m, Number(r.requests || 0)), 0)
    const peakTok = minuteRows.reduce((m, r) => Math.max(m, Number(r.tokens || 0)), 0)
    const lastMinute = minuteRows.length ? minuteRows[minuteRows.length - 1] : null

    const ttftWhere = where ? `${where} AND first_token_ms IS NOT NULL` : 'WHERE first_token_ms IS NOT NULL'
    const durWhere = where ? `${where} AND duration_ms IS NOT NULL` : 'WHERE duration_ms IS NOT NULL'
    const ttfts = this.db
      .prepare(`SELECT first_token_ms AS v FROM usage_logs ${ttftWhere}`)
      .all(...allParams)
      .map((r) => r.v)
    const durs = this.db
      .prepare(`SELECT duration_ms AS v FROM usage_logs ${durWhere}`)
      .all(...allParams)
      .map((r) => r.v)

    const byModel = this.db
      .prepare(`
      SELECT COALESCE(NULLIF(upstream_model, ''), NULLIF(model, ''), '—') AS model,
             COUNT(*) AS requests,
             SUM(CASE WHEN ${SLA_ERROR_PRED} THEN 1 ELSE 0 END) AS errors,
             SUM(CASE WHEN first_token_ms IS NOT NULL THEN 1 ELSE 0 END) AS ttft_samples,
             CAST(AVG(first_token_ms) AS INTEGER) AS avg_first_token_ms,
             CAST(AVG(duration_ms) AS INTEGER) AS avg_duration_ms
      FROM usage_logs ${where}
      GROUP BY 1
      ORDER BY requests DESC
      LIMIT 12
    `)
      .all(...allParams)

    return {
      since: since ? new Date(since).toISOString() : null,
      until: until ? new Date(until).toISOString() : new Date().toISOString(),
      window_seconds: Math.round(windowSeconds),
      requests,
      success,
      errors,
      sla: requests ? success / requests : 1,
      error_rate: requests ? errors / requests : 0,
      status_429: Number(counts?.status_429 || 0),
      status_529: Number(counts?.status_529 || 0),
      status_503: Number(counts?.status_503 || 0),
      stream_requests: Number(counts?.stream_requests || 0),
      input_tokens: inputTokens,
      output_tokens: outputTokens,
      cache_read_tokens: cacheRead,
      cache_creation_tokens: cacheWrite,
      prompt_tokens: cache.prompt_tokens,
      cache_hit_rate: cache.cache_hit_rate,
      sticky: this._stickyWindow({ since, until, owner_user_id }),
      total_cost: Number(counts?.total_cost || 0),
      qps: {
        current: lastMinute ? Number(lastMinute.requests || 0) / 60 : 0,
        peak: peakReq / 60,
        avg: requests / windowSeconds,
      },
      tps: {
        current: lastMinute ? Number(lastMinute.tokens || 0) / 60 : 0,
        peak: peakTok / 60,
        avg: tokens / windowSeconds,
      },
      duration: summarizeLatency(durs),
      ttft: summarizeLatency(ttfts),
      by_model: byModel.map((r) => ({
        model: r.model,
        requests: Number(r.requests || 0),
        errors: Number(r.errors || 0),
        ttft_samples: Number(r.ttft_samples || 0),
        avg_first_token_ms: r.avg_first_token_ms ?? null,
        avg_duration_ms: r.avg_duration_ms ?? null,
      })),
      error_collection: this.errorCollection({ since, until, owner_user_id }),
    }
  }

  _stickyWindow({ since = null, until = null, owner_user_id = null } = {}) {
    const empty = { selections: 0, hits: 0, rate: null }
    try {
      const parts = []
      const params = []
      if (since) {
        parts.push('started_at >= ?')
        params.push(since)
      }
      if (until) {
        parts.push('started_at <= ?')
        params.push(until)
      }
      const own = ownerPred(owner_user_id)
      if (own.sql) {
        parts.push(`request_id IN (SELECT request_id FROM usage_logs WHERE ${own.sql})`)
        params.push(...own.params)
      }
      const where = parts.length ? `WHERE ${parts.join(' AND ')}` : ''
      const rows = this.db
        .prepare(`
        SELECT selection_reason, COUNT(*) AS n
        FROM request_attempts ${where}
        GROUP BY 1
      `)
        .all(...params)
      let selections = 0
      let hits = 0
      for (const row of rows) {
        const n = Number(row.n || 0)
        selections += n
        if (String(row.selection_reason || '') === 'sticky') hits += n
      }
      return {
        selections,
        hits,
        rate: selections > 0 ? hits / selections : null,
      }
    } catch {
      return empty
    }
  }

  errorCollection({ since = null, until = null, owner_user_id = null } = {}) {
    const { cond, params } = timeCond(since, until)
    const own = ownerPred(owner_user_id)
    const errCond = `${cond ? `${cond} AND ` : 'WHERE '}${ERROR_PRED}${own.sql ? ` AND ${own.sql}` : ''}`
    const rows = this.db
      .prepare(`
      SELECT created_at AS ts, request_id, status, error_code, error_message, model, upstream_model, vm_id, ip_address AS ip, api_key_presented
      FROM usage_logs ${errCond}
      ORDER BY created_at DESC, id DESC
      LIMIT 2000
    `)
      .all(...params, ...own.params)
    return {
      ...collectErrors(rows),
      ingress_hits: this.ingressHits({ since, until, owner_user_id }),
    }
  }

  /** Invalid-key 401s grouped by presented token + IP (still counted when muted). */
  ingressHits({ since = null, until = null, owner_user_id = null } = {}) {
    const { cond, params } = timeCond(since, until)
    const own = ownerPred(owner_user_id)
    const authPred = own.sql ? `${ingressAuthSql()} AND ${own.sql}` : ingressAuthSql()
    const where = cond ? `${cond} AND ${authPred}` : `WHERE ${authPred}`
    try {
      return this.db
        .prepare(`
        SELECT
          COALESCE(api_key_presented, '') AS api_key_presented,
          COALESCE(ip_address, '') AS ip,
          COUNT(*) AS count,
          MAX(created_at) AS last_ts,
          MAX(error_code) AS error_code
        FROM usage_logs
        ${where}
        GROUP BY 1, 2
        ORDER BY count DESC
        LIMIT 40
      `)
        .all(...params, ...own.params)
        .map((row) => ({
          api_key_presented: row.api_key_presented || '',
          ip: row.ip || '',
          count: Number(row.count || 0),
          last_ts: row.last_ts || null,
          error_code: row.error_code || null,
        }))
    } catch {
      return []
    }
  }

  /**
   * Age out summaries/debug independently, then trim debug blobs to maxBytes.
   * Number form `cleanup(7)` keeps the old retainDays-only call.
   */
  cleanup(opts = {}) {
    const cfg = this._cleanupOpts(opts)
    const usage_logs = this._deleteOldUsage.run(cfg.usageCutoff).changes
    const debug_by_age = this._deleteOldDebug.run(cfg.debugCutoff).changes
    const debug_by_size = this.trimDebugToMaxBytes(cfg.maxBytes)
    if (usage_logs + debug_by_age + debug_by_size > 0) {
      try {
        this.db.exec('PRAGMA wal_checkpoint(TRUNCATE)')
      } catch {}
    }
    try {
      this.db.exec('PRAGMA optimize')
    } catch {}
    return {
      usage_logs,
      request_log_debug: debug_by_age + debug_by_size,
      debug_by_age,
      debug_by_size,
    }
  }

  _cleanupOpts(opts) {
    const raw = typeof opts === 'number' ? { retainDays: opts } : opts || {}
    const retainDays = Math.max(1, Number(raw.retainDays) || 7)
    let debugDays = Math.max(1, Number(raw.debugRetainDays) || 3)
    if (debugDays > retainDays) debugDays = retainDays
    return {
      usageCutoff: new Date(Date.now() - retainDays * 86400_000).toISOString(),
      debugCutoff: new Date(Date.now() - debugDays * 86400_000).toISOString(),
      maxBytes: Math.max(0, Number(raw.maxBytes) || 0),
    }
  }

  debugBytes() {
    return Number(this._debugBytes.get().n || 0)
  }

  countDebug() {
    return Number(this._debugCount.get().n || 0)
  }

  /** Delete oldest debug rows until SUM(LENGTH(record_json)) <= maxBytes. */
  trimDebugToMaxBytes(maxBytes) {
    const cap = Math.max(0, Number(maxBytes) || 0)
    if (cap <= 0) return 0
    let deleted = 0
    for (let i = 0; i < 8; i++) {
      const used = this.debugBytes()
      if (used <= cap) return deleted
      const count = this.countDebug()
      if (!count) return deleted
      const avg = Math.max(1, Math.floor(used / count))
      const need = Math.min(count, Math.max(1, Math.ceil((used - cap) / avg) + 1))
      const n = this._deleteOldestDebug.run(need).changes
      if (!n) return deleted
      deleted += n
    }
    return deleted
  }

  count() {
    return this.db.prepare('SELECT COUNT(*) c FROM usage_logs').get().c
  }
}

/** Transitional alias while callers migrate to the sub2api name. */
export { UsageLogsRepo as RequestLogsRepo }
