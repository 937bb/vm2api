/**
 * api_keys repository — persistence for managed client API keys.
 * Pure SQL layer; business rules stay in lib/api-keys.mjs (ApiKeyStore).
 *
 * sub2api-shaped: user_id/group_id ownership, USD quota (quota/quota_used)
 * plus 5h/1d/7d USD rate-limit windows. The legacy request-count quota pair
 * is quota_requests / quota_requests_used. Soft delete via deleted_at.
 */

import { getDb } from '../database.mjs'

const COLUMNS = [
  'id',
  'name',
  'key',
  'status',
  'max_concurrency',
  'quota_requests',
  'quota_requests_used',
  'rpm',
  'expires_at',
  'created_at',
  'updated_at',
  'last_used_at',
  'requests',
  'tokens_in',
  'tokens_out',
  'cache_read_tokens',
  'cache_creation_tokens',
  'key_hash',
  'key_prefix',
  'key_suffix',
  'category',
  'group_type',
  'allowed_vms',
  'vm_pool_id',
  'key_secret',
  'user_id',
  'group_id',
  'quota',
  'quota_used',
  'rate_limit_5h',
  'rate_limit_1d',
  'rate_limit_7d',
  'usage_5h',
  'usage_1d',
  'usage_7d',
  'window_5h_start',
  'window_1d_start',
  'window_7d_start',
  'ip_whitelist',
  'ip_blacklist',
  'deleted_at',
]

const WINDOWS = [
  { usage: 'usage_5h', start: 'window_5h_start', ms: 5 * 3600 * 1000 },
  { usage: 'usage_1d', start: 'window_1d_start', ms: 24 * 3600 * 1000 },
  { usage: 'usage_7d', start: 'window_7d_start', ms: 7 * 24 * 3600 * 1000 },
]

const NUMERIC_DEFAULT_0 = new Set([
  'quota_requests',
  'quota_requests_used',
  'requests',
  'tokens_in',
  'tokens_out',
  'quota',
  'quota_used',
  'rate_limit_5h',
  'rate_limit_1d',
  'rate_limit_7d',
  'usage_5h',
  'usage_1d',
  'usage_7d',
])

function rowToRec(row) {
  if (!row) return null
  return { ...row }
}

function toValue(rec, c) {
  const v = rec[c]
  if (v == null) {
    if (c === 'category') return 'oauth'
    if (c === 'group_type') return 'all'
    if (c === 'allowed_vms') return '[]'
    return NUMERIC_DEFAULT_0.has(c) ? 0 : null
  }
  return v
}

export class ApiKeysRepo {
  constructor(db = getDb()) {
    this.db = db
    this._list = db.prepare('SELECT * FROM api_keys WHERE deleted_at IS NULL ORDER BY created_at')
    this._get = db.prepare('SELECT * FROM api_keys WHERE id = ? AND deleted_at IS NULL')
    this._getByKey = db.prepare('SELECT * FROM api_keys WHERE key = ? AND deleted_at IS NULL')
    this._getByHash = db.prepare('SELECT * FROM api_keys WHERE key_hash = ? AND deleted_at IS NULL')
    this._insert = db.prepare(`
      INSERT INTO api_keys (${COLUMNS.join(', ')})
      VALUES (${COLUMNS.map(() => '?').join(', ')})
    `)
    this._delete = db.prepare('DELETE FROM api_keys WHERE id = ?')
    this._recordUsage = db.prepare(`
      UPDATE api_keys SET
        requests = COALESCE(requests, 0) + 1,
        quota_requests_used = COALESCE(quota_requests_used, 0) + 1,
        tokens_in = COALESCE(tokens_in, 0) + ?1,
        tokens_out = COALESCE(tokens_out, 0) + ?2,
        cache_read_tokens = COALESCE(cache_read_tokens, 0) + ?3,
        cache_creation_tokens = COALESCE(cache_creation_tokens, 0) + ?4,
        quota_used = COALESCE(quota_used, 0) + ?5,
        status = CASE
          WHEN status = 'active' AND quota > 0 AND COALESCE(quota_used, 0) + ?5 >= quota THEN 'quota_exhausted'
          ELSE status
        END,
        usage_5h = CASE
          WHEN window_5h_start IS NULL OR window_5h_start <= ?6 THEN ?5
          ELSE COALESCE(usage_5h, 0) + ?5
        END,
        window_5h_start = CASE
          WHEN window_5h_start IS NULL OR window_5h_start <= ?6 THEN ?9
          ELSE window_5h_start
        END,
        usage_1d = CASE
          WHEN window_1d_start IS NULL OR window_1d_start <= ?7 THEN ?5
          ELSE COALESCE(usage_1d, 0) + ?5
        END,
        window_1d_start = CASE
          WHEN window_1d_start IS NULL OR window_1d_start <= ?7 THEN ?9
          ELSE window_1d_start
        END,
        usage_7d = CASE
          WHEN window_7d_start IS NULL OR window_7d_start <= ?8 THEN ?5
          ELSE COALESCE(usage_7d, 0) + ?5
        END,
        window_7d_start = CASE
          WHEN window_7d_start IS NULL OR window_7d_start <= ?8 THEN ?9
          ELSE window_7d_start
        END,
        last_used_at = ?9,
        updated_at = ?9
      WHERE id = ?10 AND deleted_at IS NULL
      RETURNING *
    `)
  }

  list() {
    return this._list.all().map(rowToRec)
  }

  getById(id) {
    return rowToRec(this._get.get(id))
  }

  getByKey(key) {
    return rowToRec(this._getByKey.get(key))
  }

  getByHash(hash) {
    return rowToRec(this._getByHash.get(hash))
  }

  insert(rec) {
    this._insert.run(...COLUMNS.map((c) => toValue(rec, c)))
    return this.getById(rec.id)
  }

  /** Full-row update from a record object (id immutable). */
  update(rec) {
    const cols = COLUMNS.filter((c) => c !== 'id')
    const sql = `UPDATE api_keys SET ${cols.map((c) => `${c} = ?`).join(', ')} WHERE id = ?`
    this.db.prepare(sql).run(...cols.map((c) => toValue(rec, c)), rec.id)
    return this.getById(rec.id)
  }

  remove(id) {
    const info = this._delete.run(id)
    return info.changes > 0
  }

  recordUsage(id, { tokens_in = 0, tokens_out = 0, cache_read_tokens = 0, cache_creation_tokens = 0, cost = 0 } = {}) {
    const nowMs = Date.now()
    const now = new Date(nowMs).toISOString()
    const usd = Number(cost) || 0
    const cutoffs = WINDOWS.map((window) => new Date(nowMs - window.ms).toISOString())
    const row = this._recordUsage.get(
      Number(tokens_in) || 0,
      Number(tokens_out) || 0,
      Number(cache_read_tokens) || 0,
      Number(cache_creation_tokens) || 0,
      usd,
      cutoffs[0],
      cutoffs[1],
      cutoffs[2],
      now,
      id,
    )
    return rowToRec(row)
  }

  count() {
    return this.db.prepare('SELECT COUNT(*) c FROM api_keys WHERE deleted_at IS NULL').get().c
  }
}
