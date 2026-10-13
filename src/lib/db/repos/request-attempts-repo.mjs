import { getDb, withTransaction } from '../database.mjs'
import { BufferedWriteQueue } from '../buffered-write-queue.mjs'

const USAGE_SCALARS = [
  'input_tokens',
  'output_tokens',
  'total_tokens',
  'prompt_tokens',
  'completion_tokens',
  'cache_read_input_tokens',
  'cache_creation_input_tokens',
  'cache_read_tokens',
  'cache_creation_tokens',
  'cache_creation_5m_tokens',
  'cache_creation_1h_tokens',
  'cached_tokens',
  'cache_write_tokens',
  'service_tier',
  'stop_reason',
]

const USAGE_DETAIL_KEYS = {
  input_tokens_details: ['cached_tokens', 'cache_write_tokens', 'cache_creation_tokens'],
  prompt_tokens_details: ['cached_tokens', 'cache_write_tokens', 'cache_creation_tokens'],
  output_tokens_details: ['reasoning_tokens', 'accepted_prediction_tokens', 'rejected_prediction_tokens'],
  completion_tokens_details: ['reasoning_tokens', 'accepted_prediction_tokens', 'rejected_prediction_tokens'],
  cache_creation: ['ephemeral_5m_input_tokens', 'ephemeral_1h_input_tokens'],
}

/**
 * Attempt rows are an operational decision ledger, not a raw upstream trace.
 * Codex usage can contain a per-item `attribution` tree hundreds of KB large;
 * persisting it synchronously blocks the gateway event loop and grows SQLite
 * by gigabytes. Keep only fields used for billing and diagnostics.
 */
export function compactAttemptUsage(usage) {
  if (!usage || typeof usage !== 'object' || Array.isArray(usage)) return usage
  const out = {}
  for (const key of USAGE_SCALARS) {
    const value = usage[key]
    if (value == null || (typeof value !== 'number' && typeof value !== 'string' && typeof value !== 'boolean')) {
      continue
    }
    out[key] = value
  }
  for (const [key, allowed] of Object.entries(USAGE_DETAIL_KEYS)) {
    const source = usage[key]
    if (!source || typeof source !== 'object' || Array.isArray(source)) continue
    const detail = {}
    for (const field of allowed) {
      const value = source[field]
      if (value != null && (typeof value === 'number' || typeof value === 'string' || typeof value === 'boolean')) {
        detail[field] = value
      }
    }
    if (Object.keys(detail).length) out[key] = detail
  }
  return out
}

function parse(value, fallback = null) {
  if (value == null) return fallback
  try {
    return JSON.parse(value)
  } catch {
    return fallback
  }
}

function rowToAttempt(row) {
  if (!row) return null
  return {
    id: row.id,
    request_id: row.request_id,
    attempt_no: row.attempt_no,
    vm_id: row.vm_id,
    account_id: row.account_id,
    model: row.model,
    selection_reason: row.selection_reason,
    started_at: row.started_at,
    completed_at: row.completed_at,
    upstream_status: row.upstream_status,
    error_scope: row.error_scope,
    action: row.action,
    cooldown_until: row.cooldown_until,
    downstream_committed: !!row.downstream_committed,
    terminal_state: row.terminal_state,
    usage: parse(row.usage_json),
    wait_ms: row.wait_ms,
    ttft_ms: row.ttft_ms,
    latency_ms: row.latency_ms,
  }
}

export class RequestAttemptsRepo {
  constructor(
    db = getDb(),
    {
      bufferedWrites = false,
      writeBatchSize = Number(process.env.KIN_DB_WRITE_BATCH_SIZE || 256),
      writeQueueSize = Number(process.env.KIN_DB_WRITE_QUEUE_SIZE || 16_384),
      writeFlushIntervalMs = Number(process.env.KIN_DB_WRITE_FLUSH_MS || 10),
    } = {},
  ) {
    this.db = db
    this._insert = db.prepare(`
      INSERT INTO request_attempts (
        request_id, attempt_no, vm_id, account_id, model, selection_reason,
        started_at, completed_at, upstream_status, error_scope, action,
        cooldown_until, downstream_committed, terminal_state, usage_json,
        wait_ms, ttft_ms, latency_ms
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      RETURNING *
    `)
    this._complete = db.prepare(`
      UPDATE request_attempts SET
        completed_at = ?, upstream_status = ?, error_scope = ?, action = ?,
        cooldown_until = ?, downstream_committed = ?, terminal_state = ?,
        usage_json = ?, ttft_ms = ?, latency_ms = ?
      WHERE request_id = ? AND attempt_no = ?
      RETURNING *
    `)
    this._list = db.prepare(`
      SELECT * FROM request_attempts WHERE request_id = ? ORDER BY attempt_no
    `)
    this._removeOld = db.prepare(`
      DELETE FROM request_attempts WHERE started_at < ?
    `)
    this._writeQueue = bufferedWrites
      ? new BufferedWriteQueue({
          batchSize: writeBatchSize,
          maxPending: writeQueueSize,
          flushIntervalMs: writeFlushIntervalMs,
          flush: (operations) => this._persistBatch(operations),
          onError: (error, count) => {
            console.error(
              JSON.stringify({
                event: 'request_attempt_write_failed',
                count,
                code: error?.code || null,
                message: String(error?.message || error).slice(0, 200),
              }),
            )
          },
        })
      : null
  }

  begin({ requestId, attemptNo, vmId, accountId, model, selectionReason, waitMs = 0 }) {
    const startedAt = new Date().toISOString()
    const args = [
      requestId,
      attemptNo,
      vmId ?? null,
      accountId ?? null,
      model ?? null,
      selectionReason ?? null,
      startedAt,
      null,
      null,
      null,
      null,
      null,
      0,
      'started',
      null,
      waitMs ?? 0,
      null,
      null,
    ]
    if (this._writeQueue) {
      this._writeQueue.enqueue({ kind: 'begin', args })
      return {
        request_id: requestId,
        attempt_no: attemptNo,
        vm_id: vmId ?? null,
        account_id: accountId ?? null,
        model: model ?? null,
        selection_reason: selectionReason ?? null,
        started_at: startedAt,
        completed_at: null,
        upstream_status: null,
        error_scope: null,
        action: null,
        cooldown_until: null,
        downstream_committed: false,
        terminal_state: 'started',
        usage: null,
        wait_ms: waitMs ?? 0,
        ttft_ms: null,
        latency_ms: null,
      }
    }
    const row = this._insert.get(...args)
    return rowToAttempt(row)
  }

  complete(requestId, attemptNo, result = {}) {
    const args = [
      new Date().toISOString(),
      result.upstreamStatus ?? null,
      result.errorScope ?? null,
      result.action ?? null,
      result.cooldownUntil ?? null,
      result.downstreamCommitted ? 1 : 0,
      result.terminalState ?? null,
      result.usage != null ? JSON.stringify(compactAttemptUsage(result.usage)) : null,
      result.ttftMs ?? null,
      result.latencyMs ?? null,
      requestId,
      attemptNo,
    ]
    if (this._writeQueue) {
      this._writeQueue.enqueue({ kind: 'complete', args })
      return null
    }
    const row = this._complete.get(...args)
    return rowToAttempt(row)
  }

  list(requestId) {
    this.flushPending()
    return this._list.all(requestId).map(rowToAttempt)
  }

  cleanup(retainDays = 7) {
    this.flushPending()
    const days = Math.max(1, Number(retainDays) || 7)
    const cutoff = new Date(Date.now() - days * 86400_000).toISOString()
    return this._removeOld.run(cutoff).changes
  }

  _persistBatch(operations) {
    withTransaction(this.db, () => {
      for (const operation of operations) {
        if (operation.kind === 'begin') this._insert.get(...operation.args)
        else if (operation.kind === 'complete') this._complete.get(...operation.args)
      }
    })
  }

  flushPending() {
    return this._writeQueue?.flush() || 0
  }

  stop() {
    return this._writeQueue?.close() || 0
  }

  writeQueueStats() {
    return this._writeQueue?.snapshot() || null
  }
}
