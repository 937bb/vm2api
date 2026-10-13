const DEFAULT_BATCH_SIZE = 256
const DEFAULT_MAX_PENDING = 16_384
const DEFAULT_FLUSH_INTERVAL_MS = 10

function positiveInt(value, fallback) {
  const n = Number(value)
  return Number.isFinite(n) && n > 0 ? Math.trunc(n) : fallback
}

/**
 * Bounded, ordered write buffer for synchronous SQLite repositories.
 * The flush callback owns the transaction so a whole burst commits once.
 */
export class BufferedWriteQueue {
  constructor({ flush, batchSize, maxPending, flushIntervalMs, onError } = {}) {
    if (typeof flush !== 'function') throw new TypeError('flush callback is required')
    this.flushBatch = flush
    this.batchSize = positiveInt(batchSize, DEFAULT_BATCH_SIZE)
    this.maxPending = positiveInt(maxPending, DEFAULT_MAX_PENDING)
    this.flushIntervalMs = positiveInt(flushIntervalMs, DEFAULT_FLUSH_INTERVAL_MS)
    this.onError = typeof onError === 'function' ? onError : null
    this.pending = []
    this.timer = null
    this.closed = false
    this.counters = {
      submitted: 0,
      completed: 0,
      failed: 0,
      batches: 0,
      batch_fallbacks: 0,
      pressure_flushes: 0,
    }
  }

  enqueue(item) {
    if (this.closed) {
      this._write([item])
      return false
    }
    if (this.pending.length >= this.maxPending) {
      this.counters.pressure_flushes += 1
      this.flush({ all: false })
    }
    this.pending.push(item)
    this.counters.submitted += 1
    this._schedule(this.pending.length >= this.batchSize ? 0 : this.flushIntervalMs)
    return true
  }

  _schedule(delayMs) {
    if (this.timer) {
      if (delayMs > 0) return
      clearTimeout(this.timer)
    }
    this.timer = setTimeout(() => {
      this.timer = null
      this.flush({ all: false })
    }, delayMs)
    this.timer.unref?.()
  }

  _write(batch) {
    if (!batch.length) return 0
    try {
      this.flushBatch(batch)
      this.counters.completed += batch.length
      this.counters.batches += 1
      return batch.length
    } catch (error) {
      this.counters.batch_fallbacks += 1
      try {
        this.onError?.(error, batch.length)
      } catch {}
      let written = 0
      for (const item of batch) {
        try {
          this.flushBatch([item])
          this.counters.completed += 1
          written += 1
        } catch (itemError) {
          this.counters.failed += 1
          try {
            this.onError?.(itemError, 1)
          } catch {}
        }
      }
      this.counters.batches += batch.length
      return written
    }
  }

  flush({ all = true } = {}) {
    if (this.timer) {
      clearTimeout(this.timer)
      this.timer = null
    }
    let written = 0
    do {
      const batch = this.pending.splice(0, this.batchSize)
      if (!batch.length) break
      written += this._write(batch)
    } while (all && this.pending.length)
    if (this.pending.length) this._schedule(0)
    return written
  }

  close() {
    if (this.closed) return 0
    this.closed = true
    return this.flush()
  }

  snapshot() {
    return {
      ...this.counters,
      pending: this.pending.length,
      batch_size: this.batchSize,
      max_pending: this.maxPending,
      flush_interval_ms: this.flushIntervalMs,
    }
  }
}

export const BUFFERED_WRITE_DEFAULTS = Object.freeze({
  batchSize: DEFAULT_BATCH_SIZE,
  maxPending: DEFAULT_MAX_PENDING,
  flushIntervalMs: DEFAULT_FLUSH_INTERVAL_MS,
})
