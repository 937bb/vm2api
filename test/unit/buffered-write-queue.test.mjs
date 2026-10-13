import test from 'node:test'
import assert from 'node:assert/strict'
import { BufferedWriteQueue } from '../../src/lib/db/buffered-write-queue.mjs'

test('buffered write queue preserves order and batches a burst', () => {
  const batches = []
  const queue = new BufferedWriteQueue({
    batchSize: 3,
    flushIntervalMs: 60_000,
    flush: (items) => batches.push(items.slice()),
  })
  for (let i = 0; i < 7; i += 1) queue.enqueue(i)
  assert.equal(queue.flush(), 7)
  assert.deepEqual(batches, [[0, 1, 2], [3, 4, 5], [6]])
  assert.deepEqual(queue.snapshot(), {
    submitted: 7,
    completed: 7,
    failed: 0,
    batches: 3,
    batch_fallbacks: 0,
    pressure_flushes: 0,
    pending: 0,
    batch_size: 3,
    max_pending: 16_384,
    flush_interval_ms: 60_000,
  })
})

test('buffered write queue isolates a failed batch to individual writes', () => {
  const written = []
  const queue = new BufferedWriteQueue({
    batchSize: 8,
    flushIntervalMs: 60_000,
    flush: (items) => {
      if (items.length > 1) throw new Error('batch rejected')
      if (items[0] === 'bad') throw new Error('bad row')
      written.push(items[0])
    },
  })
  queue.enqueue('a')
  queue.enqueue('bad')
  queue.enqueue('b')
  assert.equal(queue.flush(), 2)
  assert.deepEqual(written, ['a', 'b'])
  assert.equal(queue.snapshot().batch_fallbacks, 1)
  assert.equal(queue.snapshot().failed, 1)
})
