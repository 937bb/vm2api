import { test } from 'node:test'
import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'
import { writeKeepaliveIfWritable, writeWithBackpressure } from '../../src/lib/protocol/stream-backpressure.mjs'

function blockedResponse() {
  const res = new EventEmitter()
  res.destroyed = false
  res.writableEnded = false
  res.writableNeedDrain = true
  res.write = () => false
  return res
}

test('writeWithBackpressure pauses until drain and removes lifecycle listeners', async () => {
  const res = blockedResponse()
  const pending = writeWithBackpressure(res, 'chunk')
  let settled = false
  pending.then(() => {
    settled = true
  })

  await new Promise((resolve) => setImmediate(resolve))
  assert.equal(settled, false)
  assert.equal(res.listenerCount('drain'), 1)
  assert.equal(res.listenerCount('close'), 1)
  assert.equal(res.listenerCount('error'), 1)

  res.writableNeedDrain = false
  res.emit('drain')
  assert.equal(await pending, true)
  assert.equal(res.listenerCount('drain'), 0)
  assert.equal(res.listenerCount('close'), 0)
  assert.equal(res.listenerCount('error'), 0)
})

test('writeWithBackpressure releases promptly when the client closes or aborts', async () => {
  const closed = blockedResponse()
  const closePending = writeWithBackpressure(closed, 'chunk')
  closed.destroyed = true
  closed.emit('close')
  assert.equal(await closePending, false)
  assert.equal(closed.listenerCount('drain'), 0)

  const aborted = blockedResponse()
  const controller = new AbortController()
  const abortPending = writeWithBackpressure(aborted, 'chunk', controller.signal)
  controller.abort()
  assert.equal(await abortPending, false)
  assert.equal(aborted.listenerCount('drain'), 0)
})

test('keepalive skips writes while a real SSE chunk is backpressured', () => {
  const res = blockedResponse()
  res.headersSent = false
  let writes = 0
  let headers = 0
  res.write = () => {
    writes += 1
    return false
  }

  assert.equal(
    writeKeepaliveIfWritable(res, ': keepalive\n\n', () => {
      headers += 1
    }),
    false,
  )
  assert.equal(writes, 0)
  assert.equal(headers, 0)
})
