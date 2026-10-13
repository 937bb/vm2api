import assert from 'node:assert/strict'
import test from 'node:test'
import { handleHttpClientError } from '../../src/lib/http/client-error.mjs'

function fakeSocket(overrides = {}) {
  return {
    destroyed: false,
    writable: true,
    ended: [],
    destroyCalls: 0,
    end(data) {
      this.ended.push(data)
    },
    destroy() {
      this.destroyCalls += 1
      this.destroyed = true
    },
    ...overrides,
  }
}

test('connection resets are destroyed without writing a response', () => {
  const socket = fakeSocket()
  const logs = []

  handleHttpClientError(Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' }), socket, {
    log: (line) => logs.push(line),
  })

  assert.equal(socket.destroyCalls, 1)
  assert.deepEqual(socket.ended, [])
  assert.match(logs[0], /"code":"ECONNRESET"/)
})

test('parser errors receive a 400 response while the socket is writable', () => {
  const socket = fakeSocket()

  handleHttpClientError(Object.assign(new Error('Parse Error'), { code: 'HPE_INVALID_EOF_STATE' }), socket, {
    log() {},
  })

  assert.equal(socket.destroyCalls, 0)
  assert.equal(socket.ended.length, 1)
  assert.match(socket.ended[0], /^HTTP\/1\.1 400 Bad Request/)
})

test('already destroyed sockets are not written', () => {
  const socket = fakeSocket({ destroyed: true })

  handleHttpClientError(Object.assign(new Error('late parser error'), { code: 'HPE_INVALID_EOF_STATE' }), socket, {
    log() {},
  })

  assert.equal(socket.destroyCalls, 1)
  assert.deepEqual(socket.ended, [])
})
