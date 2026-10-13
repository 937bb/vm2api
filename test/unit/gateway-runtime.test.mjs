import test from 'node:test'
import assert from 'node:assert/strict'
import { createGatewayRuntime, isInferenceRequest } from '../../src/lib/http/gateway-runtime.mjs'

test('inference route classification excludes control-plane traffic', () => {
  assert.equal(isInferenceRequest('POST', '/v1/responses'), true)
  assert.equal(isInferenceRequest('POST', '/v1/messages'), true)
  assert.equal(isInferenceRequest('GET', '/v1/responses'), false)
  assert.equal(isInferenceRequest('POST', '/api/panel/vms'), false)
})

test('a closed client transport does not make an unfinished handler idle', async () => {
  const runtime = createGatewayRuntime()
  const scope = runtime.beginRequest()
  scope.markInference()
  runtime.startDraining()
  let idle = false
  const drained = runtime.whenIdle().then(() => {
    idle = true
  })
  await Promise.resolve()
  assert.equal(idle, false)
  assert.deepEqual(
    {
      draining: runtime.snapshot().draining,
      active_http: runtime.snapshot().active_http,
      active_inference: runtime.snapshot().active_inference,
    },
    { draining: true, active_http: 1, active_inference: 1 },
  )
  scope.release()
  scope.release()
  await drained
  assert.equal(idle, true)
  assert.equal(runtime.snapshot().active_http, 0)
  assert.equal(runtime.snapshot().active_inference, 0)
  runtime.stop()
})
