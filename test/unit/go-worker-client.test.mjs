import test from 'node:test'
import assert from 'node:assert/strict'
import http from 'node:http'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import {
  callGoWorker,
  finalizeWorkerPayload,
  streamGoWorker,
  workerHealth,
  usageFromSseEvent,
  isDownstreamCommitEvent,
  isUpstreamAcceptedEvent,
  restoreUncommittedHop,
  semanticStatusForStreamError,
} from '../../src/lib/transport/go-worker-client.mjs'
import { extractOpenaiUsage } from '../../src/lib/protocol/openai-usage.mjs'
import { callRustKernel, rustKernelHealth } from '../../src/lib/transport/rust-kernel-client.mjs'
import { forwardApi, readApiJson } from '../../src/lib/transport/api-kernel-client.mjs'

test('restoreUncommittedHop keeps a structured upstream code', () => {
  const restored = restoreUncommittedHop({
    ok: false,
    status: 200,
    committed: false,
    terminalState: 'incomplete',
    body: {
      type: 'error',
      error: { type: 'api_error', code: 'upstream_stream_incomplete', message: 'job idle timeout' },
    },
  })
  assert.equal(restored.body.error.code, 'upstream_stream_incomplete')
  assert.notEqual(restored.body.error.code, 'empty_response')
})

test('a coded kernel stream error keeps its code and the status it names', () => {
  const restored = restoreUncommittedHop({
    ok: false,
    status: 200,
    committed: false,
    terminalState: 'incomplete',
    headers: {},
    body: {
      type: 'error',
      error: {
        type: 'api_error',
        code: 'kernel_unavailable',
        status: 503,
        message: 'native cli host unavailable',
      },
    },
  })
  assert.equal(restored.status, 503)
  assert.equal(restored.body.error.code, 'kernel_unavailable')

  const limited = restoreUncommittedHop({
    ok: false,
    status: 200,
    committed: false,
    headers: {},
    body: {
      type: 'error',
      error: {
        type: 'rate_limit_error',
        code: 'upstream_rate_limit',
        status: 429,
        retry_after: '42',
        message: 'slow down',
      },
    },
  })
  assert.equal(limited.status, 429)
  assert.equal(limited.headers['retry-after'], '42')
})

test('semanticStatusForStreamError reads outer Codex status 500 instead of defaulting to 502', () => {
  assert.equal(
    semanticStatusForStreamError({
      type: 'error',
      status: 500,
      error: {
        type: 'server_error',
        code: 'server_error',
        message: 'An error occurred while processing your request...',
      },
    }),
    500,
  )
  assert.equal(
    semanticStatusForStreamError({
      type: 'error',
      error: { type: 'api_error', code: 'kernel_unavailable', status: 503, message: 'named' },
    }),
    503,
  )
  assert.equal(
    semanticStatusForStreamError({
      type: 'error',
      error: { type: 'server_error', code: 'server_error', message: 'no explicit status' },
    }),
    502,
  )
})

test('semanticStatusForStreamError restores a kernel-flattened content policy error to 403', () => {
  assert.equal(
    semanticStatusForStreamError({
      type: 'error',
      status: 502,
      error: {
        type: 'api_error',
        code: 'cyber_policy',
        message: 'This content was flagged for possible cybersecurity risk.',
      },
    }),
    403,
  )
})

test('restoreUncommittedHop reports Codex outer status 500 rather than 502', () => {
  const restored = restoreUncommittedHop({
    ok: false,
    status: 200,
    committed: false,
    terminalState: 'incomplete',
    headers: {},
    body: {
      type: 'error',
      status: 500,
      error: {
        type: 'server_error',
        code: 'server_error',
        message: 'An error occurred while processing your request...',
      },
    },
  })
  assert.equal(restored.status, 500)
  assert.equal(restored.streamError, true)
  assert.equal(restored.terminalState, 'rejected')
  assert.equal(restored.body.error.code, 'server_error')
})

test('setup-token worker envelope is inference-only', () => {
  const out = finalizeWorkerPayload({
    body: { model: 'claude-sonnet-5', messages: [{ role: 'user', content: 'hi' }] },
    reqHeaders: { 'user-agent': 'kin-console-test/1.0' },
    exec: { homeDir: '', vm: { claude: { mode: 'setup-token', scope: 'user:inference' } } },
    identity: null,
  })
  assert.equal(out.headers['user-agent'], 'kin-inference/1.0')
  assert.doesNotMatch(String(out.headers['anthropic-beta'] || ''), /claude-code-20250219/)
  assert.match(String(out.headers['anthropic-beta'] || ''), /oauth-2025-04-20/)
})

test('cli-hop envelope keeps role=system turns that the VM betas do not declare', () => {
  const body = {
    model: 'claude-opus-5-5',
    system: [{ type: 'text', text: 'main prompt' }],
    messages: [
      { role: 'user', content: 'u1' },
      { role: 'system', content: 'reminder' },
      { role: 'assistant', content: 'a1' },
      { role: 'user', content: 'u2' },
    ],
  }
  const out = finalizeWorkerPayload({
    body,
    reqHeaders: {},
    // Request fields and their beta gates share the existing CLI body channel.
    exec: { homeDir: '', vm: { claude: { mode: 'setup-token', scope: 'user:inference' } } },
    identity: null,
    cliHop: true,
  })
  assert.ok(out.body.betas.includes('mid-conversation-system-2026-04-07'))
  assert.deepEqual(
    out.body.messages.map((message) => message.role),
    ['user', 'system', 'assistant', 'user'],
  )
  assert.deepEqual(out.body.system, body.system)
})

test('setup-token cli-hop preserves updates with its beta and isolates later requests', () => {
  const exec = { homeDir: '', vm: { claude: { mode: 'setup-token', scope: 'user:inference' } } }
  const updates = finalizeWorkerPayload({
    body: {
      model: 'claude-opus-5-5',
      thinking: { type: 'adaptive', display: 'updates' },
      messages: [{ role: 'user', content: 'hi' }],
    },
    reqHeaders: {},
    exec,
    identity: null,
    cliHop: true,
  })
  assert.equal(updates.body.thinking.type, 'adaptive')
  assert.equal(updates.body.thinking.display, 'updates')
  assert.ok(updates.body.betas.includes('thinking-display-updates-2026-08-18'))
  assert.ok(!updates.body.betas.includes('claude-code-20250219'))
  const summarized = finalizeWorkerPayload({
    body: {
      model: 'claude-opus-5-5',
      thinking: { type: 'adaptive', display: 'summarized' },
      messages: [{ role: 'user', content: 'hi' }],
    },
    reqHeaders: {},
    exec,
    identity: null,
    cliHop: true,
  })
  assert.equal(summarized.body.thinking.display, 'summarized')
  assert.ok(!summarized.body.betas.includes('thinking-display-updates-2026-08-18'))
})

const unix = process.platform !== 'win32'
const unixTest = unix ? test : test.skip

async function fixture(handler) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-go-client-'))
  const slot = path.join(root, 'vm-01')
  const runDir = path.join(slot, 'run')
  const homeDir = path.join(slot, 'cli-home')
  fs.mkdirSync(runDir, { recursive: true })
  fs.mkdirSync(homeDir, { recursive: true })
  fs.writeFileSync(path.join(runDir, 'internal.token'), 'internal-test\n', { mode: 0o600 })
  const socket = path.join(runDir, 'worker.sock')
  const server = http.createServer(handler)
  await new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen(socket, resolve)
  })
  return {
    exec: {
      vmId: 'vm-01',
      homeDir,
      vm: {
        runtime: {
          worker_socket: socket,
          worker_run_dir: runDir,
          worker_token_file: path.join(runDir, 'internal.token'),
        },
      },
    },
    async close() {
      await new Promise((resolve) => server.close(resolve))
      fs.rmSync(root, { recursive: true, force: true })
    },
  }
}

function writeData(res, event) {
  res.write(`data: ${JSON.stringify(event)}\n\n`)
}

function gate() {
  let release
  const ready = new Promise((resolve) => {
    release = resolve
  })
  return { ready, release }
}

unixTest('callGoWorker sends envelope over authenticated Unix socket', async () => {
  const fx = await fixture(async (req, res) => {
    assert.equal(req.headers['x-internal-token'], 'internal-test')
    assert.equal(req.headers['x-kin-internal-token'], 'internal-test')
    assert.equal(req.url, '/internal/v1/messages')
    const chunks = []
    for await (const chunk of req) chunks.push(chunk)
    const envelope = JSON.parse(Buffer.concat(chunks).toString('utf8'))
    assert.equal(envelope.body.model, 'claude-test')
    assert.equal(envelope.cache_ttl, '1h')
    assert.equal(envelope.preserve_cache_breakpoints, false)
    assert.equal(envelope.stream, false)
    assert.match(envelope.headers['user-agent'], /^claude-cli\//)
    res.setHeader('content-type', 'application/json')
    res.setHeader('x-terminal-state', 'verified')
    res.end(
      JSON.stringify({
        type: 'message',
        id: 'msg_test',
        role: 'assistant',
        content: [{ type: 'text', text: 'ok' }],
        usage: { input_tokens: 1, output_tokens: 1 },
      }),
    )
  })
  try {
    const result = await callGoWorker({
      exec: fx.exec,
      body: { model: 'claude-test', messages: [{ role: 'user', content: 'hi' }] },
      cacheTtl: '1h',
      reqHeaders: { 'user-agent': 'test-client' },
    })
    assert.equal(result.ok, true)
    assert.equal(result.terminalState, 'verified')
    assert.equal(result.body.content[0].text, 'ok')
  } finally {
    await fx.close()
  }
})

unixTest('Rust health and inference authenticate with the namespaced kernel header', async () => {
  const fx = await fixture(async (req, res) => {
    res.setHeader('content-type', 'application/json')
    if (req.headers['x-kin-internal-token'] !== 'internal-test') {
      res.writeHead(401).end(JSON.stringify({ ok: false, error: { code: 'internal_auth_failed' } }))
      return
    }
    if (req.url === '/internal/health') {
      res.end(JSON.stringify({ ok: true, engine: 'rust', healthy: true, ready_slots: 20 }))
      return
    }
    for await (const _chunk of req) {
    }
    res.end(JSON.stringify({ type: 'message', role: 'assistant', content: [{ type: 'text', text: 'ok' }] }))
  })
  try {
    fx.exec.vm.runtime.kernel_socket = fx.exec.vm.runtime.worker_socket
    const health = await rustKernelHealth(fx.exec)
    assert.equal(health.status, 200)
    assert.equal(health.ok, true)
    const result = await callRustKernel({
      exec: fx.exec,
      body: { model: 'claude-test', messages: [{ role: 'user', content: 'hello' }] },
    })
    assert.equal(result.ok, true)
    assert.equal(result.body.content[0].text, 'ok')
  } finally {
    await fx.close()
  }
})

unixTest('API kernel forwarding uses the header required by its Go authorize handler', async () => {
  const fx = await fixture(async (req, res) => {
    res.setHeader('content-type', 'application/json')
    if (req.headers['x-kin-internal-token'] !== 'internal-test') {
      res.writeHead(401).end(JSON.stringify({ ok: false, error: { code: 'internal_auth_failed' } }))
      return
    }
    assert.equal(req.url, '/internal/forward')
    for await (const _chunk of req) {
    }
    res.end(JSON.stringify({ ok: true }))
  })
  const previousSocket = process.env.KIN_API_KERNEL_SOCK
  const previousToken = process.env.KIN_API_KERNEL_TOKEN
  try {
    process.env.KIN_API_KERNEL_SOCK = fx.exec.vm.runtime.worker_socket
    process.env.KIN_API_KERNEL_TOKEN = fx.exec.vm.runtime.worker_token_file
    const response = await forwardApi({ cfg: {}, url: 'https://api.example.test/v1/messages', body: {} })
    assert.equal(response.statusCode, 200)
    assert.deepEqual(await readApiJson(response), { ok: true })
  } finally {
    if (previousSocket == null) delete process.env.KIN_API_KERNEL_SOCK
    else process.env.KIN_API_KERNEL_SOCK = previousSocket
    if (previousToken == null) delete process.env.KIN_API_KERNEL_TOKEN
    else process.env.KIN_API_KERNEL_TOKEN = previousToken
    await fx.close()
  }
})

unixTest('callGoWorker marks null TTL as client-owned cache breakpoints', async () => {
  const fx = await fixture(async (req, res) => {
    const chunks = []
    for await (const chunk of req) chunks.push(chunk)
    const envelope = JSON.parse(Buffer.concat(chunks).toString('utf8'))
    assert.equal(envelope.cache_ttl, null)
    assert.equal(envelope.preserve_cache_breakpoints, true)
    res.setHeader('content-type', 'application/json')
    res.end(JSON.stringify({ type: 'message', role: 'assistant', content: [{ type: 'text', text: 'ok' }] }))
  })
  try {
    const result = await callGoWorker({
      exec: fx.exec,
      body: { model: 'claude-test', messages: [{ role: 'user', content: 'hi' }] },
      cacheTtl: null,
    })
    assert.equal(result.ok, true)
  } finally {
    await fx.close()
  }
})

unixTest('streamGoWorker accepts message_stop before delayed EOF', async () => {
  const fx = await fixture((req, res) => {
    res.setHeader('content-type', 'text/event-stream')
    res.setHeader('trailer', 'x-terminal-state')
    res.write('event: message_start\ndata: {"type":"message_start","message":{}}\n\n')
    res.write('event: message_stop\ndata: {"type":"message_stop"}\n\n')
    res.addTrailers({ 'x-terminal-state': 'verified' })
    setTimeout(() => res.end(), 50)
  })
  try {
    const lines = []
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'claude-test', stream: true, messages: [{ role: 'user', content: 'hi' }] },
      onEvent: (line) => lines.push(line),
    })
    assert.equal(result.ok, true)
    assert.equal(result.terminalState, 'verified')
    assert.equal(result.committed, true)
    assert.ok(lines.some((line) => line.includes('message_stop')))
  } finally {
    await fx.close()
  }
})

unixTest('streamGoWorker exposes the kernel HTTP status before consuming the stream', async () => {
  const fx = await fixture((_req, res) => {
    res.statusCode = 503
    res.setHeader('content-type', 'application/json')
    res.end(JSON.stringify({ error: { code: 'no_capacity', message: 'credential busy' } }))
  })
  try {
    const responses = []
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'claude-test', stream: true, messages: [{ role: 'user', content: 'hi' }] },
      onResponse: (response) => responses.push(response),
    })
    assert.equal(result.status, 503)
    assert.equal(result.committed, false)
    assert.equal(responses.length, 1)
    assert.equal(responses[0].status, 503)
  } finally {
    await fx.close()
  }
})

unixTest('kernel metadata keeps the first-event deadline until OpenAI accepts the turn', async () => {
  const fx = await fixture(async (_req, res) => {
    res.setHeader('content-type', 'text/event-stream')
    writeData(res, { type: 'response_headers', headers: { 'x-request-id': 'upstream-pending' } })
    await new Promise((resolve) => setTimeout(resolve, 60))
    writeData(res, { type: 'response.created', response: { id: 'resp_after_metadata' } })
    writeData(res, { type: 'response.completed', response: { id: 'resp_after_metadata' } })
    res.end()
  })
  try {
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'gpt-5.5', stream: true, input: [] },
      timeoutMs: 250,
      idleTimeoutMs: 25,
      sseEventMode: 'responses-passthrough',
      onEvent: () => {},
    })
    assert.equal(result.ok, true)
    assert.equal(result.terminalState, 'verified')
  } finally {
    await fx.close()
  }
})

unixTest('idle timeout starts after a real upstream lifecycle event', async () => {
  const fx = await fixture(async (_req, res) => {
    res.setHeader('content-type', 'text/event-stream')
    writeData(res, { type: 'response.created', response: { id: 'resp_idle' } })
    await new Promise((resolve) => setTimeout(resolve, 1200))
    res.end()
  })
  try {
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'gpt-5.5', stream: true, input: [] },
      timeoutMs: 250,
      idleTimeoutMs: 100,
      sseEventMode: 'responses-passthrough',
      onEvent: () => {},
    })
    assert.equal(result.ok, false)
    assert.equal(result.transportError, true)
    assert.equal(result.body.error.code, 'worker_timeout')
    assert.match(result.body.error.message, /idle timeout after 100ms/)
  } finally {
    await fx.close()
  }
})

unixTest('overall stream deadline cannot be refreshed by kernel retry events', async () => {
  let hopId = null
  let cancelled = null
  const sawCancel = new Promise((resolve) => {
    cancelled = resolve
  })
  const fx = await fixture(async (req, res) => {
    const chunks = []
    for await (const chunk of req) chunks.push(chunk)
    const payload = JSON.parse(Buffer.concat(chunks).toString('utf8'))
    if (req.url === '/internal/v1/cancel') {
      res.setHeader('content-type', 'application/json')
      res.end('{"ok":true,"cancelled":true}')
      cancelled(payload.request_id)
      return
    }
    hopId = payload.request_id
    res.setHeader('content-type', 'text/event-stream')
    const ticker = setInterval(() => {
      writeData(res, { type: 'response.created', response: { id: 'retrying' } })
    }, 10)
    res.once('close', () => clearInterval(ticker))
  })
  try {
    const startedAt = Date.now()
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'gpt-5.5', stream: true, input: [] },
      timeoutMs: 500,
      idleTimeoutMs: 500,
      overallTimeoutMs: 80,
      sseEventMode: 'responses-passthrough',
      onEvent: () => {},
    })
    assert.equal(result.status, 504)
    assert.equal(result.body.error.code, 'kernel_retry_deadline')
    assert.equal(result.transportError, true)
    assert.ok(Date.now() - startedAt < 500)
    assert.equal(await sawCancel, hopId)
  } finally {
    await fx.close()
  }
})

unixTest('streamGoWorker names the aborted hop to the kernel cancel route', async () => {
  let hopId = null
  let cancelled = null
  const sawCancel = new Promise((resolve) => {
    cancelled = resolve
  })
  const fx = await fixture(async (req, res) => {
    const chunks = []
    for await (const chunk of req) chunks.push(chunk)
    const payload = JSON.parse(Buffer.concat(chunks).toString('utf8'))
    if (req.url === '/internal/v1/cancel') {
      res.setHeader('content-type', 'application/json')
      res.end('{"ok":true,"cancelled":true}')
      cancelled(payload.request_id)
      return
    }
    hopId = payload.request_id
    res.setHeader('content-type', 'text/event-stream')
    res.write('event: message_start\ndata: {"type":"message_start","message":{}}\n\n')
  })
  try {
    const controller = new AbortController()
    const pending = streamGoWorker({
      exec: fx.exec,
      body: { model: 'claude-test', stream: true, messages: [{ role: 'user', content: 'hi' }] },
      signal: controller.signal,
    })
    while (!hopId) await new Promise((resolve) => setTimeout(resolve, 5))
    controller.abort()
    const result = await pending
    assert.equal(result.body.error.code, 'client_cancelled')
    assert.equal(await sawCancel, hopId)
  } finally {
    await fx.close()
  }
})

unixTest('streamGoWorker accepts message_stop with usage trailers', async () => {
  const fx = await fixture((req, res) => {
    assert.equal(req.headers.te, 'trailers')
    res.setHeader('content-type', 'text/event-stream')
    res.setHeader('trailer', 'x-terminal-state, x-usage, x-model, x-stop-reason')
    res.write('data: {"type":"message_start","message":{"model":"claude-haiku-4-5-20251001"}}\n\n')
    res.write('data: {"type":"message_stop"}\n\n')
    res.addTrailers({
      'x-terminal-state': 'verified',
      'x-usage': JSON.stringify({ input_tokens: 12, output_tokens: 0 }),
      'x-model': 'claude-haiku-4-5-20251001',
      'x-stop-reason': 'end_turn',
    })
    res.end()
  })
  try {
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'claude-haiku-4-5-20251001', stream: true, messages: [{ role: 'user', content: 'hi' }] },
      onEvent: () => {},
    })
    assert.equal(result.ok, true)
    assert.equal(result.terminalState, 'verified')
    assert.equal(result.usage.input_tokens, 12)
    assert.equal(result.model, 'claude-haiku-4-5-20251001')
    assert.equal(result.stopReason, 'end_turn')
    assert.equal(result.ttftMs, null)
    assert.equal(result.firstTokenAt, null)
  } finally {
    await fx.close()
  }
})

unixTest('streamGoWorker does not stamp TTFT on metadata-only Responses complete', async () => {
  const fx = await fixture((req, res) => {
    res.setHeader('content-type', 'text/event-stream')
    res.setHeader('trailer', 'x-terminal-state, x-usage, x-model, x-stop-reason')
    writeData(res, { type: 'response.created', response: { id: 'resp_1', model: 'gpt-5.4' } })
    writeData(res, { type: 'response.in_progress', response: { id: 'resp_1' } })
    writeData(res, {
      type: 'response.completed',
      response: { id: 'resp_1', model: 'gpt-5.4', usage: { input_tokens: 12, output_tokens: 0 } },
    })
    res.addTrailers({
      'x-terminal-state': 'verified',
      'x-usage': JSON.stringify({ input_tokens: 12, output_tokens: 0 }),
      'x-model': 'gpt-5.4',
      'x-stop-reason': 'completed',
    })
    res.end()
  })
  try {
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'gpt-5.4', stream: true, input: [] },
      onEvent: () => {},
    })
    assert.equal(result.usage.input_tokens, 12)
    assert.equal(result.model, 'gpt-5.4')
    assert.equal(result.stopReason, 'completed')
    assert.equal(result.ttftMs, null)
    assert.equal(result.firstTokenAt, null)
  } finally {
    await fx.close()
  }
})

unixTest('streamGoWorker stamps TTFT on a name-only tool_use with no parameter delta', async () => {
  const fx = await fixture((req, res) => {
    res.setHeader('content-type', 'text/event-stream')
    writeData(res, { type: 'message_start', message: { model: 'claude-haiku-4-5-20251001' } })
    writeData(res, {
      type: 'content_block_start',
      index: 0,
      content_block: { type: 'tool_use', id: 'toolu_1', name: 'Read', input: {} },
    })
    writeData(res, { type: 'content_block_stop', index: 0 })
    writeData(res, { type: 'message_delta', delta: { stop_reason: 'tool_use' }, usage: { output_tokens: 12 } })
    writeData(res, { type: 'message_stop' })
    res.end()
  })
  try {
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'claude-haiku-4-5-20251001', stream: true, messages: [{ role: 'user', content: 'hi' }] },
      onEvent: () => {},
    })
    assert.equal(result.ok, true)
    assert.equal(result.stopReason, 'tool_use')
    assert.ok(result.ttftMs != null && result.ttftMs >= 0)
    assert.ok(Number.isFinite(result.firstTokenAt) && result.firstTokenAt > 1_000_000_000_000)
  } finally {
    await fx.close()
  }
})

unixTest('streamGoWorker stamps TTFT on a Responses function name, not response.created', async () => {
  const fx = await fixture((req, res) => {
    res.setHeader('content-type', 'text/event-stream')
    res.setHeader('trailer', 'x-terminal-state, x-model')
    writeData(res, { type: 'response.created', response: { id: 'resp_1', model: 'gpt-5.4' } })
    writeData(res, {
      type: 'response.output_item.added',
      item: { type: 'function_call', name: 'Read', arguments: '', call_id: 'c1' },
    })
    writeData(res, { type: 'response.completed', response: { id: 'resp_1', model: 'gpt-5.4' } })
    res.addTrailers({ 'x-terminal-state': 'verified', 'x-model': 'gpt-5.4' })
    res.end()
  })
  try {
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'gpt-5.4', stream: true, input: [] },
      onEvent: () => {},
    })
    assert.ok(result.ttftMs != null && result.ttftMs >= 0)
    assert.ok(Number.isFinite(result.firstTokenAt) && result.firstTokenAt > 1_000_000_000_000)
  } finally {
    await fx.close()
  }
})

unixTest('streamGoWorker stamps firstTokenAt on the emitted tool name, not later args', async () => {
  const sawName = gate()
  let argsWrittenAt = null
  const fx = await fixture(async (req, res) => {
    res.setHeader('content-type', 'text/event-stream')
    writeData(res, { type: 'message_start', message: { model: 'claude-haiku-4-5-20251001' } })
    writeData(res, {
      type: 'content_block_start',
      index: 0,
      content_block: { type: 'tool_use', id: 'toolu_1', name: 'Read', input: {} },
    })
    await sawName.ready
    argsWrittenAt = Date.now()
    writeData(res, {
      type: 'content_block_delta',
      index: 0,
      delta: { type: 'input_json_delta', partial_json: '{"path":"a"}' },
    })
    writeData(res, { type: 'message_delta', delta: { stop_reason: 'tool_use' } })
    writeData(res, { type: 'message_stop' })
    res.end()
  })
  try {
    let nameSeenAt = null
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'claude-haiku-4-5-20251001', stream: true, messages: [{ role: 'user', content: 'hi' }] },
      onEvent: (line) => {
        if (nameSeenAt == null && line.includes('"name":"Read"')) {
          nameSeenAt = Date.now()
          sawName.release()
        }
      },
    })
    assert.ok(result.ttftMs != null && result.ttftMs >= 0)
    assert.ok(Number.isFinite(result.firstTokenAt))
    assert.ok(result.firstTokenAt <= nameSeenAt)
    assert.ok(result.firstTokenAt <= argsWrittenAt)
  } finally {
    await fx.close()
  }
})

unixTest('streamGoWorker stamps TTFT on text and thinking deltas', async () => {
  for (const delta of [
    { type: 'text_delta', text: 'hello' },
    { type: 'thinking_delta', thinking: 'hmm' },
  ]) {
    const fx = await fixture((req, res) => {
      res.setHeader('content-type', 'text/event-stream')
      writeData(res, { type: 'message_start', message: { model: 'claude-haiku-4-5-20251001' } })
      writeData(res, {
        type: 'content_block_start',
        index: 0,
        content_block: { type: delta.text != null ? 'text' : 'thinking', text: '', thinking: '' },
      })
      writeData(res, { type: 'content_block_delta', index: 0, delta })
      writeData(res, { type: 'message_delta', delta: { stop_reason: 'end_turn' }, usage: { output_tokens: 1 } })
      writeData(res, { type: 'message_stop' })
      res.end()
    })
    try {
      const result = await streamGoWorker({
        exec: fx.exec,
        body: { model: 'claude-haiku-4-5-20251001', stream: true, messages: [{ role: 'user', content: 'hi' }] },
        onEvent: () => {},
      })
      assert.equal(result.ok, true, delta.type)
      assert.ok(result.ttftMs != null && result.ttftMs >= 0, delta.type)
      assert.ok(Number.isFinite(result.firstTokenAt), delta.type)
    } finally {
      await fx.close()
    }
  }
})

unixTest('streamGoWorker keeps observed firstTokenAt when the client cancels', async () => {
  const sawText = gate()
  const fx = await fixture(async (req, res) => {
    if (req.url === '/internal/v1/cancel') {
      res.setHeader('content-type', 'application/json')
      res.end('{"ok":true,"cancelled":true}')
      return
    }
    res.setHeader('content-type', 'text/event-stream')
    writeData(res, { type: 'message_start', message: {} })
    writeData(res, { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'hi' } })
    await sawText.ready
  })
  try {
    const controller = new AbortController()
    const pending = streamGoWorker({
      exec: fx.exec,
      body: { model: 'claude-test', stream: true, messages: [{ role: 'user', content: 'hi' }] },
      signal: controller.signal,
      onEvent: (line) => {
        if (String(line).includes('text_delta')) sawText.release()
      },
    })
    await sawText.ready
    controller.abort()
    const result = await pending
    assert.equal(result.body.error.code, 'client_cancelled')
    assert.ok(result.ttftMs != null && result.ttftMs >= 0)
    assert.ok(Number.isFinite(result.firstTokenAt))
  } finally {
    await fx.close()
  }
})

unixTest('streamGoWorker keeps observed firstTokenAt when a later error ends the hop', async () => {
  const fx = await fixture((req, res) => {
    res.setHeader('content-type', 'text/event-stream')
    writeData(res, { type: 'message_start', message: { model: 'claude-haiku-4-5-20251001' } })
    writeData(res, { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'hi' } })
    writeData(res, { type: 'error', error: { type: 'api_error', message: 'upstream failed' } })
    res.end()
  })
  try {
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'claude-haiku-4-5-20251001', stream: true, messages: [{ role: 'user', content: 'hi' }] },
      onEvent: () => {},
    })
    assert.equal(result.ok, false)
    assert.ok(result.ttftMs != null && result.ttftMs >= 0)
    assert.ok(Number.isFinite(result.firstTokenAt))
  } finally {
    await fx.close()
  }
})

unixTest('streamGoWorker keeps rate-limit trailers after message_stop', async () => {
  const fx = await fixture((req, res) => {
    res.setHeader('content-type', 'text/event-stream')
    res.setHeader('trailer', 'x-terminal-state, x-rate-limit-headers')
    res.write('data: {"type":"message_start","message":{}}\n\n')
    res.write('data: {"type":"message_stop"}\n\n')
    res.addTrailers({
      'x-terminal-state': 'verified',
      'x-rate-limit-headers': JSON.stringify({
        'anthropic-ratelimit-unified-5h-utilization': '0.81',
        'set-cookie': 'nope',
      }),
    })
    res.end()
  })
  try {
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'claude-sonnet-5', stream: true, messages: [{ role: 'user', content: 'hi' }] },
      onEvent: () => {},
    })
    assert.equal(result.ok, true)
    assert.equal(result.terminalState, 'verified')
    assert.equal(result.headers['anthropic-ratelimit-unified-5h-utilization'], '0.81')
    assert.equal(result.headers['set-cookie'], undefined)
  } finally {
    await fx.close()
  }
})

unixTest('streamGoWorker restores a streamed plan-limit error to 429 with a parsed reset', async () => {
  // Kernel cli-hop: HTTP 200, then `event: error` (sub2api sseStreamErrorEventError).
  const fx = await fixture((req, res) => {
    res.setHeader('content-type', 'text/event-stream')
    res.write('event: message_start\ndata: {"type":"message_start","message":{"content":[]}}\n\n')
    res.write(
      'event: error\ndata: {"type":"error","error":{"type":"api_error","message":"provider error: You\'ve hit your limit · resets 11am (America/New_York)"}}\n\n',
    )
    res.end()
  })
  try {
    const lines = []
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'claude-haiku-4-5', stream: true, messages: [{ role: 'user', content: 'hi' }] },
      onEvent: (line) => lines.push(line),
    })
    assert.equal(result.ok, false)
    assert.equal(result.committed, false)
    assert.equal(result.status, 429)
    assert.equal(result.terminalState, 'rejected')
    assert.equal(result.streamError, true)
    assert.equal(result.headers['anthropic-ratelimit-unified-5h-status'], 'rejected')
    assert.ok(Number(result.headers['anthropic-ratelimit-unified-5h-reset']) * 1000 > Date.now())
    assert.equal(lines.length, 0)
  } finally {
    await fx.close()
  }
})

unixTest('streamGoWorker restores a streamed overloaded_error to 529', async () => {
  const fx = await fixture((req, res) => {
    res.setHeader('content-type', 'text/event-stream')
    res.write('event: error\ndata: {"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}\n\n')
    res.end()
  })
  try {
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'claude-haiku-4-5', stream: true, messages: [{ role: 'user', content: 'hi' }] },
      onEvent: () => {},
    })
    assert.equal(result.status, 529)
    assert.equal(result.terminalState, 'rejected')
  } finally {
    await fx.close()
  }
})

unixTest('streamGoWorker accepts an empty message_stop terminal', async () => {
  const fx = await fixture((req, res) => {
    res.setHeader('content-type', 'text/event-stream')
    res.write('data: {"type":"message_start","message":{"content":[]}}\n\n')
    res.write('data: {"type":"message_stop"}\n\n')
    res.end()
  })
  try {
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'claude-haiku-4-5', stream: true, messages: [{ role: 'user', content: 'hi' }] },
      onEvent: () => {},
    })
    assert.equal(result.ok, true)
    assert.equal(result.terminalState, 'verified')
    assert.equal(result.sawMessageStop, true)
  } finally {
    await fx.close()
  }
})

unixTest('streamGoWorker does not turn a generic stream error into HTTP 502', async () => {
  const fx = await fixture((req, res) => {
    res.setHeader('content-type', 'text/event-stream')
    res.write('event: error\ndata: {"type":"error","error":{"type":"api_error","message":"provider error"}}\n\n')
    res.end()
  })
  try {
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'claude-haiku-4-5', stream: true, messages: [{ role: 'user', content: 'hi' }] },
      onEvent: () => {},
    })
    assert.equal(result.status, 200)
    assert.equal(result.body.error.code, 'empty_response')
    assert.equal(result.terminalState, 'incomplete')
  } finally {
    await fx.close()
  }
})

unixTest('streamGoWorker preserves organization permission denial before downstream commit', async () => {
  const message =
    'provider error: provider error: Your organization does not have access to Claude. Please login again or contact your administrator.'
  const fx = await fixture((req, res) => {
    res.setHeader('content-type', 'text/event-stream')
    res.setHeader('trailer', 'x-terminal-state')
    res.write(`event: error\ndata: ${JSON.stringify({ type: 'error', error: { type: 'api_error', message } })}\n\n`)
    res.addTrailers({ 'x-terminal-state': 'incomplete' })
    res.end()
  })
  try {
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'claude-haiku-4-5', stream: true, messages: [{ role: 'user', content: 'hi' }] },
      onCommit: () => assert.fail('permission denial must not commit the downstream'),
      onEvent: () => {},
    })
    assert.equal(result.status, 403)
    assert.equal(result.body.error.message, message)
    assert.equal(result.terminalState, 'rejected')
    assert.equal(result.committed, false)
  } finally {
    await fx.close()
  }
})

unixTest('callGoWorker restores organization permission denial from a kernel provider error', async () => {
  const message =
    'provider error: Your organization does not have access to Claude. Please login again or contact your administrator.'
  const fx = await fixture((req, res) => {
    res.statusCode = 502
    res.setHeader('content-type', 'application/json')
    res.end(JSON.stringify({ type: 'error', error: { type: 'worker_error', code: 'provider_error', message } }))
  })
  try {
    const result = await callGoWorker({
      exec: fx.exec,
      body: { model: 'claude-haiku-4-5', messages: [{ role: 'user', content: 'hi' }] },
    })
    assert.equal(result.status, 403)
    assert.equal(result.body.error.message, message)
    assert.equal(result.terminalState, 'rejected')
  } finally {
    await fx.close()
  }
})

unixTest('callGoWorker restores a kernel 502 provider_error plan limit to 429', async () => {
  const fx = await fixture((req, res) => {
    res.statusCode = 502
    res.setHeader('content-type', 'application/json')
    res.end(
      JSON.stringify({
        type: 'error',
        error: { type: 'worker_error', code: 'provider_error', message: "provider error: You've hit your limit" },
      }),
    )
  })
  try {
    const result = await callGoWorker({
      exec: fx.exec,
      body: { model: 'claude-haiku-4-5', messages: [{ role: 'user', content: 'hi' }] },
    })
    assert.equal(result.status, 429)
    assert.equal(result.headers['anthropic-ratelimit-unified-5h-status'], 'rejected')
  } finally {
    await fx.close()
  }
})

unixTest('streamGoWorker scrapes usage from SSE when trailers are missing', async () => {
  const fx = await fixture((req, res) => {
    res.setHeader('content-type', 'text/event-stream')
    res.write(
      'data: {"type":"message_start","message":{"model":"claude-sonnet-5","usage":{"input_tokens":80,"cache_read_input_tokens":20}}}\n\n',
    )
    res.write('data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":"ok"}}\n\n')

    res.write('data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":6}}\n\n')
    res.write('data: {"type":"message_stop"}\n\n')
    res.end()
  })
  try {
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'claude-sonnet-5', stream: true, messages: [{ role: 'user', content: 'hi' }] },
      onEvent: () => {},
    })
    assert.equal(result.usage.input_tokens, 80)
    assert.equal(result.usage.output_tokens, 6)
    assert.equal(result.usage.cache_read_input_tokens, 20)
    assert.equal(result.model, 'claude-sonnet-5')
    assert.equal(result.stopReason, 'end_turn')
  } finally {
    await fx.close()
  }
})

unixTest('streamGoWorker merges SSE cache details into a totals-only usage trailer', async () => {
  const fx = await fixture((req, res) => {
    res.setHeader('content-type', 'text/event-stream')
    res.setHeader('trailer', 'x-terminal-state, x-usage')
    res.write('data: {"type":"response.created","response":{"id":"resp_1","model":"gpt-5.4"}}\n\n')
    res.write('data: {"type":"response.output_text.delta","delta":"hi"}\n\n')
    res.write(
      'data: {"type":"response.completed","response":{"id":"resp_1","model":"gpt-5.4","usage":{"input_tokens":120,"output_tokens":9,"total_tokens":129,"input_tokens_details":{"cached_tokens":8}}}}\n\n',
    )
    res.addTrailers({
      'x-terminal-state': 'verified',
      'x-usage': JSON.stringify({ input_tokens: 120, output_tokens: 9 }),
    })
    res.end()
  })
  try {
    let completedEvent = null
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'gpt-5.4', stream: true, input: [] },
      onEvent: (_line, event) => {
        if (event?.type === 'response.completed') completedEvent = event
      },
    })
    assert.equal(result.ok, true)
    assert.equal(result.terminalState, 'verified')
    assert.equal(completedEvent?.response?.id, 'resp_1')
    assert.equal(result.usage.input_tokens, 120)
    assert.equal(result.usage.output_tokens, 9)
    assert.equal(result.usage.input_tokens_details.cached_tokens, 8)
    assert.equal(extractOpenaiUsage(result.usage).cached_tokens, 8)
  } finally {
    await fx.close()
  }
})

unixTest('Responses passthrough uses lightweight delta events and still parses terminal usage', async () => {
  const delta = 'x'.repeat(32 * 1024)
  const fx = await fixture((req, res) => {
    res.setHeader('content-type', 'text/event-stream')
    writeData(res, { type: 'response.created', response: { id: 'resp_fast', model: 'gpt-5.4' } })
    writeData(res, { type: 'response.output_text.delta', delta })
    writeData(res, {
      type: 'response.completed',
      response: { id: 'resp_fast', model: 'gpt-5.4', usage: { input_tokens: 50, output_tokens: 4 } },
    })
    res.end()
  })
  try {
    let lightweightDelta = null
    let completed = null
    let rawDelta = ''
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'gpt-5.4', stream: true, input: [] },
      sseEventMode: 'responses-passthrough',
      onEvent: (line, event) => {
        if (event?.type === 'response.output_text.delta') {
          lightweightDelta = event
          rawDelta = line
        }
        if (event?.type === 'response.completed') completed = event
      },
    })
    assert.equal(result.ok, true)
    assert.deepEqual(lightweightDelta, { type: 'response.output_text.delta', delta: '1' })
    assert.match(rawDelta, /"delta":"x+/)
    assert.equal(completed?.response?.id, 'resp_fast')
    assert.equal(result.usage.input_tokens, 50)
    assert.equal(result.usage.output_tokens, 4)
    assert.ok(result.ttftMs != null && result.ttftMs >= 0)
  } finally {
    await fx.close()
  }
})

unixTest('Responses passthrough batches complete SSE lines from one socket chunk', async () => {
  const frames = [
    'event: response',
    'data: {"type":"response.created","response":{"id":"resp_batch"}}',
    '',
    'event: response',
    'data: {"type":"response.output_text.delta","delta":"hello"}',
    '',
    'event: response',
    'data: {"type":"response.completed","response":{"id":"resp_batch","usage":{"output_tokens":1}}}',
    '',
    '',
  ].join('\n')
  const fx = await fixture((req, res) => {
    res.setHeader('content-type', 'text/event-stream')
    res.end(frames)
  })
  try {
    const batches = []
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'gpt-5.4', stream: true, input: [] },
      sseEventMode: 'responses-passthrough',
      onEvent: () => assert.fail('batch mode must not also call the per-line callback'),
      onEventBatch: (entries) => batches.push(entries),
    })
    const entries = batches.flat()
    assert.equal(result.ok, true)
    assert.equal(batches.length, 1)
    assert.deepEqual(
      entries.map((entry) => entry.line),
      frames.split('\n').slice(0, -1),
    )
    assert.deepEqual(
      entries.filter((entry) => entry.event).map((entry) => entry.event.type),
      ['response.created', 'response.output_text.delta', 'response.completed'],
    )
    assert.deepEqual(entries.find((entry) => entry.event?.type === 'response.output_text.delta').event, {
      type: 'response.output_text.delta',
      delta: '1',
    })
  } finally {
    await fx.close()
  }
})

test('terminal metadata is not a downstream commit', () => {
  assert.equal(isDownstreamCommitEvent({ type: 'message_start', message: {} }), false)
  assert.equal(isDownstreamCommitEvent({ type: 'error', error: { message: 'Connection error' } }), false)
  assert.equal(isDownstreamCommitEvent({ type: 'message_stop' }), false)
  assert.equal(isDownstreamCommitEvent({ type: 'message_delta', delta: { stop_reason: 'end_turn' } }), false)
  assert.equal(isDownstreamCommitEvent({ type: 'message_delta', delta: {} }), false)
  assert.equal(
    isDownstreamCommitEvent({ type: 'content_block_delta', delta: { type: 'text_delta', text: 'hi' } }),
    true,
  )
  assert.equal(
    isDownstreamCommitEvent({
      type: 'content_block_start',
      content_block: { type: 'server_tool_use', id: 'srvtoolu_1', name: 'web_search' },
    }),
    true,
  )
})

test('only a real upstream lifecycle frame accepts a streamed request', () => {
  assert.equal(isUpstreamAcceptedEvent({ type: 'error', error: { message: 'rejected' } }), false)
  assert.equal(isUpstreamAcceptedEvent({ type: 'response_headers', headers: {} }), false)
  assert.equal(isUpstreamAcceptedEvent({ type: 'response.created', response: { id: 'resp_1' } }), true)
  assert.equal(isDownstreamCommitEvent({ type: 'response.created', response: { id: 'resp_1' } }), false)
  assert.equal(isUpstreamAcceptedEvent({ type: 'message_start', message: {} }), false)
})

unixTest('streamGoWorker keeps response.created uncommitted when the next event is a server error', async () => {
  const fx = await fixture((req, res) => {
    res.setHeader('content-type', 'text/event-stream')
    writeData(res, { type: 'response.created', response: { id: 'resp_rejected' } })
    writeData(res, {
      type: 'error',
      status: 500,
      error: { type: 'server_error', code: 'server_error', message: 'Upstream rejected the response' },
    })
    res.end()
  })
  try {
    const lines = []
    let commits = 0
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'gpt-5.5', stream: true, input: [] },
      sseEventMode: 'responses-passthrough',
      onCommit: () => {
        commits += 1
      },
      onEvent: (line) => lines.push(line),
    })
    assert.equal(result.ok, false)
    assert.equal(result.status, 500)
    assert.equal(result.committed, false)
    assert.equal(result.body.error.code, 'server_error')
    assert.equal(commits, 0)
    assert.deepEqual(lines, [])
  } finally {
    await fx.close()
  }
})

unixTest('streamGoWorker assembles text+stop_reason even without message_stop', async () => {
  const fx = await fixture((req, res) => {
    res.setHeader('content-type', 'text/event-stream')
    res.write('data: {"type":"message_start","message":{"type":"message","role":"assistant","content":[]}}\n\n')
    res.write('data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}\n\n')
    res.write('data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}}\n\n')
    res.write('data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}\n\n')
    res.end()
  })
  try {
    const lines = []
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'claude-sonnet-5', stream: true, messages: [{ role: 'user', content: 'hi' }] },
      onEvent: (line) => lines.push(line),
    })
    assert.equal(result.ok, true)
    assert.equal(result.committed, true)
    assert.equal(result.terminalState, 'verified')
    assert.equal(result.stopReason, 'end_turn')
    assert.equal(result.body.content[0].text, 'hello')
    assert.ok(lines.some((line) => line.includes('text_delta')))
  } finally {
    await fx.close()
  }
})

unixTest('streamGoWorker does not commit or forward a Connection error after message_start', async () => {
  const fx = await fixture((req, res) => {
    res.setHeader('content-type', 'text/event-stream')
    res.write('data: {"type":"message_start","message":{}}\n\n')
    res.write(
      'data: {"type":"error","error":{"type":"api_error","message":"provider error: provider error: Connection error."}}\n\n',
    )
    res.end()
  })
  try {
    const lines = []
    const result = await streamGoWorker({
      exec: fx.exec,
      body: { model: 'claude-haiku-4-5', stream: true, messages: [{ role: 'user', content: 'hi' }] },
      onEvent: (line) => lines.push(line),
    })
    assert.equal(result.committed, false)
    assert.equal(result.ok, false)
    assert.match(String(result.body?.error?.message || ''), /Connection error/)
    assert.equal(lines.length, 0)
  } finally {
    await fx.close()
  }
})

test('usageFromSseEvent reads Anthropic message_start and message_delta', () => {
  assert.deepEqual(
    usageFromSseEvent({ type: 'message_start', message: { usage: { input_tokens: 80, cache_read_input_tokens: 20 } } }),
    { input_tokens: 80, cache_read_input_tokens: 20 },
  )
  assert.deepEqual(usageFromSseEvent({ type: 'message_delta', usage: { output_tokens: 6 } }), { output_tokens: 6 })
  assert.equal(usageFromSseEvent({ type: 'content_block_delta' }), null)
})

test('usageFromSseEvent reads OpenAI Responses nested usage', () => {
  assert.deepEqual(
    usageFromSseEvent({
      type: 'response.completed',
      response: { usage: { input_tokens: 41, output_tokens: 12, input_tokens_details: { cached_tokens: 8 } } },
    }),
    { input_tokens: 41, output_tokens: 12, input_tokens_details: { cached_tokens: 8 } },
  )
})

test('workerHealth fails closed when socket is absent', async () => {
  const result = await workerHealth(
    {
      homeDir: '/tmp/not-present/cli-home',
      vm: { runtime: { worker_socket: '/tmp/not-present/worker.sock' } },
    },
    { timeoutMs: 20 },
  )
  assert.equal(result.ok, false)
})
