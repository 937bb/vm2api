import { test } from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { EventEmitter } from 'node:events'
import {
  isRetryableCodexTransport,
  pickCodexCandidates,
  responseIdFromSseLine,
  runCodexKernelHop,
  handleCodexProtocol,
} from '../../src/lib/protocol/handle-codex.mjs'
import { persistCodexUsage, getVm, syncCodexQuotaSchedule } from '../../src/lib/vm/vm-registry.mjs'
import { StickyRouter } from '../../src/lib/pool/sticky-router.mjs'

test('502 upstream_transport is retryable before commit', () => {
  assert.equal(
    isRetryableCodexTransport({
      ok: false,
      status: 502,
      body: { error: { code: 'upstream_transport' } },
    }),
    true,
  )
  assert.equal(isRetryableCodexTransport({ ok: false, transportError: true, status: 0, committed: false }), true)
  assert.equal(
    isRetryableCodexTransport({
      ok: false,
      status: 502,
      committed: true,
      body: { error: { code: 'upstream_transport' } },
    }),
    false,
  )
  assert.equal(isRetryableCodexTransport({ ok: true, status: 200 }), false)
  assert.equal(isRetryableCodexTransport({ ok: false, status: 401, body: { error: { code: 'oauth_revoked' } } }), false)
})

test('runCodexKernelHop retries a silent first-hop 502 only when explicitly enabled', async () => {
  let n = 0
  const events = []
  const result = await runCodexKernelHop({
    hop: async ({ onEvent }) => {
      n += 1
      if (n === 1) return { ok: false, status: 502, body: { error: { code: 'upstream_transport' } } }
      await onEvent('data: {"type":"response.completed"}\n')
      return { ok: true, status: 200, terminalState: 'verified' }
    },
    onEvent: async (line) => events.push(line),
    retryTransport: true,
  })
  assert.equal(n, 2)
  assert.equal(result.ok, true)
  assert.equal(result.transport_retried, true)
  assert.equal(events.length, 1)
})

test('runCodexKernelHop does not replay an ambiguous transport failure by default', async () => {
  let n = 0
  const result = await runCodexKernelHop({
    hop: async () => {
      n += 1
      return { ok: false, status: 502, body: { error: { code: 'upstream_transport' } } }
    },
  })
  assert.equal(n, 1)
  assert.equal(result.transport_retried, undefined)
})

test('runCodexKernelHop does not retry after SSE has started', async () => {
  let n = 0
  const result = await runCodexKernelHop({
    hop: async ({ onEvent }) => {
      n += 1
      await onEvent('data: {"type":"response.created"}\n')
      return { ok: false, status: 502, body: { error: { code: 'upstream_transport' } } }
    },
  })
  assert.equal(n, 1)
  assert.equal(result.ok, false)
  assert.equal(result.transport_retried, undefined)
})

test('runCodexKernelHop does not retry after a batched SSE callback', async () => {
  let n = 0
  const batches = []
  const result = await runCodexKernelHop({
    hop: async ({ onEventBatch }) => {
      n += 1
      await onEventBatch([{ line: 'data: {"type":"response.created"}', event: { type: 'response.created' } }])
      return { ok: false, status: 502, body: { error: { code: 'upstream_transport' } } }
    },
    onEventBatch: async (entries) => batches.push(entries),
    retryTransport: true,
  })
  assert.equal(n, 1)
  assert.equal(result.ok, false)
  assert.equal(result.transport_retried, undefined)
  assert.equal(batches.length, 1)
})

test('runCodexKernelHop never retries after its downstream retry signal aborts', async () => {
  const controller = new AbortController()
  let n = 0
  const result = await runCodexKernelHop({
    hop: async () => {
      n += 1
      controller.abort()
      return { ok: false, status: 502, body: { error: { code: 'upstream_transport' } } }
    },
    retrySignal: controller.signal,
    retryTransport: true,
  })
  assert.equal(n, 1)
  assert.equal(result.transport_retried, undefined)
})

function writeGptVm(root, id) {
  const dir = path.join(root, 'vms')
  fs.mkdirSync(path.join(dir, id), { recursive: true })
  fs.writeFileSync(
    path.join(dir, `${id}.json`),
    JSON.stringify({
      id,
      name: id,
      platform: 'openai',
      family: 'codex',
      schedulable: true,
      status: 'running',
      has_token: true,
    }),
  )
  fs.writeFileSync(
    path.join(dir, id, 'codex-credentials.json'),
    JSON.stringify({
      accounts: [{ id: `${id}-acc`, access_token: 'at', refresh_token: 'rt', chatgpt_account_id: id }],
    }),
  )
}

test('quota 429 hops the next GPT slot and records 5h/7d extra', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-hop-'))
  writeGptVm(root, 'vm-gpt-a')
  writeGptVm(root, 'vm-gpt-b')
  const seen = []
  const res = {
    headersSent: false,
    statusCode: 0,
    body: null,
    ended: false,
    write() {},
    end() {
      this.ended = true
    },
  }
  const logBag = {}
  const out = await handleCodexProtocol({
    req: { headers: {}, apiKeyKind: 'user' },
    res,
    protocol: 'openai.responses',
    ctx: { body: { model: 'gpt-5.4', input: 'hi', stream: false } },
    inbound: { stream: false },
    logBag,
    stats: { errors: 0, requests: 0, by_route: {} },
    json: (_res, status, body) => {
      res.statusCode = status
      res.body = body
      return body
    },
    writeSSEHeaders() {
      res.headersSent = true
    },
    routing: {},
    projectRoot: root,
    ops: {
      writeCodexKernelConfig() {},
      ensureCodexKernel: async () => ({ ok: true }),
      streamCodexKernel: async ({ exec }) => {
        seen.push(exec.vmId)
        if (exec.vmId === 'vm-gpt-a') {
          return {
            ok: false,
            status: 429,
            committed: false,
            headers: {
              'x-codex-primary-used-percent': '100',
              'x-codex-primary-window-minutes': '300',
              'x-codex-primary-reset-after-seconds': '60',
            },
            body: { error: { type: 'api_error', code: 'usage_limit_reached', message: '5h' } },
          }
        }
        return {
          ok: true,
          status: 200,
          terminalState: 'verified',
          headers: {
            'x-codex-primary-used-percent': '8',
            'x-codex-primary-window-minutes': '300',
            'x-codex-secondary-used-percent': '3',
            'x-codex-secondary-window-minutes': '10080',
          },
          body: { id: 'resp_ok' },
        }
      },
    },
  })
  assert.deepEqual(seen, ['vm-gpt-a', 'vm-gpt-b'])
  assert.equal(res.statusCode, 200)
  assert.equal(out.id, 'resp_ok')
  assert.equal(logBag.codex_failed_over, true)
  assert.equal(logBag.vm_id, 'vm-gpt-b')
  const spent = getVm(root, 'vm-gpt-a')
  assert.equal(spent.codex.extra.codex_primary_used_percent, 100)
  assert.ok(spent.codex.extra.codex_limited_until)
  assert.equal(spent.codex.usage.quota.utilization_5h, 1)
  assert.equal(spent.schedulable, true)
  assert.equal(spent.schedule_disabled_reason ?? null, null)
  assert.equal(spent.claude?.temp_unschedulable_reason || spent.temp_unschedulable_reason, 'quota_5h_header')
  assert.equal(spent.status, 'running')
  fs.rmSync(root, { recursive: true, force: true })
})

test('persistCodexUsage writes cluster 5h/7d quota', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-persist-'))
  writeGptVm(root, 'vm-gpt-a')
  const now = Date.parse('2026-09-17T00:00:00.000Z')
  persistCodexUsage(root, 'vm-gpt-a', {
    now,
    headers: {
      'x-codex-primary-used-percent': '25',
      'x-codex-primary-window-minutes': '300',
      'x-codex-secondary-used-percent': '10',
      'x-codex-secondary-window-minutes': '10080',
    },
  })
  const vm = getVm(root, 'vm-gpt-a')
  assert.equal(vm.codex.usage.quota.utilization_5h, 0.25)
  assert.equal(vm.codex.usage.quota.utilization_7d, 0.1)
  fs.rmSync(root, { recursive: true, force: true })
})

test('persistCodexUsage keeps switch on and clears restriction after the 5h window opens', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-restore-'))
  writeGptVm(root, 'vm-gpt-a')
  const now = Date.parse('2026-09-17T00:00:00.000Z')
  persistCodexUsage(root, 'vm-gpt-a', {
    now,
    headers: {
      'x-codex-primary-used-percent': '100',
      'x-codex-primary-window-minutes': '300',
      'x-codex-primary-reset-after-seconds': '60',
    },
  })
  const spent = getVm(root, 'vm-gpt-a')
  assert.equal(spent.schedulable, true)
  assert.equal(spent.claude?.temp_unschedulable_reason || spent.temp_unschedulable_reason, 'quota_5h_header')
  persistCodexUsage(root, 'vm-gpt-a', {
    now: now + 61_000,
    headers: {
      'x-codex-primary-used-percent': '12',
      'x-codex-primary-window-minutes': '300',
      'x-codex-primary-reset-after-seconds': '300',
    },
  })
  const restored = getVm(root, 'vm-gpt-a')
  assert.equal(restored.schedulable, true)
  assert.equal(restored.schedule_disabled_reason ?? null, null)
  assert.equal(restored.claude?.temp_unschedulable_reason, undefined)
  fs.rmSync(root, { recursive: true, force: true })
})

test('unchanged Codex quota restriction does not rewrite the VM record', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-idempotent-'))
  writeGptVm(root, 'vm-gpt-a')
  const file = path.join(root, 'vms', 'vm-gpt-a.json')
  const now = Date.now()
  const reset = new Date(now + 60_000).toISOString()
  const vm = JSON.parse(fs.readFileSync(file, 'utf8'))
  vm.codex = {
    extra: {
      codex_5h_used_percent: 100,
      codex_5h_window_minutes: 300,
      codex_5h_reset_at: reset,
    },
  }
  fs.writeFileSync(file, JSON.stringify(vm))
  syncCodexQuotaSchedule(root, vm, { now })
  const first = fs.readFileSync(file, 'utf8')
  await new Promise((resolve) => setTimeout(resolve, 10))
  syncCodexQuotaSchedule(root, JSON.parse(first), { now })
  assert.equal(fs.readFileSync(file, 'utf8'), first)
  fs.rmSync(root, { recursive: true, force: true })
})

test('openai.chat GPT request is washed to Responses before the kernel hop', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-wash-'))
  writeGptVm(root, 'vm-gpt-a')
  let hopBody = null
  const writes = []
  const res = {
    headersSent: false,
    write(chunk) {
      this.headersSent = true
      writes.push(String(chunk))
    },
    end() {},
  }
  const logBag = {}
  await handleCodexProtocol({
    req: { headers: { 'user-agent': 'cursor' } },
    res,
    protocol: 'openai.chat',
    ctx: {
      path: '/v1/chat/completions',
      body: {
        model: 'gpt-5.6-sol',
        messages: [
          { role: 'system', content: 'be brief' },
          { role: 'user', content: 'hello' },
        ],
        stream: true,
        max_completion_tokens: 32,
      },
    },
    inbound: { stream: true },
    logBag,
    stats: { errors: 0, requests: 0, by_route: {} },
    json: (_res, status, body) => {
      res.statusCode = status
      res.body = body
      return body
    },
    writeSSEHeaders() {
      res.headersSent = true
    },
    routing: {},
    projectRoot: root,
    ops: {
      writeCodexKernelConfig() {},
      ensureCodexKernel: async () => ({ ok: true }),
      streamCodexKernel: async ({ body, envelope, onEvent }) => {
        hopBody = envelope?.body || body
        await onEvent('data: {"type":"response.output_text.delta","delta":"Hi"}')
        await onEvent('data: {"type":"response.completed"}')
        return { ok: true, status: 200, terminalState: 'verified' }
      },
    },
  })
  assert.ok(hopBody)
  assert.equal(Array.isArray(hopBody.input), true)
  assert.equal(hopBody.messages, undefined)
  assert.equal(hopBody.input[0].role, 'developer')
  assert.equal(logBag.protocol, 'openai.responses')
  assert.equal(logBag.path, '/v1/responses')
  assert.equal(logBag.hop_meta.inbound_path, '/v1/chat/completions')
  assert.equal(logBag.hop_meta.inbound_protocol, 'openai.chat')
  assert.match(writes.join(''), /chat\.completion\.chunk/)
  fs.rmSync(root, { recursive: true, force: true })
})

test('GPT on anthropic.messages converts and pins a GPT slot', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-anth-'))
  writeGptVm(root, 'vm-gpt-a')
  const writes = []
  const res = {
    headersSent: false,
    write(chunk) {
      this.headersSent = true
      writes.push(String(chunk))
    },
    end() {
      this.ended = true
    },
  }
  const logBag = {}
  await handleCodexProtocol({
    req: { headers: { 'user-agent': 'curl/8.0' } },
    res,
    protocol: 'anthropic.messages',
    ctx: {
      body: {
        model: 'gpt-6-astra',
        max_tokens: 32,
        messages: [{ role: 'user', content: 'hello' }],
        stream: true,
      },
    },
    inbound: { stream: true },
    logBag,
    stats: { errors: 0, requests: 0, by_route: {} },
    json: (_res, status, body) => {
      res.statusCode = status
      res.body = body
      return body
    },
    writeSSEHeaders() {
      res.headersSent = true
    },
    routing: {
      codex: {
        protocols: { 'anthropic.messages': { mode: 'convert', enabled: true } },
        convert: { anthropic_to_codex: true },
        clients: { unknown: 'allow', openai_compatible: 'allow' },
      },
    },
    projectRoot: root,
    ops: {
      writeCodexKernelConfig() {},
      ensureCodexKernel: async () => ({ ok: true }),
      streamCodexKernel: async ({ onEvent }) => {
        await onEvent('data: {"type":"response.output_text.delta","delta":"Hi"}')
        await onEvent('data: {"type":"response.completed"}')
        return { ok: true, status: 200, terminalState: 'verified' }
      },
    },
  })
  assert.equal(logBag.vm_id, 'vm-gpt-a')
  assert.equal(logBag.error_code, null)
  assert.match(writes.join(''), /message_start/)
  assert.match(writes.join(''), /Hi/)
  fs.rmSync(root, { recursive: true, force: true })
})

test('nested Responses cached_tokens reaches the request log cache column', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-cache-'))
  writeGptVm(root, 'vm-gpt-a')
  const res = {
    headersSent: false,
    write() {
      this.headersSent = true
    },
    end() {
      this.ended = true
    },
  }
  const logBag = {}
  await handleCodexProtocol({
    req: { headers: {} },
    res,
    protocol: 'openai.responses',
    ctx: { body: { model: 'gpt-5.4', input: [], stream: true } },
    inbound: { stream: true },
    logBag,
    stats: { errors: 0, requests: 0, by_route: {} },
    json: (_res, status, body) => {
      res.statusCode = status
      res.body = body
      return body
    },
    writeSSEHeaders() {
      res.headersSent = true
    },
    routing: {},
    projectRoot: root,
    ops: {
      writeCodexKernelConfig() {},
      ensureCodexKernel: async () => ({ ok: true }),
      streamCodexKernel: async ({ onEvent }) => {
        await onEvent('data: {"type":"response.completed"}')
        return {
          ok: true,
          status: 200,
          terminalState: 'verified',
          // merged trailer + SSE usage: totals flat, cache read nested
          usage: { input_tokens: 120, output_tokens: 9, input_tokens_details: { cached_tokens: 8 } },
        }
      },
    },
  })
  assert.equal(logBag.input_tokens, 120)
  assert.equal(logBag.output_tokens, 9)
  assert.equal(logBag.cache_read_tokens, 8)
  fs.rmSync(root, { recursive: true, force: true })
})

test('request log first_token_ms uses firstTokenAt minus injected ingress t0', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-ttft-'))
  writeGptVm(root, 'vm-gpt-a')
  const res = {
    headersSent: false,
    write() {
      this.headersSent = true
    },
    end() {
      this.ended = true
    },
  }
  const t0 = 1_700_000_000_000
  const logBag = {}
  await handleCodexProtocol({
    req: { headers: {} },
    res,
    protocol: 'openai.responses',
    ctx: { body: { model: 'gpt-5.4', input: [], stream: true } },
    inbound: { stream: true },
    logBag,
    stats: { errors: 0, requests: 0, by_route: {} },
    json: (_res, status, body) => {
      res.statusCode = status
      res.body = body
      return body
    },
    writeSSEHeaders() {
      res.headersSent = true
    },
    routing: {},
    projectRoot: root,
    requestStartedAt: t0,
    ops: {
      writeCodexKernelConfig() {},
      ensureCodexKernel: async () => ({ ok: true }),
      streamCodexKernel: async () => ({
        ok: true,
        status: 200,
        terminalState: 'verified',
        ttftMs: 300,
        firstTokenAt: t0 + 1500,
        usage: { input_tokens: 4, output_tokens: 1 },
      }),
    },
  })
  assert.equal(logBag.first_token_ms, 1500)
  fs.rmSync(root, { recursive: true, force: true })
})

test('request log first_token_ms falls back to hop ttftMs without firstTokenAt', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-ttft-fb-'))
  writeGptVm(root, 'vm-gpt-a')
  const res = {
    headersSent: false,
    write() {
      this.headersSent = true
    },
    end() {
      this.ended = true
    },
  }
  const logBag = {}
  await handleCodexProtocol({
    req: { headers: {} },
    res,
    protocol: 'openai.responses',
    ctx: { body: { model: 'gpt-5.4', input: [], stream: true } },
    inbound: { stream: true },
    logBag,
    stats: { errors: 0, requests: 0, by_route: {} },
    json: (_res, status, body) => {
      res.statusCode = status
      res.body = body
      return body
    },
    writeSSEHeaders() {
      res.headersSent = true
    },
    routing: {},
    projectRoot: root,
    requestStartedAt: 1_700_000_000_000,
    ops: {
      writeCodexKernelConfig() {},
      ensureCodexKernel: async () => ({ ok: true }),
      streamCodexKernel: async () => ({
        ok: true,
        status: 200,
        terminalState: 'verified',
        ttftMs: 300,
        usage: { input_tokens: 4, output_tokens: 1 },
      }),
    },
  })
  assert.equal(logBag.first_token_ms, 300)
  fs.rmSync(root, { recursive: true, force: true })
})

test('incomplete Responses stream still bills tokens from the completed event', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-bill-'))
  writeGptVm(root, 'vm-gpt-a')
  const res = {
    headersSent: false,
    write() {
      this.headersSent = true
    },
    end() {
      this.ended = true
    },
  }
  const logBag = {}
  const stats = { errors: 0, requests: 0, by_route: {} }
  await handleCodexProtocol({
    req: { headers: {} },
    res,
    protocol: 'openai.responses',
    ctx: { body: { model: 'gpt-5.5', input: [], stream: true } },
    inbound: { stream: true },
    logBag,
    stats,
    json: (_res, status, body) => {
      res.statusCode = status
      res.body = body
      return body
    },
    writeSSEHeaders() {
      res.headersSent = true
    },
    routing: {},
    projectRoot: root,
    ops: {
      writeCodexKernelConfig() {},
      ensureCodexKernel: async () => ({ ok: true }),
      streamCodexKernel: async ({ onEvent }) => {
        await onEvent('data: {"type":"response.output_text.delta","delta":"Hi"}')
        await onEvent(
          'data: {"type":"response.completed","response":{"service_tier":"priority","usage":{"input_tokens":40,"output_tokens":2,"input_tokens_details":{"cached_tokens":5}}}}',
        )
        return {
          ok: false,
          status: 200,
          terminalState: 'incomplete',
          usage: { input_tokens: 0, output_tokens: 0 },
        }
      },
    },
  })
  assert.equal(logBag.input_tokens, 40)
  assert.equal(logBag.output_tokens, 2)
  assert.equal(logBag.cache_read_tokens, 5)
  assert.equal(logBag.usage.service_tier, 'priority')
  assert.equal(logBag.error_code, null)
  assert.equal(logBag.final_state, 'verified')
  assert.equal(stats.errors, 0)
  fs.rmSync(root, { recursive: true, force: true })
})

test('Codex drains an accepted kernel job after the client disconnects', async () => {
  const runtime = await import('../../src/lib/pool/openai-account-runtime.mjs')
  runtime.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-drain-'))
  writeGptVm(root, 'vm-gpt-a')
  const req = new EventEmitter()
  req.headers = {}
  const res = new EventEmitter()
  res.headersSent = false
  res.destroyed = false
  res.writableEnded = false
  let writes = 0
  let ends = 0
  res.write = () => {
    writes += 1
    return true
  }
  res.end = () => {
    ends += 1
  }
  const logBag = {}

  await handleCodexProtocol({
    req,
    res,
    protocol: 'openai.responses',
    ctx: { body: { model: 'gpt-5.4', input: 'hi', stream: true } },
    inbound: { stream: true },
    logBag,
    stats: { errors: 0, requests: 0, by_route: {} },
    json: () => assert.fail('a disconnected client must not receive JSON'),
    writeSSEHeaders: () => assert.fail('a disconnected client must not receive SSE headers'),
    routing: {},
    projectRoot: root,
    ops: {
      writeCodexKernelConfig() {},
      ensureCodexKernel: async () => ({ ok: true }),
      streamCodexKernel: async ({ signal, onEvent }) => {
        assert.equal(signal, undefined, 'the client signal must not detach an accepted kernel job')
        req.emit('aborted')
        res.destroyed = true
        await onEvent('data: {"type":"response.completed","response":{"usage":{"input_tokens":12,"output_tokens":3}}}')
        return {
          ok: true,
          status: 200,
          terminalState: 'verified',
          usage: { input_tokens: 12, output_tokens: 3 },
        }
      },
    },
  })

  assert.equal(writes, 0)
  assert.equal(ends, 0)
  assert.equal(logBag.final_state, 'client_cancelled_after_submit')
  assert.equal(logBag.input_tokens, 12)
  assert.equal(logBag.output_tokens, 3)
  assert.equal(runtime.openAIRuntimeSignals('vm-gpt-a').inFlight, 0)
  runtime.resetOpenAIAccountRuntime()
  fs.rmSync(root, { recursive: true, force: true })
})

test('panel allowed_models on the VM record rejects other GPT models before the hop', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-allow-'))
  writeGptVm(root, 'vm-gpt-a')
  const file = path.join(root, 'vms', 'vm-gpt-a.json')
  const raw = JSON.parse(fs.readFileSync(file, 'utf8'))
  fs.writeFileSync(file, JSON.stringify({ ...raw, policy: { allowed_models: ['gpt-5.5'] } }))
  let hops = 0
  const run = (model) => {
    const res = { headersSent: false, write() {}, end() {} }
    return handleCodexProtocol({
      req: { headers: { 'user-agent': 'curl/8.0' } },
      res,
      protocol: 'openai.responses',
      ctx: { path: '/v1/responses', body: { model, input: 'hi', stream: false } },
      inbound: { stream: false },
      logBag: {},
      stats: { errors: 0, requests: 0, by_route: {} },
      json: (_res, status, body) => ({ status, body }),
      writeSSEHeaders() {},
      routing: {},
      projectRoot: root,
      ops: {
        writeCodexKernelConfig() {},
        ensureCodexKernel: async () => ({ ok: true }),
        streamCodexKernel: async ({ onEvent }) => {
          hops++
          await onEvent('data: {"type":"response.completed"}')
          return { ok: true, status: 200, terminalState: 'verified' }
        },
      },
    })
  }
  const denied = await run('gpt-5.4')
  assert.equal(denied.status, 400)
  assert.equal(denied.body.error.code, 'model_not_allowed')
  assert.equal(hops, 0)
  await run('gpt-5.5')
  assert.equal(hops, 1)
  fs.rmSync(root, { recursive: true, force: true })
})

test('GPT slot at max concurrency queues until a seat frees instead of 503', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-queue-'))
  writeGptVm(root, 'vm-gpt-a')
  const file = path.join(root, 'vms', 'vm-gpt-a.json')
  const raw = JSON.parse(fs.readFileSync(file, 'utf8'))
  fs.writeFileSync(file, JSON.stringify({ ...raw, policy: { maxConcurrency: 1 } }))
  let release
  const gate = new Promise((resolve) => {
    release = resolve
  })
  let hops = 0
  const run = (wait) =>
    handleCodexProtocol({
      req: { headers: { 'user-agent': 'curl/8.0' } },
      res: { headersSent: false, write() {}, end() {} },
      protocol: 'openai.responses',
      ctx: { path: '/v1/responses', body: { model: 'gpt-5.5', input: 'hi', stream: false } },
      inbound: { stream: false },
      logBag: {},
      stats: { errors: 0, requests: 0, by_route: {} },
      json: (_res, status, body) => ({ status, body }),
      writeSSEHeaders() {},
      routing: { pool: { fallback_wait_timeout_ms: 5000 } },
      projectRoot: root,
      ops: {
        writeCodexKernelConfig() {},
        ensureCodexKernel: async () => ({ ok: true }),
        streamCodexKernel: async ({ onEvent }) => {
          hops++
          if (wait) await gate
          await onEvent('data: {"type":"response.completed"}')
          return { ok: true, status: 200, terminalState: 'verified' }
        },
      },
    })
  const busy = run(true)
  await new Promise((resolve) => setImmediate(resolve))
  const queued = run(false)
  await new Promise((resolve) => setTimeout(resolve, 50))
  assert.equal(hops, 1)
  assert.equal(rt.openAIWaiterCount(), 1)
  release()
  const [a, b] = await Promise.all([busy, queued])
  assert.equal(a.status, 200)
  assert.equal(b.status, 200)
  assert.equal(hops, 2)
  rt.resetOpenAIAccountRuntime()
  fs.rmSync(root, { recursive: true, force: true })
})

function codexArgs(root, ops, extras = {}) {
  return {
    req: { headers: { 'user-agent': 'curl/8.0', ...(extras.headers || {}) }, apiKeyKind: 'user' },
    res: { headersSent: false, write() {}, end() {} },
    protocol: 'openai.responses',
    ctx: { path: '/v1/responses', body: extras.body || { model: 'gpt-5.5', input: 'hi', stream: false } },
    inbound: { stream: false },
    logBag: extras.logBag || {},
    stats: { errors: 0, requests: 0, by_route: {} },
    json: (_res, status, body) => ({ status, body }),
    writeSSEHeaders() {},
    routing: { pool: { fallback_wait_timeout_ms: 5000 } },
    projectRoot: root,
    requestId: extras.requestId || null,
    attemptsRepo: extras.attemptsRepo || null,
    stickyRouter: extras.stickyRouter || null,
    ops: { writeCodexKernelConfig() {}, ...ops },
  }
}

test('kernel credential persistence cannot erase the VM scheduling policy', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-scheduling-guard-'))
  writeGptVm(root, 'vm-gpt-a')
  const vmFile = path.join(root, 'vms', 'vm-gpt-a.json')
  const credentialFile = path.join(root, 'vms', 'vm-gpt-a', 'codex-credentials.json')
  fs.writeFileSync(
    vmFile,
    JSON.stringify({
      ...JSON.parse(fs.readFileSync(vmFile, 'utf8')),
      policy: { maxConcurrency: 40, maxRpm: 0 },
    }),
  )
  const out = await handleCodexProtocol(
    codexArgs(root, {
      ensureCodexKernel: async () => ({ ok: true }),
      streamCodexKernel: async ({ onEvent }) => {
        const stored = JSON.parse(fs.readFileSync(credentialFile, 'utf8'))
        stored.accounts[0].max_concurrency = 0
        stored.accounts[0].rpm = 0
        fs.writeFileSync(credentialFile, JSON.stringify(stored))
        await onEvent('data: {"type":"response.completed","response":{"id":"resp_ok"}}')
        return { ok: true, status: 200, terminalState: 'verified', body: { id: 'resp_ok' } }
      },
    }),
  )
  assert.equal(out.status, 200)
  const account = JSON.parse(fs.readFileSync(credentialFile, 'utf8')).accounts[0]
  assert.equal(account.max_concurrency, 40)
  assert.equal(account.rpm, 0)
  rt.resetOpenAIAccountRuntime()
  fs.rmSync(root, { recursive: true, force: true })
})

test('#A07: two admissions racing through kernel init never exceed a cap of 1', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-atomic-'))
  writeGptVm(root, 'vm-gpt-a')
  const file = path.join(root, 'vms', 'vm-gpt-a.json')
  fs.writeFileSync(
    file,
    JSON.stringify({ ...JSON.parse(fs.readFileSync(file, 'utf8')), policy: { maxConcurrency: 1 } }),
  )
  let active = 0
  let maxActive = 0
  const ops = {
    ensureCodexKernel: async () => {
      await new Promise((resolve) => setTimeout(resolve, 20))
      return { ok: true }
    },
    streamCodexKernel: async ({ onEvent }) => {
      active += 1
      maxActive = Math.max(maxActive, active, rt.openAIRuntimeSignals('vm-gpt-a').inFlight)
      await new Promise((resolve) => setTimeout(resolve, 10))
      await onEvent('data: {"type":"response.completed"}')
      active -= 1
      return { ok: true, status: 200, terminalState: 'verified' }
    },
  }
  const [a, b] = await Promise.all([
    handleCodexProtocol(codexArgs(root, ops)),
    handleCodexProtocol(codexArgs(root, ops)),
  ])
  assert.equal(a.status, 200)
  assert.equal(b.status, 200)
  assert.equal(maxActive, 1)
  assert.equal(rt.openAIRuntimeSignals('vm-gpt-a').inFlight, 0)
  rt.resetOpenAIAccountRuntime()
  fs.rmSync(root, { recursive: true, force: true })
})

test('#A04: the fifth GPT slot is reached after four slots fail over', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-fifth-'))
  const ids = ['vm-gpt-a', 'vm-gpt-b', 'vm-gpt-c', 'vm-gpt-d', 'vm-gpt-e']
  for (const id of ids) writeGptVm(root, id)
  const seen = []
  const logBag = {}
  const out = await handleCodexProtocol(
    codexArgs(
      root,
      {
        ensureCodexKernel: async () => ({ ok: true }),
        streamCodexKernel: async ({ exec, onEvent }) => {
          seen.push(exec.vm.id)
          if (seen.length < 5) {
            return { ok: false, status: 429, body: { error: { code: 'rate_limit_exceeded', message: 'slow down' } } }
          }
          await onEvent('data: {"type":"response.completed"}')
          return { ok: true, status: 200, terminalState: 'verified' }
        },
      },
      { logBag },
    ),
  )
  assert.equal(out.status, 200)
  assert.equal(seen.length, 5)
  assert.equal(new Set(seen).size, 5)
  assert.equal(logBag.attempt_count, 5)
  assert.equal(logBag.vm_id, seen[4])
  rt.resetOpenAIAccountRuntime()
  fs.rmSync(root, { recursive: true, force: true })
})

test('upstream 403 mirrors the kernel cooldown before failing over', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-kernel-cooldown-'))
  writeGptVm(root, 'vm-gpt-a')
  writeGptVm(root, 'vm-gpt-b')
  const seen = []
  try {
    const out = await handleCodexProtocol(
      codexArgs(root, {
        ensureCodexKernel: async () => ({ ok: true }),
        streamCodexKernel: async ({ exec, onEvent }) => {
          seen.push(exec.vm.id)
          if (seen.length === 1) {
            return {
              ok: false,
              status: 403,
              committed: false,
              body: { error: { code: 'upstream_auth', message: 'forbidden' } },
            }
          }
          await onEvent('data: {"type":"response.completed"}')
          return { ok: true, status: 200, terminalState: 'verified' }
        },
      }),
    )
    assert.equal(out.status, 200)
    assert.equal(seen.length, 2)
    assert.notEqual(seen[0], seen[1])
    const blocked = rt.openAIRuntimeSignals(seen[0])
    assert.ok(blocked.kernelCooldownUntil > Date.now())
    assert.equal(rt.tryAcquireOpenAISlot(seen[0], { concurrency: 40 }), null)
  } finally {
    rt.resetOpenAIAccountRuntime()
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('request-scoped content policy 403 does not cool down the account', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-policy-403-'))
  writeGptVm(root, 'vm-gpt-a')
  writeGptVm(root, 'vm-gpt-b')
  const seen = []
  try {
    const out = await handleCodexProtocol(
      codexArgs(root, {
        ensureCodexKernel: async () => ({ ok: true }),
        streamCodexKernel: async ({ exec, onEvent }) => {
          seen.push(exec.vm.id)
          if (seen.length === 1) {
            return {
              ok: false,
              status: 403,
              committed: false,
              body: { error: { code: 'cyber_policy', message: 'request denied' } },
            }
          }
          await onEvent('data: {"type":"response.completed"}')
          return { ok: true, status: 200, terminalState: 'verified' }
        },
      }),
    )
    assert.equal(out.status, 200)
    assert.equal(rt.openAIRuntimeSignals(seen[0]).kernelCooldownUntil, null)
  } finally {
    rt.resetOpenAIAccountRuntime()
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('kernel no_capacity is absorbed by bounded admission retry and records a capacity rejection', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-capacity-'))
  writeGptVm(root, 'vm-gpt-a')
  writeGptVm(root, 'vm-gpt-b')
  const seen = []
  const logBag = {
    error_code: 'no_capacity',
    error_message: 'credential busy',
    upstream_status: 503,
  }
  const keepAlive = setTimeout(() => {}, 5000)
  const attempts = []
  const attemptsRepo = {
    begin(value) {
      attempts.push({ ...value })
    },
    complete(requestId, attemptNo, value) {
      Object.assign(
        attempts.find((item) => item.requestId === requestId && item.attemptNo === attemptNo),
        value,
      )
    },
  }
  try {
    const out = await handleCodexProtocol(
      codexArgs(
        root,
        {
          ensureCodexKernel: async () => ({ ok: true }),
          streamCodexKernel: async ({ exec, onEvent }) => {
            seen.push(exec.vm.id)
            if (seen.length <= 2) {
              return {
                ok: false,
                status: 503,
                committed: false,
                body: { error: { code: 'no_capacity', message: 'credential busy' } },
              }
            }
            await onEvent('data: {"type":"response.completed"}')
            return { ok: true, status: 200, terminalState: 'verified' }
          },
        },
        { logBag, requestId: 'req-capacity', attemptsRepo },
      ),
    )
    assert.equal(out.status, 200)
    assert.equal(seen.length, 3)
    assert.equal(new Set(seen).size, 2)
    assert.equal(logBag.attempt_count, 3)
    assert.equal(logBag.codex_failed_over, true)
    assert.equal(logBag.error_code, null)
    assert.equal(logBag.error_message, null)
    assert.equal(logBag.upstream_status, null)
    assert.ok(rt.openAIRuntimeSignals(seen[0]).failureRateBps > 0)
    assert.equal(attempts.length, 3)
    assert.equal(attempts[0].action, 'wait')
    assert.equal(attempts[0].errorScope, 'account')
    assert.equal(attempts[2].action, 'return')
  } finally {
    clearTimeout(keepAlive)
    rt.resetOpenAIAccountRuntime()
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('sticky kernel no_capacity waits on its home account without dropping cache affinity', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-sticky-capacity-'))
  const sticky = new StickyRouter({ dataDir: path.join(root, 'data'), config: { sticky: { enabled: true } } })
  writeGptVm(root, 'vm-gpt-a')
  writeGptVm(root, 'vm-gpt-b')
  const headers = { 'x-session-id': 'sess-sticky-capacity' }
  const body = { model: 'gpt-5.5', input: 'warm this conversation', stream: false }
  let homeVmId = null
  let warmSessionId = null
  const attempts = []
  const keepAlive = setTimeout(() => {}, 5000)
  try {
    const warm = await hopCodex({
      root,
      stickyRouter: sticky,
      headers,
      body,
      onHop: async ({ exec, envelope }) => {
        homeVmId = exec.vmId
        warmSessionId = envelope.session.session_id
        return { ok: true, status: 200, terminalState: 'verified', body: { id: 'resp_warm' } }
      },
    })
    assert.equal(warm.res.statusCode, 200)

    const retried = await hopCodex({
      root,
      stickyRouter: sticky,
      headers,
      body: { ...body, input: 'continue this conversation' },
      onHop: async ({ exec, envelope }) => {
        attempts.push({ vmId: exec.vmId, sessionId: envelope.session.session_id })
        if (attempts.length === 1) {
          return {
            ok: false,
            status: 503,
            committed: false,
            body: { error: { code: 'no_capacity', message: 'credential busy' } },
          }
        }
        return { ok: true, status: 200, terminalState: 'verified', body: { id: 'resp_retry' } }
      },
    })

    assert.equal(retried.res.statusCode, 200)
    assert.deepEqual(
      attempts.map((attempt) => attempt.vmId),
      [homeVmId, homeVmId],
    )
    assert.deepEqual(
      attempts.map((attempt) => attempt.sessionId),
      [warmSessionId, warmSessionId],
    )
  } finally {
    clearTimeout(keepAlive)
    rt.resetOpenAIAccountRuntime()
    sticky.db?.close?.()
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('persistent kernel no_capacity expires as local pool backpressure, not an upstream 503', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-capacity-timeout-'))
  writeGptVm(root, 'vm-gpt-a')
  const logBag = {}
  const args = codexArgs(
    root,
    {
      ensureCodexKernel: async () => ({ ok: true }),
      streamCodexKernel: async () => ({
        ok: false,
        status: 503,
        committed: false,
        body: { error: { code: 'no_capacity', message: 'credential busy' } },
      }),
    },
    { logBag },
  )
  args.routing = { pool: { fallback_wait_timeout_ms: 1000 } }
  const headers = {}
  args.res.setHeader = (name, value) => {
    headers[name] = value
  }
  const keepAlive = setTimeout(() => {}, 5000)
  try {
    const out = await handleCodexProtocol(args)
    assert.equal(out.status, 429)
    assert.equal(out.body.error.code, 'pool_overloaded')
    assert.equal(logBag.error_code, 'capacity_unavailable')
    assert.equal(logBag.error_message, 'credential busy')
    assert.equal(logBag.account_id, 'vm-gpt-a')
    assert.ok(Number(headers['retry-after']) >= 1)
  } finally {
    clearTimeout(keepAlive)
    rt.resetOpenAIAccountRuntime()
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('persistent no_capacity honors the configured total attempt ceiling', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-capacity-attempts-'))
  writeGptVm(root, 'vm-gpt-a')
  let calls = 0
  const args = codexArgs(root, {
    ensureCodexKernel: async () => ({ ok: true }),
    streamCodexKernel: async () => {
      calls += 1
      return {
        ok: false,
        status: 503,
        committed: false,
        body: { error: { code: 'no_capacity', message: 'credential busy' } },
      }
    },
  })
  args.routing.failover = { max_total_attempts: 2 }
  const keepAlive = setTimeout(() => {}, 5000)
  try {
    const out = await handleCodexProtocol(args)
    assert.equal(out.status, 429)
    assert.equal(calls, 2)
  } finally {
    clearTimeout(keepAlive)
    rt.resetOpenAIAccountRuntime()
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('generic upstream overload is returned without cross-account replay', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-overload-'))
  writeGptVm(root, 'vm-gpt-a')
  writeGptVm(root, 'vm-gpt-b')
  const seen = []
  const out = await handleCodexProtocol(
    codexArgs(root, {
      ensureCodexKernel: async () => ({ ok: true }),
      streamCodexKernel: async ({ exec, onEvent }) => {
        seen.push(exec.vm.id)
        if (seen.length === 1) {
          return { ok: false, status: 502, committed: false, body: { error: { code: 'server_is_overloaded' } } }
        }
        await onEvent('data: {"type":"response.completed"}')
        return { ok: true, status: 200, terminalState: 'verified' }
      },
    }),
  )
  assert.equal(out.status, 502)
  assert.equal(seen.length, 1)

  const committedSeen = []
  const committed = await handleCodexProtocol(
    codexArgs(root, {
      ensureCodexKernel: async () => ({ ok: true }),
      streamCodexKernel: async ({ exec }) => {
        committedSeen.push(exec.vm.id)
        return { ok: false, status: 502, committed: true, body: { error: { code: 'server_is_overloaded' } } }
      },
    }),
  )
  assert.equal(committed.status, 502)
  assert.equal(committedSeen.length, 1)
  rt.resetOpenAIAccountRuntime()
  fs.rmSync(root, { recursive: true, force: true })
})

test('kernel stream diagnostics reach the request log without cross-account replay', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-stream-diag-'))
  writeGptVm(root, 'vm-gpt-a')
  writeGptVm(root, 'vm-gpt-b')
  const logBag = {}
  const diagnostic = {
    request_body_bytes: 512,
    input_items: 1,
    tool_count: 0,
    event_types: { 'response.created': 1, 'responsesapi.websocket_timing': 9 },
    termination: 'idle_timeout',
  }
  let hops = 0
  let warning = ''
  const originalWarn = console.warn
  console.warn = (...args) => {
    warning += args.join(' ')
  }
  try {
    const out = await handleCodexProtocol(
      codexArgs(
        root,
        {
          ensureCodexKernel: async () => ({ ok: true }),
          streamCodexKernel: async () => {
            hops += 1
            return {
              ok: false,
              status: 0,
              committed: false,
              transportError: true,
              terminalState: 'transport_error',
              body: { error: { code: 'worker_timeout', message: 'idle timeout' } },
              kernelStream: diagnostic,
            }
          },
        },
        { logBag, requestId: 'req_stream_diag' },
      ),
    )
    assert.equal(out.status, 502)
    assert.equal(hops, 1)
    assert.deepEqual(logBag.kernel_stream, diagnostic)
    assert.match(warning, /\[codex-stream-diagnostic\]/)
    assert.match(warning, /"termination":"idle_timeout"/)
  } finally {
    console.warn = originalWarn
    rt.resetOpenAIAccountRuntime()
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('a committed Responses failure emits a terminal SSE error instead of a silent EOF', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-stream-error-'))
  writeGptVm(root, 'vm-gpt-a')
  writeGptVm(root, 'vm-gpt-b')
  const writes = []
  let hops = 0
  const logBag = {}
  const args = codexArgs(
    root,
    {
      ensureCodexKernel: async () => ({ ok: true }),
      streamCodexKernel: async ({ onEvent }) => {
        hops += 1
        await onEvent('data: {"type":"response.created","response":{"id":"resp_partial"}}')
        return {
          ok: false,
          status: 500,
          committed: true,
          terminalState: 'incomplete',
          body: { error: { type: 'server_error', code: 'server_error', message: 'upstream failed' } },
        }
      },
    },
    { body: { model: 'gpt-5.5', input: 'hi', stream: true }, logBag },
  )
  args.inbound = { stream: true }
  args.res.write = (chunk) => {
    args.res.headersSent = true
    writes.push(String(chunk))
  }
  args.res.end = () => {
    args.res.ended = true
  }
  args.writeSSEHeaders = () => {
    args.res.headersSent = true
  }
  try {
    await handleCodexProtocol(args)
    const wire = writes.join('')
    assert.equal(hops, 1)
    assert.match(wire, /^event: response\.created/m)
    assert.match(wire, /event: error\ndata: \{"type":"error","code":"server_error","message":"upstream failed"/)
    assert.equal(logBag.error_code, 'server_error')
    assert.equal(logBag.upstream_status, 500)
    assert.equal(logBag.final_state, 'incomplete')
  } finally {
    rt.resetOpenAIAccountRuntime()
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('Codex streaming starts downstream keepalives only after real output commits', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-keepalive-'))
  writeGptVm(root, 'vm-gpt-a')
  const writes = []
  const args = codexArgs(
    root,
    {
      ensureCodexKernel: async () => ({ ok: true }),
      streamCodexKernel: async ({ onEvent, onResponse, onCommit }) => {
        await onResponse({ status: 200, headers: {} })
        await new Promise((resolve) => setTimeout(resolve, 10))
        assert.deepEqual(writes, [])
        onCommit()
        await onEvent('data: {"type":"response.created","response":{"id":"resp_keepalive"}}')
        await onEvent('data: {"type":"response.output_text.delta","delta":"hello"}')
        await new Promise((resolve) => setTimeout(resolve, 25))
        await onEvent('data: {"type":"response.completed"}')
        return { ok: true, status: 200, terminalState: 'verified' }
      },
    },
    { body: { model: 'gpt-5.5', input: 'hi', stream: true } },
  )
  args.inbound = { stream: true }
  args.routing.failover = { stream_keepalive_ms: 5 }
  args.res.write = (chunk) => {
    args.res.headersSent = true
    writes.push(String(chunk))
  }
  args.writeSSEHeaders = () => {
    args.res.headersSent = true
  }
  try {
    await handleCodexProtocol(args)
    assert.ok(writes.some((chunk) => chunk === ': keepalive\n\n'))
    assert.match(writes.join(''), /event: response\.completed/)
  } finally {
    rt.resetOpenAIAccountRuntime()
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('Codex kernel hop receives bounded first-byte, stream-idle and overall timeouts', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-timeouts-'))
  writeGptVm(root, 'vm-gpt-a')
  let observed = null
  const args = codexArgs(root, {
    ensureCodexKernel: async () => ({ ok: true }),
    streamCodexKernel: async ({ timeoutMs, idleTimeoutMs, overallTimeoutMs, onEvent }) => {
      observed = { timeoutMs, idleTimeoutMs, overallTimeoutMs }
      await onEvent('data: {"type":"response.completed"}')
      return { ok: true, status: 200, terminalState: 'verified' }
    },
  })
  args.upstreamTimeoutMs = 456_000
  args.routing.failover = { stream_idle_timeout_ms: 234_000 }
  try {
    const out = await handleCodexProtocol(args)
    assert.equal(out.status, 200)
    assert.deepEqual(observed, { timeoutMs: 456_000, idleTimeoutMs: 234_000, overallTimeoutMs: 300_000 })
  } finally {
    rt.resetOpenAIAccountRuntime()
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('kernel HTTP 200 followed by an SSE rejection does not commit a false downstream 200', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-sse-reject-'))
  writeGptVm(root, 'vm-gpt-a')
  const writes = []
  const args = codexArgs(
    root,
    {
      ensureCodexKernel: async () => ({ ok: true }),
      streamCodexKernel: async ({ onResponse }) => {
        await onResponse({ status: 200, headers: {} })
        await new Promise((resolve) => setTimeout(resolve, 15))
        return {
          ok: false,
          status: 503,
          committed: false,
          body: { error: { code: 'server_error', message: 'rejected before lifecycle' } },
        }
      },
    },
    { body: { model: 'gpt-5.5', input: 'hi', stream: true } },
  )
  args.inbound = { stream: true }
  args.routing.failover = { stream_keepalive_ms: 5, max_total_attempts: 1 }
  args.res.write = (chunk) => {
    args.res.headersSent = true
    writes.push(String(chunk))
  }
  args.res.setHeader = () => {}
  args.writeSSEHeaders = () => {
    args.res.headersSent = true
  }
  const out = await handleCodexProtocol(args)
  assert.equal(out.status, 503)
  assert.deepEqual(writes, [])
  rt.resetOpenAIAccountRuntime()
  fs.rmSync(root, { recursive: true, force: true })
})

test('kernel rejection does not let keepalive commit a false 200', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-keepalive-reject-'))
  writeGptVm(root, 'vm-gpt-a')
  const writes = []
  const args = codexArgs(
    root,
    {
      ensureCodexKernel: async () => ({ ok: true }),
      streamCodexKernel: async ({ onResponse }) => {
        await onResponse({ status: 503, headers: {} })
        await new Promise((resolve) => setTimeout(resolve, 15))
        return {
          ok: false,
          status: 503,
          committed: false,
          body: { error: { code: 'no_capacity', message: 'credential busy' } },
        }
      },
    },
    { body: { model: 'gpt-5.5', input: 'hi', stream: true } },
  )
  args.inbound = { stream: true }
  args.routing.failover = { stream_keepalive_ms: 5, max_total_attempts: 1 }
  args.res.write = (chunk) => {
    args.res.headersSent = true
    writes.push(String(chunk))
  }
  args.res.setHeader = () => {}
  args.writeSSEHeaders = () => {
    args.res.headersSent = true
  }
  const out = await handleCodexProtocol(args)
  assert.equal(out.status, 429)
  assert.deepEqual(writes, [])
  rt.resetOpenAIAccountRuntime()
  fs.rmSync(root, { recursive: true, force: true })
})

test('invalidated Codex access drains the account before background refresh and restart', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-auth-refresh-'))
  writeGptVm(root, 'vm-gpt-a')
  const vmFile = path.join(root, 'vms', 'vm-gpt-a.json')
  fs.writeFileSync(
    vmFile,
    JSON.stringify({ ...JSON.parse(fs.readFileSync(vmFile, 'utf8')), policy: { maxConcurrency: 40, maxRpm: 0 } }),
  )
  let calls = 0
  let refreshes = 0
  let restarts = 0
  const logBag = {}
  const out = await handleCodexProtocol(
    codexArgs(
      root,
      {
        ensureCodexKernel: async () => ({ ok: true }),
        refreshCodexAccessToken: async () => {
          refreshes += 1
          return { ok: true, access_token: 'at-new', refresh_token: 'rt-new', expires_at: Date.now() + 3600_000 }
        },
        restartCodexKernel: async () => {
          restarts += 1
          const credentialFile = path.join(root, 'vms', 'vm-gpt-a', 'codex-credentials.json')
          const stored = JSON.parse(fs.readFileSync(credentialFile, 'utf8'))
          stored.accounts[0].max_concurrency = 0
          fs.writeFileSync(credentialFile, JSON.stringify(stored))
          return { ok: true }
        },
        streamCodexKernel: async ({ onEvent }) => {
          calls += 1
          return {
            ok: false,
            status: 401,
            committed: false,
            body: { error: { code: 'upstream_auth', message: 'Encountered invalidated oauth token' } },
          }
        },
      },
      { logBag },
    ),
  )
  assert.equal(out.status, 401)
  assert.equal(calls, 1)
  const credentialFile = path.join(root, 'vms', 'vm-gpt-a', 'codex-credentials.json')
  let saved
  for (let i = 0; i < 100; i++) {
    saved = JSON.parse(fs.readFileSync(credentialFile, 'utf8'))
    if (restarts > 0 && saved.accounts[0].max_concurrency === 40) break
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
  assert.equal(refreshes, 1)
  assert.equal(restarts, 1)
  assert.equal(logBag.codex_oauth_recovery_deferred, true)
  assert.equal(logBag.attempt_count, 1)
  assert.equal(saved.accounts[0].access_token, 'at-new')
  assert.equal(saved.accounts[0].max_concurrency, 40)
  rt.resetOpenAIAccountRuntime()
  fs.rmSync(root, { recursive: true, force: true })
})

test('concurrent invalidated access failures release every lease and share one background recovery', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-auth-refresh-concurrent-'))
  writeGptVm(root, 'vm-gpt-a')
  let arrived = 0
  let releaseBoth
  const bothArrived = new Promise((resolve) => {
    releaseBoth = resolve
  })
  let refreshes = 0
  let restarts = 0
  const ops = {
    ensureCodexKernel: async () => ({ ok: true }),
    refreshCodexAccessToken: async () => {
      refreshes += 1
      return { ok: true, access_token: 'at-concurrent', refresh_token: 'rt-concurrent' }
    },
    restartCodexKernel: async () => {
      restarts += 1
      return { ok: true }
    },
    streamCodexKernel: async () => {
      arrived += 1
      if (arrived === 2) releaseBoth()
      await bothArrived
      return {
        ok: false,
        status: 401,
        committed: false,
        body: { error: { code: 'upstream_auth', message: 'Encountered invalidated oauth token' } },
      }
    },
  }
  try {
    const [first, second] = await Promise.all([
      handleCodexProtocol(codexArgs(root, ops)),
      handleCodexProtocol(codexArgs(root, ops)),
    ])
    assert.equal(first.status, 401)
    assert.equal(second.status, 401)
    assert.equal(rt.openAIRuntimeSignals('vm-gpt-a').inFlight, 0)
    for (let i = 0; i < 100 && restarts === 0; i++) await new Promise((resolve) => setTimeout(resolve, 10))
    assert.equal(refreshes, 1)
    assert.equal(restarts, 1)
  } finally {
    rt.resetOpenAIAccountRuntime()
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('Codex SSE consumption pauses when the downstream response is backpressured', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-backpressure-'))
  writeGptVm(root, 'vm-gpt-a')
  const writes = []
  let producerAdvanced = false
  let firstWriteResolve
  const firstWrite = new Promise((resolve) => {
    firstWriteResolve = resolve
  })
  const res = new EventEmitter()
  Object.assign(res, {
    headersSent: false,
    destroyed: false,
    writableEnded: false,
    writableNeedDrain: false,
    write(chunk) {
      writes.push(String(chunk))
      if (writes.length === 1) {
        this.writableNeedDrain = true
        firstWriteResolve()
        return false
      }
      return true
    },
    end() {
      this.writableEnded = true
    },
  })
  const args = codexArgs(
    root,
    {
      ensureCodexKernel: async () => ({ ok: true }),
      streamCodexKernel: async ({ onEventBatch }) => {
        await onEventBatch([
          {
            line: 'data: {"type":"response.created","response":{"id":"resp_backpressure"}}',
            event: { type: 'response.created', response: { id: 'resp_backpressure' } },
          },
        ])
        producerAdvanced = true
        await onEventBatch([
          {
            line: 'data: {"type":"response.completed","response":{"id":"resp_backpressure"}}',
            event: { type: 'response.completed', response: { id: 'resp_backpressure' } },
          },
        ])
        return { ok: true, status: 200, terminalState: 'verified' }
      },
    },
    { body: { model: 'gpt-5.5', input: 'hi', stream: true } },
  )
  args.inbound = { stream: true }
  args.res = res
  args.writeSSEHeaders = () => {
    res.headersSent = true
  }

  try {
    const pending = handleCodexProtocol(args)
    await firstWrite
    await new Promise((resolve) => setImmediate(resolve))
    assert.equal(producerAdvanced, false)
    assert.equal(writes.length, 1)

    res.writableNeedDrain = false
    res.emit('drain')
    await pending
    assert.equal(producerAdvanced, true)
    assert.match(writes.join(''), /response\.completed/)
  } finally {
    rt.resetOpenAIAccountRuntime()
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('#A11: an OpenAI pool that stays full answers 429 pool_overloaded', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-full-'))
  const vmId = 'vm-gpt-full'
  writeGptVm(root, vmId)
  const file = path.join(root, 'vms', `${vmId}.json`)
  fs.writeFileSync(
    file,
    JSON.stringify({ ...JSON.parse(fs.readFileSync(file, 'utf8')), policy: { maxConcurrency: 1 } }),
  )
  const held = rt.tryAcquireOpenAISlot(vmId, { concurrency: 1 })
  const args = codexArgs(root, { ensureCodexKernel: async () => ({ ok: true }) })
  args.routing = { pool: { fallback_wait_timeout_ms: 1000 } }
  const headers = {}
  args.res.setHeader = (name, value) => {
    headers[name] = value
  }
  // Waiter timers are unref'd so a systemd process can idle-exit. Keep one
  // ref'd handle so this isolated file cannot drain before the 1s deadline.
  const keepAlive = setTimeout(() => {}, 15_000)
  try {
    const out = await handleCodexProtocol(args)
    assert.equal(out.status, 429)
    assert.equal(out.body.error.code, 'pool_overloaded')
    assert.equal(out.body.error.message, '号池负载过高，稍后再试')
    assert.ok(Number(headers['retry-after']) >= 1)
  } finally {
    clearTimeout(keepAlive)
    held.release()
    rt.resetOpenAIAccountRuntime()
    fs.rmSync(root, { recursive: true, force: true })
  }
})

function emptyCodexRes() {
  return { headersSent: false, write() {}, end() {} }
}

async function hopCodex({ root, stickyRouter, routing = {}, headers = {}, body, onHop }) {
  const envelopes = []
  const res = emptyCodexRes()
  const out = await handleCodexProtocol({
    req: { headers, apiKeyKind: 'user' },
    res,
    protocol: 'openai.responses',
    ctx: { body },
    inbound: { stream: false },
    logBag: {},
    stats: { errors: 0, requests: 0, by_route: {} },
    json: (_res, status, payload) => {
      res.statusCode = status
      res.body = payload
      return payload
    },
    writeSSEHeaders() {
      res.headersSent = true
    },
    routing,
    projectRoot: root,
    stickyRouter,
    ops: {
      writeCodexKernelConfig() {},
      ensureCodexKernel: async () => ({ ok: true }),
      streamCodexKernel: async (args) => {
        envelopes.push(args.envelope)
        if (onHop) return onHop(args)
        return { ok: true, status: 200, terminalState: 'verified', body: { id: 'resp_ok' } }
      },
    },
  })
  return { out, envelopes, res }
}

test('Responses lifecycle parser extracts only response ids', () => {
  assert.equal(
    responseIdFromSseLine('data: {"type":"response.created","response":{"id":"resp_created"}}'),
    'resp_created',
  )
  assert.equal(responseIdFromSseLine('data: {"type":"response.output_item.added","item":{"id":"item_1"}}'), null)
  assert.equal(responseIdFromSseLine('event: response.created'), null)
})

test('previous_response_id affinity wins over a conflicting conversation pin', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-response-route-'))
  const dataDir = path.join(root, 'data')
  const sticky = new StickyRouter({ dataDir, config: { sticky: { enabled: true } } })
  writeGptVm(root, 'vm-gpt-a')
  writeGptVm(root, 'vm-gpt-b')
  const req = { headers: { 'x-session-id': 'conversation-1' }, apiKeyRecord: { id: 'key-1' } }
  const body = { model: 'gpt-5.4', input: 'continue', previous_response_id: 'resp_home' }
  const sessionKey = sticky.extractPoolKey(req, body, { platform: 'openai' })
  sticky.bind(sessionKey, { accountId: 'vm-gpt-a', vmId: 'vm-gpt-a', sessionId: 'out-a' })
  sticky.bind(sticky.responseKey(req, 'resp_home'), {
    accountId: 'vm-gpt-b',
    vmId: 'vm-gpt-b',
    sessionId: 'out-b',
  })

  const picked = pickCodexCandidates(root, req, { stickyRouter: sticky, body })
  assert.equal(picked.error, undefined)
  assert.deepEqual(
    picked.candidates.map((candidate) => candidate.id),
    ['vm-gpt-b'],
  )
  assert.equal(picked.routeBound.vmId, 'vm-gpt-b')
  assert.equal(picked.routeBound.sessionId, 'out-b')
  sticky.db?.close?.()
  fs.rmSync(root, { recursive: true, force: true })
})

test('previous_response_id routing skips ordinary sticky SQLite lookups', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-response-hot-path-'))
  writeGptVm(root, 'vm-gpt-a')
  const resolved = []
  const stickyRouter = {
    collectPoolKeys: () => ['session-a', 'session-b'],
    responseKey: (_req, id) => `response:${id}`,
    resolve: (key) => {
      resolved.push(key)
      if (key === 'response:resp_home') return { accountId: 'vm-gpt-a', vmId: 'vm-gpt-a', sessionId: 'out-a' }
      throw new Error(`unexpected ordinary sticky lookup: ${key}`)
    },
  }

  const picked = pickCodexCandidates(
    root,
    { headers: {} },
    {
      stickyRouter,
      body: { model: 'gpt-5.4', input: 'continue', previous_response_id: 'resp_home' },
    },
  )
  assert.equal(picked.error, undefined)
  assert.deepEqual(resolved, ['response:resp_home'])
  assert.deepEqual(
    picked.candidates.map((candidate) => candidate.id),
    ['vm-gpt-a'],
  )
  fs.rmSync(root, { recursive: true, force: true })
})

test('successful response id affinity persists and an unknown continuation never hops randomly', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-response-persist-'))
  const dataDir = path.join(root, 'data')
  const sticky = new StickyRouter({ dataDir, config: { sticky: { enabled: true } } })
  writeGptVm(root, 'vm-gpt-a')
  writeGptVm(root, 'vm-gpt-b')
  const headers = { 'x-session-id': 'response-owner-session' }
  let ownerVm = null
  const first = await hopCodex({
    root,
    stickyRouter: sticky,
    headers,
    body: { model: 'gpt-5.4', input: 'first', stream: false },
    onHop: async ({ exec, onEvent }) => {
      ownerVm = exec.vmId
      await onEvent('data: {"type":"response.created","response":{"id":"resp_persisted"}}')
      await onEvent('data: {"type":"response.completed","response":{"id":"resp_persisted"}}')
      return { ok: true, status: 200, terminalState: 'verified' }
    },
  })
  assert.equal(first.res.statusCode, 200)
  const key = sticky.responseKey({ headers }, 'resp_persisted')
  const stored = sticky.resolve(key)
  assert.equal(stored.vmId, ownerVm)
  assert.ok(stored.sessionId)

  const reopened = new StickyRouter({ dataDir, config: { sticky: { enabled: true } } })
  assert.equal(reopened.resolve(key).vmId, ownerVm)
  assert.equal(reopened.resolve(key).sessionId, stored.sessionId)

  let unknownHops = 0
  const unknown = await hopCodex({
    root,
    stickyRouter: reopened,
    body: { model: 'gpt-5.4', input: 'continue', stream: false, previous_response_id: 'resp_unknown' },
    onHop: async () => {
      unknownHops += 1
      return { ok: true, status: 200, terminalState: 'verified' }
    },
  })
  assert.equal(unknown.res.statusCode, 409)
  assert.equal(unknown.out.error.code, 'response_not_portable')
  assert.equal(unknownHops, 0)
  reopened.db?.close?.()
  fs.rmSync(root, { recursive: true, force: true })
})

test('codex rebuild outbound session is not the inbound session', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-sess-'))
  const sticky = new StickyRouter({ dataDir: path.join(root, 'data'), config: { sticky: { enabled: true } } })
  writeGptVm(root, 'vm-gpt-a')
  const inbound = '11111111-1111-4111-8111-111111111111'
  sticky.bind(sticky.responseKey({ headers: { 'x-session-id': inbound } }, 'resp_keep'), {
    accountId: 'vm-gpt-a',
    vmId: 'vm-gpt-a',
  })
  const { envelopes } = await hopCodex({
    root,
    stickyRouter: sticky,
    headers: { 'x-session-id': inbound },
    body: {
      model: 'gpt-5.4',
      input: 'hi',
      stream: false,
      prompt_cache_key: inbound,
      conversation_id: inbound,
      session_id: inbound,
      previous_response_id: 'resp_keep',
    },
  })
  const session = envelopes[0].session
  assert.notEqual(session.session_id, inbound)
  assert.match(session.session_id, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/)
  assert.equal(session.previous_response_id, 'resp_keep')
  assert.equal(envelopes[0].body.prompt_cache_key, session.session_id)
  assert.equal(envelopes[0].body.conversation_id, undefined)
  assert.equal(envelopes[0].body.session_id, undefined)
  sticky.db?.close?.()
  fs.rmSync(root, { recursive: true, force: true })
})

test('codex kernel envelope preserves official routing headers without client auth', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-routing-headers-'))
  const sticky = new StickyRouter({ dataDir: path.join(root, 'data'), config: { sticky: { enabled: true } } })
  writeGptVm(root, 'vm-gpt-a')
  const { envelopes } = await hopCodex({
    root,
    stickyRouter: sticky,
    headers: {
      authorization: 'Bearer sub2api-key',
      originator: 'Codex Desktop',
      'openai-beta': 'responses_websockets=2026-02-06',
      'session-id': 'session:t',
      'thread-id': 'thread:t',
      'x-client-request-id': 'request:t',
      'x-codex-beta-features': 'remote_compaction_v2',
      'x-codex-turn-metadata': '{"thread_id":"t"}',
      'x-codex-window-id': 'window:2',
      'x-openai-internal-codex-responses-lite': 'true',
    },
    body: { model: 'gpt-6.1-sol', input: 'hi', stream: false, service_tier: 'fast' },
  })
  const session = envelopes[0].session
  assert.deepEqual(envelopes[0].headers, {
    originator: 'Codex Desktop',
    'openai-beta': 'responses_websockets=2026-02-06',
    'session-id': session.session_id,
    'thread-id': 'thread:t',
    'x-client-request-id': 'request:t',
    'x-codex-beta-features': 'remote_compaction_v2',
    'x-codex-turn-metadata': '{"thread_id":"t"}',
    'x-codex-window-id': 'window:2',
    'x-openai-internal-codex-responses-lite': 'true',
    'x-codex-routing-hint': 'model=gpt-6.1-sol;tier=priority',
  })
  assert.notEqual(envelopes[0].headers['session-id'], 'session:t')
  assert.equal(envelopes[0].headers['session-id'], session.session_id)
  assert.equal(envelopes[0].headers.authorization, undefined)
  assert.equal(envelopes[0].body.service_tier, 'priority')
  sticky.db?.close?.()
  fs.rmSync(root, { recursive: true, force: true })
})

test('codex websocket transport stays on the kernel envelope and never enters the OpenAI body', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-ws-'))
  const sticky = new StickyRouter({ dataDir: path.join(root, 'data'), config: { sticky: { enabled: true } } })
  writeGptVm(root, 'vm-gpt-a')
  const { envelopes } = await hopCodex({
    root,
    stickyRouter: sticky,
    routing: { codex: { transport: 'websocket' } },
    body: { model: 'gpt-6.1-sol', input: 'hi', stream: false },
  })
  assert.equal(envelopes[0].transport, 'websocket')
  assert.equal(envelopes[0].body.transport, undefined)
  sticky.db?.close?.()
  fs.rmSync(root, { recursive: true, force: true })
})

test('codex rebuild outbound session is stable on the same slot', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-stable-'))
  const sticky = new StickyRouter({ dataDir: path.join(root, 'data'), config: { sticky: { enabled: true } } })
  writeGptVm(root, 'vm-gpt-a')
  const inbound = 'sess-stable'
  const first = await hopCodex({
    root,
    stickyRouter: sticky,
    headers: { 'x-session-id': inbound },
    body: { model: 'gpt-5.4', input: 'hi', stream: false },
  })
  const second = await hopCodex({
    root,
    stickyRouter: sticky,
    headers: { 'x-session-id': inbound },
    body: { model: 'gpt-5.4', input: 'hi again', stream: false },
  })
  assert.equal(first.envelopes[0].session.session_id, second.envelopes[0].session.session_id)
  sticky.db?.close?.()
  fs.rmSync(root, { recursive: true, force: true })
})

test('codex rebuild remints session after failover to another slot', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-fail-'))
  const sticky = new StickyRouter({ dataDir: path.join(root, 'data'), config: { sticky: { enabled: true } } })
  writeGptVm(root, 'vm-gpt-a')
  writeGptVm(root, 'vm-gpt-b')
  const inbound = 'sess-fail'
  const seen = []
  const envelopes = []
  const res = emptyCodexRes()
  await handleCodexProtocol({
    req: { headers: { 'x-session-id': inbound }, apiKeyKind: 'user' },
    res,
    protocol: 'openai.responses',
    ctx: { body: { model: 'gpt-5.4', input: 'hi', stream: false } },
    inbound: { stream: false },
    logBag: {},
    stats: { errors: 0, requests: 0, by_route: {} },
    json: (_res, status, payload) => {
      res.statusCode = status
      return payload
    },
    writeSSEHeaders() {
      res.headersSent = true
    },
    routing: {},
    projectRoot: root,
    stickyRouter: sticky,
    ops: {
      writeCodexKernelConfig() {},
      ensureCodexKernel: async () => ({ ok: true }),
      streamCodexKernel: async ({ exec, envelope }) => {
        seen.push(exec.vmId)
        envelopes.push(envelope)
        if (exec.vmId === 'vm-gpt-a') {
          return {
            ok: false,
            status: 429,
            committed: false,
            headers: {
              'x-codex-primary-used-percent': '100',
              'x-codex-primary-window-minutes': '300',
              'x-codex-primary-reset-after-seconds': '60',
            },
            body: { error: { type: 'api_error', code: 'usage_limit_reached', message: '5h' } },
          }
        }
        return { ok: true, status: 200, terminalState: 'verified', body: { id: 'resp_ok' } }
      },
    },
  })
  assert.deepEqual(seen, ['vm-gpt-a', 'vm-gpt-b'])
  assert.equal(envelopes.length, 2)
  assert.notEqual(envelopes[0].session.session_id, inbound)
  assert.notEqual(envelopes[1].session.session_id, inbound)
  assert.notEqual(envelopes[0].session.session_id, envelopes[1].session.session_id)
  rt.resetOpenAIAccountRuntime()
  sticky.db?.close?.()
  fs.rmSync(root, { recursive: true, force: true })
})

test('codex rebuild recovers the same cache key when a conversation returns to its original slot', async () => {
  const rt = await import('../../src/lib/pool/openai-account-runtime.mjs')
  rt.resetOpenAIAccountRuntime()
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-return-'))
  const sticky = new StickyRouter({ dataDir: path.join(root, 'data'), config: { sticky: { enabled: true } } })
  writeGptVm(root, 'vm-gpt-a')
  writeGptVm(root, 'vm-gpt-b')
  const headers = { 'x-session-id': 'sess-return' }
  const envelopes = []
  let phase = 1
  const run = () =>
    hopCodex({
      root,
      stickyRouter: sticky,
      headers,
      body: { model: 'gpt-5.4', input: 'hi', stream: false },
      onHop: async ({ exec, envelope }) => {
        envelopes.push({ vmId: exec.vmId, envelope })
        const fail = (phase === 1 && exec.vmId === 'vm-gpt-a') || (phase === 2 && exec.vmId === 'vm-gpt-b')
        if (fail) {
          return {
            ok: false,
            status: 429,
            committed: false,
            body: { error: { code: 'upstream_rate_limit', message: 'try another slot' } },
          }
        }
        return { ok: true, status: 200, terminalState: 'verified', body: { id: `resp-${phase}` } }
      },
    })
  try {
    await run()
    phase = 2
    await run()
    assert.deepEqual(
      envelopes.map((item) => item.vmId),
      ['vm-gpt-a', 'vm-gpt-b', 'vm-gpt-b', 'vm-gpt-a'],
    )
    assert.equal(envelopes[0].envelope.session.session_id, envelopes[3].envelope.session.session_id)
    assert.notEqual(envelopes[0].envelope.session.session_id, envelopes[1].envelope.session.session_id)
  } finally {
    rt.resetOpenAIAccountRuntime()
    sticky.db?.close?.()
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('codex passthrough keeps the inbound session and cache key', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-pass-'))
  const sticky = new StickyRouter({ dataDir: path.join(root, 'data'), config: { sticky: { enabled: true } } })
  writeGptVm(root, 'vm-gpt-a')
  const inbound = 'sess-pass'
  const { envelopes } = await hopCodex({
    root,
    stickyRouter: sticky,
    routing: { sticky: { outbound_session: 'passthrough' } },
    headers: { 'x-session-id': inbound },
    body: {
      model: 'gpt-5.4',
      input: 'hi',
      stream: false,
      prompt_cache_key: 'cache-pass',
      conversation_id: inbound,
    },
  })
  assert.equal(envelopes[0].session.session_id, inbound)
  assert.equal(envelopes[0].body.prompt_cache_key, 'cache-pass')
  assert.equal(envelopes[0].body.conversation_id, inbound)
  sticky.db?.close?.()
  fs.rmSync(root, { recursive: true, force: true })
})

test('codex passthrough without an inbound session derives a stable fallback session', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-pass-fallback-'))
  const sticky = new StickyRouter({ dataDir: path.join(root, 'data'), config: { sticky: { enabled: true } } })
  writeGptVm(root, 'vm-gpt-a')
  const opener = (question) => ({
    model: 'gpt-5.4',
    stream: false,
    input: [
      {
        type: 'message',
        role: 'user',
        content: [
          { type: 'input_text', text: '<system-reminder>Today: 2026-10-04; cwd: /work/demo</system-reminder>' },
          { type: 'input_text', text: question },
        ],
      },
    ],
  })
  const routing = { sticky: { outbound_session: 'passthrough' } }
  const first = await hopCodex({ root, stickyRouter: sticky, routing, body: opener('会话 A 的问题') })
  const again = await hopCodex({ root, stickyRouter: sticky, routing, body: opener('会话 A 的问题') })
  const other = await hopCodex({ root, stickyRouter: sticky, routing, body: opener('会话 B 的问题') })
  const sessionId = first.envelopes[0].session.session_id
  assert.match(sessionId, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/)
  assert.equal(again.envelopes[0].session.session_id, sessionId)
  assert.notEqual(other.envelopes[0].session.session_id, sessionId)
  sticky.db?.close?.()
  fs.rmSync(root, { recursive: true, force: true })
})

test('openai.responses stream names each kernel SSE frame after its payload type', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-sse-names-'))
  writeGptVm(root, 'vm-gpt-a')
  const writes = []
  const res = {
    headersSent: false,
    write(chunk) {
      this.headersSent = true
      writes.push(String(chunk))
    },
    end() {},
  }
  await handleCodexProtocol({
    req: { headers: {} },
    res,
    protocol: 'openai.responses',
    ctx: { path: '/v1/responses', body: { model: 'gpt-5.5', input: 'hi', stream: true } },
    inbound: { stream: true },
    logBag: {},
    stats: { errors: 0, requests: 0, by_route: {} },
    json: (_res, status, body) => {
      res.statusCode = status
      res.body = body
      return body
    },
    writeSSEHeaders() {
      res.headersSent = true
    },
    routing: {},
    projectRoot: root,
    ops: {
      writeCodexKernelConfig() {},
      ensureCodexKernel: async () => ({ ok: true }),
      streamCodexKernel: async ({ onEvent }) => {
        // kin-codex-kernel labels every frame "event: response"
        for (const line of [
          'event: response',
          'data: {"type":"response.created","response":{"id":"resp_1"}}',
          '',
          'event: response',
          'data: {"type":"response.output_text.delta","delta":"pong"}',
          '',
          'event: response',
          'data: {"type":"response.completed","response":{"id":"resp_1","status":"completed"}}',
          '',
        ]) {
          await onEvent(line)
        }
        return { ok: true, status: 200, terminalState: 'verified' }
      },
    },
  })
  const out = writes.join('')
  assert.doesNotMatch(out, /^event: response$/m)
  assert.match(out, /^event: response\.created\ndata: \{"type":"response\.created"/m)
  assert.match(out, /^event: response\.output_text\.delta\ndata: /m)
  assert.match(out, /^event: response\.completed\ndata: /m)
  fs.rmSync(root, { recursive: true, force: true })
})

test('openai.responses writes one downstream chunk for one kernel event batch', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-sse-batch-'))
  writeGptVm(root, 'vm-gpt-a')
  const writes = []
  const res = {
    headersSent: false,
    write(chunk) {
      this.headersSent = true
      writes.push(String(chunk))
    },
    end() {},
  }
  try {
    await handleCodexProtocol({
      req: { headers: {} },
      res,
      protocol: 'openai.responses',
      ctx: { path: '/v1/responses', body: { model: 'gpt-5.5', input: 'hi', stream: true } },
      inbound: { stream: true },
      logBag: {},
      stats: { errors: 0, requests: 0, by_route: {} },
      json: (_res, status, body) => {
        res.statusCode = status
        res.body = body
        return body
      },
      writeSSEHeaders() {
        res.headersSent = true
      },
      routing: {},
      projectRoot: root,
      ops: {
        writeCodexKernelConfig() {},
        ensureCodexKernel: async () => ({ ok: true }),
        streamCodexKernel: async ({ onEventBatch }) => {
          await onEventBatch([
            { line: 'event: response', event: null },
            {
              line: 'data: {"type":"response.created","response":{"id":"resp_batch"}}',
              event: { type: 'response.created', response: { id: 'resp_batch' } },
            },
            { line: '', event: null },
            { line: 'event: response', event: null },
            {
              line: 'data: {"type":"response.output_text.delta","delta":"pong"}',
              event: { type: 'response.output_text.delta', delta: '1' },
            },
            { line: '', event: null },
            { line: 'event: response', event: null },
            {
              line: 'data: {"type":"response.completed","response":{"id":"resp_batch","status":"completed"}}',
              event: { type: 'response.completed', response: { id: 'resp_batch', status: 'completed' } },
            },
            { line: '', event: null },
          ])
          return { ok: true, status: 200, terminalState: 'verified' }
        },
      },
    })
    assert.equal(writes.length, 1)
    assert.match(writes[0], /^event: response\.created/m)
    assert.match(writes[0], /^event: response\.output_text\.delta/m)
    assert.match(writes[0], /^event: response\.completed/m)
  } finally {
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('tool argument disagreement terminates the client stream instead of recording token usage as success', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-args-'))
  writeGptVm(root, 'vm-gpt-tools')
  let wire = ''
  const res = {
    headersSent: false,
    write: (chunk) => {
      wire += chunk
    },
    end() {},
  }
  const stats = { errors: 0, requests: 0, by_route: {} }
  const logBag = {}
  try {
    await handleCodexProtocol({
      req: { headers: {}, apiKeyKind: 'user' },
      res,
      protocol: 'openai.chat',
      ctx: { body: { model: 'gpt-5.4', messages: [{ role: 'user', content: 'edit' }], stream: true } },
      inbound: { stream: true },
      logBag,
      stats,
      projectRoot: root,
      routing: {},
      json: (_res, status, body) => {
        throw new Error(`Unexpected JSON ${status}: ${JSON.stringify(body)}`)
      },
      writeSSEHeaders: () => {
        res.headersSent = true
      },
      ops: {
        writeCodexKernelConfig() {},
        ensureCodexKernel: async () => ({ ok: true }),
        streamCodexKernel: async ({ onEvent }) => {
          for (const event of [
            {
              type: 'response.output_item.added',
              output_index: 0,
              item: { type: 'function_call', call_id: 'call_a', id: 'fc_a', name: 'edit', arguments: '' },
            },
            { type: 'response.function_call_arguments.delta', output_index: 0, delta: '{"path":' },
            { type: 'response.function_call_arguments.done', output_index: 0, arguments: '{"other":true}' },
            { type: 'response.completed', response: { usage: { input_tokens: 20, output_tokens: 3 } } },
          ])
            await onEvent(`data: ${JSON.stringify(event)}`)
          return { ok: true, status: 200, usage: { input_tokens: 20, output_tokens: 3 }, terminalState: 'verified' }
        },
      },
    })
    assert.match(wire, /"code":"tool_arguments_mismatch"/)
    assert.doesNotMatch(wire, /"finish_reason":"tool_calls"/)
    assert.equal(stats.errors, 1)
    assert.equal(logBag.error_code, 'tool_arguments_mismatch')
    assert.equal(logBag.upstream_status, 502)
  } finally {
    fs.rmSync(root, { recursive: true, force: true })
  }
})
