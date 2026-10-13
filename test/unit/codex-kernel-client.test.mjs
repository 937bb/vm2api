import { test } from 'node:test'
import assert from 'node:assert/strict'
import http from 'node:http'
import path from 'node:path'
import { codexKernelPaths, withCodexKernelExec } from '../../src/lib/transport/codex-kernel-client.mjs'
import {
  ensureCodexKernel,
  invalidateCodexKernelHealth,
  restartCodexKernel,
  stopCodexKernel,
  stopCodexKernelAndWait,
  writeCodexKernelConfig,
} from '../../src/lib/transport/codex-kernel-supervisor.mjs'
import fs from 'node:fs'
import os from 'node:os'

const PROXY_ENV_KEYS = [
  'HTTPS_PROXY',
  'https_proxy',
  'HTTP_PROXY',
  'http_proxy',
  'ALL_PROXY',
  'all_proxy',
  'NO_PROXY',
  'no_proxy',
]
const LOCAL_CODEX = {
  id: 'vm-codex',
  platform: 'openai',
  proxy: { id: 'px-local', scheme: 'local', host: 'local', port: 0 },
}

function setEnv(t, values) {
  const keys = [...new Set([...PROXY_ENV_KEYS, ...Object.keys(values)])]
  const saved = Object.fromEntries(keys.map((k) => [k, process.env[k]]))
  for (const k of PROXY_ENV_KEYS) delete process.env[k]
  Object.assign(process.env, values)
  t.after(() => {
    for (const [k, v] of Object.entries(saved)) {
      if (v === undefined) delete process.env[k]
      else process.env[k] = v
    }
  })
}
test('codex kernel socket is not worker.sock', () => {
  const exec = {
    homeDir: '/tmp/vms/vm-codex/cli-home',
    vm: { id: 'vm-codex', runtime: {} },
  }
  const paths = codexKernelPaths(exec)
  assert.equal(paths.socketPath, path.join('/tmp/vms/vm-codex/run', 'codex-kernel.sock'))
  assert.equal(paths.pidPath, path.join('/tmp/vms/vm-codex/run', 'codex-kernel.pid'))
  const remapped = withCodexKernelExec(exec)
  assert.match(remapped.vm.runtime.worker_socket, /codex-kernel\.sock$/)
})

test('codex kernel instance isolates blue-green runtime files', () => {
  const previous = process.env.KIN_CODEX_KERNEL_INSTANCE
  process.env.KIN_CODEX_KERNEL_INSTANCE = 'canary/v34'
  try {
    const paths = codexKernelPaths({ homeDir: '/tmp/vms/vm-codex/cli-home', vm: {} })
    assert.equal(paths.socketPath, path.join('/tmp/vms/vm-codex/run', 'codex-kernel-canary-v34.sock'))
    assert.equal(paths.configPath, path.join('/tmp/vms/vm-codex/run', 'codex-kernel-canary-v34.json'))
    assert.equal(paths.logPath, path.join('/tmp/vms/vm-codex/run', 'codex-kernel-canary-v34.log'))
    assert.equal(paths.pidPath, path.join('/tmp/vms/vm-codex/run', 'codex-kernel-canary-v34.pid'))
    assert.equal(paths.tokenPath, path.join('/tmp/vms/vm-codex/run', 'internal.token'))
  } finally {
    if (previous == null) delete process.env.KIN_CODEX_KERNEL_INSTANCE
    else process.env.KIN_CODEX_KERNEL_INSTANCE = previous
  }
})

test('writeCodexKernelConfig writes bound SOCKS proxy', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-'))
  const vm = {
    id: 'vm-codex',
    platform: 'openai',
    family: 'codex',
    policy: { maxConcurrency: 40, maxRpm: 0 },
    proxy: { url: 'socks5://127.0.0.1:1080' },
  }
  fs.mkdirSync(path.join(root, 'vms', vm.id), { recursive: true })
  fs.writeFileSync(
    path.join(root, 'vms', vm.id, 'codex-credentials.json'),
    JSON.stringify({ accounts: [{ access_token: 'at', max_concurrency: 1, rpm: 99 }] }),
  )
  const written = writeCodexKernelConfig(root, vm, {
    token: 'secret',
    proxyUrl: 'socks5h://127.0.0.1:1080',
    proxyRequired: true,
  })
  const cfg = JSON.parse(fs.readFileSync(written.configPath, 'utf8'))
  assert.equal(cfg.proxy_required, true)
  assert.equal(cfg.proxy_url, 'socks5h://127.0.0.1:1080')
  assert.equal(cfg.socket_path, written.socketPath)
  assert.equal(cfg.internal_token, 'secret')
  assert.equal(cfg.max_request_bytes, 32 * 1024 * 1024)
  const account = JSON.parse(fs.readFileSync(written.credentialPath, 'utf8')).accounts[0]
  assert.equal(account.max_concurrency, 40)
  assert.equal(account.rpm, 0)
  fs.rmSync(root, { recursive: true, force: true })
})

test('writeCodexKernelConfig does not rewrite unchanged hot-path files', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-stable-config-'))
  const vm = { id: 'vm-codex' }
  const first = writeCodexKernelConfig(root, vm, { token: 'secret', proxyUrl: '', proxyRequired: false })
  const before = {
    token: fs.statSync(first.tokenPath).mtimeMs,
    config: fs.statSync(first.configPath).mtimeMs,
  }
  await new Promise((resolve) => setTimeout(resolve, 20))
  writeCodexKernelConfig(root, vm, { token: 'secret', proxyUrl: '', proxyRequired: false })
  assert.deepEqual(
    {
      token: fs.statSync(first.tokenPath).mtimeMs,
      config: fs.statSync(first.configPath).mtimeMs,
    },
    before,
  )
  fs.rmSync(root, { recursive: true, force: true })
})

test('ensureCodexKernel caches a healthy shared socket instead of probing on every request', async () => {
  const root = fs.mkdtempSync('/tmp/kin-codex-health-cache-')
  const vmId = 'vm-codex-health-cache'
  const vm = { id: vmId }
  const exec = { vmId, homeDir: path.join(root, 'vms', vmId, 'cli-home'), vm: { ...vm, runtime: {} } }
  const paths = writeCodexKernelConfig(root, vm, { token: 'secret', proxyUrl: '', proxyRequired: false })
  let healthCalls = 0
  const server = http.createServer((req, res) => {
    healthCalls += 1
    res.setHeader('content-type', 'application/json')
    res.end(JSON.stringify({ ok: true, vm_id: vmId }))
  })
  await new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen(paths.socketPath, resolve)
  })
  try {
    const first = await ensureCodexKernel(exec, { timeoutMs: 500 })
    const second = await ensureCodexKernel(exec, { timeoutMs: 500 })
    assert.equal(first.ok, true)
    assert.equal(second.ok, true)
    assert.equal(second.cached, true)
    assert.equal(healthCalls, 1)
  } finally {
    invalidateCodexKernelHealth(vmId)
    await new Promise((resolve) => server.close(resolve))
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('ensureCodexKernel does not replace an existing shared socket when its health route is busy', async () => {
  const root = fs.mkdtempSync('/tmp/kin-codex-health-busy-')
  const vmId = 'vm-codex-health-busy'
  const vm = { id: vmId }
  const exec = { vmId, homeDir: path.join(root, 'vms', vmId, 'cli-home'), vm: { ...vm, runtime: {} } }
  const paths = writeCodexKernelConfig(root, vm, { token: 'secret', proxyUrl: '', proxyRequired: false })
  const sockets = new Set()
  const server = http.createServer(() => {})
  server.on('connection', (socket) => {
    sockets.add(socket)
    socket.once('close', () => sockets.delete(socket))
  })
  await new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen(paths.socketPath, resolve)
  })
  try {
    const result = await ensureCodexKernel(exec, { timeoutMs: 200 })
    assert.equal(result.ok, true)
    assert.equal(result.reused, true)
    assert.equal(result.degraded, true)
    assert.equal(fs.existsSync(paths.socketPath), true)
  } finally {
    invalidateCodexKernelHealth(vmId)
    for (const socket of sockets) socket.destroy()
    await new Promise((resolve) => server.close(resolve))
    fs.rmSync(root, { recursive: true, force: true })
  }
})

test('writeCodexKernelConfig fail-closes proxy_required without a URL', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-'))
  const written = writeCodexKernelConfig(
    root,
    { id: 'vm-codex' },
    { token: 'secret', proxyUrl: '', proxyRequired: true },
  )
  const cfg = JSON.parse(fs.readFileSync(written.configPath, 'utf8'))
  assert.equal(cfg.proxy_required, true)
  assert.equal(cfg.proxy_url, '')
  fs.rmSync(root, { recursive: true, force: true })
})

test('local egress without a deployment proxy is a direct exit', (t) => {
  setEnv(t, {})
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-'))
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  const written = writeCodexKernelConfig(root, LOCAL_CODEX, {
    token: 'secret',
    proxyUrl: 'socks5h://127.0.0.1:1080',
    proxyRequired: true,
  })
  const cfg = JSON.parse(fs.readFileSync(written.configPath, 'utf8'))
  assert.equal(cfg.proxy_required, false)
  assert.equal(cfg.proxy_url, '')
})

test('local egress hands the kernel the deployment proxy and fails closed on it', (t) => {
  setEnv(t, { HTTPS_PROXY: 'http://proxy.test:8443', HTTP_PROXY: 'http://other.test:3128' })
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-'))
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  const written = writeCodexKernelConfig(root, LOCAL_CODEX, { token: 'secret', proxyUrl: '', proxyRequired: false })
  const cfg = JSON.parse(fs.readFileSync(written.configPath, 'utf8'))
  assert.equal(cfg.proxy_url, 'http://proxy.test:8443')
  assert.equal(cfg.proxy_required, true)
})

test('codex kernel starts without inherited proxy variables', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-'))
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  const out = path.join(root, 'env.txt')
  const bin = path.join(root, 'fake-kernel')
  fs.writeFileSync(bin, `#!/bin/sh\nenv > "${out}.tmp" && mv "${out}.tmp" "${out}"\n`, { mode: 0o755 })
  setEnv(t, { HTTPS_PROXY: 'http://proxy.test:8443', no_proxy: 'chatgpt.com', KIN_CODEX_KERNEL_BIN: bin })
  const exec = {
    vmId: 'vm-codex',
    homeDir: path.join(root, 'vm-codex', 'cli-home'),
    vm: { id: 'vm-codex', runtime: {} },
  }
  t.after(() => stopCodexKernel('vm-codex'))
  await ensureCodexKernel(exec, { timeoutMs: 300 })
  for (let i = 0; i < 40 && !fs.existsSync(out); i++) await new Promise((r) => setTimeout(r, 50))
  const keys = fs
    .readFileSync(out, 'utf8')
    .split('\n')
    .map((line) => line.split('=')[0])
  assert.ok(keys.includes('KIN_CODEX_KERNEL_BIN'))
  assert.deepEqual(
    keys.filter((k) => /_proxy$/i.test(k)),
    [],
  )
  const logPath = path.join(root, 'vm-codex', 'run', 'codex-kernel.log')
  assert.ok(fs.existsSync(logPath))
  assert.equal(fs.statSync(logPath).mode & 0o777, 0o600)
})

test('codex kernel stdout and stderr are retained in a bounded per-slot log', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-log-'))
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  const bin = path.join(root, 'fake-kernel')
  fs.writeFileSync(bin, '#!/bin/sh\necho kernel-out\necho kernel-err >&2\n', { mode: 0o755 })
  setEnv(t, { KIN_CODEX_KERNEL_BIN: bin })
  const vmId = 'vm-codex-log'
  const exec = {
    vmId,
    homeDir: path.join(root, vmId, 'cli-home'),
    vm: { id: vmId, runtime: {} },
  }
  fs.mkdirSync(path.join(root, vmId, 'run'), { recursive: true })
  fs.writeFileSync(path.join(root, vmId, 'run', 'internal.token'), 'test\n')
  await ensureCodexKernel(exec, { timeoutMs: 250 })
  const logPath = path.join(root, vmId, 'run', 'codex-kernel.log')
  for (let i = 0; i < 40 && !fs.readFileSync(logPath, 'utf8').includes('kernel-err'); i++) {
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
  assert.match(fs.readFileSync(logPath, 'utf8'), /kernel-out[\s\S]*kernel-err/)
})

test('an exited Codex kernel can be started again', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-restart-'))
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  const count = path.join(root, 'starts.txt')
  const bin = path.join(root, 'fake-kernel')
  fs.writeFileSync(bin, `#!/bin/sh\necho start >> "${count}"\n`, { mode: 0o755 })
  setEnv(t, { KIN_CODEX_KERNEL_BIN: bin })
  const exec = {
    vmId: 'vm-codex-restart',
    homeDir: path.join(root, 'vm-codex-restart', 'cli-home'),
    vm: { id: 'vm-codex-restart', runtime: {} },
  }
  fs.mkdirSync(path.join(root, 'vm-codex-restart', 'run'), { recursive: true })
  fs.writeFileSync(path.join(root, 'vm-codex-restart', 'run', 'internal.token'), 'test\n')
  await ensureCodexKernel(exec, { timeoutMs: 250 })
  await new Promise((resolve) => setTimeout(resolve, 50))
  await ensureCodexKernel(exec, { timeoutMs: 250 })
  await new Promise((resolve) => setTimeout(resolve, 50))
  const starts = fs.readFileSync(count, 'utf8').trim().split('\n')
  assert.equal(starts.length, 2)
})

test('restart and concurrent ensure share one per-VM lifecycle operation', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-lifecycle-'))
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  const bin = path.join(root, 'fake-kernel')
  fs.writeFileSync(bin, '#!/bin/sh\nexit 0\n', { mode: 0o755 })
  setEnv(t, { KIN_CODEX_KERNEL_BIN: bin })
  const vmId = 'vm-codex-lifecycle'
  const exec = {
    vmId,
    homeDir: path.join(root, vmId, 'cli-home'),
    vm: { id: vmId, runtime: {} },
  }
  fs.mkdirSync(path.join(root, vmId, 'run'), { recursive: true })
  fs.writeFileSync(path.join(root, vmId, 'run', 'internal.token'), 'test\n')
  t.after(() => stopCodexKernel(vmId))

  await ensureCodexKernel(exec, { timeoutMs: 200 })
  const results = await Promise.all([
    restartCodexKernel(exec, { timeoutMs: 250 }),
    ensureCodexKernel(exec, { timeoutMs: 250 }),
    ensureCodexKernel(exec, { timeoutMs: 250 }),
  ])
  assert.strictEqual(results[0], results[1])
  assert.strictEqual(results[0], results[2])
})

test('stop and delete lifecycle waits until a tracked kernel has exited', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-stop-wait-'))
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  const bin = path.join(root, 'fake-kernel')
  fs.writeFileSync(bin, '#!/bin/sh\ntrap "exit 0" TERM\nwhile :; do sleep 1; done\n', { mode: 0o755 })
  setEnv(t, { KIN_CODEX_KERNEL_BIN: bin })
  const vmId = 'vm-codex-stop-wait'
  const exec = {
    vmId,
    homeDir: path.join(root, vmId, 'cli-home'),
    vm: { id: vmId, runtime: {} },
  }
  fs.mkdirSync(path.join(root, vmId, 'run'), { recursive: true })
  fs.writeFileSync(path.join(root, vmId, 'run', 'internal.token'), 'test\n')
  await ensureCodexKernel(exec, { timeoutMs: 200 })
  const pidPath = path.join(root, vmId, 'run', 'codex-kernel.pid')
  const pid = Number(fs.readFileSync(pidPath, 'utf8').trim())
  t.after(() => {
    try {
      process.kill(pid, 'SIGKILL')
    } catch {}
  })

  const stopped = await stopCodexKernelAndWait(vmId, { timeoutMs: 1500 })
  assert.equal(stopped.ok, true)
  assert.equal(stopped.stopped, true)
  assert.equal(fs.existsSync(pidPath), false)
  assert.throws(() => process.kill(pid, 0), /ESRCH/)
})

test('a new Node supervisor can stop a PID-file kernel only after argv ownership validation', {
  skip: process.platform !== 'linux',
}, async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-codex-adopt-'))
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  const stopped = path.join(root, 'stopped.txt')
  const bin = path.join(root, 'kin-codex-kernel')
  fs.writeFileSync(bin, `#!/bin/sh\ntrap 'echo stopped > "${stopped}"; exit 0' TERM\nwhile :; do sleep 1; done\n`, {
    mode: 0o755,
  })
  setEnv(t, { KIN_CODEX_KERNEL_BIN: bin })
  const vmId = 'vm-codex-adopt'
  const vm = { id: vmId }
  const exec = { vmId, homeDir: path.join(root, 'vms', vmId, 'cli-home'), vm: { ...vm, runtime: {} } }
  writeCodexKernelConfig(root, vm, { token: 'secret', proxyUrl: '', proxyRequired: false })
  await ensureCodexKernel(exec, { timeoutMs: 250 })
  const pidPath = path.join(root, 'vms', vmId, 'run', 'codex-kernel.pid')
  assert.ok(Number(fs.readFileSync(pidPath, 'utf8')) > 0)
  assert.equal(fs.statSync(pidPath).mode & 0o777, 0o600)

  const fresh = await import(`../../src/lib/transport/codex-kernel-supervisor.mjs?adopt=${Date.now()}`)
  fresh.writeCodexKernelConfig(root, vm, { token: 'secret', proxyUrl: '', proxyRequired: false })
  const adopted = fresh.stopCodexKernel(vmId)
  assert.equal(adopted?.detached, true)
  for (let i = 0; i < 80 && !fs.existsSync(stopped); i++) await new Promise((resolve) => setTimeout(resolve, 25))
  assert.ok(fs.existsSync(stopped))
})
