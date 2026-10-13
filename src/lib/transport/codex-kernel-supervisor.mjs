/**
 * Host-side Codex kernel lifecycle. Dedicated Codex VMs listen on
 * vms/<id>/run/codex-kernel.sock; Claude workers are never reused.
 */
import { spawn } from 'node:child_process'
import crypto from 'node:crypto'
import fs from 'node:fs'
import path from 'node:path'
import { boundProxyUrl, isLocalEgressProxy, localEgressProxyUrl } from '../vm/egress.mjs'
import { assertProxyAllowed } from '../vm/proxy-policy.mjs'
import { syncCodexAccountScheduling } from '../vm/codex-slot.mjs'
import { codexKernelHealth, codexKernelPaths } from './codex-kernel-client.mjs'

const starts = new Map()
const lifecycleOps = new Map()
const writtenConfig = new Map()
const knownPaths = new Map()
const readyCache = new Map()
const DEFAULT_KERNEL_LOG_MAX_BYTES = 5 * 1024 * 1024
const DEFAULT_KERNEL_HEALTH_CACHE_MS = 1500

function kernelHealthCacheMs() {
  const configured = Number(process.env.KIN_CODEX_KERNEL_HEALTH_CACHE_MS)
  if (!Number.isFinite(configured)) return DEFAULT_KERNEL_HEALTH_CACHE_MS
  return Math.min(10_000, Math.max(100, Math.trunc(configured)))
}

export function invalidateCodexKernelHealth(vmId) {
  readyCache.delete(String(vmId || ''))
}

function rememberKernelReady(vmId, result) {
  readyCache.set(String(vmId || ''), {
    until: Date.now() + kernelHealthCacheMs(),
    result,
  })
  return result
}

function cachedKernelReady(exec) {
  const key = String(exec?.vmId || '')
  const cached = readyCache.get(key)
  if (!cached || cached.until <= Date.now()) {
    readyCache.delete(key)
    return null
  }
  const socketPath = codexKernelPaths(exec).socketPath
  if (!socketPath || !fs.existsSync(socketPath)) {
    readyCache.delete(key)
    return null
  }
  return { ...cached.result, cached: true }
}

function healthFailureCode(result) {
  return String(result?.health?.code || result?.code || '')
}

function endpointDefinitelyMissing(paths, result) {
  if (!paths?.socketPath || !fs.existsSync(paths.socketPath)) return true
  return /^(?:ENOENT|ECONNREFUSED|worker_socket_missing)$/i.test(healthFailureCode(result))
}

function endpointTemporarilyBusy(paths, result) {
  if (!paths?.socketPath || !fs.existsSync(paths.socketPath)) return false
  if (Number(result?.health?.status ?? result?.status ?? 0) !== 0) return false
  return /worker_timeout|ETIMEDOUT|ECONNRESET|EPIPE|UND_ERR_SOCKET/i.test(healthFailureCode(result))
}

async function withKernelLifecycle(vmId, operation) {
  const previous = lifecycleOps.get(vmId) || Promise.resolve()
  const current = previous.catch(() => {}).then(operation)
  lifecycleOps.set(vmId, current)
  try {
    return await current
  } finally {
    if (lifecycleOps.get(vmId) === current) lifecycleOps.delete(vmId)
  }
}

function rememberPaths(vmId, paths) {
  if (vmId && paths?.runDir) knownPaths.set(String(vmId), paths)
  return paths
}

function readKernelPid(pidPath) {
  if (!pidPath) return 0
  try {
    const pid = Number(fs.readFileSync(pidPath, 'utf8').trim())
    return Number.isInteger(pid) && pid > 0 ? pid : 0
  } catch {
    return 0
  }
}

function pidAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 0) return false
  try {
    process.kill(pid, 0)
    return true
  } catch {
    return false
  }
}

function ownsKernelProcess(pid, configPath) {
  if (!pidAlive(pid) || !configPath) return false
  const tracked = [...starts.values()].find((child) => child?.pid === pid)
  if (tracked) return true
  try {
    // PID files survive a Node restart. Never signal a reused PID unless its
    // argv still names this exact slot config.
    const args = fs.readFileSync(`/proc/${pid}/cmdline`, 'utf8').split('\0').filter(Boolean)
    return (
      args.some((arg) => path.basename(arg).replace(/ \(deleted\)$/, '') === 'kin-codex-kernel') &&
      args.some((arg) => path.resolve(arg) === path.resolve(configPath))
    )
  } catch {
    return false
  }
}

function kernelProcess(vmId, paths = knownPaths.get(String(vmId))) {
  const child = starts.get(String(vmId))
  if (child && child.exitCode == null && child.signalCode == null) {
    return { pid: child.pid, child, paths }
  }
  const pid = readKernelPid(paths?.pidPath)
  if (!ownsKernelProcess(pid, paths?.configPath)) return null
  return { pid, child: null, paths }
}

function removePidIfMatches(paths, pid) {
  if (!paths?.pidPath || readKernelPid(paths.pidPath) !== Number(pid)) return
  try {
    fs.rmSync(paths.pidPath, { force: true })
  } catch {}
}

function signalKernel(proc, signal) {
  if (!proc?.pid) return false
  try {
    if (proc.child) proc.child.kill(signal)
    else process.kill(proc.pid, signal)
    return true
  } catch (error) {
    return error?.code === 'ESRCH'
  }
}

async function waitForKernelExit(proc, timeoutMs) {
  const deadline = Date.now() + Math.max(1, Number(timeoutMs) || 1)
  while (pidAlive(proc?.pid) && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
  const exited = !pidAlive(proc?.pid)
  if (exited) removePidIfMatches(proc?.paths, proc?.pid)
  return exited
}

function writePrivateFileIfChanged(file, content) {
  if (writtenConfig.get(file) === content) return false
  if (!writtenConfig.has(file)) {
    try {
      if (fs.readFileSync(file, 'utf8') === content) {
        writtenConfig.set(file, content)
        return false
      }
    } catch {}
  }
  fs.writeFileSync(file, content, { mode: 0o600 })
  writtenConfig.set(file, content)
  return true
}

function kernelLogMaxBytes() {
  const configured = Number(process.env.KIN_CODEX_KERNEL_LOG_MAX_BYTES)
  if (!Number.isFinite(configured)) return DEFAULT_KERNEL_LOG_MAX_BYTES
  return Math.min(100 * 1024 * 1024, Math.max(64 * 1024, Math.trunc(configured)))
}

function openKernelLog(runDir, logPath = path.join(runDir, 'codex-kernel.log')) {
  fs.mkdirSync(runDir, { recursive: true, mode: 0o700 })
  const rotatedPath = `${logPath}.1`
  try {
    if (fs.statSync(logPath).size >= kernelLogMaxBytes()) {
      try {
        fs.unlinkSync(rotatedPath)
      } catch {}
      fs.renameSync(logPath, rotatedPath)
    }
  } catch {}
  return { logPath, fd: fs.openSync(logPath, 'a', 0o600) }
}

// The kernel exits only through config.proxy_url; inherited proxy variables would give it a second, unseen exit.
function kernelEnv(env = process.env) {
  return Object.fromEntries(Object.entries(env).filter(([key]) => !/^(https?|all|no)_proxy$/i.test(key)))
}

function existingBin(candidate) {
  const bin = String(candidate || '').trim()
  if (!bin) return ''
  try {
    fs.accessSync(bin, fs.constants.X_OK)
    return bin
  } catch {
    return ''
  }
}

export function codexKernelBinPath() {
  const env = existingBin(process.env.KIN_CODEX_KERNEL_BIN)
  if (env) return env
  const kernel = String(process.env.KIN_KERNEL_BIN || process.env.KIN_API_KERNEL_BIN || '').trim()
  if (kernel) {
    const sibling = existingBin(path.join(path.dirname(kernel), 'kin-codex-kernel'))
    if (sibling) return sibling
  }
  return existingBin(path.resolve('bin/kin-codex-kernel'))
}

export function writeCodexKernelConfig(projectRoot, vm, { token, proxyUrl, proxyRequired } = {}) {
  if (!projectRoot || !vm?.id) return null
  syncCodexAccountScheduling(projectRoot, vm)
  assertProxyAllowed(vm.proxy)
  if (proxyUrl) assertProxyAllowed({ url: proxyUrl })
  const runDir = path.join(projectRoot, 'vms', vm.id, 'run')
  fs.mkdirSync(runDir, { recursive: true, mode: 0o700 })
  const paths = codexKernelPaths({
    vmId: vm.id,
    vm,
    homeDir: path.join(projectRoot, 'vms', vm.id, 'cli-home'),
  })
  const { socketPath, configPath, tokenPath, logPath, pidPath, credentialPath } = paths
  let secret = String(token || '').trim()
  if (!secret) {
    try {
      secret = fs.readFileSync(tokenPath, 'utf8').trim()
    } catch {}
  }
  if (!secret) secret = crypto.randomBytes(24).toString('hex')
  writePrivateFileIfChanged(tokenPath, secret + '\n')
  const local = isLocalEgressProxy(vm?.proxy)
  const proxy = local ? localEgressProxyUrl() : String(proxyUrl || boundProxyUrl(vm?.proxy) || '').trim()
  // A configured deployment proxy is this slot's exit: fail closed instead of going direct.
  const required = local ? !!proxy : proxyRequired == null ? !!proxy : !!proxyRequired
  const deviceId = String(vm.device_id || vm.fingerprint?.device_id || vm.id).trim() || vm.id
  const config = {
    vm_id: vm.id,
    device_id: deviceId,
    socket_path: socketPath,
    credential_path: credentialPath,
    proxy_url: proxy,
    proxy_required: required,
    internal_token: secret,
    // Keep Codex slots aligned with the 32 MiB body budget in the bundled
    // kernel. Newer kernels can read this value; older binaries use the
    // patched 32 MiB default directly.
    max_request_bytes: 32 * 1024 * 1024,
    test_endpoints: process.env.KIN_CODEX_TEST_ENDPOINTS === '1',
  }
  writePrivateFileIfChanged(configPath, JSON.stringify(config, null, 2) + '\n')
  return rememberPaths(vm.id, { ...paths, runDir })
}

async function ensureCodexKernelUnlocked(exec, { timeoutMs = 8000 } = {}) {
  const paths = rememberPaths(exec.vmId, codexKernelPaths(exec))
  if (!paths.configPath) return { ok: false, reason: 'config_missing' }
  const live = await waitForHealth(exec, Math.min(800, Math.max(200, Number(timeoutMs) || 800)))
  if (live.ok) return { ok: true, reused: true, health: live.health }
  // A second blue/green gateway has no child-process handle for the live
  // kernel. A saturated health route is not proof that its Unix socket is
  // stale; replacing that socket disconnects the other gateway's whole pool.
  if (endpointTemporarilyBusy(paths, live)) {
    return { ok: true, reused: true, degraded: true, health: live.health }
  }
  const current = starts.get(exec.vmId)
  if (current && !current.killed) {
    const waited = await waitForHealth(exec, timeoutMs)
    if (waited.ok) return waited
    if (endpointTemporarilyBusy(paths, waited)) {
      return { ok: true, reused: true, degraded: true, health: waited.health }
    }
    if (!endpointDefinitelyMissing(paths, waited)) return waited
    const proc = kernelProcess(exec.vmId, paths)
    if (proc) {
      signalKernel(proc, 'SIGTERM')
      starts.delete(exec.vmId)
      if (!(await waitForKernelExit(proc, Math.min(3000, Math.max(200, Number(timeoutMs) || 8000))))) {
        signalKernel(proc, 'SIGKILL')
        if (!(await waitForKernelExit(proc, 1000))) return { ok: false, reason: 'kernel_stop_timeout' }
      }
    }
  } else if (!endpointDefinitelyMissing(paths, live)) {
    return live
  }
  const bin = codexKernelBinPath()
  if (!bin) return { ok: false, reason: 'bin_missing' }
  try {
    if (paths.socketPath) fs.unlinkSync(paths.socketPath)
  } catch {}
  const { fd: logFd } = openKernelLog(paths.runDir, paths.logPath)
  let child
  try {
    child = spawn(bin, [paths.configPath], {
      env: kernelEnv(),
      stdio: ['ignore', logFd, logFd],
      detached: true,
    })
    fs.writeFileSync(paths.pidPath, String(child.pid) + '\n', { mode: 0o600 })
  } finally {
    fs.closeSync(logFd)
  }
  const forget = () => {
    if (starts.get(exec.vmId) === child) starts.delete(exec.vmId)
    removePidIfMatches(paths, child.pid)
  }
  child.once('exit', forget)
  child.once('error', forget)
  child.unref()
  starts.set(exec.vmId, child)
  return waitForHealth(exec, timeoutMs)
}

export async function ensureCodexKernel(exec, { timeoutMs = 8000 } = {}) {
  const vmId = String(exec?.vmId || '')
  if (!vmId) return { ok: false, reason: 'vm_missing' }
  const cached = cachedKernelReady(exec)
  if (cached) return cached
  const pending = lifecycleOps.get(vmId)
  const result = pending
    ? await pending
    : await withKernelLifecycle(vmId, () => ensureCodexKernelUnlocked(exec, { timeoutMs }))
  return result?.ok ? rememberKernelReady(vmId, result) : result
}

async function waitForHealth(exec, timeoutMs) {
  const deadline = Date.now() + Math.max(200, Number(timeoutMs) || 8000)
  let last = { ok: false, reason: 'not_ready' }
  while (Date.now() < deadline) {
    last = await codexKernelHealth(exec, { timeoutMs: 500 }).catch((error) => ({
      ok: false,
      reason: 'health_error',
      error: String(error.message || error),
    }))
    if (last?.ok || Number(last?.status) === 200) return { ok: true, health: last }
    await new Promise((r) => setTimeout(r, 100))
  }
  return { ok: false, reason: 'health_timeout', health: last }
}

export function stopCodexKernel(vmId) {
  const key = String(vmId || '')
  invalidateCodexKernelHealth(key)
  const proc = kernelProcess(key)
  if (!proc) return null
  signalKernel(proc, 'SIGTERM')
  starts.delete(key)
  return proc.child || { pid: proc.pid, exitCode: null, signalCode: 'SIGTERM', detached: true }
}

export async function stopCodexKernelAndWait(vmId, { timeoutMs = 3000 } = {}) {
  const key = String(vmId || '')
  if (!key) return { ok: true, stopped: false }
  invalidateCodexKernelHealth(key)
  return withKernelLifecycle(key, async () => {
    const proc = kernelProcess(key)
    if (!proc) return { ok: true, stopped: false }
    signalKernel(proc, 'SIGTERM')
    starts.delete(key)
    const graceMs = Math.min(30_000, Math.max(200, Number(timeoutMs) || 3000))
    if (await waitForKernelExit(proc, graceMs)) return { ok: true, stopped: true, pid: proc.pid, signal: 'SIGTERM' }
    signalKernel(proc, 'SIGKILL')
    if (await waitForKernelExit(proc, 1000)) return { ok: true, stopped: true, pid: proc.pid, signal: 'SIGKILL' }
    return { ok: false, stopped: false, pid: proc.pid, reason: 'kernel_stop_timeout' }
  })
}

export async function restartCodexKernel(exec, { timeoutMs = 8000 } = {}) {
  const vmId = String(exec?.vmId || '')
  if (!vmId) return { ok: false, reason: 'vm_missing' }
  invalidateCodexKernelHealth(vmId)
  return withKernelLifecycle(vmId, async () => {
    const paths = rememberPaths(vmId, codexKernelPaths(exec))
    const proc = kernelProcess(vmId, paths)
    if (proc) {
      signalKernel(proc, 'SIGTERM')
      starts.delete(vmId)
      const exited = await waitForKernelExit(proc, Math.min(3000, Math.max(200, timeoutMs)))
      if (!exited) {
        signalKernel(proc, 'SIGKILL')
        if (!(await waitForKernelExit(proc, 1000))) return { ok: false, reason: 'kernel_stop_timeout' }
      }
    }
    try {
      if (paths.socketPath) fs.unlinkSync(paths.socketPath)
    } catch {}
    return ensureCodexKernelUnlocked(exec, { timeoutMs })
  })
}

export function stopAllCodexKernels({ includeAdopted = false } = {}) {
  const ids = includeAdopted ? new Set([...starts.keys(), ...knownPaths.keys()]) : new Set(starts.keys())
  for (const vmId of ids) stopCodexKernel(vmId)
}
