/**
 * Codex kernel hop client. Same Unix-socket + internal token contract as
 * go-worker-client, socket is run/codex-kernel.sock.
 */
import path from 'node:path'
import { callGoWorker, streamGoWorker, workerHealth, workerPaths } from './go-worker-client.mjs'

function codexKernelStem() {
  const instance = String(process.env.KIN_CODEX_KERNEL_INSTANCE || '')
    .trim()
    .replace(/[^a-zA-Z0-9._-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 64)
  return instance ? `codex-kernel-${instance}` : 'codex-kernel'
}

export function codexKernelPaths(exec = {}) {
  const base = workerPaths(exec)
  const runDir = base.runDir
  const stem = codexKernelStem()
  return {
    runDir,
    socketPath: exec.vm?.runtime?.codex_kernel_socket || (runDir ? path.join(runDir, `${stem}.sock`) : null),
    tokenPath: base.tokenPath,
    configPath: runDir ? path.join(runDir, `${stem}.json`) : null,
    logPath: runDir ? path.join(runDir, `${stem}.log`) : null,
    pidPath: runDir ? path.join(runDir, `${stem}.pid`) : null,
    credentialPath: runDir ? path.join(path.dirname(runDir), 'codex-credentials.json') : null,
  }
}

export function withCodexKernelExec(exec) {
  const paths = codexKernelPaths(exec)
  return {
    ...exec,
    vm: {
      ...(exec?.vm || {}),
      runtime: {
        ...(exec?.vm?.runtime || {}),
        worker_socket: paths.socketPath,
        worker_token_file: paths.tokenPath,
        worker_run_dir: paths.runDir,
      },
    },
  }
}

function remapVia(result) {
  if (!result || typeof result !== 'object') return result
  return {
    ...result,
    via: String(result.via || 'go-worker').replace(/go-worker/g, 'codex-kernel'),
  }
}

export async function callCodexKernel(opts = {}) {
  return remapVia(
    await callGoWorker({
      ...opts,
      exec: withCodexKernelExec(opts.exec),
      requestPath: '/internal/v1/codex/responses',
    }),
  )
}

export async function streamCodexKernel(opts = {}) {
  const { responsesPassthrough = false, ...request } = opts
  return remapVia(
    await streamGoWorker({
      ...request,
      exec: withCodexKernelExec(request.exec),
      requestPath: '/internal/v1/codex/responses',
      sseEventMode: responsesPassthrough ? 'responses-passthrough' : null,
    }),
  )
}

export async function codexKernelHealth(exec, opts = {}) {
  const health = await workerHealth(withCodexKernelExec(exec), opts)
  if (health?.source) {
    return { ...health, source: String(health.source).replace(/go-worker/g, 'codex-kernel') }
  }
  return health
}
