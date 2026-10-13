/**
 * vm2api — Node control plane.
 * Inference data plane: one long-lived Go slot worker per VM
 * (slot SOCKS5 + OAuth owner + SSE terminal validation).
 * The gateway converts protocols, schedules the account pool with bounded
 * failover, and never talks to Anthropic on the inference path. After a live
 * OAuth import it may spawn official Claude Code once (slot SOCKS5) to seed
 * ~/.claude.json and complete a plan turn.
 */
import http from 'node:http'
import fs from 'node:fs'
import path from 'node:path'
import { loadConfig, reloadActiveVm, routingConfigFile } from './lib/core/config.mjs'
import {
  extractApiKey,
  timingSafeEqualStr,
  createRateLimiter,
  verifyPanelSession,
  extractPanelToken,
  attachPanelAuth,
  getPanelAdmin,
} from './lib/core/security.mjs'
import { loadModelPolicy } from './lib/protocol/model-policy.mjs'
import { gatewayModelCatalog } from './lib/protocol/models.mjs'
import {
  createCredentialRefreshMonitor,
  normalizeCredentialRefreshConfig,
} from './lib/oauth/credential-refresh-monitor.mjs'
import { createKernelWatchdog, normalizeKernelWatchdogConfig } from './lib/transport/kernel-watchdog.mjs'
import { ensureSlotSubscriptionType } from './lib/oauth/oauth-credentials.mjs'
import { createCliNodeGuard } from './lib/vm/cli-node-guard.mjs'
import { ensureTelemetrySidecar } from './lib/vm/slot-process-status.mjs'

import { createUsageProbeMonitor, normalizeUsageProbeConfig } from './lib/oauth/usage-probe-monitor.mjs'
import { normalizeOfficialCcConfig } from './lib/oauth/official-cc-bootstrap.mjs'
import { invalidateLiveCredentialCache } from './lib/admin/panel-live-credentials.mjs'
import { normalizeHealthProbeConfig, createHealthProbeMonitor, HEALTH_REAL_HEADER } from './lib/admin/health-probe.mjs'
import { normalizeNotifyConfig, createNotifyMonitor, dispatchNotify } from './lib/admin/notify.mjs'
import { runVmTestChat } from './lib/admin/vm-test-chat.mjs'
import { StickyRouter } from './lib/pool/sticky-router.mjs'
import { setManualScheduleWins } from './lib/pool/schedule-policy.mjs'
import { AccountQuota } from './lib/pool/account-quota.mjs'
import { closeOpenAIGlobalLeaseClient } from './lib/pool/openai-global-lease.mjs'
import { normalizeTiers } from './lib/pool/quota-tiers.mjs'
import { ApiKeyStore } from './lib/admin/api-keys.mjs'
import { keyScopeFromRequest } from './lib/admin/key-scope.mjs'
import { GroupsRepo } from './lib/db/repos/groups-repo.mjs'
import { ApiEndpointStore } from './lib/admin/api-endpoints.mjs'
import { ApiScheduler } from './lib/pool/api-scheduler.mjs'
import { resolveInferenceBackend } from './lib/pool/api-protocol.mjs'
import {
  startApiKernelProcess,
  apiKernelPaths,
  ensureApiKernelToken,
  apiKernelSnapshot,
} from './lib/transport/api-kernel-client.mjs'
import { PanelUserStore } from './lib/admin/panel-users.mjs'
import { RequestLogStore } from './lib/admin/request-log.mjs'
import {
  listVms,
  getVm,
  getActiveVmId,
  setVmSchedulable,
  isCodexVm,
  syncCodexQuotaSchedule,
} from './lib/vm/vm-registry.mjs'
import { syncCodexAccountScheduling } from './lib/vm/codex-slot.mjs'
import { makeError, ErrorType, ErrorCode } from './lib/core/errors.mjs'
import * as panel from './lib/admin/panel-api.mjs'
import { ProxyPool } from './lib/vm/proxy-pool.mjs'
import { ensureProxyEgress, proxyEgressReady } from './lib/vm/egress.mjs'
import { syncIpv6ProxyEgress } from './lib/vm/proxy-policy-runtime.mjs'
import { GATEWAY_CAPABILITIES } from './lib/vm/execution-context.mjs'
import { isTelemetryPath, telemetryInterceptResponse } from './lib/identity/telemetry-rewrite.mjs'
import { openDatabase, closeDatabase } from './lib/db/database.mjs'
import { runLegacyImport } from './lib/db/legacy-import.mjs'
import { initVmDbSync, stopVmWatch } from './lib/vm/vm-db-sync.mjs'
import { BackupService } from './lib/admin/backup-service.mjs'
import { ClusterNodesRepo } from './lib/db/repos/cluster-nodes-repo.mjs'
import { ClusterManager } from './lib/cluster/cluster-manager.mjs'
import { createClusterRoutes } from './lib/cluster/cluster-routes.mjs'
import { bindPlacement } from './lib/cluster/placement.mjs'
import { startNodeEgressSocks } from './lib/cluster/node-egress-socks.mjs'
import { createSlotShell } from './lib/vm/slot-shell.mjs'

import {
  classifyCredentialRefresh,
  markVmRefreshError,
  mirrorWorkerCredentialsToVm,
} from './lib/oauth/oauth-credentials.mjs'
import { normalizeCredentialMode } from './lib/oauth/credential-mode.mjs'
import { ensureWorkerCredential } from './lib/transport/go-worker-client.mjs'
import { stopAllRustKernels } from './lib/transport/rust-kernel-supervisor.mjs'
import { createRespond, parseRequestUrl } from './lib/http/respond.mjs'
import { handleHttpClientError } from './lib/http/client-error.mjs'
import { createGatewayRuntime, isInferenceRequest } from './lib/http/gateway-runtime.mjs'
import { isConsoleEntry, tryServeWebDist } from './lib/http/web-dist.mjs'
import { createRoutingRuntime } from './lib/admin/routing-runtime.mjs'
import { createImportCommit } from './lib/oauth/import-commit.mjs'
import { createHandleProtocol } from './lib/protocol/handle-protocol.mjs'
import { handleUserCountTokens, handleUserUsage } from './lib/protocol/user-count-tokens.mjs'
import { createPanelHandler } from './lib/admin/panel-routes.mjs'

const FEATURES = [
  'passthrough',
  'stream',
  'verified-stream',
  'protocol-convert',
  'go-slot-worker',
  'account-pool-failover',
  'tools',
  'client-workspace',
  'api-direct-kernel',
  'count_tokens',
  'account_usage',
]
const LIMITATIONS = {
  client_tools: 'kept in native Messages and executed by the caller',
  images: 'all native Messages image blocks are forwarded by the Go slot worker',
  multi_turn_native: 'full Messages history is preserved for every account attempt',
  claude_session: 'sticky commits only after a terminally verified response',
  kernel: 'one Docker container and one long-lived Go worker per slot; not a KVM guest',
  workspace: 'client only; VM/Claude-CLI inference has been removed',
  forward: 'Rust kernel uses slot egress (SOCKS5 or local) with no Go hop',
  oauth: 'the Go slot worker Refresher.Ensure is the sole refresh manager and uses the same slot SOCKS5',
  realtime_stream:
    'account failover stops after the first downstream business event; verified mode buffers to message_stop',
}

const cfg = loadConfig()
const gatewayRuntime = createGatewayRuntime()
const buildVersion = (() => {
  if (String(process.env.VM2API_BUILD || '').trim()) return String(process.env.VM2API_BUILD).trim()
  try {
    return fs.readFileSync(path.join(cfg.paths.project, 'VERSION'), 'utf8').trim() || 'unknown'
  } catch {
    return 'unknown'
  }
})()
if (!getPanelAdmin().password) {
  throw new Error('VM2API_ADMIN_PASSWORD must be set; insecure default panel credentials are disabled')
}
fs.mkdirSync(cfg.paths.captures, { recursive: true })

const allowRate = createRateLimiter({
  capacity: cfg.limits.rate_capacity,
  refillPerSec: cfg.limits.rate_refill,
})

// --- Persistent store (SQLite, sub2api-inspired) ---
const dataDir = cfg.paths.data || path.join(cfg.paths.root, 'data')
openDatabase({ dataDir })
try {
  loadModelPolicy()
} catch (e) {
  console.warn('[model-policy] boot load failed', e?.message || e)
}
// one-time migration of legacy JSON files (data/*.json + request-logs) into the DB
const legacyImport = runLegacyImport({ dataDir, projectRoot: cfg.paths.project })
if (legacyImport?.imported) {
  console.log('[db] legacy JSON import done:', JSON.stringify(legacyImport.counts || {}))
}
// VM/credential mirror: write-through hook + startup reconcile + fs.watch
const vmSync = initVmDbSync(cfg.paths.project)
if (vmSync.upserted || vmSync.rebuilt) {
  console.log(`[db] vm mirror reconciled: upserted=${vmSync.upserted} rebuilt=${vmSync.rebuilt}`)
}
if (vmSync.proxy_cli_repaired) {
  console.log(`[vm] repaired proxy_cli_enabled on ${vmSync.proxy_cli_repaired} slot(s)`)
}

// --- P3 sticky + quota ---

const routingConfigPath = routingConfigFile(cfg.paths.project)
let healthMonitor = null
let credentialRefreshMonitor = null
let kernelWatchdog = null
let cliNodeGuard = null
let usageProbeMonitor = null
let notifyMonitor = null

let routingConfig = {}

let stickyRouter
let accountQuota
let requestLog
let proxyPool
let runtimeRepo
let attemptsRepo
let poolScheduler
let failoverRunner
let groupsRepo

const routingRt = createRoutingRuntime({
  get routingConfig() {
    return routingConfig
  },
  set routingConfig(v) {
    routingConfig = v
  },
  getRoutingConfig: () => routingConfig,
  setRoutingConfig: (v) => {
    routingConfig = v
  },
  cfg,
  routingConfigPath,
  get requestLog() {
    return requestLog
  },
  get stickyRouter() {
    return stickyRouter
  },
  get accountQuota() {
    return accountQuota
  },
  get poolScheduler() {
    return poolScheduler
  },
  setPoolScheduler: (v) => {
    poolScheduler = v
  },
  get healthMonitor() {
    return healthMonitor
  },
  setHealthMonitor: (v) => {
    healthMonitor = v
  },
  get credentialRefreshMonitor() {
    return credentialRefreshMonitor
  },
  get kernelWatchdog() {
    return kernelWatchdog
  },
  get usageProbeMonitor() {
    return usageProbeMonitor
  },

  get notifyMonitor() {
    return notifyMonitor
  },
  get failoverRunner() {
    return failoverRunner
  },
  setFailoverRunner: (v) => {
    failoverRunner = v
  },
  get runtimeRepo() {
    return runtimeRepo
  },
  setRuntimeRepo: (v) => {
    runtimeRepo = v
  },
  get attemptsRepo() {
    return attemptsRepo
  },
  setAttemptsRepo: (v) => {
    attemptsRepo = v
  },
  get proxyPool() {
    return proxyPool
  },
  // Lazy: the monitor is built after the runtime. An unknown 429 asks for one
  // /usage hop through the same single-flight, bounded queue as the timer.
  probeUsageOne: (vmId) => usageProbeMonitor?.probeNow?.(vmId, 'rate_limited_unknown'),
})

const {
  persistRoutingPatch,
  loadRoutingConfig,
  syncTierDefaultsIntoRouting,
  initPoolRuntime,
  poolSchedulerConfig,
  applyRoutingTierConcurrency,
  applyVmConcurrency,
  applyVmRpm,
  applyVmSessionSlots,
  applyVmMaxSessions,
  inheritVmScheduling,
  applyVmQuotaOverride,
  migrateLegacyOpenAIPolicies,
} = routingRt

routingConfig = loadRoutingConfig()
migrateLegacyOpenAIPolicies(routingConfig)
let codexSchedulingUpdated = 0
for (const listed of listVms(cfg.paths.project)) {
  const vm = getVm(cfg.paths.project, listed.id) || listed
  if (!isCodexVm(vm)) continue
  if (syncCodexAccountScheduling(cfg.paths.project, vm).updated) codexSchedulingUpdated += 1
}
if (codexSchedulingUpdated) {
  console.log(`[codex] synchronized kernel scheduling for ${codexSchedulingUpdated} slot(s)`)
}
setManualScheduleWins(routingConfig.pool?.manual_schedule_wins)
if (routingConfig.official_cc) {
  routingConfig.official_cc = normalizeOfficialCcConfig(routingConfig.official_cc)
}
routingConfig.health_probe = normalizeHealthProbeConfig(routingConfig.health_probe)
routingConfig.usage_probe = normalizeUsageProbeConfig(routingConfig.usage_probe)
routingConfig.notify = normalizeNotifyConfig(routingConfig.notify)
routingConfig.tiers = normalizeTiers(routingConfig.tiers, routingConfig.quota, routingConfig.concurrency)
syncTierDefaultsIntoRouting()
stickyRouter = new StickyRouter({ dataDir, config: routingConfig })
accountQuota = new AccountQuota({
  dataDir,
  config: routingConfig,
  accounts: listVms(cfg.paths.project).map((v) => ({
    account_id: v.account_uuid || v.id,
    vm_id: v.id,
    email: v.email,
    type: normalizeCredentialMode(v.credential_mode),
    max_concurrency: v.policy?.maxConcurrency ?? routingConfig?.concurrency?.default_max_per_account ?? 2,
    max_rpm: v.policy?.maxRpm ?? routingConfig?.concurrency?.default_max_rpm ?? 0,
  })),
})
accountQuota.loadVmQuotaOverrides(listVms(cfg.paths.project))

const apiKeyStore = new ApiKeyStore({ dataDir: cfg.paths.data })
groupsRepo = new GroupsRepo()
const apiEndpointStore = new ApiEndpointStore({ dataDir: cfg.paths.data })
const apiScheduler = new ApiScheduler()
apiScheduler.reload(apiEndpointStore.listRaw())
ensureApiKernelToken(apiKernelPaths(cfg).tokenPath)
startApiKernelProcess(cfg)
const panelUsers = new PanelUserStore({ dataDir: cfg.paths.data })
try {
  panelUsers.bootstrapFromEnv(getPanelAdmin())
} catch (e) {
  console.warn('[panel-users] bootstrap failed', e?.message || e)
}
attachPanelAuth({
  authenticate: (username, password) => panelUsers.authenticate(username, password),
  lookupRole: (username) => panelUsers.getByUsername(username)?.role || null,
})
requestLog = new RequestLogStore({
  dataDir: cfg.paths.data,
  mode: process.env.KIN_REQUEST_LOG_MODE || routingConfig?.logging?.mode || 'normal',
  bufferedWrites: process.env.KIN_DB_WRITE_BUFFER !== '0',
})
if (routingConfig?.logging) {
  requestLog.setConfig({
    // Env wins at boot so e2e / ops can force off|debug. Panel PUT still hot-updates.
    mode: process.env.KIN_REQUEST_LOG_MODE || routingConfig.logging.mode,
    retainDays: routingConfig.logging.retain_days,
    debugRetainDays: routingConfig.logging.debug_retain_days,
    maxMb: routingConfig.logging.max_mb,
    mutedErrorClasses: routingConfig.logging.muted_error_classes,
  })
}
try {
  requestLog.cleanup()
} catch {}
requestLog.startCleanupScheduler()

proxyPool = new ProxyPool({
  dataDir,
  onDisableVm: (vmId, reason, proxyId) => {
    setVmSchedulable(cfg.paths.project, vmId, false, `${reason}|proxy=${proxyId}`)
  },
  onDisconnectVm: (vmId, reason, proxyId) => {
    setVmSchedulable(cfg.paths.project, vmId, false, `${reason}|proxy=${proxyId}`)
  },
  onEnableVm: (vmId, _reason, proxyId) => {
    const vm = getVm(cfg.paths.project, vmId)
    const why = String(vm?.schedule_disabled_reason || '')
    if (!why.includes(`proxy=${proxyId}`) && !/egress_down|proxy_probe_failed/.test(why)) return
    setVmSchedulable(cfg.paths.project, vmId, true)
  },
  egressCheck: (proxy) => proxyEgressReady(proxy, cfg.paths.project),
  repairEgress: (proxy) => ensureProxyEgress(cfg.paths.project, proxy),
})
proxyPool.startScheduler()

// 首次安装的出口池是空的，槽没有可绑出口就起不来。本机出口永远成立，先补上。
try {
  if (!proxyPool.snapshot().proxies?.length) {
    const seeded = proxyPool.ensureLocal()
    if (seeded.created) {
      ensureProxyEgress(cfg.paths.project, proxyPool.getProxyByIdWithAuth(seeded.proxy.id))
      console.log('[bootstrap] seeded local egress px-local')
    }
  }
} catch (e) {
  console.warn('[bootstrap] local egress seed failed', e?.message || e)
}

initPoolRuntime()
try {
  applyRoutingTierConcurrency(routingConfig.tiers)
} catch (e) {
  console.warn('[quota] apply tier concurrency failed', e?.message || e)
}

function accountForUsageProbe(vm) {
  const keys = [vm?.claude?.account_uuid, vm?.account_uuid, vm?.id].filter(Boolean)
  for (const key of keys) {
    try {
      const acc = accountQuota.repo.get(key)
      if (acc) return acc
    } catch {}
  }
  try {
    return accountQuota.repo.list().find((a) => a.vm_id === vm.id) || null
  } catch {
    return null
  }
}

const USAGE_ACTIVE_WINDOW_MS = 5 * 3600_000

/** Serving traffic within one 5h window, or still holding pinned conversations (#163). */
function usageProbeActive(vm, account) {
  if (isCodexVm(vm)) return false
  const id = account?.account_id || vm?.claude?.account_uuid || vm?.account_uuid || vm?.id
  const lastUsed = Number(poolScheduler?.lastUsed?.get?.(id) || runtimeRepo?.get?.(id)?.last_used_at || 0)
  if (lastUsed && Date.now() - lastUsed < USAGE_ACTIVE_WINDOW_MS) return true
  return (stickyRouter?.boundKeys?.({ accountId: id, vmId: vm?.id }) || []).length > 0
}

function attachSlotRuntime(vm) {
  const acc = accountForUsageProbe(vm)
  const keys = [acc?.account_id, vm?.claude?.account_uuid, vm?.account_uuid, vm?.id].filter(Boolean)
  for (const key of keys) {
    try {
      const rt = accountQuota.runtimeRepo?.get?.(key)
      if (rt) {
        return {
          ...vm,
          cooldown_until: rt.cooldown_until || 0,
          cooldown_reason: rt.cooldown_reason || null,
        }
      }
    } catch {}
  }
  return vm
}

healthMonitor = createHealthProbeMonitor({
  config: routingConfig.health_probe,
  listTargets: () => listVms(cfg.paths.project).map(attachSlotRuntime),
  runChat: (vm, real) =>
    runVmTestChat({
      projectRoot: cfg.paths.project,
      vmId: vm.id,
      model: real.model,
      prompt: real.prompt,
      max_tokens: real.max_tokens,
      timeoutMs: real.timeout_ms,
      personaMode: 'overwrite',
      baseUrl: `http://127.0.0.1:${cfg.port}`,
      apiKey: cfg.api_key,
      unofficial: false,
      extraHeaders: { [HEALTH_REAL_HEADER]: '1' },
    }),
})
credentialRefreshMonitor = createCredentialRefreshMonitor({
  config: normalizeCredentialRefreshConfig(routingConfig.credential_refresh),
  listTargets: () =>
    listVms(cfg.paths.project).map((vm) => {
      let lastProbe = vm.last_probe || null
      try {
        const acc = accountQuota?.repo?.get?.(vm.account_uuid || vm.id)
        lastProbe = acc?.unified?.last_probe || acc?.last_probe || lastProbe
      } catch {}
      return { ...vm, last_probe: lastProbe }
    }),
  refreshOne: (vm) =>
    refreshWorkerCredentialForVm({
      vmId: vm.id,
      vmPath: path.join(cfg.paths.project, 'vms', `${vm.id}.json`),
      homeDir: path.join(cfg.paths.project, 'vms', vm.id, 'cli-home'),
      vm: getVm(cfg.paths.project, vm.id) || vm,
      force: false,
    }),
})
kernelWatchdog = createKernelWatchdog({
  config: routingConfig.kernel_watchdog,
  listTargets: () => listVms(cfg.paths.project),
  getRouting: () => routingConfig,
  homeDirFor: (vm) => path.join(cfg.paths.project, 'vms', vm.id, 'cli-home'),
  ensureTelemetry: (vm) => ensureTelemetrySidecar({ projectRoot: cfg.paths.project, vm }),
  onFault: (vm, reason) => {
    const title = `槽内核故障 ${vm.id}`
    dispatchNotify(routingConfig.notify, {
      title,
      text: `【KIN】${title}\n\n${reason}\n已停止调度该槽；内核恢复健康后自动恢复调度。`,
      subject: `KIN · ${title}`,
    }).catch((error) => console.warn('[kernel-watchdog] notify failed', error?.message || error))
  },
})

usageProbeMonitor = createUsageProbeMonitor({
  config: routingConfig.usage_probe,
  listTargets: () => listVms(cfg.paths.project),
  accountForVm: accountForUsageProbe,
  isActive: usageProbeActive,
  reconcile: (vm, account) => {
    const id = account?.account_id || vm?.claude?.account_uuid || vm?.account_uuid || vm?.id
    if (isCodexVm(vm)) {
      try {
        syncCodexQuotaSchedule(cfg.paths.project, getVm(cfg.paths.project, vm.id) || vm, {
          policy: routingConfig.codex?.quota,
        })
      } catch {}
      return
    }
    try {
      accountQuota.persistEffectiveWindows(id)
    } catch {}
    try {
      poolScheduler.syncQuotaSchedule(vm, account)
    } catch {}
  },
  probeOne: (vm) => panel.buildProbeOne({ cfg, accountQuota, id: vm.id, routingConfig }),
})
notifyMonitor = createNotifyMonitor({
  config: routingConfig.notify,
  baseUrl: cfg.base_url,
  snapshot: () =>
    panel.snapshotAccountPool({
      cfg,
      accountQuota,
      routingConfig,
      poolScheduler,
      proxyPool,
      requestLog,
    }),
})

// --- Local backup service (auto schedule default ON; no S3 by design) ---
const backupService = new BackupService({
  dataDir,
  projectRoot: cfg.paths.project,
  configDir: path.dirname(routingConfigPath),
})
backupService.onBeforeRestore(() => {
  requestLog.flushPending()
  attemptsRepo?.flushPending?.()
})
backupService.onRestored((db) => {
  // A restore swaps the DatabaseSync instance and routing files. Rebuild every
  // holder before reporting success; otherwise requests keep stale snapshots
  // or silently write through statements owned by the closed connection.
  for (const store of [apiKeyStore, apiEndpointStore, panelUsers, accountQuota, stickyRouter, proxyPool, requestLog]) {
    store.rebind(db)
  }
  groupsRepo = new GroupsRepo(db)
  apiScheduler.reload(apiEndpointStore.listRaw())

  routingConfig = loadRoutingConfig()
  migrateLegacyOpenAIPolicies(routingConfig)
  setManualScheduleWins(routingConfig.pool?.manual_schedule_wins)
  if (routingConfig.official_cc) {
    routingConfig.official_cc = normalizeOfficialCcConfig(routingConfig.official_cc)
  }
  routingConfig.health_probe = normalizeHealthProbeConfig(routingConfig.health_probe)
  routingConfig.usage_probe = normalizeUsageProbeConfig(routingConfig.usage_probe)
  routingConfig.notify = normalizeNotifyConfig(routingConfig.notify)
  routingConfig.tiers = normalizeTiers(routingConfig.tiers, routingConfig.quota, routingConfig.concurrency)
  syncTierDefaultsIntoRouting()

  stickyRouter.reloadConfig(routingConfig)
  accountQuota.reloadConfig(routingConfig)
  accountQuota.loadVmQuotaOverrides(listVms(cfg.paths.project))
  requestLog.setConfig({
    mode: process.env.KIN_REQUEST_LOG_MODE || routingConfig.logging?.mode,
    retainDays: routingConfig.logging?.retain_days,
    debugRetainDays: routingConfig.logging?.debug_retain_days,
    maxMb: routingConfig.logging?.max_mb,
    mutedErrorClasses: routingConfig.logging?.muted_error_classes,
  })
  healthMonitor?.setConfig(routingConfig.health_probe)
  credentialRefreshMonitor?.setConfig(normalizeCredentialRefreshConfig(routingConfig.credential_refresh))
  kernelWatchdog?.setConfig(normalizeKernelWatchdogConfig(routingConfig.kernel_watchdog))
  usageProbeMonitor?.setConfig(routingConfig.usage_probe)
  notifyMonitor?.setConfig(routingConfig.notify)

  initPoolRuntime()
  reloadActiveVm(cfg)
})
backupService.startScheduler()

const stats = {
  requests: 0,
  passthrough: 0,
  convert: 0,
  rewrite: 0,
  stream: 0,
  errors: 0,
  by_route: {},
}

function credentialRefreshClass(result) {
  return classifyCredentialRefresh(result)
}

function publicCredentialRefreshResult(vmId, result, { force = false } = {}) {
  const refreshClass = credentialRefreshClass(result)
  const credential = result?.credential || {}
  const out = {
    ok: !!result?.ok,
    vm_id: vmId,
    refresh_owner: 'go-slot-worker',
    proxy_required: true,
    force: !!force,
    refreshed: !!result?.refreshed,
    shared: !!result?.shared,
    refresh_class: refreshClass,
    credential: {
      has_access: !!credential.has_access,
      has_refresh: !!credential.has_refresh,
      needs_refresh: !!credential.needs_refresh,
      expires_at: credential.expires_at ?? null,
      ttl_seconds: credential.ttl_seconds ?? null,
      generation: credential.generation ?? null,
    },
  }
  if (!result?.ok) {
    out.error = {
      code: result?.error?.code || 'credential_refresh_failed',
      message:
        refreshClass === 'fatal'
          ? 'OAuth credential was rejected'
          : refreshClass === 'retryable'
            ? 'OAuth credential refresh is temporarily unavailable'
            : 'OAuth credential refresh failed',
    }
  }
  return out
}

async function refreshWorkerCredentialForVm({ vmId, vmPath, homeDir, vm, force = false }) {
  // Host RefreshIfNeeded is the credential writer. Slot workers only read AT.
  const result = await ensureWorkerCredential({ vmId, vm, homeDir }, { force: !!force })

  const published = publicCredentialRefreshResult(vmId, result, { force })
  if (result.ok) {
    try {
      mirrorWorkerCredentialsToVm(vmPath, homeDir, { acceptLiveGrant: true })
    } catch {}
    try {
      invalidateLiveCredentialCache()
    } catch {}
    try {
      const accountId = vm?.claude?.account_uuid || vmId
      accountQuota.clearGrantRevokeLeftover(accountId)
      poolScheduler?.clearAuthCooldownFor?.(vmId, accountId)
      poolScheduler?.clearGrantRevokeCooldownFor?.(vmId, accountId)
    } catch {}
  } else if (published.refresh_class !== 'retryable') {
    try {
      markVmRefreshError(vmPath, result)
    } catch {}
    try {
      invalidateLiveCredentialCache()
    } catch {}
  }
  return published
}

const { json, writeSSEHeaders, readBody, readRawBody } = createRespond(cfg, {
  tcpNodelay: () => routingConfig?.inference?.tcp_nodelay !== false,
})

function rejectAuth(req, res, error) {
  req.authError = {
    code: error?.body?.error?.code || ErrorCode.INVALID_API_KEY,
    message: error?.body?.error?.message || 'Invalid credentials',
    status: error?.status || 401,
  }
  json(res, error.status, error.body)
  return false
}

function requireAuth(req, res) {
  req.presentedApiKey = extractApiKey(req)
  const token = extractPanelToken(req) || req.presentedApiKey
  if (!token) {
    return rejectAuth(
      req,
      res,
      makeError({
        type: ErrorType.AUTH,
        code: ErrorCode.MISSING_API_KEY,
        message: 'Missing credentials. Login at /api/panel/login or provide Authorization Bearer token.',
        status: 401,
      }),
    )
  }
  // Panel session
  const session = verifyPanelSession(token)
  if (session) {
    if (!allowRate('panel:' + session.user)) {
      return rejectAuth(
        req,
        res,
        makeError({
          type: ErrorType.RATE_LIMIT,
          code: ErrorCode.GATEWAY_RATE_LIMIT,
          message: 'Gateway rate limit exceeded. Retry later.',
          status: 429,
        }),
      )
    }
    req.panelUser = session.user
    req.panelRole = session.role || panelUsers.getByUsername(session.user)?.role || 'user'
    const rec = panelUsers.getByUsername(session.user)
    req.panelUserId = rec?.id || null
    req.panelVmCreateQuota = rec?.vm_create_quota ?? 0
    return true
  }
  // Master env API key — unlimited admin
  if (timingSafeEqualStr(token, cfg.api_key)) {
    if (!allowRate(token)) {
      return rejectAuth(
        req,
        res,
        makeError({
          type: ErrorType.RATE_LIMIT,
          code: ErrorCode.GATEWAY_RATE_LIMIT,
          message: 'Gateway rate limit exceeded. Retry later.',
          status: 429,
        }),
      )
    }
    req.apiKeyKind = 'master'
    req.panelRole = 'admin'
    req.panelUser = 'master'
    return true
  }

  // Managed multi-keys (sub2api-style)
  const managed = apiKeyStore.authenticate(token)
  if (!managed.ok) {
    return rejectAuth(
      req,
      res,
      makeError({
        type: ErrorType.AUTH,
        code: ErrorCode.INVALID_API_KEY,
        message: 'Invalid credentials',
        status: 401,
      }),
    )
  }
  const gate = apiKeyStore.canAccept(managed.record)
  if (!gate.ok) {
    const type =
      gate.status === 429
        ? gate.code.includes('quota')
          ? ErrorType.QUOTA
          : ErrorType.RATE_LIMIT
        : ErrorType.PERMISSION
    return rejectAuth(
      req,
      res,
      makeError({
        type,
        code: gate.code,
        message: gate.message,
        status: gate.status,
        details: gate.detail || undefined,
      }),
    )
  }
  if (!allowRate(token)) {
    return rejectAuth(
      req,
      res,
      makeError({
        type: ErrorType.RATE_LIMIT,
        code: ErrorCode.GATEWAY_RATE_LIMIT,
        message: 'Gateway rate limit exceeded. Retry later.',
        status: 429,
      }),
    )
  }
  req.apiKeyKind = 'managed'
  req.apiKeyRecord = managed.record
  return true
}

function fetchWorkerModels(opts) {
  return gatewayModelCatalog(opts)
}

const importCommit = createImportCommit({
  cfg,
  routingConfigPath,
  getRoutingConfig: () => routingConfig,
  get routingConfig() {
    return routingConfig
  },
  get accountQuota() {
    return accountQuota
  },
  get poolScheduler() {
    return poolScheduler
  },
  get proxyPool() {
    return proxyPool
  },
  applyVmConcurrency,
})

const { commitImportedOauth, requireSlotProxy, officialCcStatsHandler } = importCommit

const { handleProtocol, handleSearch } = createHandleProtocol({
  json,
  writeSSEHeaders,
  readBody,
  requireAuth,
  cfg,
  requestLog,
  stickyRouter,
  accountQuota,
  apiKeyStore,
  apiScheduler,
  apiEndpointStore,
  stats,
  routingConfigPath,
  getRoutingConfig: () => routingConfig,
  get routingConfig() {
    return routingConfig
  },
  getHealthMonitor: () => healthMonitor,
  get healthMonitor() {
    return healthMonitor
  },
  getFailoverRunner: () => failoverRunner,
  get failoverRunner() {
    return failoverRunner
  },
  getPoolScheduler: () => poolScheduler,
  get poolScheduler() {
    return poolScheduler
  },
  getAttemptsRepo: () => attemptsRepo,
  get attemptsRepo() {
    return attemptsRepo
  },
  get groupsRepo() {
    return groupsRepo
  },
})

const clusterManager = new ClusterManager({
  repo: new ClusterNodesRepo(),
  dataDir,
  listen: { host: cfg.host, port: cfg.port },
  vmsOnNode: (nodeId) => listVms(cfg.paths.project).filter((vm) => vm.node_id === nodeId),
})
bindPlacement({ manager: clusterManager, projectRoot: cfg.paths.project })
await startNodeEgressSocks({ clientFor: (nodeId) => clusterManager.client(nodeId) })
clusterManager.start()
clusterManager.restoreSlotRelays()
if (proxyPool.snapshot().config.ipv6_enabled !== true) {
  const exits = await syncIpv6ProxyEgress(cfg.paths.project, proxyPool)
  for (const exit of exits.filter((item) => !item.ok)) {
    console.warn('[ipv6-policy] exit not blocked', exit.vm_id || exit.proxy_id, exit.error)
  }
}
const clusterRoutes = createClusterRoutes({ manager: clusterManager, json, readBody, ok: panel.ok })
const slotShell = createSlotShell({ projectRoot: cfg.paths.project })
for (const vm of listVms(cfg.paths.project)) {
  if (isCodexVm(vm)) continue
  try {
    ensureSlotSubscriptionType(path.join(cfg.paths.project, 'vms', vm.id, 'cli-home'), vm.account_tier)
  } catch (error) {
    console.warn('[credential-subscription]', vm.id, error?.message || error)
  }
}
cliNodeGuard = createCliNodeGuard({
  listTargets: () => listVms(cfg.paths.project),
  liveTokens: () => slotShell.liveShellTokens?.() || [],
})

const handlePanel = createPanelHandler({
  json,
  writeSSEHeaders,
  readBody,
  readRawBody,
  requireAuth,
  cfg,
  routingConfigPath,
  get routingConfig() {
    return routingConfig
  },
  set routingConfig(v) {
    routingConfig = v
  },
  stickyRouter,
  accountQuota,
  get poolScheduler() {
    return poolScheduler
  },
  get healthMonitor() {
    return healthMonitor
  },
  get usageProbeMonitor() {
    return usageProbeMonitor
  },
  get notifyMonitor() {
    return notifyMonitor
  },
  get runtimeRepo() {
    return runtimeRepo
  },
  get groupsRepo() {
    return groupsRepo
  },
  getAttemptsRepo: () => attemptsRepo,
  get attemptsRepo() {
    return attemptsRepo
  },
  get proxyPool() {
    return proxyPool
  },
  apiKeyStore,
  apiEndpointStore,
  apiScheduler,
  panelUsers,
  requestLog,
  backupService,
  stats,
  persistRoutingPatch,
  applyVmConcurrency,
  applyVmRpm,
  applyVmSessionSlots,
  applyVmMaxSessions,
  inheritVmScheduling,
  applyVmQuotaOverride,
  initPoolRuntime,
  poolSchedulerConfig,
  commitImportedOauth,
  requireSlotProxy,
  officialCcStatsHandler,
  refreshWorkerCredentialForVm,
  fetchWorkerModels,
  clusterRoutes,
  clusterManager,
  slotShell,
})

const server = http.createServer(async (req, res) => {
  const requestScope = gatewayRuntime.beginRequest()
  try {
    if (req.method === 'OPTIONS') {
      res.writeHead(204, {
        'access-control-allow-origin': '*',
        'access-control-allow-headers':
          'authorization, content-type, x-api-key, anthropic-version, anthropic-beta, x-session-id, x-kin-rewrite, x-panel-token, x-kin-vm, x-kin-backend',
        'access-control-allow-methods': 'GET,POST,OPTIONS,PUT,DELETE',
      })
      return res.end()
    }

    const url = parseRequestUrl(req)
    if (!url) {
      return json(res, 400, { error: { message: 'Invalid URL', type: 'invalid_request' } })
    }
    const p = url.pathname
    if (isInferenceRequest(req.method, p)) requestScope.markInference()

    if (isTelemetryPath(p)) {
      return json(res, 200, telemetryInterceptResponse(p))
    }

    if (
      backupService.isRestoring &&
      (p.startsWith('/v1/') || p === '/messages' || p === '/chat/completions' || p === '/responses')
    ) {
      return json(
        res,
        503,
        makeError({
          type: ErrorType.OVERLOADED,
          code: 'restore_in_progress',
          message: 'Gateway is restoring from backup; retry shortly',
          status: 503,
        }).body,
      )
    }

    if (p.startsWith('/admin') || p.startsWith('/api/panel')) {
      const handled = await handlePanel(req, res, url)
      if (handled !== false) return
    }

    if (req.method === 'GET' && tryServeWebDist(res, cfg.paths.project, p)) return
    if (req.method === 'GET' && isConsoleEntry(p)) {
      return json(res, 404, { error: { message: 'console not found; run pnpm -C web build' } })
    }

    // CLIProxyAPI-style liveness: no auth, no inference.
    if ((req.method === 'GET' || req.method === 'HEAD') && p === '/healthz') {
      if (req.method === 'HEAD') {
        res.writeHead(200)
        return res.end()
      }
      return json(res, 200, { status: 'ok' })
    }

    if (req.method === 'GET' && (p === '/' || p === '/health')) {
      return json(res, 200, {
        status: 'ok',
        service: 'vm2api',
        version: buildVersion,
        base_url: cfg.base_url,
        rewrite: cfg.rewrite.enabled ? 'on' : 'off',
        intercept_rules: cfg.intercept.rules.length,
        active_vm: getActiveVmId(cfg.paths.project),
        features: FEATURES,
        capabilities: GATEWAY_CAPABILITIES,
        limitations: LIMITATIONS,
        stats,
        runtime: gatewayRuntime.snapshot(),
        health_probe: healthMonitor?.getSnapshot?.() || null,
        api_kernel: {
          ...apiKernelSnapshot(cfg),
          endpoints: apiEndpointStore.listRaw().filter((ep) => !ep.disabled).length,
        },
      })
    }

    if (req.method === 'GET' && p === '/v1/meta') {
      return json(res, 200, {
        base_url: cfg.base_url,
        rewrite_default: cfg.rewrite.enabled,
        features: FEATURES,
        capabilities: GATEWAY_CAPABILITIES,
        limitations: LIMITATIONS,
        endpoints: {
          chat_completions: '/v1/chat/completions',
          responses: '/v1/responses',
          messages: '/v1/messages',
          count_tokens: '/v1/messages/count_tokens',
          usage: '/v1/usage',
          models: '/v1/models',
          intercept_rules: '/admin/intercept/rules',
        },
        vm: { id: cfg.vm.id, email: cfg.vm.email },
      })
    }

    if (req.method === 'GET' && p === '/v1/models') {
      if (!requireAuth(req, res)) return
      if (resolveInferenceBackend(req) === 'api') {
        return json(res, 200, apiScheduler.catalog())
      }
      const result = await fetchWorkerModels({ groupType: keyScopeFromRequest(req).group_type })
      return json(res, 200, result)
    }
    if (req.method === 'POST' && p === '/admin/models/refresh') {
      if (!requireAuth(req, res)) return
      const result = await fetchWorkerModels()
      return json(res, 200, result)
    }
    if (req.method === 'GET' && p === '/v1/usage') {
      return await handleUserUsage(req, res, {
        json,
        requireAuth,
        stickyRouter,
        accountQuota,
        getPoolScheduler: () => poolScheduler,
      })
    }
    if (req.method === 'POST' && (p === '/v1/messages/count_tokens' || p === '/messages/count_tokens')) {
      return await handleUserCountTokens(req, res, {
        json,
        readBody,
        requireAuth,
        cfg,
        stickyRouter,
        accountQuota,
        getPoolScheduler: () => poolScheduler,
        getRoutingConfig: () => routingConfig,
      })
    }

    if (req.method === 'POST' && (p === '/v1/chat/completions' || p === '/chat/completions')) {
      return await handleProtocol(req, res, 'openai.chat', p)
    }
    if (req.method === 'POST' && (p === '/v1/completions' || p === '/completions')) {
      return await handleProtocol(req, res, 'openai.completions', p)
    }
    if (req.method === 'POST' && (p === '/v1/responses' || p === '/responses')) {
      return await handleProtocol(req, res, 'openai.responses', p)
    }
    if (req.method === 'POST' && (p === '/v1/messages' || p === '/messages')) {
      return await handleProtocol(req, res, 'anthropic.messages', p)
    }
    if (req.method === 'POST' && (p === '/v1/alpha/search' || p === '/alpha/search')) {
      return await handleSearch(req, res, p)
    }

    json(res, 404, { error: { message: `not found: ${p}` } })
  } catch (e) {
    stats.errors++
    const status = e.status || 500
    if (!res.headersSent) {
      json(res, status, { error: { message: e.message || String(e), type: 'server_error' } })
    } else {
      try {
        res.end()
      } catch {}
    }
  } finally {
    requestScope.release()
  }
})

server.on('upgrade', (req, socket, head) => {
  if (clusterRoutes.handleUpgrade(req, socket, head)) return
  if (!slotShell.handleUpgrade(req, socket, head)) socket.destroy()
})

server.on('clientError', handleHttpClientError)

const pub = {
  base_url: cfg.base_url,
  api_key_set: !!cfg.api_key,
  rewrite_default: false,
  vm_id: cfg.vm.id,
  version: '2.1',
  features: FEATURES,
  capabilities: GATEWAY_CAPABILITIES,
  limitations: LIMITATIONS,
  endpoints: {
    health: `${cfg.base_url}/health`,
    chat: `${cfg.base_url}/v1/chat/completions`,
    responses: `${cfg.base_url}/v1/responses`,
    messages: `${cfg.base_url}/v1/messages`,
    count_tokens: `${cfg.base_url}/v1/messages/count_tokens`,
    usage: `${cfg.base_url}/v1/usage`,
    intercept_rules: `${cfg.base_url}/admin/intercept/rules`,
  },
}
// Never persist the raw API key. Public snapshot only.
fs.writeFileSync(path.join(cfg.paths.root, 'config', 'gateway-v2.public.json'), JSON.stringify(pub, null, 2))

process.on('uncaughtException', (e) => console.error('[uncaught]', e))
process.on('unhandledRejection', (e) => console.error('[unhandled]', e))

// Graceful shutdown: stop accepting work, drain active HTTP streams, then stop
// child kernels and close the DB. Docker must allow at least this grace period.
let _shuttingDown = false
function shutdown(signal) {
  if (_shuttingDown) return
  _shuttingDown = true
  gatewayRuntime.startDraining()
  console.log(`[shutdown] ${signal} — closing`)
  try {
    backupService.stopScheduler()
  } catch {}
  try {
    proxyPool.stopScheduler()
  } catch {}
  try {
    healthMonitor?.stop?.()
  } catch {}
  try {
    credentialRefreshMonitor?.stop?.()
  } catch {}
  try {
    kernelWatchdog?.stop?.()
  } catch {}
  try {
    cliNodeGuard?.stop?.()
  } catch {}
  try {
    usageProbeMonitor?.stop?.()
  } catch {}

  try {
    notifyMonitor?.stop?.()
  } catch {}
  try {
    stopVmWatch()
  } catch {}
  let finalized = false
  const finalize = (reason) => {
    if (finalized) return
    finalized = true
    console.log(`[shutdown] ${signal} — ${reason}`)
    try {
      stopAllRustKernels()
    } catch {}
    try {
      clusterManager.stop()
    } catch {}
    try {
      requestLog.stop()
    } catch {}
    try {
      attemptsRepo?.stop?.()
    } catch {}
    try {
      closeOpenAIGlobalLeaseClient()
    } catch {}
    try {
      closeDatabase()
    } catch {}
    gatewayRuntime.stop()
    process.exit(0)
  }
  const graceMs = Math.min(300_000, Math.max(1000, Number(process.env.KIN_SHUTDOWN_GRACE_MS) || 120_000))
  const timer = setTimeout(() => finalize('drain timeout'), graceMs)
  timer.unref?.()
  let closeReason = 'drained'
  const serverClosed = new Promise((resolve) => {
    try {
      server.close(resolve)
      server.closeIdleConnections?.()
    } catch {
      closeReason = 'close failed'
      resolve()
    }
  })
  Promise.all([serverClosed, gatewayRuntime.whenIdle()])
    .then(() => {
      clearTimeout(timer)
      finalize(closeReason)
    })
    .catch(() => finalize('drain failed'))
}
process.on('SIGTERM', () => shutdown('SIGTERM'))
process.on('SIGINT', () => shutdown('SIGINT'))

const listenBacklog = Math.trunc(Math.min(65_535, Math.max(511, Number(process.env.KIN_LISTEN_BACKLOG) || 4096)))
server.listen({ port: cfg.port, host: cfg.host, backlog: listenBacklog }, () => {
  try {
    healthMonitor?.start?.({ immediate: true })
  } catch (e) {
    console.warn('[health-probe] start failed', e?.message || e)
  }
  try {
    credentialRefreshMonitor?.start?.({ immediate: true })
  } catch (e) {
    console.warn('[credential-refresh] start failed', e?.message || e)
  }
  try {
    kernelWatchdog?.start?.({ immediate: true })
  } catch (e) {
    console.warn('[kernel-watchdog] start failed', e?.message || e)
  }
  try {
    cliNodeGuard?.start?.({ immediate: true })
  } catch (e) {
    console.warn('[cli-node-guard] start failed', e?.message || e)
  }
  try {
    usageProbeMonitor?.start?.({ immediate: true })
  } catch (e) {
    console.warn('[usage-probe] start failed', e?.message || e)
  }
  try {
    notifyMonitor?.start?.({ immediate: true })
  } catch (e) {
    console.warn('[notify] start failed', e?.message || e)
  }
  const addr = server.address()
  const boundPort = typeof addr === 'object' && addr ? addr.port : cfg.port
  try {
    const catalog = fetchWorkerModels()
    console.log(
      JSON.stringify({
        event: 'go-worker-model-catalog',
        source: catalog.source,
        total: Array.isArray(catalog.data) ? catalog.data.length : 0,
        vm_id: catalog.vm_id || null,
      }),
    )
  } catch (error) {
    console.warn('[worker-models] fetch failed', error.message)
  }
  console.log(
    JSON.stringify({
      event: 'vm2api-started',
      port: boundPort,
      listen_backlog: listenBacklog,
      base_url: cfg.base_url,
      active_vm: getActiveVmId(cfg.paths.project),
      features: pub.features,
      capabilities: GATEWAY_CAPABILITIES,
      rewrite: cfg.rewrite.enabled,
    }),
  )
})
