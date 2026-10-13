import { getDb, isDbOpen } from '../db/database.mjs'
import { SettingsRepo } from '../db/repos/settings-repo.mjs'
import { socksProxyFamily } from './socks-address.mjs'

let settingsDb = null
let settingsRepo = null
let cachedConfig = null
let cachedAt = 0

const CONFIG_CACHE_MS = 500

export function invalidateProxyPolicyCache() {
  cachedConfig = null
  cachedAt = 0
}

function proxyPoolConfig() {
  const db = getDb()
  if (settingsDb !== db || !settingsRepo) {
    settingsDb = db
    settingsRepo = new SettingsRepo(db)
    invalidateProxyPolicyCache()
  }
  const now = Date.now()
  if (cachedConfig && now - cachedAt < CONFIG_CACHE_MS) return cachedConfig
  cachedConfig = settingsRepo.get('proxy_pool_config') || {}
  cachedAt = now
  return cachedConfig
}

export function configuredIpv6Enabled() {
  return isDbOpen() && proxyPoolConfig().ipv6_enabled === true
}

export function proxyBlockedReason(proxy, ipv6Enabled = configuredIpv6Enabled()) {
  return ipv6Enabled !== true && socksProxyFamily(proxy) === 6 ? 'ipv6_disabled' : null
}

export function assertProxyAllowed(proxy) {
  const reason = proxyBlockedReason(proxy)
  if (reason)
    throw Object.assign(new Error('IPv6 已关闭，请在设置 → SOCKS5 开启 IPv6 代理出口'), { code: reason, status: 409 })
}
