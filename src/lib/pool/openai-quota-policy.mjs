/**
 * OpenAI/Codex scheduling policy.
 *
 * This module is deliberately independent from Claude tier policy.  The
 * resolver is used by config, admission, persistence and panel projections so
 * a value cannot silently acquire a different meaning at another boundary.
 */
import { extraToCodexSnapshot, normalizeCodexLimits } from '../protocol/codex-usage.mjs'

export const OPENAI_QUOTA_DEFAULTS = Object.freeze({
  limit_5h: 1,
  limit_7d: 1,
  max_concurrency: 2,
  max_rpm: 0,
  max_sessions: 0,
})

export const OPENAI_QUOTA_LIMITS = Object.freeze({
  limit_5h: { min: 0.3, max: 1 },
  limit_7d: { min: 0.3, max: 1 },
  max_concurrency: { min: 1, max: 256 },
  max_rpm: { min: 0, max: 1_000_000 },
  max_sessions: { min: 0, max: 256 },
})

const FIELDS = Object.keys(OPENAI_QUOTA_DEFAULTS)
const NORMALIZED_POLICIES = new WeakMap()

function rememberNormalizedPolicy(policy) {
  NORMALIZED_POLICIES.set(
    policy,
    FIELDS.map((key) => policy[key]),
  )
  return policy
}

function isRememberedPolicy(policy) {
  const values = NORMALIZED_POLICIES.get(policy)
  return !!values && FIELDS.every((key, index) => policy[key] === values[index])
}

rememberNormalizedPolicy(OPENAI_QUOTA_DEFAULTS)

function finite(value) {
  if (value == null || value === '' || typeof value !== 'number') return null
  return Number.isFinite(value) ? value : null
}

function integer(value) {
  const n = finite(value)
  return n != null && Number.isInteger(n) ? n : null
}

function normalizedValue(key, value, { strict = false } = {}) {
  const limits = OPENAI_QUOTA_LIMITS[key]
  const n = key === 'limit_5h' || key === 'limit_7d' ? finite(value) : integer(value)
  if (n == null) {
    if (strict)
      throw new TypeError(`codex.quota.${key} must be ${key.startsWith('limit_') ? 'a number' : 'an integer'}`)
    return OPENAI_QUOTA_DEFAULTS[key]
  }
  if (n < limits.min || n > limits.max) {
    if (strict) {
      throw new RangeError(`codex.quota.${key} must be between ${limits.min} and ${limits.max}`)
    }
    return Math.min(limits.max, Math.max(limits.min, n))
  }
  return n
}

export function normalizeOpenAIQuotaPolicy(raw = {}, { strict = false } = {}) {
  const source = raw && typeof raw === 'object' && !Array.isArray(raw) ? raw : {}
  if (!strict && isRememberedPolicy(source)) return source
  const out = {}
  for (const key of FIELDS) {
    if (strict && Object.prototype.hasOwnProperty.call(source, key))
      out[key] = normalizedValue(key, source[key], { strict })
    else out[key] = normalizedValue(key, source[key] ?? OPENAI_QUOTA_DEFAULTS[key], { strict: false })
  }
  return rememberNormalizedPolicy(out)
}

export function validateOpenAIQuotaPatch(raw = {}) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    throw new TypeError('codex.quota must be an object')
  }
  const out = {}
  for (const key of Object.keys(raw)) {
    if (!FIELDS.includes(key)) throw new TypeError(`codex.quota.${key} is not supported`)
    out[key] = normalizedValue(key, raw[key], { strict: true })
  }
  return out
}

export function mergeOpenAIQuotaPolicy(current, patch) {
  return normalizeOpenAIQuotaPolicy({
    ...normalizeOpenAIQuotaPolicy(current),
    ...validateOpenAIQuotaPatch(patch),
  })
}

function concurrencyOf(vm, policy) {
  const raw = vm?.policy?.maxConcurrency ?? vm?.max_concurrency
  const own = vm?.policy?.concurrencyOverride ?? vm?.concurrency_override ?? vm?.max_concurrency_override
  if (own !== false && Number.isInteger(raw) && raw >= 0) return raw || 2
  return policy.max_concurrency
}

export function openAIConcurrencyOf(vm, policy = OPENAI_QUOTA_DEFAULTS) {
  return concurrencyOf(vm, normalizeOpenAIQuotaPolicy(policy))
}

function rpmOf(vm, policy) {
  const raw = vm?.policy?.maxRpm ?? vm?.max_rpm
  const own = vm?.policy?.rpmOverride ?? vm?.rpm_override ?? vm?.max_rpm_override
  if (own !== false && Number.isInteger(raw) && raw >= 0) return raw
  return policy.max_rpm
}

export function openAIRpmOf(vm, policy = OPENAI_QUOTA_DEFAULTS) {
  return rpmOf(vm, normalizeOpenAIQuotaPolicy(policy))
}

function sessionsOf(vm, policy) {
  const raw = vm?.policy?.maxSessions ?? vm?.max_sessions
  const own = vm?.policy?.sessionsOverride ?? vm?.max_sessions_override
  if (own !== false && Number.isInteger(raw) && raw >= 0) return raw
  return policy.max_sessions
}

export function openAISessionsOf(vm, policy = OPENAI_QUOTA_DEFAULTS) {
  return sessionsOf(vm, normalizeOpenAIQuotaPolicy(policy))
}

function usageFraction(value, { percent = false } = {}) {
  const n = finite(value)
  if (n == null) return null
  return percent ? n / 100 : n
}

function normalizedUsagePercent(value) {
  const ratio = usageFraction(value, { percent: true })
  return ratio == null ? null : Math.min(1, Math.max(0, ratio))
}

function resetEpoch(value) {
  if (value == null || value === '') return null
  const at = typeof value === 'number' ? value : Date.parse(value)
  return Number.isFinite(at) ? at : null
}

export function evaluateOpenAIQuotaGate(vm, policy = OPENAI_QUOTA_DEFAULTS, now = Date.now(), prepared = null) {
  const effective = normalizeOpenAIQuotaPolicy(policy)
  const extra = vm?.codex?.extra || vm?.codex_extra || {}
  const hasExtra = Object.keys(extra).length > 0
  let usage = vm?.codex_usage?.quota || vm?.codex?.usage?.quota || {}
  if (hasExtra) {
    const limits =
      prepared?.extra === extra && prepared?.limits
        ? prepared.limits
        : normalizeCodexLimits(extraToCodexSnapshot(extra))
    usage = {
      utilization_5h: normalizedUsagePercent(limits.used_5h_percent),
      utilization_7d: normalizedUsagePercent(limits.used_7d_percent),
      reset_5h: limits.reset_5h_at,
      reset_7d: limits.reset_7d_at,
    }
  }
  let first = null
  let until = null
  for (const key of ['5h', '7d']) {
    const percent = extra[`codex_${key}_used_percent`]
    const normalized = usage[`utilization_${key}`] ?? vm?.[`utilization_${key}`]
    const used = percent == null ? usageFraction(normalized) : usageFraction(percent, { percent: true })
    const reset = extra[`codex_${key}_reset_at`] ?? usage[`reset_${key}`] ?? vm?.[`reset_${key}`]
    const at = resetEpoch(reset)
    if (used == null || (at != null && at <= now) || used < effective[`limit_${key}`]) continue
    if (Number.isFinite(at) && at > now) until = until == null ? at : Math.min(until, at)
    first ||= { limited: true, reason: `quota_${key}_local`, window: key, used, limit: effective[`limit_${key}`] }
  }
  return first
    ? { ...first, until }
    : { limited: false, reason: null, window: null, used: null, limit: null, until: null }
}

export function effectiveOpenAIPolicy(vm, globalPolicy = OPENAI_QUOTA_DEFAULTS) {
  const policy = normalizeOpenAIQuotaPolicy(globalPolicy)
  return {
    max_concurrency: concurrencyOf(vm, policy),
    max_rpm: rpmOf(vm, policy),
    max_sessions: sessionsOf(vm, policy),
    concurrency_override:
      vm?.policy?.concurrencyOverride === true ||
      vm?.max_concurrency_override === true ||
      vm?.concurrency_override === true ||
      (vm?.policy?.concurrencyOverride == null &&
        Number.isInteger(vm?.policy?.maxConcurrency) &&
        vm.policy.maxConcurrency > 0 &&
        vm.policy.maxConcurrency !== policy.max_concurrency),
    rpm_override:
      vm?.policy?.rpmOverride === true ||
      vm?.max_rpm_override === true ||
      vm?.rpm_override === true ||
      (vm?.policy?.rpmOverride == null && Number.isInteger(vm?.policy?.maxRpm) && vm.policy.maxRpm !== policy.max_rpm),
    sessions_override:
      vm?.policy?.sessionsOverride === true ||
      vm?.max_sessions_override === true ||
      (vm?.policy?.sessionsOverride == null && Number.isInteger(vm?.policy?.maxSessions) && vm.policy.maxSessions > 0),
  }
}
