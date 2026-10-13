import type { ProviderChainItem, UsageLogRow } from '@/types/panel-usage-logs'
import { describe, expect, it } from 'vitest'
import {
  actualCacheRate,
  cacheCostSplit,
  cacheWriteSplit,
  chainItemStatus,
  finalChainItem,
  formatShortDistance,
  latencySegments,
  resolveModelAuditDisplay,
  segmentWidth,
  statusBadgeClass,
  statusBadgeTooltip,
  thinkingEffortBadgeClass,
  unitPricePerMillion,
} from './log-display'
import {
  activeFilterCount,
  filtersToApi,
  filtersToSearch,
  searchToFilters,
  usageLogFiltersQuery,
  validateLogsSearch,
} from './search'

function attempt(over: Partial<ProviderChainItem>): ProviderChainItem {
  return {
    attemptNumber: 1,
    vmId: 'vm-1',
    vmName: 'slot-a',
    accountId: null,
    providerName: null,
    model: null,
    selectionReason: null,
    upstreamStatus: null,
    errorScope: null,
    terminalState: null,
    action: null,
    downstreamCommitted: false,
    waitMs: null,
    ttftMs: null,
    latencyMs: null,
    startedAt: null,
    completedAt: null,
    ...over,
  }
}

describe('cache TTL split', () => {
  it('prefers explicit 5m/1h counts', () => {
    expect(
      cacheWriteSplit({ total: 300, fiveM: 100, oneH: 200, ttl: 'mixed' })
    ).toEqual({ fiveM: 100, oneH: 200 })
  })

  it('attributes an aggregate to the applied TTL', () => {
    expect(
      cacheWriteSplit({ total: 300, fiveM: 0, oneH: 0, ttl: '1h' })
    ).toEqual({ fiveM: 0, oneH: 300 })
    expect(
      cacheWriteSplit({ total: 300, fiveM: 0, oneH: 0, ttl: null })
    ).toEqual({ fiveM: 300, oneH: 0 })
  })

  it('splits cache-write cost proportionally for mixed TTL', () => {
    expect(cacheCostSplit(3, { fiveM: 100, oneH: 200 }, 'mixed')).toEqual({
      fiveM: 1,
      oneH: 2,
    })
    expect(cacheCostSplit(3, { fiveM: 0, oneH: 10 }, '1h')).toEqual({
      fiveM: 0,
      oneH: 3,
    })
    expect(cacheCostSplit(null, { fiveM: 1, oneH: 1 }, 'mixed')).toEqual({
      fiveM: 0,
      oneH: 0,
    })
  })

  it('derives the per-million unit price', () => {
    expect(unitPricePerMillion(0.003, 1000)).toBe('3.00')
    expect(unitPricePerMillion(0.003, 0)).toBeNull()
  })

  it('uses inclusive OpenAI input and disjoint Anthropic input', () => {
    const tokens = {
      inputTokens: 100,
      cacheReadInputTokens: 60,
      cacheCreationInputTokens: 10,
    }
    expect(
      actualCacheRate({ ...tokens, via: 'codex-kernel' } as UsageLogRow)
    ).toBe(0.6)
    expect(
      actualCacheRate({ ...tokens, via: 'go-worker' } as UsageLogRow)
    ).toBeCloseTo(60 / 170)
  })

  it('keeps converted non-streaming Anthropic usage disjoint', () => {
    expect(
      actualCacheRate({
        inputTokens: 40,
        cacheReadInputTokens: 60,
        cacheCreationInputTokens: 0,
        via: 'api-kernel',
        protocol: 'anthropic.messages',
        stream: false,
        model: 'gpt-5.4',
      } as UsageLogRow)
    ).toBe(0.6)
  })
})

describe('latency segments', () => {
  it('splits wait / ttft / generation and keeps a minimum width', () => {
    const segments = latencySegments({
      waitMs: 10,
      ttftMs: 1000,
      durationMs: 5000,
    })
    expect(segments?.map((s) => s.ms)).toEqual([10, 990, 4000])
    expect(segmentWidth(segments![0])).toBe(3)
  })

  it('drops an out-of-range wait and rejects impossible ttft', () => {
    expect(
      latencySegments({ waitMs: 2000, ttftMs: 1000, durationMs: 5000 })?.[0].ms
    ).toBe(0)
    expect(
      latencySegments({ waitMs: 0, ttftMs: 6000, durationMs: 5000 })
    ).toBeNull()
  })
})

describe('provider chain', () => {
  it('classifies attempts by commit / upstream status / terminal state', () => {
    expect(chainItemStatus(attempt({ downstreamCommitted: true }))).toBe(
      'success'
    )
    expect(chainItemStatus(attempt({ upstreamStatus: 529 }))).toBe('failure')
    expect(chainItemStatus(attempt({ terminalState: 'exhausted' }))).toBe(
      'failure'
    )
    expect(chainItemStatus(attempt({}))).toBe('pending')
  })

  it('picks the committed attempt as the final provider', () => {
    const chain = [
      attempt({ attemptNumber: 1, vmName: 'a', upstreamStatus: 529 }),
      attempt({ attemptNumber: 2, vmName: 'b', downstreamCommitted: true }),
      attempt({ attemptNumber: 3, vmName: 'c' }),
    ]
    expect(finalChainItem(chain)?.vmName).toBe('b')
    expect(finalChainItem([])).toBeNull()
  })
})

describe('display helpers', () => {
  it('maps status codes to hub badge colours', () => {
    expect(statusBadgeClass(200)).toContain('bg-green-100')
    expect(statusBadgeClass(429)).toContain('bg-yellow-100')
    expect(statusBadgeClass(503)).toContain('bg-red-100')
    expect(statusBadgeClass(null)).toContain('bg-gray-100')
  })

  it('does not treat committed incomplete 200 as a green success', () => {
    expect(statusBadgeClass(200, 'incomplete')).toContain('bg-orange-100')
    expect(statusBadgeClass(200, 'verified')).toContain('bg-green-100')
    expect(statusBadgeClass(500, 'incomplete')).toContain('bg-red-100')
    expect(statusBadgeTooltip(200, 'incomplete', 'worker_timeout')).toBe(
      '响应未完整结束（worker_timeout）'
    )
    expect(statusBadgeTooltip(200, 'incomplete', null)).toBe('响应未完整结束')
    expect(statusBadgeTooltip(200, 'verified', 'worker_timeout')).toBeNull()
  })

  it('styles known and unknown effort levels', () => {
    expect(thinkingEffortBadgeClass('XHigh')).toContain('bg-red-50')
    expect(thinkingEffortBadgeClass('weird')).toContain('bg-muted/40')
  })

  it('formats short relative time like hub', () => {
    const now = new Date('2026-01-10T00:00:00Z')
    const ago = (ms: number) => new Date(now.getTime() - ms)
    expect(formatShortDistance(ago(0), now)).toBe('刚刚')
    expect(formatShortDistance(ago(5_000), now)).toBe('5秒前')
    expect(formatShortDistance(ago(3 * 3600_000), now)).toBe('3时前')
    expect(formatShortDistance(ago(8 * 86400_000), now)).toBe('1周前')
  })

  it('resolves redirect and actual-model mismatch', () => {
    const audit = resolveModelAuditDisplay({
      originalModel: 'claude-x',
      model: 'claude-y',
      actualResponseModel: 'claude-z',
    })
    expect(audit.primaryBillingModel).toBe('claude-y')
    expect(audit.hasRedirect).toBe(true)
    expect(audit.secondaryActualModel).toBe('claude-z')
  })
})

describe('logs search', () => {
  it('keeps legacy kind=error / error_class deep links working', () => {
    const search = validateLogsSearch({ kind: 'error', error_class: 'auth' })
    const filters = searchToFilters(search)
    expect(filters.excludeStatus200).toBe(true)
    expect(filters.errorClass).toBe('auth')
    expect(filtersToSearch(filters)).toEqual({
      statusCode: '!200',
      error_class: 'auth',
    })
  })

  it('accepts a search with only sessionId', () => {
    expect(validateLogsSearch({ sessionId: ' abc ' }).sessionId).toBe('abc')
  })

  it('maps filters onto the backend query string', () => {
    const api = filtersToApi(
      { minRetry: 1, excludeStatus200: true, startTime: 0, debugOnly: true },
      ['auth']
    )
    expect(api.minAttemptCount).toBe(2)
    expect(usageLogFiltersQuery(api).toString()).toBe(
      'exclude_status_200=1&min_attempt_count=2&exclude_error_class=auth&log_mode=debug&start_time=1970-01-01T00%3A00%3A00.000Z'
    )
    expect(
      filtersToApi({ errorClass: 'quota' }, ['auth']).excludeErrorClass
    ).toBeUndefined()
  })

  it('counts a time range as one active filter', () => {
    expect(activeFilterCount({ startTime: 1, endTime: 2, model: 'm' })).toBe(2)
  })
})
