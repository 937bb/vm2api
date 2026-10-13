import { infiniteQueryOptions, queryOptions } from '@tanstack/react-query'
import type {
  RequestLogAttemptsPayload,
  RequestLogDetailPayload,
} from '@/types/panel-logs'
import type { RequestLogStats } from '@/types/panel-overview'
import type {
  ActiveSessionsData,
  UsageLogCursor,
  UsageLogFilterOptions,
  UsageLogFilters,
  UsageLogSessionSuggestions,
  UsageLogsBatchResult,
  UsageLogsOverview,
  UsageLogsSummary,
} from '@/types/panel-usage-logs'
import { api } from '@/lib/api'
import { usageLogFiltersQuery } from './search'

/** `/request-logs` 链路（错误归集、导出、调试详情）共用前缀。 */
export const LOGS_QUERY_KEY = ['panel', 'request-logs'] as const
/** 滚动日志页全部查询的前缀：屏蔽集或手动刷新时整体失效。 */
export const USAGE_LOGS_QUERY_KEY = ['panel', 'usage-logs'] as const
export const USAGE_LOGS_BATCH_SIZE = 50

export function requestLogQueryOptions(id: string, enabled = true) {
  return queryOptions({
    queryKey: ['panel', 'request-log', id] as const,
    queryFn: async () => {
      const item = await api<RequestLogDetailPayload>(
        `/api/panel/request-logs/${encodeURIComponent(id)}`
      )
      const attempts = await api<RequestLogAttemptsPayload>(
        `/api/panel/request-logs/${encodeURIComponent(id)}/attempts`
      ).catch(() => ({ attempts: [] }))
      return { item: item.item || item, attempts: attempts.attempts || [] }
    },
    enabled,
  })
}

export function logStatsQueryOptions(
  since: string,
  refetchInterval?: number,
  bucket: 'hour' | 'day' = 'hour',
  overview = false
) {
  return queryOptions({
    queryKey: ['panel', 'request-logs-stats', bucket, since, overview] as const,
    queryFn: () =>
      api<RequestLogStats>(
        `/api/panel/request-logs/stats?bucket=${bucket}&since=${encodeURIComponent(since)}${overview ? '&view=overview' : ''}`
      ),
    ...(refetchInterval
      ? { refetchInterval, refetchOnWindowFocus: false }
      : {}),
  })
}

/**
 * 键集游标无限列表。`shouldPoll` 由表格按滚动位置给出：用户往下翻时
 * 停止轮询，否则 React Query 会把已加载的每一页都顺序重拉一遍。
 */
export function usageLogsBatchQueryOptions({
  filters,
  pollMs,
  shouldPoll,
}: {
  filters: UsageLogFilters
  pollMs: number
  shouldPoll: () => boolean
}) {
  return infiniteQueryOptions({
    queryKey: [...USAGE_LOGS_QUERY_KEY, 'batch', filters] as const,
    queryFn: ({ pageParam }) => {
      const qs = usageLogFiltersQuery(filters)
      qs.set('limit', String(USAGE_LOGS_BATCH_SIZE))
      if (pageParam) {
        qs.set('cursor_created_at', pageParam.createdAt)
        qs.set('cursor_id', pageParam.id)
      }
      return api<UsageLogsBatchResult>(`/api/panel/usage-logs?${qs}`)
    },
    initialPageParam: null as UsageLogCursor | null,
    getNextPageParam: (last) =>
      last.hasMore && last.nextCursor ? last.nextCursor : undefined,
    staleTime: 30_000,
    refetchOnWindowFocus: false,
    refetchInterval: (query) =>
      shouldPoll() && query.state.fetchStatus === 'idle' ? pollMs : false,
  })
}

export function usageLogsSummaryQueryOptions(filters: UsageLogFilters) {
  return queryOptions({
    queryKey: [...USAGE_LOGS_QUERY_KEY, 'summary', filters] as const,
    queryFn: () =>
      api<UsageLogsSummary>(
        `/api/panel/usage-logs/summary?${usageLogFiltersQuery(filters)}`
      ),
  })
}

export function usageLogFilterOptionsQueryOptions(enabled = true) {
  return queryOptions({
    queryKey: [...USAGE_LOGS_QUERY_KEY, 'filter-options'] as const,
    queryFn: () =>
      api<UsageLogFilterOptions>('/api/panel/usage-logs/filter-options'),
    staleTime: 5 * 60_000,
    enabled,
  })
}

export function usageLogSessionSuggestionsQueryOptions(q: string) {
  return queryOptions({
    queryKey: [...USAGE_LOGS_QUERY_KEY, 'session-suggestions', q] as const,
    queryFn: () =>
      api<UsageLogSessionSuggestions>(
        `/api/panel/usage-logs/session-suggestions?q=${encodeURIComponent(q)}&limit=20`
      ),
    enabled: q.length >= 2,
    staleTime: 30_000,
  })
}

export function activeSessionsQueryOptions() {
  return queryOptions({
    queryKey: [...USAGE_LOGS_QUERY_KEY, 'active-sessions'] as const,
    queryFn: () =>
      api<ActiveSessionsData>(
        '/api/panel/usage-logs/active-sessions?minutes=5&limit=50'
      ),
    refetchInterval: 5000,
  })
}

export function usageLogsOverviewQueryOptions(
  tz: string,
  refetchInterval: number | false = false
) {
  return queryOptions({
    queryKey: [...USAGE_LOGS_QUERY_KEY, 'overview', tz] as const,
    queryFn: () =>
      api<UsageLogsOverview>(
        `/api/panel/usage-logs/overview?tz=${encodeURIComponent(tz)}`
      ),
    refetchInterval,
    refetchOnWindowFocus: false,
  })
}
