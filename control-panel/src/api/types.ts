export type AdminRole = 'NoAccess' | 'Viewer' | 'Operator' | 'Admin'

export type AdminAction =
  | 'ReadMetrics'
  | 'ReadNodes'
  | 'ReadAuditLog'
  | 'DebugCacheFingerprint'
  | 'DrainNode'
  | 'RemoveCacheObject'
  | 'WarmupCache'
  | 'PatchTenantPolicy'
  | 'PatchVendorPolicy'
  | 'ManageUsers'

export type AuditResult = 'Success' | 'Denied' | { Failed: string }

export interface AdminMetricsSnapshot {
  audit_events_total: number
  audit_success_total: number
  audit_denied_total: number
  audit_failed_total: number
  action_counts: Record<string, number>
}

export interface AuditEvent {
  actor: string
  role: AdminRole
  action: AdminAction
  resource: string
  tenant_scope: string | null
  before_summary: string | null
  after_summary: string | null
  request_id: string
  timestamp_ms: number
  result: AuditResult
}

export interface LatencyTriplet {
  p50: number
  p95: number
  p99: number
}

export interface OverviewAlert {
  id: string
  severity: 'info' | 'warning' | 'critical'
  message: string
}

export interface OverviewData {
  health: 'healthy' | 'degraded' | 'critical'
  requestRatePerSecond: number
  cacheHitRate: number
  vendorCallsAvoided: number
  estimatedCostSavedUsd: number
  hitLatencyMs: LatencyTriplet
  missOverheadMs: LatencyTriplet
  activeAlerts: OverviewAlert[]
  actionItems: string[]
}

export interface CacheAnalyticsData {
  outcomes: {
    hit: number
    miss: number
    bypass: number
    ineligible: number
    writebackFailed: number
  }
  hotKeys: Array<{
    fingerprint: string
    hits: number
    tenantId: string
    model: string
  }>
  tenantUsage: Array<{
    tenantId: string
    storageBytes: number
    requests: number
  }>
  tierHitRatio: {
    dram: number
    ssd: number
  }
  evictionReasons: Array<{
    reason: string
    count: number
  }>
  singleflight: {
    leaders: number
    waiters: number
  }
}

export interface NodeView {
  node_id: string
  draining: boolean
  address?: string
  state?: 'Ready' | 'Draining' | 'Offline' | 'Recovering'
  dram_bytes_used?: number
  dram_bytes_capacity?: number
  ssd_bytes_used?: number
  ssd_bytes_capacity?: number
  segments?: number
  replicas?: number
  heartbeat_age_ms?: number
}

export interface TenantView {
  tenantId: string
  apiKeyRef: string
  dramQuotaBytes: number
  ssdQuotaBytes: number
  requestRateLimitPerMinute: number
  streamConcurrencyLimit: number
  vendorSpendBudgetUsd: number
  defaultTtlSeconds: number
  policy: string
}

export interface VendorView {
  vendorId: string
  health: 'healthy' | 'degraded' | 'down'
  models: string[]
  resolvedVersion: string
  rateLimitRemaining: number
  errorRate: number
  retryCount: number
  costPerMillionTokensUsd: number
  routingPolicy: string
}

export interface CacheObjectView {
  tenantId: string
  fingerprint: string
  model: string
  tier: 'DRAM' | 'SSD'
  sizeBytes: number
  ttlSecondsRemaining: number
  pinned: boolean
}

export interface CacheDebugRequest {
  tenant_id: string
  endpoint_version: string
  vendor_id: string
  resolved_model_version: string
  adapter_version: string
  cache_policy: string
  body: unknown
}

export interface CacheDebugApiResponse {
  cache_key: string
}

export interface CacheDebugResponse {
  cache_key_redacted: string
  eligible: boolean
  reason?: string
}

export interface WarmupCacheRequest {
  tenant_id: string
  request_body: unknown
}

export interface AlertView {
  id: string
  severity: 'info' | 'warning' | 'critical'
  status: 'active' | 'acknowledged' | 'silenced' | 'resolved'
  message: string
  resource: string
  startedAtMs: number
  lastSeenAtMs: number
}
