import type {
  AlertView,
  AuditEvent,
  CacheAnalyticsData,
  CacheObjectView,
  NodeView,
  OverviewData,
  TenantView,
  VendorView,
} from './types'

export const overviewData: OverviewData = {
  health: 'healthy',
  requestRatePerSecond: 1280,
  cacheHitRate: 0.89,
  vendorCallsAvoided: 12400,
  estimatedCostSavedUsd: 1860,
  hitLatencyMs: { p50: 12, p95: 38, p99: 72 },
  missOverheadMs: { p50: 45, p95: 184, p99: 330 },
  activeAlerts: [
    { id: 'dram-pressure', severity: 'warning', message: 'Node sfo-2 DRAM pressure is elevated' },
    { id: 'vendor-error', severity: 'info', message: 'OpenAI retries are above the weekly baseline' },
  ],
  actionItems: ['Tenant acme is near SSD quota', 'Node sfo-2 needs pressure review'],
}

export const cacheAnalyticsData: CacheAnalyticsData = {
  outcomes: {
    hit: 89000,
    miss: 7000,
    bypass: 2500,
    ineligible: 1000,
    writebackFailed: 500,
  },
  hotKeys: [
    { fingerprint: 'sha256:abc123', hits: 9200, tenantId: 'acme', model: 'gpt-4.1' },
    { fingerprint: 'sha256:def456', hits: 5400, tenantId: 'globex', model: 'claude-3.7' },
  ],
  tenantUsage: [
    { tenantId: 'acme', storageBytes: 1_200_000_000, requests: 64000 },
    { tenantId: 'globex', storageBytes: 420_000_000, requests: 18000 },
    { tenantId: 'initech', storageBytes: 260_000_000, requests: 9300 },
  ],
  tierHitRatio: { dram: 0.74, ssd: 0.15 },
  evictionReasons: [
    { reason: 'quota', count: 320 },
    { reason: 'ttl-expired', count: 210 },
    { reason: 'manual-purge', count: 18 },
  ],
  singleflight: { leaders: 1200, waiters: 8300 },
}

export const nodes: NodeView[] = [
  {
    node_id: 'sfo-1',
    draining: false,
    address: '10.0.0.11:7100',
    state: 'Ready',
    dram_bytes_used: 70,
    dram_bytes_capacity: 100,
    ssd_bytes_used: 300,
    ssd_bytes_capacity: 1000,
    segments: 24,
    replicas: 3,
    heartbeat_age_ms: 1400,
  },
  {
    node_id: 'sfo-2',
    draining: true,
    address: '10.0.0.12:7100',
    state: 'Draining',
    dram_bytes_used: 91,
    dram_bytes_capacity: 100,
    ssd_bytes_used: 640,
    ssd_bytes_capacity: 1000,
    segments: 19,
    replicas: 3,
    heartbeat_age_ms: 2200,
  },
]

export const tenants: TenantView[] = [
  {
    tenantId: 'acme',
    apiKeyRef: 'keyref_live_acme',
    dramQuotaBytes: 2_000_000_000,
    ssdQuotaBytes: 10_000_000_000,
    requestRateLimitPerMinute: 12000,
    streamConcurrencyLimit: 80,
    vendorSpendBudgetUsd: 5000,
    defaultTtlSeconds: 3600,
    policy: 'cache-first with streaming writeback',
  },
  {
    tenantId: 'globex',
    apiKeyRef: 'keyref_live_globex',
    dramQuotaBytes: 800_000_000,
    ssdQuotaBytes: 4_000_000_000,
    requestRateLimitPerMinute: 5000,
    streamConcurrencyLimit: 32,
    vendorSpendBudgetUsd: 1800,
    defaultTtlSeconds: 1800,
    policy: 'bypass unsafe tools',
  },
]

export const vendors: VendorView[] = [
  {
    vendorId: 'openai',
    health: 'degraded',
    models: ['gpt-4.1', 'gpt-4.1-mini'],
    resolvedVersion: 'gpt-4.1-2026-06-10',
    rateLimitRemaining: 42000,
    errorRate: 0.027,
    retryCount: 240,
    costPerMillionTokensUsd: 6.2,
    routingPolicy: 'prefer mini for warmup retries',
  },
  {
    vendorId: 'anthropic',
    health: 'healthy',
    models: ['claude-sonnet-4', 'claude-haiku-4'],
    resolvedVersion: 'claude-sonnet-4-20260618',
    rateLimitRemaining: 87000,
    errorRate: 0.004,
    retryCount: 38,
    costPerMillionTokensUsd: 5.4,
    routingPolicy: 'primary for long-context tenants',
  },
]

export const cacheObjects: CacheObjectView[] = [
  {
    tenantId: 'acme',
    fingerprint: 'sha256:abc123',
    model: 'gpt-4.1',
    tier: 'DRAM',
    sizeBytes: 48_000,
    ttlSecondsRemaining: 2700,
    pinned: false,
  },
  {
    tenantId: 'globex',
    fingerprint: 'sha256:def456',
    model: 'claude-3.7',
    tier: 'SSD',
    sizeBytes: 112_000,
    ttlSecondsRemaining: 9100,
    pinned: true,
  },
]

export const alerts: AlertView[] = [
  {
    id: 'dram-pressure',
    severity: 'warning',
    status: 'active',
    message: 'Node sfo-2 DRAM pressure is elevated',
    resource: 'node/sfo-2',
    startedAtMs: Date.UTC(2026, 6, 4, 11, 55),
    lastSeenAtMs: Date.UTC(2026, 6, 4, 12, 30),
  },
  {
    id: 'vendor-error',
    severity: 'info',
    status: 'acknowledged',
    message: 'OpenAI retries are above the weekly baseline',
    resource: 'vendor/openai',
    startedAtMs: Date.UTC(2026, 6, 4, 9, 20),
    lastSeenAtMs: Date.UTC(2026, 6, 4, 12, 20),
  },
]

export const auditEvents: AuditEvent[] = [
  {
    actor: 'operator@example.com',
    role: 'Operator',
    action: 'DrainNode',
    resource: 'node/sfo-1',
    tenant_scope: null,
    before_summary: 'draining=false',
    after_summary: 'draining=true',
    request_id: 'req-123',
    timestamp_ms: Date.UTC(2026, 6, 4, 12, 30),
    result: 'Success',
  },
  {
    actor: 'viewer@example.com',
    role: 'Viewer',
    action: 'ReadAuditLog',
    resource: 'audit-log',
    tenant_scope: 'acme',
    before_summary: null,
    after_summary: 'returned 25 events',
    request_id: 'req-124',
    timestamp_ms: Date.UTC(2026, 6, 4, 12, 35),
    result: 'Success',
  },
]
