import { fireEvent, render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'
import { Overview } from './Overview'
import { CacheAnalytics } from './CacheAnalytics'
import { Nodes } from './Nodes'
import { AuditLog } from './AuditLog'
import { CacheOperations } from './CacheOperations'
import { Tenants } from './Tenants'
import { Vendors } from './Vendors'
import { ConfirmationModal } from '../components/ConfirmationModal'
import type {
  AuditEvent,
  CacheAnalyticsData,
  CacheObjectView,
  NodeView,
  OverviewData,
  TenantView,
  VendorView,
} from '../api/types'

const overviewData: OverviewData = {
  health: 'healthy',
  requestRatePerSecond: 1280,
  cacheHitRate: 0.89,
  vendorCallsAvoided: 12400,
  estimatedCostSavedUsd: 1860,
  hitLatencyMs: { p50: 12, p95: 38, p99: 72 },
  missOverheadMs: { p50: 45, p95: 184, p99: 330 },
  activeAlerts: [{ id: 'alert-pressure', severity: 'warning', message: 'Node sfo-2 DRAM pressure is elevated' }],
  actionItems: ['Tenant acme is near SSD quota', 'Node sfo-2 needs pressure review'],
}

const analyticsData: CacheAnalyticsData = {
  outcomes: {
    hit: 89000,
    miss: 7000,
    bypass: 2500,
    ineligible: 1000,
    writebackFailed: 500,
  },
  hotKeys: [
    { fingerprint: 'sha256:abc123', hits: 9200, tenantId: 'acme', model: 'gpt-4.1' },
  ],
  tenantUsage: [
    { tenantId: 'acme', storageBytes: 1_200_000_000, requests: 64000 },
    { tenantId: 'globex', storageBytes: 420_000_000, requests: 18000 },
  ],
  tierHitRatio: { dram: 0.74, ssd: 0.15 },
  evictionReasons: [
    { reason: 'quota', count: 320 },
    { reason: 'ttl-expired', count: 210 },
  ],
  singleflight: { leaders: 1200, waiters: 8300 },
}

const nodes: NodeView[] = [
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
]

const cacheObjects: CacheObjectView[] = [
  {
    tenantId: 'acme',
    fingerprint: 'sha256:abc123',
    model: 'gpt-4.1',
    tier: 'DRAM',
    sizeBytes: 48_000,
    ttlSecondsRemaining: 2700,
    pinned: false,
  },
]

const tenants: TenantView[] = [
  {
    tenantId: 'acme',
    apiKeyRef: 'keyref_live_acme',
    dramQuotaBytes: 2_000_000_000,
    ssdQuotaBytes: 10_000_000_000,
    requestRateLimitPerMinute: 12000,
    streamConcurrencyLimit: 80,
    vendorSpendBudgetUsd: 5000,
    defaultTtlSeconds: 3600,
    policy: 'cache-first',
  },
]

const vendors: VendorView[] = [
  {
    vendorId: 'openai',
    health: 'degraded',
    models: ['gpt-4.1'],
    resolvedVersion: 'gpt-4.1-2026-06-10',
    rateLimitRemaining: 42000,
    errorRate: 0.027,
    retryCount: 240,
    costPerMillionTokensUsd: 6.2,
    routingPolicy: 'prefer mini for retries',
  },
]

const auditEvents: AuditEvent[] = [
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
]

describe('control panel data pages', () => {
  it('hides node drain from viewers and shows it to operators', () => {
    const { rerender } = render(<Nodes role="viewer" nodes={nodes} />)

    expect(screen.queryByRole('button', { name: /drain node sfo-1/i })).not.toBeInTheDocument()
    expect(screen.getByText(/viewer role can inspect nodes but cannot drain them/i)).toBeInTheDocument()

    rerender(<Nodes role="operator" nodes={nodes} />)

    expect(screen.getByRole('button', { name: /drain node sfo-1/i })).toBeInTheDocument()
  })

  it('gates purge, warmup, and policy operations by role', () => {
    const { rerender } = render(<CacheOperations role="viewer" objects={cacheObjects} />)

    expect(screen.queryByRole('button', { name: /purge cache object/i })).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /warm up cache/i })).not.toBeInTheDocument()
    expect(screen.getByText(/viewer role can inspect metadata but cannot purge/i)).toBeInTheDocument()

    rerender(<CacheOperations role="operator" objects={cacheObjects} />)

    expect(screen.getByRole('button', { name: /purge cache object sha256:abc123/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /warm up cache/i })).toBeInTheDocument()

    rerender(<Tenants role="operator" tenants={tenants} />)
    expect(screen.queryByRole('button', { name: /change policy for acme/i })).not.toBeInTheDocument()
    expect(screen.getByText(/only admins can change tenant policies/i)).toBeInTheDocument()

    rerender(<Tenants role="admin" tenants={tenants} />)
    expect(screen.getByRole('button', { name: /change policy for acme/i })).toBeInTheDocument()

    rerender(<Vendors role="operator" vendors={vendors} />)
    expect(screen.queryByRole('button', { name: /change vendor policy for openai/i })).not.toBeInTheDocument()
    expect(screen.getByText(/only admins can change vendor routing policies/i)).toBeInTheDocument()

    rerender(<Vendors role="admin" vendors={vendors} />)
    expect(screen.getByRole('button', { name: /change vendor policy for openai/i })).toBeInTheDocument()
  })

  it('debugs cache fingerprints with redacted result metadata', async () => {
    const user = userEvent.setup()
    const onDebugCache = vi.fn().mockResolvedValue({
      cache_key_redacted: 'sha256:redacted-abc',
      eligible: true,
      reason: 'Policy matched',
    })

    render(<CacheOperations role="operator" objects={cacheObjects} onDebugCache={onDebugCache} />)

    await user.clear(screen.getByLabelText(/tenant id/i))
    await user.type(screen.getByLabelText(/tenant id/i), 'tenant-debug')
    fireEvent.change(screen.getByLabelText(/request body/i), {
      target: { value: '{"messages":[{"role":"user","content":"top secret"}]}' },
    })
    await user.click(screen.getByRole('button', { name: /debug fingerprint/i }))

    expect(onDebugCache).toHaveBeenCalledWith({
      tenant_id: 'tenant-debug',
      endpoint_version: 'responses.v1',
      vendor_id: 'default',
      resolved_model_version: 'default',
      adapter_version: 'control-panel.v1',
      cache_policy: 'default',
      body: { messages: [{ role: 'user', content: 'top secret' }] },
    })
    const result = await screen.findByRole('status')
    expect(within(result).getByText(/redacted cache key: sha256:redacted-abc/i)).toBeInTheDocument()
    expect(within(result).getByText(/eligibility: eligible/i)).toBeInTheDocument()
    expect(within(result).getByText(/reason: policy matched/i)).toBeInTheDocument()
    expect(within(result).queryByText(/top secret/i)).not.toBeInTheDocument()
  })

  it('shows a safe validation error for invalid debug JSON', async () => {
    const user = userEvent.setup()
    const onDebugCache = vi.fn()

    render(<CacheOperations role="operator" objects={cacheObjects} onDebugCache={onDebugCache} />)

    fireEvent.change(screen.getByLabelText(/request body/i), { target: { value: '{"messages":' } })
    await user.click(screen.getByRole('button', { name: /debug fingerprint/i }))
    expect(onDebugCache).not.toHaveBeenCalled()
    expect(screen.getByRole('alert')).toHaveTextContent('Debug request body must be valid JSON.')
  })

  it('sanitizes stack-like debug API failures at the component boundary', async () => {
    const user = userEvent.setup()
    const onDebugCache = vi.fn().mockRejectedValue(
      new Error('TypeError: failed\n    at debug (/Users/nanxin/secret.ts:1:1)\nrequest body: top secret'),
    )

    render(<CacheOperations role="operator" objects={cacheObjects} onDebugCache={onDebugCache} />)

    await user.click(screen.getByRole('button', { name: /debug fingerprint/i }))

    const alert = await screen.findByRole('alert')
    expect(alert).toHaveTextContent('Request failed. Please try again.')
    expect(alert).not.toHaveTextContent(/secret|request body|TypeError|\/Users/i)
  })

  it('supports confirm, cancel, Escape, focus trap, and focus restore in confirmation modals', async () => {
    const user = userEvent.setup()
    const onConfirm = vi.fn()

    render(
      <div>
        <button type="button">Before trigger</button>
        <button type="button">Drain node sfo-1</button>
        <ConfirmationModal
          isOpen
          title="Drain node sfo-1"
          message="Drain this node?"
          confirmLabel="Confirm drain"
          cancelLabel="Cancel drain"
          onConfirm={onConfirm}
          onClose={() => undefined}
        />
        <button type="button">After dialog</button>
      </div>,
    )

    const dialog = screen.getByRole('dialog', { name: /drain node sfo-1/i })
    expect(screen.getByRole('button', { name: /cancel drain/i })).toHaveFocus()

    await user.tab()
    expect(screen.getByRole('button', { name: /confirm drain/i })).toHaveFocus()
    await user.tab()
    expect(screen.getByRole('button', { name: /cancel drain/i })).toHaveFocus()
    expect(within(dialog).getByText(/drain this node/i)).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: /confirm drain/i }))
    expect(onConfirm).toHaveBeenCalledTimes(1)
  })

  it('closes node drain modal with cancel and Escape while restoring focus to the trigger', async () => {
    const user = userEvent.setup()
    render(<Nodes role="operator" nodes={nodes} />)

    const trigger = screen.getByRole('button', { name: /drain node sfo-1/i })
    await user.click(trigger)
    await user.click(screen.getByRole('button', { name: /cancel drain/i }))
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    expect(trigger).toHaveFocus()

    await user.click(trigger)
    expect(screen.getByRole('dialog', { name: /drain node sfo-1/i })).toBeInTheDocument()
    await user.keyboard('{Escape}')
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    expect(trigger).toHaveFocus()
  })

  it('renders audit events with actor, action, result, and request ID', () => {
    render(<AuditLog events={auditEvents} />)

    expect(screen.getByRole('heading', { name: /audit log/i })).toBeInTheDocument()
    expect(screen.getByRole('cell', { name: 'operator@example.com' })).toBeInTheDocument()
    expect(screen.getByRole('cell', { name: 'DrainNode' })).toBeInTheDocument()
    expect(screen.getByText('Success')).toBeInTheDocument()
    expect(screen.getByText('req-123')).toBeInTheDocument()
  })

  it('answers overview operator questions from provided data', () => {
    render(<Overview data={overviewData} />)

    expect(screen.getByText(/system health/i)).toHaveTextContent(/healthy/i)
    expect(screen.getByText(/cache value is improving/i)).toBeInTheDocument()
    expect(screen.getByText(/89% hit rate/i)).toBeInTheDocument()
    expect(screen.getByText(/12,400 vendor calls avoided/i)).toBeInTheDocument()
    expect(screen.getByText(/\$1,860 saved/i)).toBeInTheDocument()
    expect(screen.getByText(/latency pressure comes from miss p95 at 184 ms/i)).toBeInTheDocument()
    expect(screen.getByText(/Tenant acme is near SSD quota/i)).toBeInTheDocument()
    expect(screen.getByText(/Node sfo-2 needs pressure review/i)).toBeInTheDocument()
  })

  it('answers cache analytics questions from provided data', () => {
    render(<CacheAnalytics data={analyticsData} />)

    expect(screen.getByText(/89,000 hits/i)).toBeInTheDocument()
    expect(screen.getByText(/500 writeback failures/i)).toBeInTheDocument()
    expect(screen.getByText(/DRAM serves 74% of hits; SSD serves 15%/i)).toBeInTheDocument()
    expect(screen.getByText(/sha256:abc123/i)).toBeInTheDocument()
    expect(screen.getByText(/acme is the top tenant by storage and traffic/i)).toBeInTheDocument()
    expect(screen.getByText(/quota evictions are the leading pressure source/i)).toBeInTheDocument()
    expect(screen.getByText(/8,300 waiters collapsed behind 1,200 leaders/i)).toBeInTheDocument()
  })
})
