import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { App } from '../App'

afterEach(() => {
  vi.restoreAllMocks()
  window.location.hash = ''
})

function stubControlPanelFetch() {
  const responses: Record<string, unknown> = {
    '/admin/metrics': {
      audit_events_total: 2,
      audit_success_total: 2,
      audit_denied_total: 0,
      audit_failed_total: 0,
      action_counts: { read_metrics: 12 },
    },
    '/admin/audit-events': [
      {
        actor: 'operator@example.com',
        role: 'Operator',
        action: 'ReadMetrics',
        resource: 'metrics',
        tenant_scope: null,
        before_summary: null,
        after_summary: 'snapshot_returned',
        request_id: 'req-1',
        timestamp_ms: Date.UTC(2026, 0, 1),
        result: 'Success',
      },
    ],
    '/admin/nodes': [{ node_id: 'api-store-1', draining: false }],
    '/admin/tenants': [
      {
        tenantId: 'api-tenant',
        apiKeyRef: 'sha256:e46ea83e…',
        dramQuotaBytes: 1024,
        ssdQuotaBytes: 4096,
        requestRateLimitPerMinute: 120,
        streamConcurrencyLimit: 8,
        vendorSpendBudgetUsd: 500,
        defaultTtlSeconds: 3600,
        policy: 'cache_first',
      },
    ],
    '/admin/vendors': [
      {
        vendorId: 'api-vendor',
        health: 'healthy',
        models: ['gpt-4.1'],
        resolvedVersion: 'adapter-v1',
        rateLimitRemaining: 0,
        errorRate: 0,
        retryCount: 0,
        costPerMillionTokensUsd: 0,
        routingPolicy: 'openai-responses',
      },
    ],
    '/admin/alerts': [],
  }

  vi.stubGlobal(
    'fetch',
    vi.fn((url: string) =>
      Promise.resolve(
        new Response(JSON.stringify(responses[url]), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        }),
      ),
    ),
  )
}

describe('App shell', () => {
  it('loads backend data and exposes primary navigation', async () => {
    const user = userEvent.setup()
    stubControlPanelFetch()

    render(<App />)

    const main = screen.getByRole('main')
    expect(within(main).getByRole('heading', { level: 1, name: /mooncache control panel/i })).toBeInTheDocument()
    expect(await within(main).findByRole('heading', { name: /^overview$/i })).toBeInTheDocument()
    expect(within(main).getByText(/no active operational actions/i)).toBeInTheDocument()

    const primaryNav = screen.getByRole('navigation', { name: /primary/i })
    const navItems = within(primaryNav).getAllByRole('link')
    expect(navItems.map((item) => item.textContent?.trim())).toEqual([
      'Overview',
      'Cache Analytics',
      'Nodes',
      'Tenants',
      'Vendors',
      'Cache Operations',
      'Alerts',
      'Audit Log',
    ])

    await user.click(within(primaryNav).getByRole('link', { name: /^cache operations$/i }))

    expect(within(main).getByRole('heading', { name: /^cache operations$/i })).toBeInTheDocument()
    expect(within(main).getByRole('button', { name: /warm up cache/i })).toBeInTheDocument()
  })
})
