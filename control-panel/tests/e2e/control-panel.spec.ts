import { expect, test } from '@playwright/test'

const apiResponses: Record<string, unknown> = {
  '/admin/metrics': {
    audit_events_total: 12,
    audit_success_total: 11,
    audit_denied_total: 1,
    audit_failed_total: 0,
    action_counts: { read_metrics: 42 },
  },
  '/admin/audit-events': Array.from({ length: 11 }, (_, index) => ({
    actor: 'operator@example.com',
    role: 'Operator',
    action: index === 0 ? 'DrainNode' : 'ReadMetrics',
    resource: index === 0 ? 'node:store-a' : 'metrics',
    tenant_scope: index === 0 ? 'tenant-a' : null,
    before_summary: null,
    after_summary: 'ok',
    request_id: `req-${index + 1}`,
    timestamp_ms: Date.UTC(2026, 0, 1, 0, 0, index),
    result: 'Success',
  })),
  '/admin/nodes': [{ node_id: 'store-a', draining: false }],
  '/admin/tenants': [
    {
      tenantId: 'tenant-a',
      apiKeyRef: 'sha256:e46ea83e…',
      dramQuotaBytes: 1073741824,
      ssdQuotaBytes: 10737418240,
      requestRateLimitPerMinute: 12000,
      streamConcurrencyLimit: 80,
      vendorSpendBudgetUsd: 5000,
      defaultTtlSeconds: 86400,
      policy: 'cache_first',
    },
  ],
  '/admin/vendors': [
    {
      vendorId: 'openai',
      health: 'healthy',
      models: ['gpt-4.1', 'gpt-4.1-mini'],
      resolvedVersion: 'openai-responses-v1',
      rateLimitRemaining: 0,
      errorRate: 0,
      retryCount: 0,
      costPerMillionTokensUsd: 0,
      routingPolicy: 'openai-responses',
    },
  ],
  '/admin/alerts': Array.from({ length: 11 }, (_, index) => ({
    id: `alert-${index + 1}`,
    severity: 'info',
    status: 'resolved',
    message: `Resolved alert ${index + 1}`,
    resource: 'cache',
    startedAtMs: Date.UTC(2026, 0, 1, 0, 0, index),
    lastSeenAtMs: Date.UTC(2026, 0, 1, 0, 0, index),
  })),
}

test.beforeEach(async ({ page }) => {
  await page.route('/admin/**', async (route) => {
    const url = new URL(route.request().url())
    await route.fulfill({ json: apiResponses[url.pathname] ?? { ok: true } })
  })
})

test('loads live control panel data and paginates operator tables', async ({ page }) => {
  await page.goto('/')

  await expect(page.getByRole('heading', { name: 'Overview' })).toBeVisible()
  await expect(page.getByText('No active operational actions.')).toBeVisible()

  await page.getByRole('link', { name: 'Tenants' }).click()
  await expect(page.getByRole('rowheader', { name: 'tenant-a' })).toBeVisible()

  await page.getByRole('link', { name: 'Vendors' }).click()
  await expect(page.getByRole('rowheader', { name: 'openai' })).toBeVisible()
  await expect(page.getByText('gpt-4.1, gpt-4.1-mini')).toBeVisible()

  await page.getByRole('link', { name: 'Audit Log' }).click()
  await expect(page.getByText('Page 1 of 2')).toBeVisible()
  await expect(page.getByRole('cell', { name: 'req-1', exact: true })).toBeVisible()
  await expect(page.getByRole('cell', { name: 'req-11', exact: true })).toHaveCount(0)
  await page.getByRole('button', { name: 'Next audit events' }).click()
  await expect(page.getByText('Page 2 of 2')).toBeVisible()
  await expect(page.getByRole('cell', { name: 'req-11', exact: true })).toBeVisible()

  await page.getByRole('link', { name: 'Alerts' }).click()
  await expect(page.getByRole('cell', { name: 'Resolved alert 1', exact: true })).toBeVisible()
  await page.getByRole('button', { name: 'Next alert history' }).click()
  await expect(page.getByRole('cell', { name: 'Resolved alert 11', exact: true })).toBeVisible()
})
