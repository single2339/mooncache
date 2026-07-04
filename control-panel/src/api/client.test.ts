import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiError, debugCache, getUserVisibleError, listAlerts, listTenants, listVendors } from './client'

describe('admin API user-visible errors', () => {
  it('does not expose stack-like error messages', () => {
    expect(getUserVisibleError(new Error('stack: TypeError: secret internal detail'))).toBe(
      'Request failed. Please try again.',
    )
    expect(getUserVisibleError(new ApiError('failed\n    at internalFunction (/Users/app/server.ts:10)', 500))).toBe(
      'Request failed. Please try again.',
    )
  })

  it('keeps short safe API messages visible', () => {
    expect(getUserVisibleError(new ApiError('Node is already draining.', 409))).toBe('Node is already draining.')
  })
})

afterEach(() => {
  vi.restoreAllMocks()
})

describe('admin API cache debugger', () => {
  it('posts the backend fingerprint debug request shape and maps raw cache key to a redacted result', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ cache_key: '1234567890abcdef1234567890abcdef' }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    )
    vi.stubGlobal('fetch', fetchMock)

    const result = await debugCache({
      tenant_id: 'tenant-a',
      endpoint_version: 'responses.v1',
      vendor_id: 'openai',
      resolved_model_version: 'gpt-4.1',
      adapter_version: 'adapter.v1',
      cache_policy: 'default',
      body: { messages: [{ role: 'user', content: 'secret prompt' }] },
    })

    expect(fetchMock).toHaveBeenCalledWith('/admin/cache/fingerprint/debug', {
      method: 'POST',
      body: JSON.stringify({
        tenant_id: 'tenant-a',
        endpoint_version: 'responses.v1',
        vendor_id: 'openai',
        resolved_model_version: 'gpt-4.1',
        adapter_version: 'adapter.v1',
        cache_policy: 'default',
        body: { messages: [{ role: 'user', content: 'secret prompt' }] },
      }),
      headers: { accept: 'application/json', 'content-type': 'application/json' },
    })
    expect(result).toEqual({
      cache_key_redacted: '12345678…cdef',
      eligible: true,
    })
  })
})

describe('admin API inventory readers', () => {
  it('reads tenants, vendors, and alerts from backend endpoints', async () => {
    const fetchMock = vi.fn((url: string) => {
      const responses: Record<string, unknown> = {
        '/admin/tenants': [
          {
            tenantId: 'tenant-a',
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
            vendorId: 'openai',
            health: 'healthy',
            models: ['gpt-4.1'],
            resolvedVersion: 'openai-responses-v1',
            rateLimitRemaining: 0,
            errorRate: 0,
            retryCount: 0,
            costPerMillionTokensUsd: 0,
            routingPolicy: 'openai-responses',
          },
        ],
        '/admin/alerts': [],
      }

      return Promise.resolve(
        new Response(JSON.stringify(responses[url]), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        }),
      )
    })
    vi.stubGlobal('fetch', fetchMock)

    await expect(listTenants()).resolves.toHaveLength(1)
    await expect(listVendors()).resolves.toHaveLength(1)
    await expect(listAlerts()).resolves.toEqual([])

    expect(fetchMock).toHaveBeenCalledWith('/admin/tenants', {
      headers: { accept: 'application/json' },
    })
    expect(fetchMock).toHaveBeenCalledWith('/admin/vendors', {
      headers: { accept: 'application/json' },
    })
    expect(fetchMock).toHaveBeenCalledWith('/admin/alerts', {
      headers: { accept: 'application/json' },
    })
  })
})
