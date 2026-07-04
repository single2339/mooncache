import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiError, debugCache, getUserVisibleError } from './client'

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
