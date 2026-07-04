import type {
  AdminMetricsSnapshot,
  AlertView,
  AuditEvent,
  CacheDebugApiResponse,
  CacheDebugRequest,
  CacheDebugResponse,
  NodeView,
  TenantView,
  VendorView,
  WarmupCacheRequest,
} from './types'
const API_BASE_URL = import.meta.env.VITE_ADMIN_API_URL ?? '/admin'

export class ApiError extends Error {
  constructor(
    message: string,
    public readonly status: number,
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

const genericErrorMessage = 'Request failed. Please try again.'
const stackTracePattern = /(^|\n)\s*at\s+|\b[A-Za-z]:\\|\/Users\/|\/var\/|\/tmp\//

function sanitizeUserVisibleMessage(message: string): string {
  const normalized = message.replace(/\s+/g, ' ').trim()

  if (
    normalized.length === 0 ||
    normalized.length > 220 ||
    stackTracePattern.test(message) ||
    /\bstack\b/i.test(message)
  ) {
    return genericErrorMessage
  }

  return normalized
}

export function getUserVisibleError(error: unknown): string {
  if (error instanceof ApiError) {
    return sanitizeUserVisibleMessage(error.message)
  }

  if (error instanceof Error) {
    return sanitizeUserVisibleMessage(error.message)
  }

  return genericErrorMessage
}

export async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    ...init,
    headers: {
      accept: 'application/json',
      ...(init?.body === undefined ? {} : { 'content-type': 'application/json' }),
      ...(init?.headers ?? {}),
    },
  })

  if (!response.ok) {
    let message = `Admin API request failed (${response.status}).`

    try {
      const body = (await response.json()) as { message?: unknown; error?: unknown }
      const serverMessage = typeof body.message === 'string' ? body.message : body.error
      if (typeof serverMessage === 'string' && serverMessage.trim() !== '') {
        message = sanitizeUserVisibleMessage(serverMessage)
      }
    } catch {
      // Keep the generic status message; never surface stack traces or raw HTML.
    }

    throw new ApiError(message, response.status)
  }

  return response.json() as Promise<T>
}

export function getAdminMetrics(): Promise<AdminMetricsSnapshot> {
  return fetchJson<AdminMetricsSnapshot>('/metrics')
}

export function listAuditEvents(): Promise<AuditEvent[]> {
  return fetchJson<AuditEvent[]>('/audit-events')
}

export function listNodes(): Promise<NodeView[]> {
  return fetchJson<NodeView[]>('/nodes')
}

export function listTenants(): Promise<TenantView[]> {
  return fetchJson<TenantView[]>('/tenants')
}

export function listVendors(): Promise<VendorView[]> {
  return fetchJson<VendorView[]>('/vendors')
}

export function listAlerts(): Promise<AlertView[]> {
  return fetchJson<AlertView[]>('/alerts')
}

export async function debugCache(body: CacheDebugRequest): Promise<CacheDebugResponse> {
  const response = await fetchJson<CacheDebugApiResponse>('/cache/fingerprint/debug', {
    method: 'POST',
    body: JSON.stringify(body),
  })

  return {
    cache_key_redacted: redactCacheKey(response.cache_key),
    eligible: true,
  }
}

function redactCacheKey(cacheKey: string): string {
  return cacheKey.length <= 12 ? 'redacted' : `${cacheKey.slice(0, 8)}…${cacheKey.slice(-4)}`
}

export function drainNode(nodeId: string): Promise<NodeView> {
  return fetchJson<NodeView>(`/nodes/${encodeURIComponent(nodeId)}/drain`, { method: 'POST' })
}

export function purgeCacheObject(tenantId: string, fingerprint: string): Promise<{ ok: true }> {
  return fetchJson<{ ok: true }>('/cache/purge', {
    method: 'POST',
    body: JSON.stringify({ tenant_id: tenantId, fingerprint }),
  })
}

export function warmupCache(body: WarmupCacheRequest): Promise<{ ok: true }> {
  return fetchJson<{ ok: true }>('/cache/warmup', {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export const adminApi = {
  debugCache,
  drainNode,
  getAdminMetrics,
  listAuditEvents,
  listNodes,
  listAlerts,
  listTenants,
  listVendors,
  purgeCacheObject,
  warmupCache,
}
