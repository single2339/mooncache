import { useEffect, useMemo, useState } from 'react'
import { adminApi, getUserVisibleError } from './api/client'
import type {
  AdminMetricsSnapshot,
  AlertView,
  AuditEvent,
  CacheAnalyticsData,
  CacheObjectView,
  NodeView,
  OverviewData,
  TenantView,
  VendorView,
} from './api/types'
import type { ControlPanelPage } from './components/Layout'
import { Layout } from './components/Layout'
import type { Role } from './auth/rbac'
import { Alerts } from './pages/Alerts'
import { AuditLog } from './pages/AuditLog'
import { CacheAnalytics } from './pages/CacheAnalytics'
import { CacheOperations } from './pages/CacheOperations'
import { Nodes } from './pages/Nodes'
import { Overview } from './pages/Overview'
import { Tenants } from './pages/Tenants'
import { Vendors } from './pages/Vendors'

const pages = [
  {
    id: 'overview',
    title: 'Overview',
    description: 'Health, request volume, cache value, and pressure signals.',
  },
  {
    id: 'cache-analytics',
    title: 'Cache Analytics',
    description: 'Hit rate, avoided vendor calls, writeback outcomes, and latency trends.',
  },
  {
    id: 'nodes',
    title: 'Nodes',
    description: 'Store node capacity, drain status, and placement health.',
  },
  {
    id: 'tenants',
    title: 'Tenants',
    description: 'Tenant quotas, policies, and cache-control defaults.',
  },
  {
    id: 'vendors',
    title: 'Vendors',
    description: 'Vendor routing, retry posture, and upstream availability.',
  },
  {
    id: 'cache-operations',
    title: 'Cache Operations',
    description: 'Debug, inspect, purge, and warm up cache objects.',
  },
  {
    id: 'alerts',
    title: 'Alerts',
    description: 'Active alerts, history, and threshold context.',
  },
  {
    id: 'audit-log',
    title: 'Audit Log',
    description: 'Security-sensitive reads and write operations with request IDs.',
  },
] satisfies ControlPanelPage[]

interface ControlPanelData {
  metrics: AdminMetricsSnapshot
  auditEvents: AuditEvent[]
  nodes: NodeView[]
  tenants: TenantView[]
  vendors: VendorView[]
  alerts: AlertView[]
  cacheObjects: CacheObjectView[]
}

export function App() {
  const [activePageId, setActivePageId] = useState(() => getHashPageId())
  const [role, setRole] = useState<Role>('operator')
  const [data, setData] = useState<ControlPanelData | null>(null)
  const [error, setError] = useState<string | null>(null)
  const activePage = pages.find((page) => page.id === activePageId) ?? pages[0]

  useEffect(() => {
    const handleHashChange = () => setActivePageId(getHashPageId())

    window.addEventListener('hashchange', handleHashChange)
    return () => window.removeEventListener('hashchange', handleHashChange)
  }, [])

  useEffect(() => {
    let cancelled = false

    async function loadControlPanelData() {
      try {
        const [metrics, auditEvents, nodes, tenants, vendors, alerts] = await Promise.all([
          adminApi.getAdminMetrics(),
          adminApi.listAuditEvents(),
          adminApi.listNodes(),
          adminApi.listTenants(),
          adminApi.listVendors(),
          adminApi.listAlerts(),
        ])

        if (!cancelled) {
          setData({ metrics, auditEvents, nodes, tenants, vendors, alerts, cacheObjects: [] })
          setError(null)
        }
      } catch (err) {
        if (!cancelled) {
          setError(getUserVisibleError(err))
        }
      }
    }

    void loadControlPanelData()
    return () => {
      cancelled = true
    }
  }, [])

  const overviewData = useMemo(() => (data ? buildOverviewData(data) : null), [data])
  const cacheAnalyticsData = useMemo(() => (data ? buildCacheAnalyticsData(data) : null), [data])

  const handlePageSelect = (pageId: string) => {
    window.history.replaceState(null, '', `#${pageId}`)
    setActivePageId(pageId)
  }

  return (
    <Layout pages={pages} activePageId={activePage.id} onSelectPage={handlePageSelect}>
      <section className="hero" aria-labelledby="page-title">
        <p className="eyebrow">Operations console</p>
        <h1 id="page-title">Mooncache Control Panel</h1>
        <p>{activePage.description}</p>
        <label className="role-picker">
          Demo role
          <select value={role} onChange={(event) => setRole(event.target.value as Role)}>
            <option value="viewer">Viewer</option>
            <option value="operator">Operator</option>
            <option value="admin">Admin</option>
          </select>
        </label>
      </section>

      {error ? <p role="alert">{error}</p> : null}
      {!data || !overviewData || !cacheAnalyticsData ? (
        <p role="status">Loading control panel data…</p>
      ) : (
        renderPage(activePage.id, role, data, overviewData, cacheAnalyticsData)
      )}
    </Layout>
  )
}

function getHashPageId(): string {
  const hashPageId = window.location.hash.replace(/^#/, '') || 'overview'
  return pages.some((page) => page.id === hashPageId) ? hashPageId : 'overview'
}

function renderPage(
  pageId: string,
  role: Role,
  data: ControlPanelData,
  overviewData: OverviewData,
  cacheAnalyticsData: CacheAnalyticsData,
) {
  switch (pageId) {
    case 'cache-analytics':
      return <CacheAnalytics data={cacheAnalyticsData} />
    case 'nodes':
      return <Nodes role={role} nodes={data.nodes} onDrainNode={(nodeId) => adminApi.drainNode(nodeId).then(() => undefined)} />
    case 'tenants':
      return <Tenants role={role} tenants={data.tenants} />
    case 'vendors':
      return <Vendors role={role} vendors={data.vendors} />
    case 'cache-operations':
      return (
        <CacheOperations
          role={role}
          objects={data.cacheObjects}
          onDebugCache={(request) => adminApi.debugCache(request)}
          onPurgeObject={(tenantId, fingerprint) =>
            adminApi.purgeCacheObject(tenantId, fingerprint).then(() => undefined)
          }
          onWarmupCache={(tenantId, requestBody) =>
            adminApi.warmupCache({ tenant_id: tenantId, request_body: requestBody }).then(() => undefined)
          }
        />
      )
    case 'alerts':
      return <Alerts alerts={data.alerts} />
    case 'audit-log':
      return <AuditLog events={data.auditEvents} />
    default:
      return <Overview data={overviewData} />
  }
}

function buildOverviewData(data: ControlPanelData): OverviewData {
  const denied = data.metrics.audit_denied_total + data.metrics.audit_failed_total
  const activeAlerts = data.alerts.filter((alert) => alert.status === 'active')
  const drainingNodes = data.nodes.filter((node) => node.draining)
  const health = activeAlerts.some((alert) => alert.severity === 'critical')
    ? 'critical'
    : denied > 0 || activeAlerts.length > 0 || drainingNodes.length > 0
      ? 'degraded'
      : 'healthy'

  const actionItems = [
    ...drainingNodes.map((node) => `Node ${node.node_id} is draining.`),
    ...activeAlerts.map((alert) => alert.message),
  ]

  return {
    health,
    requestRatePerSecond: 0,
    cacheHitRate: 0,
    vendorCallsAvoided: 0,
    estimatedCostSavedUsd: 0,
    hitLatencyMs: { p50: 0, p95: 0, p99: 0 },
    missOverheadMs: { p50: 0, p95: 0, p99: 0 },
    activeAlerts,
    actionItems: actionItems.length === 0 ? ['No active operational actions.'] : actionItems,
  }
}

function buildCacheAnalyticsData(data: ControlPanelData): CacheAnalyticsData {
  const tenantUsage = data.tenants.map((tenant) => ({
    tenantId: tenant.tenantId,
    storageBytes: 0,
    requests: 0,
  }))

  return {
    outcomes: {
      hit: 0,
      miss: 0,
      bypass: 0,
      ineligible: 0,
      writebackFailed: data.metrics.audit_failed_total,
    },
    hotKeys: [],
    tenantUsage,
    tierHitRatio: { dram: 0, ssd: 0 },
    evictionReasons: [],
    singleflight: { leaders: 0, waiters: 0 },
  }
}
