import { useEffect, useState } from 'react'
import { adminApi } from './api/client'
import {
  alerts,
  auditEvents,
  cacheAnalyticsData,
  cacheObjects,
  nodes,
  overviewData,
  tenants,
  vendors,
} from './api/mockData'
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

export function App() {
  const [activePageId, setActivePageId] = useState(() => getHashPageId())
  const [role, setRole] = useState<Role>('operator')
  const activePage = pages.find((page) => page.id === activePageId) ?? pages[0]

  useEffect(() => {
    const handleHashChange = () => setActivePageId(getHashPageId())

    window.addEventListener('hashchange', handleHashChange)
    return () => window.removeEventListener('hashchange', handleHashChange)
  }, [])

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

      {renderPage(activePage.id, role)}
    </Layout>
  )
}

function getHashPageId(): string {
  const hashPageId = window.location.hash.replace(/^#/, '') || 'overview'
  return pages.some((page) => page.id === hashPageId) ? hashPageId : 'overview'
}

function renderPage(pageId: string, role: Role) {
  switch (pageId) {
    case 'cache-analytics':
      return <CacheAnalytics data={cacheAnalyticsData} />
    case 'nodes':
      return <Nodes role={role} nodes={nodes} onDrainNode={(nodeId) => adminApi.drainNode(nodeId).then(() => undefined)} />
    case 'tenants':
      return <Tenants role={role} tenants={tenants} />
    case 'vendors':
      return <Vendors role={role} vendors={vendors} />
    case 'cache-operations':
      return (
        <CacheOperations
          role={role}
          objects={cacheObjects}
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
      return <Alerts alerts={alerts} />
    case 'audit-log':
      return <AuditLog events={auditEvents} />
    default:
      return <Overview data={overviewData} />
  }
}
