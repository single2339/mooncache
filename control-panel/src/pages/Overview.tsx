import type { OverviewData } from '../api/types'

interface OverviewProps {
  data: OverviewData
}

const numberFormatter = new Intl.NumberFormat('en-US')
const currencyFormatter = new Intl.NumberFormat('en-US', {
  currency: 'USD',
  maximumFractionDigits: 0,
  style: 'currency',
})
const percentFormatter = new Intl.NumberFormat('en-US', {
  maximumFractionDigits: 0,
  style: 'percent',
})

export function Overview({ data }: OverviewProps) {
  const healthLabel = data.health[0].toUpperCase() + data.health.slice(1)
  const isImproving = data.cacheHitRate >= 0.8 && data.vendorCallsAvoided > 0
  const alertsLabel = data.activeAlerts.length === 1 ? '1 active alert' : `${data.activeAlerts.length} active alerts`

  return (
    <section className="page-stack" aria-labelledby="overview-heading">
      <div>
        <p className="eyebrow">Overview</p>
        <h2 id="overview-heading">Overview</h2>
        <p className="page-intro">
          System status, cache value, latency pressure, and the tenants or nodes that need action.
        </p>
      </div>

      <section className="metric-grid" aria-label="System health and value">
        <article className="metric-card">
          <span className={`status-dot ${data.health}`} aria-hidden="true" />
          <h2>System health {healthLabel}</h2>
          <p className="metric-value">{healthLabel}</p>
          <p>{alertsLabel} are currently open.</p>
        </article>
        <article className="metric-card">
          <h2>Request rate</h2>
          <p className="metric-value">{numberFormatter.format(data.requestRatePerSecond)}/s</p>
          <p>Current traffic through the cache gateway.</p>
        </article>
        <article className="metric-card">
          <h2>Cache value</h2>
          <p className="metric-value">{percentFormatter.format(data.cacheHitRate)}</p>
          <p>
            {isImproving ? 'Cache value is improving' : 'Cache value needs attention'} with{' '}
            {percentFormatter.format(data.cacheHitRate)} hit rate,{' '}
            {numberFormatter.format(data.vendorCallsAvoided)} vendor calls avoided, and{' '}
            {currencyFormatter.format(data.estimatedCostSavedUsd)} saved.
          </p>
        </article>
      </section>

      <section className="section-card" aria-labelledby="latency-heading">
        <h2 id="latency-heading">Latency and pressure</h2>
        <div className="two-column">
          <div>
            <h3>Hit latency</h3>
            <p>
              p50 {data.hitLatencyMs.p50} ms, p95 {data.hitLatencyMs.p95} ms, p99{' '}
              {data.hitLatencyMs.p99} ms.
            </p>
          </div>
          <div>
            <h3>Miss overhead</h3>
            <p>
              p50 {data.missOverheadMs.p50} ms, p95 {data.missOverheadMs.p95} ms, p99{' '}
              {data.missOverheadMs.p99} ms.
            </p>
            <p className="callout">
              Latency pressure comes from miss p95 at {data.missOverheadMs.p95} ms.
            </p>
          </div>
        </div>
      </section>

      <section className="section-card" aria-labelledby="action-heading">
        <h2 id="action-heading">Which tenants or nodes need action?</h2>
        <ul className="plain-list">
          {data.actionItems.map((item) => (
            <li key={item}>{item}</li>
          ))}
        </ul>
      </section>
    </section>
  )
}
