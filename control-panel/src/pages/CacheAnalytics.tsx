import type { CacheAnalyticsData } from '../api/types'

interface CacheAnalyticsProps {
  data: CacheAnalyticsData
}

const numberFormatter = new Intl.NumberFormat('en-US')
const percentFormatter = new Intl.NumberFormat('en-US', {
  maximumFractionDigits: 0,
  style: 'percent',
})
const byteFormatter = new Intl.NumberFormat('en-US', {
  maximumFractionDigits: 1,
  style: 'unit',
  unit: 'gigabyte',
})

export function CacheAnalytics({ data }: CacheAnalyticsProps) {
  const topTenant = [...data.tenantUsage].sort((left, right) => {
    const storageDelta = right.storageBytes - left.storageBytes
    return storageDelta === 0 ? right.requests - left.requests : storageDelta
  })[0]
  const leadingEviction = [...data.evictionReasons].sort((left, right) => right.count - left.count)[0]

  return (
    <section className="page-stack" aria-labelledby="cache-analytics-heading">
      <div>
        <p className="eyebrow">Cache Analytics</p>
        <h2 id="cache-analytics-heading">Cache Analytics</h2>
        <p className="page-intro">
          Hit quality, writeback failures, hot keys, storage pressure, and singleflight collapse.
        </p>
      </div>

      <section className="metric-grid" aria-label="Cache outcome breakdown">
        <article className="metric-card">
          <h2>Hits</h2>
          <p className="metric-value">{numberFormatter.format(data.outcomes.hit)}</p>
          <p>{numberFormatter.format(data.outcomes.hit)} hits served from cache.</p>
        </article>
        <article className="metric-card">
          <h2>Misses and bypasses</h2>
          <p className="metric-value">{numberFormatter.format(data.outcomes.miss)}</p>
          <p>
            {numberFormatter.format(data.outcomes.miss)} misses,{' '}
            {numberFormatter.format(data.outcomes.bypass)} bypassed, and{' '}
            {numberFormatter.format(data.outcomes.ineligible)} ineligible requests.
          </p>
        </article>
        <article className="metric-card">
          <h2>Writeback failures</h2>
          <p className="metric-value">{numberFormatter.format(data.outcomes.writebackFailed)}</p>
          <p>{numberFormatter.format(data.outcomes.writebackFailed)} writeback failures need review.</p>
        </article>
      </section>

      <section className="section-card" aria-labelledby="tier-heading">
        <h2 id="tier-heading">Where is latency or pressure coming from?</h2>
        <p>
          DRAM serves {percentFormatter.format(data.tierHitRatio.dram)} of hits; SSD serves{' '}
          {percentFormatter.format(data.tierHitRatio.ssd)}.
        </p>
        {leadingEviction ? (
          <p className="callout">{leadingEviction.reason} evictions are the leading pressure source.</p>
        ) : null}
      </section>

      <section className="section-card" aria-labelledby="hot-keys-heading">
        <h2 id="hot-keys-heading">Top hot keys</h2>
        <table>
          <caption>Hashed fingerprints with the most cache hits</caption>
          <thead>
            <tr>
              <th scope="col">Fingerprint</th>
              <th scope="col">Tenant</th>
              <th scope="col">Model</th>
              <th scope="col">Hits</th>
            </tr>
          </thead>
          <tbody>
            {data.hotKeys.map((key) => (
              <tr key={key.fingerprint}>
                <td>{key.fingerprint}</td>
                <td>{key.tenantId}</td>
                <td>{key.model}</td>
                <td>{numberFormatter.format(key.hits)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section className="section-card" aria-labelledby="tenant-usage-heading">
        <h2 id="tenant-usage-heading">Top tenants by storage and traffic</h2>
        {topTenant ? <p className="callout">{topTenant.tenantId} is the top tenant by storage and traffic.</p> : null}
        <table>
          <caption>Tenant storage and request volume</caption>
          <thead>
            <tr>
              <th scope="col">Tenant</th>
              <th scope="col">Storage</th>
              <th scope="col">Requests</th>
            </tr>
          </thead>
          <tbody>
            {data.tenantUsage.map((tenant) => (
              <tr key={tenant.tenantId}>
                <td>{tenant.tenantId}</td>
                <td>{byteFormatter.format(tenant.storageBytes / 1_000_000_000)}</td>
                <td>{numberFormatter.format(tenant.requests)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section className="section-card" aria-labelledby="singleflight-heading">
        <h2 id="singleflight-heading">Singleflight collapse</h2>
        <p>
          {numberFormatter.format(data.singleflight.waiters)} waiters collapsed behind{' '}
          {numberFormatter.format(data.singleflight.leaders)} leaders.
        </p>
      </section>
    </section>
  )
}
