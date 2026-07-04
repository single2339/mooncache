import { useEffect, useState } from 'react'

import type { AlertView } from '../api/types'

interface AlertsProps {
  alerts: AlertView[]
}

const dateFormatter = new Intl.DateTimeFormat('en-US', {
  dateStyle: 'medium',
  timeStyle: 'short',
  timeZone: 'UTC',
})
const pageSize = 10


export function Alerts({ alerts }: AlertsProps) {
  const activeAlerts = alerts.filter((alert) => alert.status === 'active')
  const historicalAlerts = alerts.filter((alert) => alert.status !== 'active')

  return (
    <section className="page-stack" aria-labelledby="alerts-heading">
      <div>
        <p className="eyebrow">Alerts</p>
        <h2 id="alerts-heading">Alerts</h2>
        <p className="page-intro">Active alerts, alert history, and operator-facing threshold context.</p>
      </div>

      <section className="section-card" aria-labelledby="active-alerts-heading">
        <h2 id="active-alerts-heading">Active alerts</h2>
        {activeAlerts.length === 0 ? <p>No active alerts.</p> : <AlertTable alerts={activeAlerts} caption="Active alerts" />}
      </section>

      <section className="section-card" aria-labelledby="alert-history-heading">
        <h2 id="alert-history-heading">Alert history</h2>
        {historicalAlerts.length === 0 ? (
          <p>No resolved, acknowledged, or silenced alerts.</p>
        ) : (
          <AlertTable alerts={historicalAlerts} caption="Alert history" />
        )}
      </section>
    </section>
  )
}

function AlertTable({ alerts, caption }: { alerts: AlertView[]; caption: string }) {
  const [page, setPage] = useState(0)
  useEffect(() => setPage(0), [alerts])

  const pageCount = Math.max(1, Math.ceil(alerts.length / pageSize))
  const safePage = Math.min(page, pageCount - 1)
  const visibleAlerts = alerts.slice(safePage * pageSize, safePage * pageSize + pageSize)
  const label = caption.toLowerCase()
  const goToPreviousPage = () => setPage((currentPage) => Math.max(0, currentPage - 1))
  const goToNextPage = () => setPage((currentPage) => Math.min(pageCount - 1, currentPage + 1))

  return (
    <>
    <table>
      <caption>{caption}</caption>
      <thead>
        <tr>
          <th scope="col">Severity</th>
          <th scope="col">Status</th>
          <th scope="col">Message</th>
          <th scope="col">Resource</th>
          <th scope="col">Started</th>
          <th scope="col">Last seen</th>
        </tr>
      </thead>
      <tbody>
        {visibleAlerts.map((alert) => (
          <tr key={alert.id}>
            <td>{alert.severity}</td>
            <td>{alert.status}</td>
            <td>{alert.message}</td>
            <td>{alert.resource}</td>
            <td>{dateFormatter.format(new Date(alert.startedAtMs))}</td>
            <td>{dateFormatter.format(new Date(alert.lastSeenAtMs))}</td>
          </tr>
        ))}
      </tbody>
    </table>
      {pageCount > 1 ? (
        <nav className="pagination" aria-label={`${caption} pagination`}>
          <button type="button" onClick={goToPreviousPage} disabled={safePage === 0}>
            Previous {label}
          </button>
          <span>
            Page {safePage + 1} of {pageCount}
          </span>
          <button type="button" onClick={goToNextPage} disabled={safePage === pageCount - 1}>
            Next {label}
          </button>
        </nav>
      ) : null}
    </>
  )
}
