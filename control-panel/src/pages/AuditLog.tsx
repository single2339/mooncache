import type { AuditEvent, AuditResult } from '../api/types'

interface AuditLogProps {
  events: AuditEvent[]
}

const dateFormatter = new Intl.DateTimeFormat('en-US', {
  dateStyle: 'medium',
  timeStyle: 'short',
  timeZone: 'UTC',
})

export function AuditLog({ events }: AuditLogProps) {
  return (
    <section className="page-stack" aria-labelledby="audit-log-heading">
      <div>
        <p className="eyebrow">Audit Log</p>
        <h2 id="audit-log-heading">Audit Log</h2>
        <p className="page-intro">
          Write action history and security-sensitive reads with actor, tenant scope, result, and request ID.
        </p>
      </div>

      <section className="section-card" aria-labelledby="audit-events-heading">
        <h2 id="audit-events-heading">Audit events</h2>
        <table>
          <caption>Recent administrative audit events</caption>
          <thead>
            <tr>
              <th scope="col">Time</th>
              <th scope="col">Actor</th>
              <th scope="col">Role</th>
              <th scope="col">Action</th>
              <th scope="col">Resource</th>
              <th scope="col">Tenant</th>
              <th scope="col">Result</th>
              <th scope="col">Request ID</th>
            </tr>
          </thead>
          <tbody>
            {events.map((event) => (
              <tr key={event.request_id}>
                <td>{dateFormatter.format(new Date(event.timestamp_ms))}</td>
                <td>{event.actor}</td>
                <td>{event.role}</td>
                <td>{event.action}</td>
                <td>{event.resource}</td>
                <td>{event.tenant_scope ?? 'All tenants'}</td>
                <td>{formatResult(event.result)}</td>
                <td>{event.request_id}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </section>
  )
}

function formatResult(result: AuditResult): string {
  if (typeof result === 'string') {
    return result
  }

  return `Failed: ${result.Failed}`
}
