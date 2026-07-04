import { useState } from 'react'
import type { TenantView } from '../api/types'
import { canPerform, type Role } from '../auth/rbac'
import { ConfirmationModal } from '../components/ConfirmationModal'

interface TenantsProps {
  role: Role
  tenants: TenantView[]
}

const numberFormatter = new Intl.NumberFormat('en-US')
const currencyFormatter = new Intl.NumberFormat('en-US', {
  currency: 'USD',
  maximumFractionDigits: 0,
  style: 'currency',
})
const byteFormatter = new Intl.NumberFormat('en-US', {
  maximumFractionDigits: 1,
  style: 'unit',
  unit: 'gigabyte',
})

export function Tenants({ role, tenants }: TenantsProps) {
  const [pendingPolicyTenant, setPendingPolicyTenant] = useState<TenantView | null>(null)
  const [status, setStatus] = useState<string | null>(null)
  const canEditPolicy = canPerform(role, 'edit-tenant-policy')

  const confirmPolicyChange = () => {
    if (!pendingPolicyTenant) {
      return
    }

    setStatus(`Policy update queued for tenant ${pendingPolicyTenant.tenantId}.`)
    setPendingPolicyTenant(null)
  }

  return (
    <section className="page-stack" aria-labelledby="tenants-heading">
      <div>
        <p className="eyebrow">Tenants</p>
        <h2 id="tenants-heading">Tenants</h2>
        <p className="page-intro">
          Tenant quotas, API key references, rate limits, streaming limits, spend budgets, and cache defaults.
        </p>
      </div>

      {!canEditPolicy ? <p className="notice">Only admins can change tenant policies.</p> : null}
      {status ? <p className="success" role="status">{status}</p> : null}

      <section className="section-card" aria-labelledby="tenant-table-heading">
        <h2 id="tenant-table-heading">Tenant policy table</h2>
        <table>
          <caption>Tenant quotas and cache policy defaults</caption>
          <thead>
            <tr>
              <th scope="col">Tenant</th>
              <th scope="col">API key reference</th>
              <th scope="col">DRAM quota</th>
              <th scope="col">SSD quota</th>
              <th scope="col">Rate limit</th>
              <th scope="col">Streams</th>
              <th scope="col">Spend budget</th>
              <th scope="col">Default TTL</th>
              <th scope="col">Policy</th>
              <th scope="col">Actions</th>
            </tr>
          </thead>
          <tbody>
            {tenants.map((tenant) => (
              <tr key={tenant.tenantId}>
                <th scope="row">{tenant.tenantId}</th>
                <td>{tenant.apiKeyRef}</td>
                <td>{byteFormatter.format(tenant.dramQuotaBytes / 1_000_000_000)}</td>
                <td>{byteFormatter.format(tenant.ssdQuotaBytes / 1_000_000_000)}</td>
                <td>{numberFormatter.format(tenant.requestRateLimitPerMinute)}/min</td>
                <td>{numberFormatter.format(tenant.streamConcurrencyLimit)}</td>
                <td>{currencyFormatter.format(tenant.vendorSpendBudgetUsd)}</td>
                <td>{numberFormatter.format(tenant.defaultTtlSeconds)} s</td>
                <td>{tenant.policy}</td>
                <td>
                  {canEditPolicy ? (
                    <button className="button" type="button" onClick={() => setPendingPolicyTenant(tenant)}>
                      Change policy for {tenant.tenantId}
                    </button>
                  ) : (
                    <span>No policy actions</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <ConfirmationModal
        isOpen={pendingPolicyTenant !== null}
        title={`Change tenant policy for ${pendingPolicyTenant?.tenantId ?? ''}`}
        message="Apply the staged tenant policy change and write an audit event?"
        confirmLabel="Confirm policy change"
        cancelLabel="Cancel policy change"
        onConfirm={confirmPolicyChange}
        onClose={() => setPendingPolicyTenant(null)}
      />
    </section>
  )
}
