import { useState } from 'react'
import type { VendorView } from '../api/types'
import { canPerform, type Role } from '../auth/rbac'
import { ConfirmationModal } from '../components/ConfirmationModal'

interface VendorsProps {
  role: Role
  vendors: VendorView[]
}

const numberFormatter = new Intl.NumberFormat('en-US')
const percentFormatter = new Intl.NumberFormat('en-US', {
  maximumFractionDigits: 1,
  style: 'percent',
})
const currencyFormatter = new Intl.NumberFormat('en-US', {
  currency: 'USD',
  maximumFractionDigits: 2,
  style: 'currency',
})

export function Vendors({ role, vendors }: VendorsProps) {
  const [pendingVendor, setPendingVendor] = useState<VendorView | null>(null)
  const [status, setStatus] = useState<string | null>(null)
  const canEditPolicy = canPerform(role, 'edit-vendor-policy')

  const confirmPolicyChange = () => {
    if (!pendingVendor) {
      return
    }

    setStatus(`Routing policy update queued for ${pendingVendor.vendorId}.`)
    setPendingVendor(null)
  }

  return (
    <section className="page-stack" aria-labelledby="vendors-heading">
      <div>
        <p className="eyebrow">Vendors</p>
        <h2 id="vendors-heading">Vendors</h2>
        <p className="page-intro">
          Vendor adapter health, model versions, rate limits, retries, cost metadata, and routing policy.
        </p>
      </div>

      {!canEditPolicy ? <p className="notice">Only admins can change vendor routing policies.</p> : null}
      {status ? <p className="success" role="status">{status}</p> : null}

      <section className="section-card" aria-labelledby="vendors-table-heading">
        <h2 id="vendors-table-heading">Vendor adapters</h2>
        <table>
          <caption>Vendor health and routing posture</caption>
          <thead>
            <tr>
              <th scope="col">Vendor</th>
              <th scope="col">Health</th>
              <th scope="col">Models</th>
              <th scope="col">Resolved version</th>
              <th scope="col">Rate limit remaining</th>
              <th scope="col">Error rate</th>
              <th scope="col">Retries</th>
              <th scope="col">Cost</th>
              <th scope="col">Routing policy</th>
              <th scope="col">Actions</th>
            </tr>
          </thead>
          <tbody>
            {vendors.map((vendor) => (
              <tr key={vendor.vendorId}>
                <th scope="row">{vendor.vendorId}</th>
                <td>{vendor.health}</td>
                <td>{vendor.models.join(', ')}</td>
                <td>{vendor.resolvedVersion}</td>
                <td>{numberFormatter.format(vendor.rateLimitRemaining)}</td>
                <td>{percentFormatter.format(vendor.errorRate)}</td>
                <td>{numberFormatter.format(vendor.retryCount)}</td>
                <td>{currencyFormatter.format(vendor.costPerMillionTokensUsd)}/M tokens</td>
                <td>{vendor.routingPolicy}</td>
                <td>
                  {canEditPolicy ? (
                    <button className="button" type="button" onClick={() => setPendingVendor(vendor)}>
                      Change vendor policy for {vendor.vendorId}
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
        isOpen={pendingVendor !== null}
        title={`Change vendor policy for ${pendingVendor?.vendorId ?? ''}`}
        message="Apply the staged vendor routing policy change and write an audit event?"
        confirmLabel="Confirm vendor policy"
        cancelLabel="Cancel vendor policy"
        onConfirm={confirmPolicyChange}
        onClose={() => setPendingVendor(null)}
      />
    </section>
  )
}
