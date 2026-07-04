import { type FormEvent, useState } from 'react'
import { getUserVisibleError } from '../api/client'
import type { CacheDebugRequest, CacheDebugResponse, CacheObjectView } from '../api/types'
import { canPerform, type Role } from '../auth/rbac'
import { ConfirmationModal } from '../components/ConfirmationModal'

interface CacheOperationsProps {
  role: Role
  objects: CacheObjectView[]
  onDebugCache?: (request: CacheDebugRequest) => Promise<CacheDebugResponse> | CacheDebugResponse
  onPurgeObject?: (tenantId: string, fingerprint: string) => Promise<void> | void
  onWarmupCache?: (tenantId: string, requestBody: unknown) => Promise<void> | void
}

const numberFormatter = new Intl.NumberFormat('en-US')
const defaultDebugFields = {
  endpoint_version: 'responses.v1',
  vendor_id: 'default',
  resolved_model_version: 'default',
  adapter_version: 'control-panel.v1',
  cache_policy: 'default',
}

export function CacheOperations({ role, objects, onDebugCache, onPurgeObject, onWarmupCache }: CacheOperationsProps) {
  const [pendingPurge, setPendingPurge] = useState<CacheObjectView | null>(null)
  const [debugTenant, setDebugTenant] = useState(objects[0]?.tenantId ?? '')
  const [debugPayload, setDebugPayload] = useState('{"messages":[{"role":"user","content":"hello"}]}')
  const [debugResult, setDebugResult] = useState<CacheDebugResponse | null>(null)
  const [isDebugging, setIsDebugging] = useState(false)
  const [warmupTenant, setWarmupTenant] = useState(objects[0]?.tenantId ?? '')
  const [warmupPayload, setWarmupPayload] = useState('{"messages":[{"role":"user","content":"hello"}]}')
  const [confirmWarmup, setConfirmWarmup] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [status, setStatus] = useState<string | null>(null)
  const canPurge = canPerform(role, 'remove-cache-object')
  const canWarmup = canPerform(role, 'warmup-cache')

  const confirmPurge = async () => {
    if (!pendingPurge) {
      return
    }

    try {
      await onPurgeObject?.(pendingPurge.tenantId, pendingPurge.fingerprint)
      setStatus(`Purge requested for ${pendingPurge.fingerprint}.`)
      setError(null)
      setPendingPurge(null)
    } catch (err) {
      setPendingPurge(null)
      setError(getUserVisibleError(err))
    }
  }

  const submitDebugRequest = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setDebugResult(null)

    let requestBody: unknown
    try {
      requestBody = JSON.parse(debugPayload)
    } catch {
      setStatus(null)
      setError('Debug request body must be valid JSON.')
      return
    }

    try {
      setIsDebugging(true)
      const result = await onDebugCache?.({
        tenant_id: debugTenant,
        ...defaultDebugFields,
        body: requestBody,
      })
      if (result) {
        setDebugResult(result)
        setStatus(null)
      } else {
        setStatus(`Debug request submitted for tenant ${debugTenant}.`)
      }
      setError(null)
    } catch (err) {
      setStatus(null)
      setError(getUserVisibleError(err))
    } finally {
      setIsDebugging(false)
    }
  }

  const confirmWarmupRequest = async () => {
    try {
      await onWarmupCache?.(warmupTenant, JSON.parse(warmupPayload))
      setStatus(`Warmup requested for tenant ${warmupTenant}.`)
      setError(null)
      setConfirmWarmup(false)
    } catch (err) {
      setConfirmWarmup(false)
      setError(err instanceof SyntaxError ? 'Warmup payload must be valid JSON.' : getUserVisibleError(err))
    }
  }

  const debugReason = debugResult?.reason ? getUserVisibleError(new Error(debugResult.reason)) : null

  return (
    <section className="page-stack" aria-labelledby="cache-operations-heading">
      <div>
        <p className="eyebrow">Cache Operations</p>
        <h2 id="cache-operations-heading">Cache Operations</h2>
        <p className="page-intro">
          Debug fingerprints, inspect safe metadata, purge cache objects, and trigger manual warmup.
        </p>
      </div>

      {error ? <p className="error" role="alert">{error}</p> : null}
      {status ? <p className="success" role="status">{status}</p> : null}

      <section className="section-card" aria-labelledby="debug-heading">
        <h2 id="debug-heading">Fingerprint debugger</h2>
        <form className="form-grid" onSubmit={submitDebugRequest}>
          <label>
            Tenant ID
            <input name="tenant" value={debugTenant} onChange={(event) => setDebugTenant(event.target.value)} />
          </label>
          <label>
            Request body
            <textarea
              name="request-body"
              rows={5}
              value={debugPayload}
              onChange={(event) => setDebugPayload(event.target.value)}
            />
          </label>
          <button className="button" type="submit" disabled={isDebugging}>
            {isDebugging ? 'Debugging fingerprint…' : 'Debug fingerprint'}
          </button>
          <p className="notice">Payloads are used only to derive redacted fingerprints in this mock boundary.</p>
          {debugResult ? (
            <div className="success" role="status" aria-live="polite">
              <p>Redacted cache key: {debugResult.cache_key_redacted}</p>
              <p>Eligibility: {debugResult.eligible ? 'Eligible' : 'Not eligible'}</p>
              {debugReason ? <p>Reason: {debugReason}</p> : null}
            </div>
          ) : null}
        </form>
      </section>

      <section className="section-card" aria-labelledby="objects-heading">
        <h2 id="objects-heading">Cache objects</h2>
        {!canPurge ? <p className="notice">Viewer role can inspect metadata but cannot purge cache objects.</p> : null}
        <table>
          <caption>Safe cache object metadata without sensitive payloads</caption>
          <thead>
            <tr>
              <th scope="col">Tenant</th>
              <th scope="col">Fingerprint</th>
              <th scope="col">Model</th>
              <th scope="col">Tier</th>
              <th scope="col">Size</th>
              <th scope="col">TTL remaining</th>
              <th scope="col">Pinned</th>
              <th scope="col">Actions</th>
            </tr>
          </thead>
          <tbody>
            {objects.map((object) => (
              <tr key={`${object.tenantId}-${object.fingerprint}`}>
                <td>{object.tenantId}</td>
                <td>{object.fingerprint}</td>
                <td>{object.model}</td>
                <td>{object.tier}</td>
                <td>{numberFormatter.format(object.sizeBytes)} bytes</td>
                <td>{numberFormatter.format(object.ttlSecondsRemaining)} s</td>
                <td>{object.pinned ? 'Pinned' : 'Not pinned'}</td>
                <td>
                  {canPurge ? (
                    <button className="button danger" type="button" onClick={() => setPendingPurge(object)}>
                      Purge cache object {object.fingerprint}
                    </button>
                  ) : (
                    <span>No purge actions</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section className="section-card" aria-labelledby="warmup-heading">
        <h2 id="warmup-heading">Manual warmup</h2>
        {!canWarmup ? <p className="notice">Viewer role cannot run warmup requests.</p> : null}
        <div className="form-grid">
          <label>
            Warmup tenant
            <input value={warmupTenant} onChange={(event) => setWarmupTenant(event.target.value)} />
          </label>
          <label>
            Warmup request payload
            <textarea rows={5} value={warmupPayload} onChange={(event) => setWarmupPayload(event.target.value)} />
          </label>
          {canWarmup ? (
            <button className="button" type="button" onClick={() => setConfirmWarmup(true)}>
              Warm up cache
            </button>
          ) : null}
        </div>
      </section>

      <ConfirmationModal
        isOpen={pendingPurge !== null}
        title={`Purge ${pendingPurge?.fingerprint ?? ''}`}
        message="Remove this cache object and write an audit event?"
        confirmLabel="Confirm purge"
        cancelLabel="Cancel purge"
        onConfirm={confirmPurge}
        onClose={() => setPendingPurge(null)}
      />
      <ConfirmationModal
        isOpen={confirmWarmup}
        title="Warm up cache"
        message={`Run a manual warmup request for tenant ${warmupTenant}?`}
        confirmLabel="Confirm warmup"
        cancelLabel="Cancel warmup"
        onConfirm={confirmWarmupRequest}
        onClose={() => setConfirmWarmup(false)}
      />
    </section>
  )
}
