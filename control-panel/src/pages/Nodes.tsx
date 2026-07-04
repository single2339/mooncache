import { useState } from 'react'
import { getUserVisibleError } from '../api/client'
import type { NodeView } from '../api/types'
import { canPerform, type Role } from '../auth/rbac'
import { ConfirmationModal } from '../components/ConfirmationModal'

interface NodesProps {
  role: Role
  nodes: NodeView[]
  onDrainNode?: (nodeId: string) => Promise<void> | void
}

const numberFormatter = new Intl.NumberFormat('en-US')

export function Nodes({ role, nodes, onDrainNode }: NodesProps) {
  const [pendingDrainNode, setPendingDrainNode] = useState<NodeView | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [status, setStatus] = useState<string | null>(null)
  const canDrain = canPerform(role, 'drain-node')

  const confirmDrain = async () => {
    if (!pendingDrainNode) {
      return
    }

    try {
      await onDrainNode?.(pendingDrainNode.node_id)
      setStatus(`Drain requested for ${pendingDrainNode.node_id}.`)
      setError(null)
      setPendingDrainNode(null)
    } catch (err) {
      setPendingDrainNode(null)
      setError(getUserVisibleError(err))
    }
  }

  return (
    <section className="page-stack" aria-labelledby="nodes-heading">
      <div>
        <p className="eyebrow">Nodes</p>
        <h2 id="nodes-heading">Nodes</h2>
        <p className="page-intro">Store node capacity, drain status, placement health, and heartbeat age.</p>
      </div>

      {!canDrain ? (
        <p className="notice">Viewer role can inspect nodes but cannot drain them.</p>
      ) : null}
      {error ? <p className="error" role="alert">{error}</p> : null}
      {status ? <p className="success" role="status">{status}</p> : null}

      <section className="section-card" aria-labelledby="nodes-table-heading">
        <h2 id="nodes-table-heading">Store nodes</h2>
        <table>
          <caption>Node capacity and drain status</caption>
          <thead>
            <tr>
              <th scope="col">Node</th>
              <th scope="col">Address</th>
              <th scope="col">State</th>
              <th scope="col">DRAM</th>
              <th scope="col">SSD</th>
              <th scope="col">Segments</th>
              <th scope="col">Replicas</th>
              <th scope="col">Heartbeat</th>
              <th scope="col">Actions</th>
            </tr>
          </thead>
          <tbody>
            {nodes.map((node) => (
              <tr key={node.node_id}>
                <th scope="row">{node.node_id}</th>
                <td>{node.address ?? 'Unknown'}</td>
                <td>{node.draining ? 'Draining' : node.state ?? 'Ready'}</td>
                <td>{formatCapacity(node.dram_bytes_used, node.dram_bytes_capacity)}</td>
                <td>{formatCapacity(node.ssd_bytes_used, node.ssd_bytes_capacity)}</td>
                <td>{formatOptionalNumber(node.segments)}</td>
                <td>{formatOptionalNumber(node.replicas)}</td>
                <td>{formatHeartbeat(node.heartbeat_age_ms)}</td>
                <td>
                  {canDrain && !node.draining ? (
                    <button className="button danger" type="button" onClick={() => setPendingDrainNode(node)}>
                      Drain node {node.node_id}
                    </button>
                  ) : (
                    <span>{node.draining ? 'Already draining' : 'No actions available'}</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <ConfirmationModal
        isOpen={pendingDrainNode !== null}
        title={`Drain node ${pendingDrainNode?.node_id ?? ''}`}
        message={`Drain ${pendingDrainNode?.node_id ?? 'this node'} and move traffic to healthy replicas?`}
        confirmLabel="Confirm drain"
        cancelLabel="Cancel drain"
        onConfirm={confirmDrain}
        onClose={() => setPendingDrainNode(null)}
      />
    </section>
  )
}

function formatCapacity(used?: number, capacity?: number): string {
  if (used === undefined || capacity === undefined || capacity === 0) {
    return 'Unknown'
  }

  return `${numberFormatter.format(used)} / ${numberFormatter.format(capacity)} (${Math.round((used / capacity) * 100)}%)`
}

function formatOptionalNumber(value?: number): string {
  return value === undefined ? 'Unknown' : numberFormatter.format(value)
}

function formatHeartbeat(value?: number): string {
  return value === undefined ? 'Unknown' : `${numberFormatter.format(value)} ms ago`
}
