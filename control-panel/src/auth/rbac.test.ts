import { describe, expect, it } from 'vitest'
import { canPerform } from './rbac'

describe('control-panel RBAC', () => {
  it('allows operators to drain nodes but blocks viewers', () => {
    expect(canPerform('operator', 'drain-node')).toBe(true)
    expect(canPerform('viewer', 'drain-node')).toBe(false)
  })

  it('allows only admins to edit tenant policy', () => {
    expect(canPerform('admin', 'edit-tenant-policy')).toBe(true)
    expect(canPerform('operator', 'edit-tenant-policy')).toBe(false)
  })
})
