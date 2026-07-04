export type Role = 'viewer' | 'operator' | 'admin'

export type Action =
  | 'read'
  | 'drain-node'
  | 'remove-cache-object'
  | 'warmup-cache'
  | 'edit-tenant-policy'
  | 'edit-vendor-policy'

const permissions: Record<Role, Partial<Record<Action, true>>> = {
  viewer: { read: true },
  operator: {
    read: true,
    'drain-node': true,
    'remove-cache-object': true,
    'warmup-cache': true,
  },
  admin: {
    read: true,
    'drain-node': true,
    'remove-cache-object': true,
    'warmup-cache': true,
    'edit-tenant-policy': true,
    'edit-vendor-policy': true,
  },
}

export function canPerform(role: Role, action: Action): boolean {
  return permissions[role][action] === true
}
