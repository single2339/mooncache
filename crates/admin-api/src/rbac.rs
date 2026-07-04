use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    NoAccess,
    Viewer,
    Operator,
    Admin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdminAction {
    ReadMetrics,
    ReadNodes,
    ReadAuditLog,
    DebugCacheFingerprint,
    DrainNode,
    RemoveCacheObject,
    WarmupCache,
    PatchTenantPolicy,
    PatchVendorPolicy,
    ManageUsers,
}

impl Role {
    #[must_use]
    pub fn allows(self, action: AdminAction) -> bool {
        match self {
            Role::NoAccess => false,
            Role::Viewer => matches!(
                action,
                AdminAction::ReadMetrics
                    | AdminAction::ReadNodes
                    | AdminAction::ReadAuditLog
                    | AdminAction::DebugCacheFingerprint
            ),
            Role::Operator => matches!(
                action,
                AdminAction::ReadMetrics
                    | AdminAction::ReadNodes
                    | AdminAction::ReadAuditLog
                    | AdminAction::DebugCacheFingerprint
                    | AdminAction::DrainNode
                    | AdminAction::RemoveCacheObject
                    | AdminAction::WarmupCache
            ),
            Role::Admin => true,
        }
    }
}
