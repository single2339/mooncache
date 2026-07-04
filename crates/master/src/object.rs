use mooncache_common::{CacheKey, TenantId};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ObjectStatus {
    Reserving,
    Writing,
    Committed,
    Revoked,
    Evicted,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReplicaDescriptor {
    pub node_id: String,
    pub offset: u64,
    pub len: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheObjectMeta {
    pub tenant_id: TenantId,
    pub cache_key: CacheKey,
    pub len: u64,
    pub status: ObjectStatus,
    pub replicas: Vec<ReplicaDescriptor>,
    pub hard_pinned: bool,
    pub soft_pinned_until_ms: Option<u64>,
}
