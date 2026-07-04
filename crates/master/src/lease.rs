use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use mooncache_common::{CacheKey, TenantId};

use crate::object::ReplicaDescriptor;

pub(crate) const DEFAULT_LEASE_TTL_MS: u64 = 5_000;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Lease {
    pub lease_id: String,
    pub expires_at_ms: u64,
}

impl Lease {
    pub(crate) fn new(sequence: u64) -> Self {
        Self::new_at(sequence, now_ms())
    }

    fn new_at(sequence: u64, now_ms: u64) -> Self {
        Self {
            lease_id: format!("lease-{sequence}"),
            expires_at_ms: now_ms.saturating_add(DEFAULT_LEASE_TTL_MS),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReplicaList {
    pub replicas: Vec<ReplicaDescriptor>,
    pub lease: Lease,
}

#[derive(Debug, Default)]
pub(crate) struct LeaseTracker {
    leases: HashMap<(TenantId, CacheKey), Lease>,
}

impl LeaseTracker {
    pub fn refresh(&mut self, tenant_id: &TenantId, cache_key: &CacheKey, sequence: u64) -> Lease {
        let lease = Lease::new(sequence);
        self.leases
            .insert((tenant_id.clone(), cache_key.clone()), lease.clone());
        lease
    }

    pub fn has_active_lease(
        &self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
        now_ms: u64,
    ) -> bool {
        self.leases
            .get(&(tenant_id.clone(), cache_key.clone()))
            .is_some_and(|lease| lease.expires_at_ms > now_ms)
    }

    pub fn remove(&mut self, tenant_id: &TenantId, cache_key: &CacheKey) {
        self.leases.remove(&(tenant_id.clone(), cache_key.clone()));
    }
}

pub(crate) fn now_ms() -> u64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant() -> TenantId {
        TenantId::parse("tenant-a").unwrap()
    }

    fn key() -> CacheKey {
        CacheKey::from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .unwrap()
    }

    #[test]
    fn lease_default_ttl_is_five_seconds() {
        let lease = Lease::new_at(1, 1000);

        assert_eq!(lease.expires_at_ms, 6000);
    }

    #[test]
    fn refreshing_lease_tracks_active_key() {
        let mut tracker = LeaseTracker::default();
        let lease = tracker.refresh(&tenant(), &key(), 1);

        assert!(tracker.has_active_lease(&tenant(), &key(), lease.expires_at_ms - 1));
        assert!(!tracker.has_active_lease(&tenant(), &key(), lease.expires_at_ms));
    }
}
