use std::collections::HashMap;

use mooncache_common::{CacheError, CacheKey, CacheResult, MasterMetricsSnapshot, TenantId};

use crate::{
    allocator::SegmentAllocator,
    eviction::{candidate_class, EvictionCandidateClass},
    lease::{now_ms, LeaseTracker, ReplicaList},
    object::{CacheObjectMeta, ObjectStatus, ReplicaDescriptor},
    quota::TenantQuota,
};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MasterMetadataSnapshot {
    pub objects: Vec<CacheObjectMeta>,
    pub quotas: Vec<(TenantId, TenantQuota)>,
    pub evictions_total: u64,
}

#[derive(Debug, Default)]
pub struct MasterState {
    objects: HashMap<(TenantId, CacheKey), CacheObjectMeta>,
    quotas: HashMap<TenantId, TenantQuota>,
    leases: LeaseTracker,
    allocator: SegmentAllocator,
    next_lease_sequence: u64,
    evictions_total: u64,
}

impl MasterState {
    pub fn new_for_test() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn metadata_snapshot(&self) -> MasterMetadataSnapshot {
        MasterMetadataSnapshot {
            objects: self.objects.values().cloned().collect(),
            quotas: self
                .quotas
                .iter()
                .map(|(tenant_id, quota)| (tenant_id.clone(), quota.clone()))
                .collect(),
            evictions_total: self.evictions_total,
        }
    }

    #[must_use]
    pub fn from_metadata_snapshot(snapshot: MasterMetadataSnapshot) -> Self {
        let objects = snapshot
            .objects
            .into_iter()
            .map(|object| (object_key(&object.tenant_id, &object.cache_key), object))
            .collect();
        let quotas = snapshot.quotas.into_iter().collect();

        Self {
            objects,
            quotas,
            leases: LeaseTracker::default(),
            allocator: SegmentAllocator::default(),
            next_lease_sequence: 0,
            evictions_total: snapshot.evictions_total,
        }
    }

    pub fn mount_segment(&mut self, node_id: impl Into<String>, len: u64) {
        self.allocator.mount_segment(node_id, len);
    }

    #[must_use]
    pub fn observability_snapshot(&self) -> MasterMetricsSnapshot {
        MasterMetricsSnapshot {
            objects_total: usize_to_u64(self.objects.len()),
            evictions_total: self.evictions_total,
        }
    }

    pub fn set_tenant_quota(
        &mut self,
        tenant_id: impl Into<String>,
        dram_bytes: u64,
        ssd_bytes: u64,
    ) -> CacheResult<()> {
        let tenant_id = TenantId::parse(tenant_id)?;
        let used_dram_bytes = self
            .objects
            .values()
            .filter(|object| object.tenant_id == tenant_id)
            .fold(0_u64, |used, object| {
                used.saturating_add(quota_reserved_bytes(object))
            });

        self.quotas.insert(
            tenant_id,
            TenantQuota::with_usage(dram_bytes, ssd_bytes, used_dram_bytes),
        );
        Ok(())
    }

    pub fn put_start(
        &mut self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
        len: u64,
        replica_count: usize,
    ) -> CacheResult<Vec<ReplicaDescriptor>> {
        let object_key = object_key(tenant_id, cache_key);
        if self.objects.contains_key(&object_key) {
            return Err(CacheError::Conflict("object already exists".into()));
        }

        self.reserve_tenant_quota(tenant_id, len)?;

        let allocations = match self.allocator.allocate(len, replica_count) {
            Ok(allocations) => allocations,
            Err(err) => {
                self.release_tenant_quota(tenant_id, len);
                return Err(err);
            }
        };

        let replicas: Vec<_> = allocations
            .into_iter()
            .map(|allocation| ReplicaDescriptor {
                node_id: allocation.node_id,
                offset: allocation.offset,
                len: allocation.len,
            })
            .collect();

        self.objects.insert(
            object_key,
            CacheObjectMeta {
                tenant_id: tenant_id.clone(),
                cache_key: cache_key.clone(),
                len,
                status: ObjectStatus::Reserving,
                replicas: replicas.clone(),
                hard_pinned: false,
                soft_pinned_until_ms: None,
            },
        );

        Ok(replicas)
    }

    pub fn put_end(&mut self, tenant_id: &TenantId, cache_key: &CacheKey) -> CacheResult<()> {
        let object = self.object_mut(tenant_id, cache_key)?;
        match object.status {
            ObjectStatus::Reserving | ObjectStatus::Writing => {
                object.status = ObjectStatus::Committed;
                Ok(())
            }
            ObjectStatus::Committed => Err(CacheError::Conflict("object already committed".into())),
            ObjectStatus::Revoked | ObjectStatus::Evicted => {
                Err(CacheError::Conflict("object is not writable".into()))
            }
        }
    }

    pub fn put_revoke(&mut self, tenant_id: &TenantId, cache_key: &CacheKey) -> CacheResult<()> {
        let released_len = {
            let object = self.object_mut(tenant_id, cache_key)?;
            let released_len = quota_reserved_bytes(object);
            object.status = ObjectStatus::Revoked;
            released_len
        };
        self.release_tenant_quota(tenant_id, released_len);
        self.leases.remove(tenant_id, cache_key);
        Ok(())
    }

    pub fn get_replica_list(
        &mut self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
    ) -> CacheResult<ReplicaList> {
        let replicas = {
            let object = self.object(tenant_id, cache_key)?;
            if object.status != ObjectStatus::Committed {
                return Err(CacheError::NotFound);
            }
            object.replicas.clone()
        };

        self.next_lease_sequence = self.next_lease_sequence.saturating_add(1);
        let lease = self
            .leases
            .refresh(tenant_id, cache_key, self.next_lease_sequence);
        Ok(ReplicaList { replicas, lease })
    }

    pub fn remove(&mut self, tenant_id: &TenantId, cache_key: &CacheKey) -> CacheResult<()> {
        let object = self
            .objects
            .remove(&object_key(tenant_id, cache_key))
            .ok_or(CacheError::NotFound)?;
        self.release_tenant_quota(tenant_id, quota_reserved_bytes(&object));
        self.leases.remove(tenant_id, cache_key);
        Ok(())
    }

    pub fn evict_for_tenant(
        &mut self,
        tenant_id: &TenantId,
        target_reclaim_bytes: u64,
    ) -> CacheResult<u64> {
        let now_ms = now_ms();
        let mut normal_candidates = Vec::new();
        let mut soft_pinned_candidates = Vec::new();

        for ((object_tenant_id, cache_key), object) in &self.objects {
            if object_tenant_id != tenant_id {
                continue;
            }

            let has_active_lease =
                self.leases
                    .has_active_lease(object_tenant_id, cache_key, now_ms);
            match candidate_class(object, has_active_lease, now_ms) {
                Some(EvictionCandidateClass::Normal) => {
                    normal_candidates.push(cache_key.clone());
                }
                Some(EvictionCandidateClass::SoftPinned) => {
                    soft_pinned_candidates.push(cache_key.clone());
                }
                None => {}
            }
        }

        let mut reclaimed =
            self.evict_candidate_keys(tenant_id, target_reclaim_bytes, &normal_candidates);
        if reclaimed < target_reclaim_bytes {
            reclaimed = reclaimed.saturating_add(self.evict_candidate_keys(
                tenant_id,
                target_reclaim_bytes - reclaimed,
                &soft_pinned_candidates,
            ));
        }

        Ok(reclaimed)
    }

    fn evict_candidate_keys(
        &mut self,
        tenant_id: &TenantId,
        target_reclaim_bytes: u64,
        cache_keys: &[CacheKey],
    ) -> u64 {
        let mut reclaimed = 0_u64;
        for cache_key in cache_keys {
            if reclaimed >= target_reclaim_bytes {
                break;
            }

            let Some(released_len) =
                self.objects
                    .get_mut(&object_key(tenant_id, cache_key))
                    .map(|object| {
                        let released_len = quota_reserved_bytes(object);
                        object.status = ObjectStatus::Evicted;
                        released_len
                    })
            else {
                continue;
            };
            self.release_tenant_quota(tenant_id, released_len);
            self.leases.remove(tenant_id, cache_key);
            self.evictions_total = self.evictions_total.saturating_add(1);
            reclaimed = reclaimed.saturating_add(released_len);
        }
        reclaimed
    }

    fn reserve_tenant_quota(&mut self, tenant_id: &TenantId, bytes: u64) -> CacheResult<()> {
        if let Some(quota) = self.quotas.get_mut(tenant_id) {
            quota.reserve_dram(bytes)?;
        }
        Ok(())
    }

    fn release_tenant_quota(&mut self, tenant_id: &TenantId, bytes: u64) {
        if let Some(quota) = self.quotas.get_mut(tenant_id) {
            quota.release_dram(bytes);
        }
    }

    fn object(&self, tenant_id: &TenantId, cache_key: &CacheKey) -> CacheResult<&CacheObjectMeta> {
        self.objects
            .get(&object_key(tenant_id, cache_key))
            .ok_or(CacheError::NotFound)
    }

    fn object_mut(
        &mut self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
    ) -> CacheResult<&mut CacheObjectMeta> {
        self.objects
            .get_mut(&object_key(tenant_id, cache_key))
            .ok_or(CacheError::NotFound)
    }
}

fn quota_reserved_bytes(object: &CacheObjectMeta) -> u64 {
    if matches!(
        object.status,
        ObjectStatus::Reserving | ObjectStatus::Writing | ObjectStatus::Committed
    ) {
        object.len
    } else {
        0
    }
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn object_key(tenant_id: &TenantId, cache_key: &CacheKey) -> (TenantId, CacheKey) {
    (tenant_id.clone(), cache_key.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mooncache_common::{CacheError, CacheKey, TenantId};

    fn tenant() -> TenantId {
        TenantId::parse("tenant-a").unwrap()
    }

    fn key() -> CacheKey {
        CacheKey::from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .unwrap()
    }

    fn second_key() -> CacheKey {
        CacheKey::from_hex("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
            .unwrap()
    }

    #[test]
    fn get_does_not_return_reserved_object_before_commit() {
        let mut state = MasterState::new_for_test();
        state.mount_segment("node-a", 1024 * 1024);
        state.put_start(&tenant(), &key(), 4096, 1).unwrap();
        assert!(state.get_replica_list(&tenant(), &key()).is_err());
    }

    #[test]
    fn put_start_returns_reserved_replicas() {
        let mut state = MasterState::new_for_test();
        state.mount_segment("node-a", 1024 * 1024);

        let replicas = state.put_start(&tenant(), &key(), 4096, 1).unwrap();

        assert_eq!(
            replicas,
            vec![ReplicaDescriptor {
                node_id: "node-a".to_owned(),
                offset: 0,
                len: 4096,
            }]
        );
    }

    #[test]
    fn committed_object_is_readable_with_lease() {
        let mut state = MasterState::new_for_test();
        state.mount_segment("node-a", 1024 * 1024);
        state.put_start(&tenant(), &key(), 4096, 1).unwrap();
        state.put_end(&tenant(), &key()).unwrap();
        let replicas = state.get_replica_list(&tenant(), &key()).unwrap();
        assert_eq!(replicas.replicas.len(), 1);
        assert!(replicas.lease.expires_at_ms > 0);
    }

    #[test]
    fn tenant_quota_blocks_write_when_eviction_cannot_reclaim() {
        let mut state = MasterState::new_for_test();
        state.set_tenant_quota("tenant-a", 4096, 0).unwrap();
        state.mount_segment("node-a", 8192);
        let first =
            CacheKey::from_hex("1111111111111111111111111111111111111111111111111111111111111111")
                .unwrap();
        let second =
            CacheKey::from_hex("2222222222222222222222222222222222222222222222222222222222222222")
                .unwrap();
        state.put_start(&tenant(), &first, 4096, 1).unwrap();
        state.put_end(&tenant(), &first).unwrap();

        let err = state.put_start(&tenant(), &second, 4096, 1).unwrap_err();

        assert!(err.to_string().contains("quota exceeded"));
    }

    #[test]
    fn set_tenant_quota_rejects_empty_tenant_id() {
        let mut state = MasterState::new_for_test();

        let err = state.set_tenant_quota("", 4096, 0).unwrap_err();

        assert_eq!(err, CacheError::EmptyTenantId);
    }

    #[test]
    fn quota_reservation_is_released_when_allocation_fails() {
        let mut state = MasterState::new_for_test();
        state.set_tenant_quota("tenant-a", 4096, 0).unwrap();

        let err = state.put_start(&tenant(), &key(), 4096, 1).unwrap_err();
        assert!(err
            .to_string()
            .contains("insufficient mounted segment capacity"));

        state.mount_segment("node-a", 4096);
        state.put_start(&tenant(), &key(), 4096, 1).unwrap();
    }

    #[test]
    fn soft_pinned_committed_objects_are_eviction_fallbacks() {
        let mut normal_only_state = state_with_normal_and_soft_pinned_committed_objects();

        let reclaimed = normal_only_state.evict_for_tenant(&tenant(), 4096).unwrap();

        assert_eq!(reclaimed, 4096);
        assert_eq!(
            normal_only_state.object(&tenant(), &key()).unwrap().status,
            ObjectStatus::Evicted
        );
        assert_eq!(
            normal_only_state
                .object(&tenant(), &second_key())
                .unwrap()
                .status,
            ObjectStatus::Committed
        );

        let mut fallback_state = state_with_normal_and_soft_pinned_committed_objects();

        let reclaimed = fallback_state.evict_for_tenant(&tenant(), 8192).unwrap();

        assert_eq!(reclaimed, 8192);
        assert_eq!(
            fallback_state.object(&tenant(), &key()).unwrap().status,
            ObjectStatus::Evicted
        );
        assert_eq!(
            fallback_state
                .object(&tenant(), &second_key())
                .unwrap()
                .status,
            ObjectStatus::Evicted
        );
    }

    fn state_with_normal_and_soft_pinned_committed_objects() -> MasterState {
        let mut state = MasterState::new_for_test();
        state.mount_segment("node-a", 16_384);
        state.put_start(&tenant(), &key(), 4096, 1).unwrap();
        state.put_end(&tenant(), &key()).unwrap();
        state.put_start(&tenant(), &second_key(), 4096, 1).unwrap();
        state.put_end(&tenant(), &second_key()).unwrap();
        state
            .object_mut(&tenant(), &second_key())
            .unwrap()
            .soft_pinned_until_ms = Some(now_ms().saturating_add(60_000));
        state
    }

    #[test]
    fn eviction_skips_active_lease() {
        let mut state = MasterState::new_for_test();
        state.mount_segment("node-a", 8192);
        state.put_start(&tenant(), &key(), 4096, 1).unwrap();
        state.put_end(&tenant(), &key()).unwrap();
        let _lease = state.get_replica_list(&tenant(), &key()).unwrap().lease;

        let reclaimed = state.evict_for_tenant(&tenant(), 4096).unwrap();

        assert_eq!(reclaimed, 0);
    }

    #[test]
    fn evict_for_tenant_releases_quota_for_reclaimed_object() {
        let mut state = MasterState::new_for_test();
        state.set_tenant_quota("tenant-a", 4096, 0).unwrap();
        state.mount_segment("node-a", 8192);
        state.put_start(&tenant(), &key(), 4096, 1).unwrap();
        state.put_end(&tenant(), &key()).unwrap();

        let reclaimed = state.evict_for_tenant(&tenant(), 4096).unwrap();

        assert_eq!(reclaimed, 4096);
        state.put_start(&tenant(), &second_key(), 4096, 1).unwrap();
    }

    #[test]
    fn revoke_releases_reserved_quota() {
        let mut state = MasterState::new_for_test();
        state.set_tenant_quota("tenant-a", 4096, 0).unwrap();
        state.mount_segment("node-a", 8192);
        state.put_start(&tenant(), &key(), 4096, 1).unwrap();

        state.put_revoke(&tenant(), &key()).unwrap();

        state.put_start(&tenant(), &second_key(), 4096, 1).unwrap();
    }

    #[test]
    fn remove_releases_reserved_quota() {
        let mut state = MasterState::new_for_test();
        state.set_tenant_quota("tenant-a", 4096, 0).unwrap();
        state.mount_segment("node-a", 8192);
        state.put_start(&tenant(), &key(), 4096, 1).unwrap();

        state.remove(&tenant(), &key()).unwrap();

        state.put_start(&tenant(), &second_key(), 4096, 1).unwrap();
    }

    #[test]
    fn revoked_object_is_not_readable() {
        let mut state = MasterState::new_for_test();
        state.mount_segment("node-a", 1024 * 1024);
        state.put_start(&tenant(), &key(), 4096, 1).unwrap();
        state.put_revoke(&tenant(), &key()).unwrap();

        assert!(state.get_replica_list(&tenant(), &key()).is_err());
    }

    #[test]
    fn removed_object_is_not_readable() {
        let mut state = MasterState::new_for_test();
        state.mount_segment("node-a", 1024 * 1024);
        state.put_start(&tenant(), &key(), 4096, 1).unwrap();
        state.put_end(&tenant(), &key()).unwrap();
        state.remove(&tenant(), &key()).unwrap();

        assert!(state.get_replica_list(&tenant(), &key()).is_err());
    }

    #[test]
    fn observability_snapshot_tracks_objects_and_evictions() {
        let mut state = MasterState::new_for_test();
        state.mount_segment("node-a", 1024 * 1024);
        state.put_start(&tenant(), &key(), 4096, 1).unwrap();
        state.put_end(&tenant(), &key()).unwrap();

        assert_eq!(state.observability_snapshot().objects_total, 1);
        assert_eq!(state.evict_for_tenant(&tenant(), 4096).unwrap(), 4096);
        let snapshot = state.observability_snapshot();
        assert_eq!(snapshot.objects_total, 1);
        assert_eq!(snapshot.evictions_total, 1);
    }

    #[test]
    fn snapshot_roundtrip_restores_committed_metadata_after_failover() {
        let mut leader = MasterState::new_for_test();
        leader.set_tenant_quota("tenant-a", 4096, 0).unwrap();
        leader.mount_segment("node-a", 8192);
        leader.put_start(&tenant(), &key(), 4096, 1).unwrap();
        leader.put_end(&tenant(), &key()).unwrap();

        let snapshot = leader.metadata_snapshot();
        let mut follower = MasterState::from_metadata_snapshot(snapshot);

        let replicas = follower.get_replica_list(&tenant(), &key()).unwrap();
        assert_eq!(replicas.replicas.len(), 1);
        assert_eq!(replicas.replicas[0].node_id, "node-a");

        let err = follower
            .put_start(&tenant(), &second_key(), 1, 1)
            .unwrap_err();
        assert!(err.to_string().contains("quota exceeded"));
    }
}
