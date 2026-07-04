use crate::object::{CacheObjectMeta, ObjectStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EvictionCandidateClass {
    Normal,
    SoftPinned,
}

pub(crate) fn candidate_class(
    object: &CacheObjectMeta,
    has_active_lease: bool,
    now_ms: u64,
) -> Option<EvictionCandidateClass> {
    if object.status != ObjectStatus::Committed || has_active_lease || object.hard_pinned {
        return None;
    }

    if object
        .soft_pinned_until_ms
        .is_some_and(|expires_at_ms| expires_at_ms > now_ms)
    {
        return Some(EvictionCandidateClass::SoftPinned);
    }

    Some(EvictionCandidateClass::Normal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mooncache_common::{CacheKey, TenantId};

    fn object_with_status(status: ObjectStatus) -> CacheObjectMeta {
        CacheObjectMeta {
            tenant_id: TenantId::parse("tenant-a").unwrap(),
            cache_key: CacheKey::from_hex(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .unwrap(),
            len: 4096,
            status,
            replicas: Vec::new(),
            hard_pinned: false,
            soft_pinned_until_ms: None,
        }
    }

    #[test]
    fn candidate_filter_rejects_incomplete_writes() {
        let object = object_with_status(ObjectStatus::Writing);

        assert_eq!(candidate_class(&object, false, 1000), None);
    }

    #[test]
    fn candidate_filter_rejects_active_leases() {
        let object = object_with_status(ObjectStatus::Committed);

        assert_eq!(candidate_class(&object, true, 1000), None);
    }

    #[test]
    fn candidate_filter_rejects_hard_pins() {
        let mut object = object_with_status(ObjectStatus::Committed);
        object.hard_pinned = true;

        assert_eq!(candidate_class(&object, false, 1000), None);
    }

    #[test]
    fn candidate_filter_marks_active_soft_pin_as_fallback() {
        let mut object = object_with_status(ObjectStatus::Committed);
        object.soft_pinned_until_ms = Some(2000);

        assert_eq!(
            candidate_class(&object, false, 1000),
            Some(EvictionCandidateClass::SoftPinned)
        );
    }
}
