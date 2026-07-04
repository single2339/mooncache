use mooncache_common::{CacheError, CacheResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentAllocation {
    pub node_id: String,
    pub offset: u64,
    pub len: u64,
}

#[derive(Debug, Default)]
pub(crate) struct SegmentAllocator {
    segments: Vec<MountedSegment>,
}

impl SegmentAllocator {
    pub fn mount_segment(&mut self, node_id: impl Into<String>, len: u64) {
        self.segments.push(MountedSegment {
            node_id: node_id.into(),
            len,
            next_offset: 0,
        });
    }

    pub fn allocate(
        &mut self,
        len: u64,
        replica_count: usize,
    ) -> CacheResult<Vec<SegmentAllocation>> {
        if len == 0 {
            return Err(CacheError::Conflict(
                "object length must be greater than zero".into(),
            ));
        }
        if replica_count == 0 {
            return Err(CacheError::Conflict(
                "replica count must be greater than zero".into(),
            ));
        }

        let segment_indexes: Vec<_> = self
            .segments
            .iter()
            .enumerate()
            .filter_map(|(index, segment)| segment.can_fit(len).then_some(index))
            .take(replica_count)
            .collect();

        if segment_indexes.len() != replica_count {
            return Err(CacheError::Conflict(
                "insufficient mounted segment capacity".into(),
            ));
        }

        let mut allocations = Vec::with_capacity(replica_count);
        for index in segment_indexes {
            let segment = &mut self.segments[index];
            let offset = segment.next_offset;
            segment.next_offset = segment.next_offset.saturating_add(len);
            allocations.push(SegmentAllocation {
                node_id: segment.node_id.clone(),
                offset,
                len,
            });
        }

        Ok(allocations)
    }
}

#[derive(Debug)]
struct MountedSegment {
    node_id: String,
    len: u64,
    next_offset: u64,
}

impl MountedSegment {
    fn can_fit(&self, object_len: u64) -> bool {
        self.next_offset
            .checked_add(object_len)
            .is_some_and(|next_offset| next_offset <= self.len)
    }
}
