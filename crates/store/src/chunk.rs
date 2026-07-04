use mooncache_master::ReplicaDescriptor;

use crate::{StoreError, StoreResult};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChunkHandle {
    offset: usize,
    len: usize,
}

impl ChunkHandle {
    #[must_use]
    pub fn new(offset: usize, len: usize) -> Self {
        Self { offset, len }
    }

    #[must_use]
    pub fn offset(&self) -> usize {
        self.offset
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn from_replica(replica: &ReplicaDescriptor) -> StoreResult<Self> {
        let offset = usize::try_from(replica.offset).map_err(|_| StoreError::InvalidHandle {
            offset: replica.offset,
            len: replica.len,
        })?;
        let len = usize::try_from(replica.len).map_err(|_| StoreError::InvalidHandle {
            offset: replica.offset,
            len: replica.len,
        })?;
        Ok(Self { offset, len })
    }
}
