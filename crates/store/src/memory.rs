use mooncache_common::StoreCapacitySnapshot;
use std::collections::HashMap;

use crate::{checksum, ChunkHandle, Store, StoreError, StoreResult};

#[derive(Debug, Clone)]
struct ChunkMeta {
    len: usize,
    checksum: Option<checksum::ChunkChecksum>,
}

#[derive(Debug, Clone)]
pub struct MemoryStore {
    arena: Vec<u8>,
    next_offset: usize,
    allocations: HashMap<usize, ChunkMeta>,
}

impl MemoryStore {
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            arena: vec![0; capacity],
            next_offset: 0,
            allocations: HashMap::new(),
        }
    }

    pub fn allocate(&mut self, len: usize) -> StoreResult<ChunkHandle> {
        if len == 0 {
            return Err(StoreError::EmptyChunk);
        }

        let end = self.next_offset.checked_add(len).ok_or({
            StoreError::InsufficientCapacity {
                requested: len,
                available: self.available_capacity(),
            }
        })?;
        if end > self.arena.len() {
            return Err(StoreError::InsufficientCapacity {
                requested: len,
                available: self.available_capacity(),
            });
        }

        let handle = ChunkHandle::new(self.next_offset, len);
        self.allocations.insert(
            handle.offset(),
            ChunkMeta {
                len,
                checksum: None,
            },
        );
        self.next_offset = end;
        Ok(handle)
    }

    pub fn write_preallocated_chunk(
        &mut self,
        handle: &ChunkHandle,
        bytes: &[u8],
    ) -> StoreResult<()> {
        self.validate_preallocated_handle(handle, bytes.len())?;
        self.allocations.insert(
            handle.offset(),
            ChunkMeta {
                len: handle.len(),
                checksum: None,
            },
        );
        let end = chunk_end(handle)?;
        self.next_offset = self.next_offset.max(end);
        self.write_chunk(handle, bytes)
    }

    pub fn write_chunk(&mut self, handle: &ChunkHandle, bytes: &[u8]) -> StoreResult<()> {
        self.validate_allocated_handle(handle)?;
        if bytes.len() != handle.len() {
            return Err(StoreError::LengthMismatch {
                expected: handle.len(),
                actual: bytes.len(),
            });
        }

        let end = chunk_end(handle)?;
        let destination = self
            .arena
            .get_mut(handle.offset()..end)
            .ok_or_else(|| invalid_handle(handle))?;
        destination.copy_from_slice(bytes);

        let checksum = checksum::sha256(bytes);
        if let Some(meta) = self.allocations.get_mut(&handle.offset()) {
            meta.checksum = Some(checksum);
        }
        Ok(())
    }

    pub fn read_chunk(&self, handle: &ChunkHandle) -> StoreResult<Vec<u8>> {
        let meta = self.validate_allocated_handle(handle)?;
        let expected_checksum = meta.checksum.ok_or(StoreError::UnwrittenChunk)?;
        let end = chunk_end(handle)?;
        let bytes = self
            .arena
            .get(handle.offset()..end)
            .ok_or_else(|| invalid_handle(handle))?;

        if checksum::sha256(bytes) != expected_checksum {
            return Err(StoreError::ChecksumMismatch {
                offset: offset_as_u64(handle),
            });
        }

        Ok(bytes.to_vec())
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.arena.len()
    }

    #[must_use]
    pub fn available_capacity(&self) -> usize {
        self.arena.len().saturating_sub(self.next_offset)
    }

    #[must_use]
    pub fn capacity_snapshot(&self) -> StoreCapacitySnapshot {
        StoreCapacitySnapshot {
            dram_bytes_used: usize_to_u64(self.next_offset),
            dram_bytes_capacity: usize_to_u64(self.arena.len()),
            ssd_bytes_used: 0,
            ssd_bytes_capacity: 0,
        }
    }

    fn validate_allocated_handle(&self, handle: &ChunkHandle) -> StoreResult<&ChunkMeta> {
        let meta = self
            .allocations
            .get(&handle.offset())
            .ok_or_else(|| invalid_handle(handle))?;
        if meta.len != handle.len() {
            return Err(invalid_handle(handle));
        }
        let end = chunk_end(handle)?;
        if end > self.arena.len() {
            return Err(invalid_handle(handle));
        }
        Ok(meta)
    }

    fn validate_preallocated_handle(
        &self,
        handle: &ChunkHandle,
        write_len: usize,
    ) -> StoreResult<()> {
        if handle.is_empty() {
            return Err(StoreError::EmptyChunk);
        }
        if write_len != handle.len() {
            return Err(StoreError::LengthMismatch {
                expected: handle.len(),
                actual: write_len,
            });
        }
        let end = chunk_end(handle)?;
        if end > self.arena.len() {
            return Err(StoreError::InsufficientCapacity {
                requested: handle.len(),
                available: self.arena.len().saturating_sub(handle.offset()),
            });
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn corrupt_for_test(&mut self, handle: &ChunkHandle, byte: u8) {
        if let Some(slot) = self.arena.get_mut(handle.offset()) {
            *slot = byte;
        }
    }
}

impl Store for MemoryStore {
    fn write_chunk(&mut self, handle: &ChunkHandle, bytes: &[u8]) -> StoreResult<()> {
        Self::write_chunk(self, handle, bytes)
    }

    fn read_chunk(&self, handle: &ChunkHandle) -> StoreResult<Vec<u8>> {
        Self::read_chunk(self, handle)
    }
}

fn chunk_end(handle: &ChunkHandle) -> StoreResult<usize> {
    handle
        .offset()
        .checked_add(handle.len())
        .ok_or_else(|| invalid_handle(handle))
}

fn invalid_handle(handle: &ChunkHandle) -> StoreError {
    StoreError::InvalidHandle {
        offset: offset_as_u64(handle),
        len: len_as_u64(handle),
    }
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn offset_as_u64(handle: &ChunkHandle) -> u64 {
    u64::try_from(handle.offset()).unwrap_or(u64::MAX)
}

fn len_as_u64(handle: &ChunkHandle) -> u64 {
    u64::try_from(handle.len()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_exact_bytes_written_to_chunk() {
        let mut store = MemoryStore::with_capacity(1024);
        let handle = store.allocate(5).unwrap();
        store.write_chunk(&handle, b"hello").unwrap();
        assert_eq!(store.read_chunk(&handle).unwrap(), b"hello");
    }

    #[test]
    fn writes_exact_preallocated_handle() {
        let mut store = MemoryStore::with_capacity(1024);
        let _offset_allocator = store.allocate(8).unwrap();
        let handle = ChunkHandle::new(16, 5);

        store.write_preallocated_chunk(&handle, b"hello").unwrap();

        assert_eq!(store.read_chunk(&handle).unwrap(), b"hello");
    }

    #[test]
    fn checksum_detects_corruption() {
        let mut store = MemoryStore::with_capacity(1024);
        let handle = store.allocate(5).unwrap();
        store.write_chunk(&handle, b"hello").unwrap();
        store.corrupt_for_test(&handle, b'H');
        let err = store.read_chunk(&handle).unwrap_err();
        assert!(err.to_string().contains("checksum"));
    }

    #[test]
    fn capacity_snapshot_tracks_dram_used_and_capacity() {
        let mut store = MemoryStore::with_capacity(128);
        let handle = store.allocate(5).unwrap();
        store.write_chunk(&handle, b"hello").unwrap();

        let snapshot = store.capacity_snapshot();
        assert_eq!(snapshot.dram_bytes_used, 5);
        assert_eq!(snapshot.dram_bytes_capacity, 128);
        assert_eq!(snapshot.ssd_bytes_used, 0);
        assert_eq!(snapshot.ssd_bytes_capacity, 0);
    }
}
