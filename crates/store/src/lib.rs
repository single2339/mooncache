pub mod checksum;
pub mod chunk;
pub mod memory;
pub mod ssd;

pub use chunk::ChunkHandle;
pub use memory::MemoryStore;
pub use ssd::{SsdError, SsdResult, SsdStore};

use thiserror::Error;

pub type StoreResult<T> = Result<T, StoreError>;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum StoreError {
    #[error("chunk length must be greater than zero")]
    EmptyChunk,
    #[error(
        "insufficient DRAM capacity: requested {requested} bytes, available {available} bytes"
    )]
    InsufficientCapacity { requested: usize, available: usize },
    #[error("invalid chunk handle: offset {offset}, len {len}")]
    InvalidHandle { offset: u64, len: u64 },
    #[error("chunk write length mismatch: expected {expected} bytes, got {actual} bytes")]
    LengthMismatch { expected: usize, actual: usize },
    #[error("chunk has not been written")]
    UnwrittenChunk,
    #[error("checksum mismatch for chunk at offset {offset}")]
    ChecksumMismatch { offset: u64 },
}

pub trait Store {
    fn write_chunk(&mut self, handle: &ChunkHandle, bytes: &[u8]) -> StoreResult<()>;
    fn read_chunk(&self, handle: &ChunkHandle) -> StoreResult<Vec<u8>>;
}
