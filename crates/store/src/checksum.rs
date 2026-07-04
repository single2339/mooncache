use sha2::{Digest, Sha256};

pub type ChunkChecksum = [u8; 32];

#[must_use]
pub fn sha256(bytes: &[u8]) -> ChunkChecksum {
    Sha256::digest(bytes).into()
}
