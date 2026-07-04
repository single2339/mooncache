use thiserror::Error;

pub type CacheResult<T> = Result<T, CacheError>;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CacheError {
    #[error("tenant id must not be empty")]
    EmptyTenantId,
    #[error("cache key must be 64 lowercase hex characters")]
    InvalidCacheKey,
    #[error("invalid id: {0}")]
    InvalidId(String),
    #[error("not found")]
    NotFound,
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("quota exceeded: {0}")]
    QuotaExceeded(String),
    #[error("upstream unavailable: {0}")]
    UpstreamUnavailable(String),
}
