use mooncache_common::{CacheKey, TenantId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CacheObjectRef {
    pub tenant_id: TenantId,
    pub cache_key: CacheKey,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AdminError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("invalid admin request: {0}")]
    InvalidRequest(String),
    #[error("cache object not found")]
    CacheObjectNotFound(CacheObjectRef),
}

#[cfg(test)]
mod tests {
    use super::*;
    use mooncache_common::{CacheKey, TenantId};

    #[test]
    fn cache_object_ref_uses_common_tenant_and_key_types() {
        let tenant_id = TenantId::parse("tenant-a").unwrap();
        let cache_key =
            CacheKey::from_hex("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
                .unwrap();

        let object = CacheObjectRef {
            tenant_id,
            cache_key,
        };

        assert_eq!(object.tenant_id.as_str(), "tenant-a");
        assert_eq!(object.cache_key.redacted(), "01234567…cdef");
    }

    #[test]
    fn admin_error_formats_invalid_request_message() {
        let err = AdminError::InvalidRequest("missing tenant id".to_owned());

        assert_eq!(err.to_string(), "invalid admin request: missing tenant id");
    }
}
