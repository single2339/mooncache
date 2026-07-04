pub mod canonical_json;
pub mod eligibility;

pub use canonical_json::canonical_json_bytes;
pub use eligibility::{classify_request, EligibilityDecision};
use mooncache_common::{CacheError, CacheKey, CacheResult, TenantId};
use serde_json::json;
use sha2::{Digest, Sha256};

pub struct FingerprintInput<'a> {
    pub tenant_id: &'a TenantId,
    pub endpoint_version: &'a str,
    pub vendor_id: &'a str,
    pub resolved_model_version: &'a str,
    pub adapter_version: &'a str,
    pub cache_policy: &'a str,
    pub body: &'a serde_json::Value,
}

pub fn compute_cache_key(input: &FingerprintInput<'_>) -> CacheResult<CacheKey> {
    let doc = json!({
        "tenant_id": input.tenant_id.as_str(),
        "endpoint_version": input.endpoint_version,
        "vendor_id": input.vendor_id,
        "resolved_model_version": input.resolved_model_version,
        "adapter_version": input.adapter_version,
        "cache_policy": input.cache_policy,
        "body": input.body,
    });
    let bytes = canonical_json_bytes(&doc).map_err(|err| CacheError::InvalidId(err.to_string()))?;
    let digest = Sha256::digest(bytes);
    CacheKey::from_hex(format!("{digest:x}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mooncache_common::TenantId;
    use serde_json::json;

    fn fingerprint_input<'a>(
        tenant_id: &'a TenantId,
        body: &'a serde_json::Value,
    ) -> FingerprintInput<'a> {
        FingerprintInput {
            tenant_id,
            endpoint_version: "responses-v1",
            vendor_id: "openai",
            resolved_model_version: "gpt-x-2026-07-03",
            adapter_version: "adapter-v1",
            cache_policy: "default",
            body,
        }
    }

    #[test]
    fn compute_cache_key_is_stable_for_canonical_body_order() {
        let tenant_id = TenantId::parse("tenant-a").unwrap();
        let left = json!({"b": 1, "a": {"z": 2, "m": 3}});
        let right = json!({"a": {"m": 3, "z": 2}, "b": 1});

        let left_key = compute_cache_key(&fingerprint_input(&tenant_id, &left)).unwrap();
        let right_key = compute_cache_key(&fingerprint_input(&tenant_id, &right)).unwrap();

        assert_eq!(left_key, right_key);
    }

    #[test]
    fn compute_cache_key_changes_with_tenant() {
        let tenant_a = TenantId::parse("tenant-a").unwrap();
        let tenant_b = TenantId::parse("tenant-b").unwrap();
        let body = json!({"model": "gpt-x", "temperature": 0, "input": "hello"});

        let tenant_a_key = compute_cache_key(&fingerprint_input(&tenant_a, &body)).unwrap();
        let tenant_b_key = compute_cache_key(&fingerprint_input(&tenant_b, &body)).unwrap();

        assert_ne!(tenant_a_key, tenant_b_key);
    }
}
