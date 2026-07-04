use std::{collections::BTreeMap, fs, path::Path};

use mooncache_common::TenantId;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read tenant config {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse tenant config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("invalid tenant config: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TenantCachePolicy {
    CacheFirst,
    Bypass,
    CacheOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantConfig {
    pub id: TenantId,
    pub name: String,
    pub enabled: bool,
    pub api_key_sha256: String,
    pub dram_quota_bytes: u64,
    pub ssd_quota_bytes: u64,
    pub request_rate_limit_per_minute: u32,
    pub stream_concurrency_limit: u32,
    pub vendor_spend_budget_usd: u32,
    pub default_ttl_seconds: u64,
    pub max_ttl_seconds: u64,
    pub policy: TenantCachePolicy,
    pub allowed_vendors: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TenantConfigSet {
    tenants: BTreeMap<String, TenantConfig>,
}

impl TenantConfigSet {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.display().to_string(),
            source,
        })?;
        Self::parse_toml(&text)
    }

    pub fn parse_toml(text: &str) -> Result<Self, ConfigError> {
        let raw: RawTenantConfigFile = toml::from_str(text)?;
        if raw.tenants.is_empty() {
            return Err(ConfigError::Invalid(
                "tenant config must contain at least one tenant".into(),
            ));
        }

        let mut tenants = BTreeMap::new();
        for raw_tenant in raw.tenants {
            let id = TenantId::parse(raw_tenant.id.clone())
                .map_err(|error| ConfigError::Invalid(error.to_string()))?;
            validate_sha256(&raw_tenant.api_key_sha256)?;
            if raw_tenant.name.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "tenant `{}` name must not be empty",
                    raw_tenant.id
                )));
            }
            if raw_tenant.request_rate_limit_per_minute == 0 {
                return Err(ConfigError::Invalid(format!(
                    "tenant `{}` request_rate_limit_per_minute must be greater than zero",
                    raw_tenant.id
                )));
            }
            if raw_tenant.stream_concurrency_limit == 0 {
                return Err(ConfigError::Invalid(format!(
                    "tenant `{}` stream_concurrency_limit must be greater than zero",
                    raw_tenant.id
                )));
            }
            if raw_tenant.default_ttl_seconds == 0 || raw_tenant.max_ttl_seconds == 0 {
                return Err(ConfigError::Invalid(format!(
                    "tenant `{}` TTL values must be greater than zero",
                    raw_tenant.id
                )));
            }
            if raw_tenant.default_ttl_seconds > raw_tenant.max_ttl_seconds {
                return Err(ConfigError::Invalid(format!(
                    "tenant `{}` default_ttl_seconds must not exceed max_ttl_seconds",
                    raw_tenant.id
                )));
            }
            if raw_tenant.allowed_vendors.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "tenant `{}` must allow at least one vendor",
                    raw_tenant.id
                )));
            }
            if raw_tenant
                .allowed_vendors
                .iter()
                .any(|vendor| vendor.trim().is_empty())
            {
                return Err(ConfigError::Invalid(format!(
                    "tenant `{}` allowed_vendors must not contain empty values",
                    raw_tenant.id
                )));
            }

            let config = TenantConfig {
                id,
                name: raw_tenant.name,
                enabled: raw_tenant.enabled,
                api_key_sha256: raw_tenant.api_key_sha256,
                dram_quota_bytes: raw_tenant.dram_quota_bytes,
                ssd_quota_bytes: raw_tenant.ssd_quota_bytes,
                request_rate_limit_per_minute: raw_tenant.request_rate_limit_per_minute,
                stream_concurrency_limit: raw_tenant.stream_concurrency_limit,
                vendor_spend_budget_usd: raw_tenant.vendor_spend_budget_usd,
                default_ttl_seconds: raw_tenant.default_ttl_seconds,
                max_ttl_seconds: raw_tenant.max_ttl_seconds,
                policy: raw_tenant.policy,
                allowed_vendors: raw_tenant.allowed_vendors,
            };

            if tenants.insert(raw_tenant.id.clone(), config).is_some() {
                return Err(ConfigError::Invalid(format!(
                    "duplicate tenant id `{}`",
                    raw_tenant.id
                )));
            }
        }

        Ok(Self { tenants })
    }

    #[must_use]
    pub fn tenant(&self, tenant_id: &str) -> Option<&TenantConfig> {
        self.tenants.get(tenant_id)
    }

    #[must_use]
    pub fn tenant_for_bearer_token(&self, token: &str) -> Option<&TenantConfig> {
        let digest = sha256_hex(token.as_bytes());
        self.tenants
            .values()
            .find(|tenant| tenant.enabled && tenant.api_key_sha256 == digest)
    }

    pub fn tenants(&self) -> impl Iterator<Item = &TenantConfig> {
        self.tenants.values()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VendorConfig {
    pub id: String,
    pub adapter: String,
    pub adapter_version: String,
    pub base_url: String,
    pub api_key_env: String,
    pub timeout_ms: u64,
    pub headers: BTreeMap<String, String>,
    pub models: Vec<VendorModelConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct VendorModelConfig {
    pub requested: String,
    pub resolved: String,
    pub cache_eligible: bool,
}

#[derive(Debug, Clone)]
pub struct VendorConfigSet {
    vendors: BTreeMap<String, VendorConfig>,
}

impl VendorConfigSet {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.display().to_string(),
            source,
        })?;
        Self::parse_toml(&text)
    }

    pub fn parse_toml(text: &str) -> Result<Self, ConfigError> {
        let raw: RawVendorConfigFile = toml::from_str(text)?;
        if raw.vendors.is_empty() {
            return Err(ConfigError::Invalid(
                "vendor config must contain at least one vendor".into(),
            ));
        }

        let mut vendors = BTreeMap::new();
        for raw_vendor in raw.vendors {
            validate_non_empty(&raw_vendor.id, "vendor id")?;
            validate_non_empty(&raw_vendor.adapter, "vendor adapter")?;
            validate_non_empty(&raw_vendor.adapter_version, "vendor adapter_version")?;
            validate_non_empty(&raw_vendor.base_url, "vendor base_url")?;
            validate_non_empty(&raw_vendor.api_key_env, "vendor api_key_env")?;
            if raw_vendor.timeout_ms == 0 {
                return Err(ConfigError::Invalid(format!(
                    "vendor `{}` timeout_ms must be greater than zero",
                    raw_vendor.id
                )));
            }
            if raw_vendor.models.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "vendor `{}` must define at least one model",
                    raw_vendor.id
                )));
            }
            for model in &raw_vendor.models {
                validate_non_empty(&model.requested, "vendor model requested")?;
                validate_non_empty(&model.resolved, "vendor model resolved")?;
            }

            let config = VendorConfig {
                id: raw_vendor.id.clone(),
                adapter: raw_vendor.adapter,
                adapter_version: raw_vendor.adapter_version,
                base_url: raw_vendor.base_url,
                api_key_env: raw_vendor.api_key_env,
                timeout_ms: raw_vendor.timeout_ms,
                headers: raw_vendor.headers,
                models: raw_vendor.models,
            };

            if vendors.insert(raw_vendor.id.clone(), config).is_some() {
                return Err(ConfigError::Invalid(format!(
                    "duplicate vendor id `{}`",
                    raw_vendor.id
                )));
            }
        }

        Ok(Self { vendors })
    }

    #[must_use]
    pub fn vendor(&self, vendor_id: &str) -> Option<&VendorConfig> {
        self.vendors.get(vendor_id)
    }

    pub fn vendors(&self) -> impl Iterator<Item = &VendorConfig> {
        self.vendors.values()
    }
}

#[derive(Debug, Deserialize)]
struct RawTenantConfigFile {
    tenants: Vec<RawTenantConfig>,
}

#[derive(Debug, Deserialize)]
struct RawTenantConfig {
    id: String,
    name: String,
    enabled: bool,
    api_key_sha256: String,
    dram_quota_bytes: u64,
    ssd_quota_bytes: u64,
    request_rate_limit_per_minute: u32,
    stream_concurrency_limit: u32,
    vendor_spend_budget_usd: u32,
    default_ttl_seconds: u64,
    max_ttl_seconds: u64,
    policy: TenantCachePolicy,
    allowed_vendors: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RawVendorConfigFile {
    vendors: Vec<RawVendorConfig>,
}

#[derive(Debug, Deserialize)]
struct RawVendorConfig {
    id: String,
    adapter: String,
    adapter_version: String,
    base_url: String,
    api_key_env: String,
    timeout_ms: u64,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    models: Vec<VendorModelConfig>,
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut out, "{byte:02x}").expect("writing to String should not fail");
    }
    out
}

fn validate_non_empty(value: &str, field: &str) -> Result<(), ConfigError> {
    if value.trim().is_empty() {
        Err(ConfigError::Invalid(format!("{field} must not be empty")))
    } else {
        Ok(())
    }
}

fn validate_sha256(value: &str) -> Result<(), ConfigError> {
    let valid = value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'));
    if valid {
        Ok(())
    } else {
        Err(ConfigError::Invalid(
            "api_key_sha256 must be 64 lowercase hex characters".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_enabled_tenant_with_policy_and_limits() {
        let config = TenantConfigSet::parse_toml(
            r#"
            [[tenants]]
            id = "demo-tenant"
            name = "Demo Tenant"
            enabled = true
            api_key_sha256 = "1a134f3f350c91084f57eb135752d773e992a1271f0fe658d441b8e45d064f3b"
            dram_quota_bytes = 1073741824

            ssd_quota_bytes = 10737418240
            request_rate_limit_per_minute = 12000
            stream_concurrency_limit = 80
            vendor_spend_budget_usd = 5000
            default_ttl_seconds = 86400
            max_ttl_seconds = 604800
            policy = "cache_first"
            allowed_vendors = ["openai"]
            "#,
        )
        .unwrap();

        let tenant = config.tenant("demo-tenant").unwrap();
        assert_eq!(tenant.id.as_str(), "demo-tenant");
        assert_eq!(tenant.policy, TenantCachePolicy::CacheFirst);
        assert_eq!(tenant.dram_quota_bytes, 1_073_741_824);
        assert_eq!(tenant.ssd_quota_bytes, 10_737_418_240);
        assert_eq!(tenant.allowed_vendors, ["openai"]);
    }

    #[test]
    fn parses_openai_vendor_with_model_aliases() {
        let config = VendorConfigSet::parse_toml(
            r#"
            [[vendors]]
            id = "openai"
            adapter = "openai-responses"
            adapter_version = "openai-responses-v1"
            base_url = "https://api.openai.com/v1"
            api_key_env = "OPENAI_API_KEY"
            timeout_ms = 30000
            headers = { "OpenAI-Beta" = "responses=v1" }

            [[vendors.models]]
            requested = "gpt-4.1"
            resolved = "gpt-4.1"
            cache_eligible = true
            "#,
        )
        .unwrap();

        let vendor = config.vendor("openai").unwrap();
        assert_eq!(vendor.adapter, "openai-responses");
        assert_eq!(vendor.adapter_version, "openai-responses-v1");
        assert_eq!(vendor.timeout_ms, 30_000);
        assert_eq!(vendor.models[0].requested, "gpt-4.1");
    }

    #[test]
    fn authenticates_enabled_tenant_by_bearer_token_digest() {
        let config = TenantConfigSet::parse_toml(
            r#"
            [[tenants]]
            id = "demo-tenant"
            name = "Demo Tenant"
            enabled = true
            api_key_sha256 = "e46ea83ec368dc44797a4b7da96ad92963dae141d417cd89fdb211b488422b0f"
            dram_quota_bytes = 1
            ssd_quota_bytes = 0
            request_rate_limit_per_minute = 1
            stream_concurrency_limit = 1
            vendor_spend_budget_usd = 1
            default_ttl_seconds = 60
            max_ttl_seconds = 60
            policy = "cache_first"
            allowed_vendors = ["openai"]
            "#,
        )
        .unwrap();

        let tenant = config
            .tenant_for_bearer_token("demo-api-key-do-not-use")
            .unwrap();

        assert_eq!(tenant.id.as_str(), "demo-tenant");
    }

    #[test]
    fn authentication_ignores_disabled_tenants() {
        let config = TenantConfigSet::parse_toml(
            r#"
            [[tenants]]
            id = "disabled-tenant"
            name = "Disabled Tenant"
            enabled = false
            api_key_sha256 = "e46ea83ec368dc44797a4b7da96ad92963dae141d417cd89fdb211b488422b0f"
            dram_quota_bytes = 1
            ssd_quota_bytes = 0
            request_rate_limit_per_minute = 1
            stream_concurrency_limit = 1
            vendor_spend_budget_usd = 1
            default_ttl_seconds = 60
            max_ttl_seconds = 60
            policy = "cache_first"
            allowed_vendors = ["openai"]
            "#,
        )
        .unwrap();

        assert!(config
            .tenant_for_bearer_token("demo-api-key-do-not-use")
            .is_none());
    }

    #[test]
    fn rejects_duplicate_tenant_ids() {
        let err = TenantConfigSet::parse_toml(
            r#"
            [[tenants]]
            id = "demo-tenant"
            name = "Demo Tenant A"
            enabled = true
            api_key_sha256 = "1a134f3f350c91084f57eb135752d773e992a1271f0fe658d441b8e45d064f3b"
            dram_quota_bytes = 1
            ssd_quota_bytes = 0
            request_rate_limit_per_minute = 1
            stream_concurrency_limit = 1
            vendor_spend_budget_usd = 1
            default_ttl_seconds = 60
            max_ttl_seconds = 60
            policy = "cache_first"
            allowed_vendors = ["openai"]

            [[tenants]]
            id = "demo-tenant"
            name = "Demo Tenant B"
            enabled = true
            api_key_sha256 = "2a134f3f350c91084f57eb135752d773e992a1271f0fe658d441b8e45d064f3b"
            dram_quota_bytes = 1
            ssd_quota_bytes = 0
            request_rate_limit_per_minute = 1
            stream_concurrency_limit = 1
            vendor_spend_budget_usd = 1
            default_ttl_seconds = 60
            max_ttl_seconds = 60
            policy = "cache_first"
            allowed_vendors = ["openai"]
            "#,
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("duplicate tenant id `demo-tenant`"));
    }

    #[test]
    fn rejects_invalid_api_key_digest() {
        let err = TenantConfigSet::parse_toml(
            r#"
            [[tenants]]
            id = "demo-tenant"
            name = "Demo Tenant"
            enabled = true
            api_key_sha256 = "not-a-sha256"
            dram_quota_bytes = 1
            ssd_quota_bytes = 0
            request_rate_limit_per_minute = 1
            stream_concurrency_limit = 1
            vendor_spend_budget_usd = 1
            default_ttl_seconds = 60
            max_ttl_seconds = 60
            policy = "cache_first"
            allowed_vendors = ["openai"]
            "#,
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("api_key_sha256 must be 64 lowercase hex characters"));
    }
}
