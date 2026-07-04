use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use mooncache_common::{CacheKey, TenantId};
use mooncache_master::{MasterState, ReplicaDescriptor, ReplicaList};
use mooncache_store::{ChunkHandle, MemoryStore};
use reqwest::Client;
use serde::Deserialize;

use crate::cache_flow::GatewayError;

// ── MasterClient ──────────────────────────────────────────────────

#[async_trait]
pub trait MasterClient: Send + Sync {
    async fn get_replica_list(
        &self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
    ) -> Result<ReplicaList, GatewayError>;

    async fn put_start(
        &self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
        len: u64,
        replica_count: usize,
    ) -> Result<Vec<ReplicaDescriptor>, GatewayError>;

    async fn put_end(&self, tenant_id: &TenantId, cache_key: &CacheKey)
        -> Result<(), GatewayError>;

    async fn put_revoke(
        &self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
    ) -> Result<(), GatewayError>;
}

// ── StoreClient ───────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreChunk {
    pub bytes: Vec<u8>,
    pub tier: String,
}

#[async_trait]
pub trait StoreClient: Send + Sync {
    async fn read_chunk(
        &self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
        handle: &ChunkHandle,
    ) -> Result<StoreChunk, GatewayError>;

    async fn write_preallocated_chunk(
        &self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
        handle: &ChunkHandle,
        bytes: &[u8],
    ) -> Result<(), GatewayError>;
}

// ── LocalMasterClient ─────────────────────────────────────────────

pub struct LocalMasterClient {
    inner: Mutex<MasterState>,
}

impl LocalMasterClient {
    pub fn new(state: MasterState) -> Self {
        Self {
            inner: Mutex::new(state),
        }
    }
}

#[async_trait]
impl MasterClient for LocalMasterClient {
    async fn get_replica_list(
        &self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
    ) -> Result<ReplicaList, GatewayError> {
        self.inner
            .lock()
            .map_err(|_| GatewayError::PoisonedLock)?
            .get_replica_list(tenant_id, cache_key)
            .map_err(Into::into)
    }

    async fn put_start(
        &self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
        len: u64,
        replica_count: usize,
    ) -> Result<Vec<ReplicaDescriptor>, GatewayError> {
        self.inner
            .lock()
            .map_err(|_| GatewayError::PoisonedLock)?
            .put_start(tenant_id, cache_key, len, replica_count)
            .map_err(Into::into)
    }

    async fn put_end(
        &self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
    ) -> Result<(), GatewayError> {
        self.inner
            .lock()
            .map_err(|_| GatewayError::PoisonedLock)?
            .put_end(tenant_id, cache_key)
            .map_err(Into::into)
    }

    async fn put_revoke(
        &self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
    ) -> Result<(), GatewayError> {
        self.inner
            .lock()
            .map_err(|_| GatewayError::PoisonedLock)?
            .put_revoke(tenant_id, cache_key)
            .map_err(Into::into)
    }
}

// ── LocalStoreClient ──────────────────────────────────────────────

pub struct LocalStoreClient {
    inner: Mutex<MemoryStore>,
}

impl LocalStoreClient {
    pub fn new(store: MemoryStore) -> Self {
        Self {
            inner: Mutex::new(store),
        }
    }
}

#[async_trait]
impl StoreClient for LocalStoreClient {
    async fn read_chunk(
        &self,
        _tenant_id: &TenantId,
        _cache_key: &CacheKey,
        handle: &ChunkHandle,
    ) -> Result<StoreChunk, GatewayError> {
        let bytes = self
            .inner
            .lock()
            .map_err(|_| GatewayError::PoisonedLock)?
            .read_chunk(handle)?;
        Ok(StoreChunk {
            bytes,
            tier: "dram".to_string(),
        })
    }

    async fn write_preallocated_chunk(
        &self,
        _tenant_id: &TenantId,
        _cache_key: &CacheKey,
        handle: &ChunkHandle,
        bytes: &[u8],
    ) -> Result<(), GatewayError> {
        self.inner
            .lock()
            .map_err(|_| GatewayError::PoisonedLock)?
            .write_preallocated_chunk(handle, bytes)
            .map_err(Into::into)
    }
}

// ── RemoteMasterClient ────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct MasterOkResponse {
    ok: bool,
}

#[derive(Debug, Deserialize)]
struct MasterReplicasResponse {
    replicas: Vec<ReplicaDescriptor>,
}

#[derive(Debug, Deserialize)]
struct MasterErrorResponse {
    error: String,
}

pub struct RemoteMasterClient {
    base_url: String,
    client: Client,
}

impl RemoteMasterClient {
    pub fn new(base_url: String) -> Self {
        Self {
            base_url,
            client: Client::new(),
        }
    }

    async fn post_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &impl serde::Serialize,
    ) -> Result<T, GatewayError> {
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .client
            .post(&url)
            .json(body)
            .send()
            .await
            .map_err(|e| GatewayError::MasterUpstream(e.to_string()))?;

        if response.status().is_success() {
            response
                .json::<T>()
                .await
                .map_err(|e| GatewayError::MasterUpstream(e.to_string()))
        } else {
            let err_body: MasterErrorResponse =
                response.json().await.unwrap_or(MasterErrorResponse {
                    error: "unknown error".to_string(),
                });
            Err(GatewayError::MasterUpstream(err_body.error))
        }
    }
}

#[async_trait]
impl MasterClient for RemoteMasterClient {
    async fn get_replica_list(
        &self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
    ) -> Result<ReplicaList, GatewayError> {
        let path = format!(
            "/objects/replicas?tenant_id={}&cache_key={}",
            tenant_id.as_str(),
            cache_key.as_str()
        );
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| GatewayError::MasterUpstream(e.to_string()))?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(GatewayError::Cache(mooncache_common::CacheError::NotFound));
        }
        if !response.status().is_success() {
            let err_body: MasterErrorResponse =
                response.json().await.unwrap_or(MasterErrorResponse {
                    error: "unknown error".to_string(),
                });
            return Err(GatewayError::MasterUpstream(err_body.error));
        }
        response
            .json::<ReplicaList>()
            .await
            .map_err(|e| GatewayError::MasterUpstream(e.to_string()))
    }

    async fn put_start(
        &self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
        len: u64,
        replica_count: usize,
    ) -> Result<Vec<ReplicaDescriptor>, GatewayError> {
        let body = serde_json::json!({
            "tenant_id": tenant_id.as_str(),
            "cache_key": cache_key.as_str(),
            "len": len,
            "replica_count": replica_count,
        });
        let response: MasterReplicasResponse = self.post_json("/objects/start", &body).await?;
        Ok(response.replicas)
    }

    async fn put_end(
        &self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
    ) -> Result<(), GatewayError> {
        let body = serde_json::json!({
            "tenant_id": tenant_id.as_str(),
            "cache_key": cache_key.as_str(),
        });
        let _response: MasterOkResponse = self.post_json("/objects/end", &body).await?;
        Ok(())
    }

    async fn put_revoke(
        &self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
    ) -> Result<(), GatewayError> {
        let body = serde_json::json!({
            "tenant_id": tenant_id.as_str(),
            "cache_key": cache_key.as_str(),
        });
        let _response: MasterOkResponse = self.post_json("/objects/revoke", &body).await?;
        Ok(())
    }
}

// Admin methods on RemoteMasterClient — needed for test setup
impl RemoteMasterClient {
    pub async fn mount_segment(&self, node_id: &str, len: u64) -> Result<(), GatewayError> {
        let body = serde_json::json!({
            "node_id": node_id,
            "len": len,
        });
        let _response: MasterOkResponse = self.post_json("/segments/mount", &body).await?;
        Ok(())
    }

    pub async fn set_tenant_quota(
        &self,
        tenant_id: &str,
        dram_bytes: u64,
        ssd_bytes: u64,
    ) -> Result<(), GatewayError> {
        let body = serde_json::json!({
            "tenant_id": tenant_id,
            "dram_bytes": dram_bytes,
            "ssd_bytes": ssd_bytes,
        });
        let _response: MasterOkResponse = self.post_json("/tenants/quota", &body).await?;
        Ok(())
    }
}

// ── RemoteStoreClient ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct StoreReadResponse {
    offset: usize,
    len: usize,
    tier: Option<String>,
    data: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct StoreErrorResponse {
    error: String,
}

pub struct RemoteStoreClient {
    node_urls: HashMap<String, String>,
    client: Client,
}

impl RemoteStoreClient {
    /// Create a client that resolves `node_id` → store HTTP base URL.
    pub fn new(node_urls: HashMap<String, String>) -> Self {
        Self {
            node_urls,
            client: Client::new(),
        }
    }

    /// Create a client that uses a single URL for all node_ids.
    pub fn new_single_node(node_id: impl Into<String>, base_url: impl Into<String>) -> Self {
        let mut node_urls = HashMap::new();
        node_urls.insert(node_id.into(), base_url.into());
        Self {
            node_urls,
            client: Client::new(),
        }
    }
}

#[async_trait]
impl StoreClient for RemoteStoreClient {
    async fn read_chunk(
        &self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
        handle: &ChunkHandle,
    ) -> Result<StoreChunk, GatewayError> {
        // For read, any node can serve — use the first configured node.
        // Sprint 3 passes object identity so Store can promote from SSD on a DRAM miss.
        let base_url =
            self.node_urls.values().next().ok_or_else(|| {
                GatewayError::StoreUpstream("no store nodes configured".to_string())
            })?;
        let url = format!(
            "{}/chunks/{}/{}?tenant_id={}&cache_key={}",
            base_url,
            handle.offset(),
            handle.len(),
            tenant_id.as_str(),
            cache_key.as_str()
        );
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| GatewayError::StoreUpstream(e.to_string()))?;

        if response.status().is_success() {
            let body: StoreReadResponse = response
                .json()
                .await
                .map_err(|e| GatewayError::StoreUpstream(e.to_string()))?;
            Ok(StoreChunk {
                bytes: body.data,
                tier: body.tier.unwrap_or_else(|| "dram".to_string()),
            })
        } else {
            let err_body: StoreErrorResponse =
                response.json().await.unwrap_or(StoreErrorResponse {
                    error: "unknown error".to_string(),
                });
            Err(GatewayError::StoreUpstream(err_body.error))
        }
    }

    async fn write_preallocated_chunk(
        &self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
        handle: &ChunkHandle,
        bytes: &[u8],
    ) -> Result<(), GatewayError> {
        // For write_preallocated_chunk called from write_reserved_replicas,
        // the caller iterates over replicas and calls us once per replica.
        // This Sprint 2/3 client remains single-node; object identity lets Store mirror to SSD.
        let base_url =
            self.node_urls.values().next().ok_or_else(|| {
                GatewayError::StoreUpstream("no store nodes configured".to_string())
            })?;
        let url = format!("{}/chunks/preallocated", base_url);
        let body = serde_json::json!({
            "tenant_id": tenant_id.as_str(),
            "cache_key": cache_key.as_str(),
            "offset": handle.offset(),
            "len": handle.len(),
            "data": bytes,
        });
        let response = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| GatewayError::StoreUpstream(e.to_string()))?;

        if response.status().is_success() {
            Ok(())
        } else {
            let err_body: StoreErrorResponse =
                response.json().await.unwrap_or(StoreErrorResponse {
                    error: "unknown error".to_string(),
                });
            Err(GatewayError::StoreUpstream(err_body.error))
        }
    }
}
