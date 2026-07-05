# Distributed API Response KVCache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Rust + React distributed, Mooncake-style API response KV object cache for OpenAI Responses-compatible vendor API traffic.

**Architecture:** A Rust API Gateway computes strict request fingerprints, checks a distributed cache, coalesces identical misses, calls vendor adapters on miss, and writes complete successful responses back to cache. A Master control plane owns metadata, leases, allocation, quotas, eviction, and HA through etcd; Store nodes own DRAM and SSD object chunks. A React + TypeScript + Vite operations console manages health, tenants, nodes, vendors, cache operations, alerts, and audit.

**Tech Stack:** Rust stable, Tokio, Axum, Serde, Reqwest, SHA-256, etcd-client, metrics/tracing, React 19-compatible TypeScript + Vite, TanStack Query, Vitest, Playwright later for E2E.

## Current Implementation Status

This plan has been implemented as a local, test-verified Rust/React skeleton with real Gateway → Master HTTP and Gateway → Store HTTP wiring, OpenAI Responses adapter support, tenant/vendor config files, local Docker Compose topology, and the React operations console. Production-grade Master HA, etcd-backed metadata persistence, multi-replica scheduling, credential rotation, and SSD cold-tier operations remain roadmap work; the unchecked task boxes below are preserved as the original implementation checklist rather than a production-completeness claim.

## Global Constraints

- The cache primitive is an API response object cache, not raw transformer KV tensors.
- Public inference surface is OpenAI Responses API compatible.
- Cache hits are strict exact matches only.
- Cache key uses tenant + vendor + resolved model + endpoint/schema/adapter versions + canonical full request body + cache-relevant headers/policy.
- Automatic writeback only caches deterministic-safe requests.
- Streaming and non-streaming are both supported.
- Streaming cache objects store both original SSE event sequence and final aggregated response JSON.
- Identical concurrent misses use singleflight.
- Only complete successful vendor responses can commit to cache.
- DRAM is the hot tier; SSD is the cold tier and promotes hits back to DRAM.
- Master HA uses etcd + multiple Master replicas with one active leader.
- Multi-tenant isolation is strict.
- Cache failure defaults to fail-open vendor fallback except auth, quota, explicit cache-only, and idempotency conflicts.
- Control panel is a full operations console with RBAC roles: Viewer, Operator, Admin.
- Current workspace was not a git repository during planning, so implementation checkpoints use verification commands rather than mandatory commit commands until a repository is initialized.

---

## File Structure

Create this structure:

```text
Cargo.toml
crates/
  common/
    Cargo.toml
    src/lib.rs
    src/error.rs
    src/ids.rs
    src/time.rs
  protocol/
    Cargo.toml
    src/lib.rs
    src/responses.rs
    src/cache_headers.rs
    src/admin.rs
  fingerprint/
    Cargo.toml
    src/lib.rs
    src/canonical_json.rs
    src/eligibility.rs
  master/
    Cargo.toml
    src/lib.rs
    src/state.rs
    src/object.rs
    src/lease.rs
    src/allocator.rs
    src/quota.rs
    src/eviction.rs
    src/etcd.rs
  store/
    Cargo.toml
    src/lib.rs
    src/chunk.rs
    src/memory.rs
    src/ssd.rs
    src/checksum.rs
  gateway/
    Cargo.toml
    src/lib.rs
    src/routes.rs
    src/cache_flow.rs
    src/singleflight.rs
    src/vendor.rs
    src/streaming.rs
  admin-api/
    Cargo.toml
    src/lib.rs
    src/routes.rs
    src/rbac.rs
    src/audit.rs
apps/
  gateway/src/main.rs
  master/src/main.rs
  store-node/src/main.rs
  admin-api/src/main.rs
control-panel/
  package.json
  index.html
  src/main.tsx
  src/App.tsx
  src/api/client.ts
  src/auth/rbac.ts
  src/pages/Overview.tsx
  src/pages/CacheAnalytics.tsx
  src/pages/Nodes.tsx
  src/pages/Tenants.tsx
  src/pages/Vendors.tsx
  src/pages/CacheOperations.tsx
  src/pages/Alerts.tsx
  src/pages/AuditLog.tsx
tests/
  integration/cache_flow.rs
  integration/streaming_flow.rs
  integration/admin_api.rs
```

---

### Task 1: Rust Workspace and Shared Domain Types

**Files:**
- Create: `Cargo.toml`
- Create: `crates/common/Cargo.toml`
- Create: `crates/common/src/lib.rs`
- Create: `crates/common/src/error.rs`
- Create: `crates/common/src/ids.rs`
- Create: `crates/common/src/time.rs`

**Interfaces:**
- Produces: `TenantId`, `CacheKey`, `RequestId`, `NodeId`, `ModelVersion`, `CacheError`, `CacheResult<T>`.
- Consumes: none.

- [ ] **Step 1: Write shared type tests**

Create `crates/common/src/ids.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_id_rejects_empty_value() {
        let err = TenantId::parse("").unwrap_err();
        assert!(err.to_string().contains("tenant id must not be empty"));
    }

    #[test]
    fn cache_key_displays_redacted_prefix() {
        let key = CacheKey::from_hex("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").unwrap();
        assert_eq!(key.redacted(), "01234567…cdef");
    }
}
```

Expected RED: `TenantId` and `CacheKey` are undefined.

- [ ] **Step 2: Create workspace manifests**

`Cargo.toml`:

```toml
[workspace]
members = [
  "crates/common",
  "crates/protocol",
  "crates/fingerprint",
  "crates/master",
  "crates/store",
  "crates/gateway",
  "crates/admin-api",
  "apps/gateway",
  "apps/master",
  "apps/store-node",
  "apps/admin-api",
]
resolver = "2"

[workspace.package]
edition = "2021"
license = "UNLICENSED"

[workspace.dependencies]
anyhow = "1"
async-trait = "0.1"
axum = "0.7"
bytes = "1"
dashmap = "6"
etcd-client = "0.14"
futures = "0.3"
reqwest = { version = "0.12", default-features = false, features = ["json", "stream", "rustls-tls"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
sha2 = "0.10"
thiserror = "1"
tokio = { version = "1", features = ["full"] }
tokio-util = "0.7"
tracing = "0.1"
uuid = { version = "1", features = ["v4", "serde"] }
```

`crates/common/Cargo.toml`:

```toml
[package]
name = "mooncache-common"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[dependencies]
serde.workspace = true
thiserror.workspace = true
uuid.workspace = true
```

- [ ] **Step 3: Implement shared IDs and errors**

`crates/common/src/lib.rs`:

```rust
pub mod error;
pub mod ids;
pub mod time;

pub use error::{CacheError, CacheResult};
pub use ids::{CacheKey, ModelVersion, NodeId, RequestId, TenantId};
```

`crates/common/src/error.rs`:

```rust
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
```

`crates/common/src/ids.rs`:

```rust
use crate::{CacheError, CacheResult};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TenantId(String);

impl TenantId {
    pub fn parse(value: impl Into<String>) -> CacheResult<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(CacheError::EmptyTenantId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str { &self.0 }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CacheKey(String);

impl CacheKey {
    pub fn from_hex(value: impl Into<String>) -> CacheResult<Self> {
        let value = value.into();
        let valid = value.len() == 64 && value.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
        if !valid {
            return Err(CacheError::InvalidCacheKey);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str { &self.0 }

    pub fn redacted(&self) -> String {
        format!("{}…{}", &self.0[..8], &self.0[60..])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ModelVersion(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestId(Uuid);

impl RequestId {
    pub fn new() -> Self { Self(Uuid::new_v4()) }
}
```

- [ ] **Step 4: Verify**

Run:

```bash
cargo test -p mooncache-common
```

Expected: tests pass.

---

### Task 2: Protocol Models for Responses API and Cache Headers

**Files:**
- Create: `crates/protocol/Cargo.toml`
- Create: `crates/protocol/src/lib.rs`
- Create: `crates/protocol/src/responses.rs`
- Create: `crates/protocol/src/cache_headers.rs`
- Create: `crates/protocol/src/admin.rs`

**Interfaces:**
- Consumes: `TenantId`, `CacheKey` from `mooncache-common`.
- Produces: `ResponsesRequest`, `ResponsesResponse`, `CacheControl`, `CacheStatus`, `AdminError`.

- [ ] **Step 1: Write header parsing tests**

`crates/protocol/src/cache_headers.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cache_control_modes() {
        assert_eq!(CacheControl::parse("bypass").unwrap(), CacheControl::Bypass);
        assert_eq!(CacheControl::parse("cache-only").unwrap(), CacheControl::CacheOnly);
        assert_eq!(CacheControl::parse("").unwrap(), CacheControl::Default);
    }

    #[test]
    fn rejects_unknown_cache_control_mode() {
        let err = CacheControl::parse("semantic").unwrap_err();
        assert!(err.to_string().contains("invalid cache control"));
    }
}
```

Expected RED: `CacheControl` is undefined.

- [ ] **Step 2: Add protocol manifest**

```toml
[package]
name = "mooncache-protocol"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[dependencies]
mooncache-common = { path = "../common" }
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
```

- [ ] **Step 3: Implement cache headers**

```rust
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheControl {
    Default,
    Bypass,
    ReadOnly,
    WriteOnly,
    CacheOnly,
    ForceReplay,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum HeaderError {
    #[error("invalid cache control: {0}")]
    InvalidCacheControl(String),
}

impl CacheControl {
    pub fn parse(value: &str) -> Result<Self, HeaderError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "default" => Ok(Self::Default),
            "bypass" => Ok(Self::Bypass),
            "read-only" => Ok(Self::ReadOnly),
            "write-only" => Ok(Self::WriteOnly),
            "cache-only" => Ok(Self::CacheOnly),
            "force-replay" => Ok(Self::ForceReplay),
            other => Err(HeaderError::InvalidCacheControl(other.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheStatus {
    Hit,
    Miss,
    Bypass,
    Ineligible,
    CacheOnlyMiss,
    Degraded,
}
```

- [ ] **Step 4: Implement flexible Responses models**

`crates/protocol/src/responses.rs`:

```rust
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResponsesRequest {
    pub model: String,
    #[serde(flatten)]
    pub body: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResponsesResponse {
    #[serde(flatten)]
    pub body: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SseEvent {
    pub event: Option<String>,
    pub data: String,
}
```

`crates/protocol/src/lib.rs`:

```rust
pub mod admin;
pub mod cache_headers;
pub mod responses;

pub use cache_headers::{CacheControl, CacheStatus};
pub use responses::{ResponsesRequest, ResponsesResponse, SseEvent};
```

- [ ] **Step 5: Verify**

Run:

```bash
cargo test -p mooncache-protocol
```

Expected: tests pass.

---

### Task 3: Canonical Fingerprinting and Eligibility

**Files:**
- Create: `crates/fingerprint/Cargo.toml`
- Create: `crates/fingerprint/src/lib.rs`
- Create: `crates/fingerprint/src/canonical_json.rs`
- Create: `crates/fingerprint/src/eligibility.rs`

**Interfaces:**
- Consumes: `ResponsesRequest`, `CacheControl`, `TenantId`, `CacheKey`.
- Produces: `FingerprintInput`, `compute_cache_key`, `EligibilityDecision`.

- [ ] **Step 1: Write canonicalization tests**

`crates/fingerprint/src/canonical_json.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn object_keys_are_sorted_recursively() {
        let left = json!({"b": 1, "a": {"z": 2, "m": 3}});
        let right = json!({"a": {"m": 3, "z": 2}, "b": 1});
        assert_eq!(canonical_json_bytes(&left).unwrap(), canonical_json_bytes(&right).unwrap());
    }

    #[test]
    fn array_order_is_preserved() {
        let a = json!([1, 2]);
        let b = json!([2, 1]);
        assert_ne!(canonical_json_bytes(&a).unwrap(), canonical_json_bytes(&b).unwrap());
    }
}
```

Expected RED: `canonical_json_bytes` is undefined.

- [ ] **Step 2: Write eligibility tests**

`crates/fingerprint/src/eligibility.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn deterministic_temperature_zero_is_cacheable() {
        let decision = classify_request(&json!({"model":"gpt-x","temperature":0,"input":"hello"}), false);
        assert_eq!(decision, EligibilityDecision::Cacheable);
    }

    #[test]
    fn stochastic_request_bypasses_without_force_replay() {
        let decision = classify_request(&json!({"model":"gpt-x","temperature":0.7,"input":"hello"}), false);
        assert!(matches!(decision, EligibilityDecision::Bypass { .. }));
    }

    #[test]
    fn force_replay_allows_stochastic_exact_replay() {
        let decision = classify_request(&json!({"model":"gpt-x","temperature":0.7,"input":"hello"}), true);
        assert_eq!(decision, EligibilityDecision::Cacheable);
    }
}
```

- [ ] **Step 3: Implement canonical JSON and SHA-256 key**

Use `BTreeMap` ordering through recursive conversion before serialization.

```rust
use serde_json::{Map, Value};

pub fn canonical_json_bytes(value: &Value) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&canonicalize(value))
}

fn canonicalize(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut sorted = Map::new();
            let mut keys: Vec<_> = map.keys().collect();
            keys.sort();
            for key in keys {
                sorted.insert(key.clone(), canonicalize(&map[key]));
            }
            Value::Object(sorted)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonicalize).collect()),
        other => other.clone(),
    }
}
```

`crates/fingerprint/src/lib.rs` key API:

```rust
pub mod canonical_json;
pub mod eligibility;

use canonical_json::canonical_json_bytes;
use mooncache_common::{CacheKey, CacheResult, TenantId};
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
    let bytes = canonical_json_bytes(&doc).map_err(|e| mooncache_common::CacheError::InvalidId(e.to_string()))?;
    let digest = Sha256::digest(bytes);
    CacheKey::from_hex(format!("{digest:x}"))
}
```

- [ ] **Step 4: Implement eligibility**

```rust
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EligibilityDecision {
    Cacheable,
    Bypass { reason: String },
}

pub fn classify_request(body: &Value, force_replay: bool) -> EligibilityDecision {
    if force_replay {
        return EligibilityDecision::Cacheable;
    }
    let temperature = body.get("temperature").and_then(Value::as_f64).unwrap_or(0.0);
    if temperature == 0.0 {
        EligibilityDecision::Cacheable
    } else {
        EligibilityDecision::Bypass { reason: "stochastic temperature requires explicit force replay".to_owned() }
    }
}
```

- [ ] **Step 5: Verify**

Run:

```bash
cargo test -p mooncache-fingerprint
```

Expected: tests pass.

---

### Task 4: Master Metadata State Machine

**Files:**
- Create: `crates/master/Cargo.toml`
- Create: `crates/master/src/lib.rs`
- Create: `crates/master/src/object.rs`
- Create: `crates/master/src/state.rs`
- Create: `crates/master/src/lease.rs`
- Create: `crates/master/src/allocator.rs`

**Interfaces:**
- Consumes: `TenantId`, `CacheKey`, `NodeId`.
- Produces: `MasterState`, `put_start`, `put_end`, `put_revoke`, `get_replica_list`, `remove`.

- [ ] **Step 1: Write lifecycle tests**

`crates/master/src/state.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use mooncache_common::{CacheKey, TenantId};

    fn tenant() -> TenantId { TenantId::parse("tenant-a").unwrap() }
    fn key() -> CacheKey { CacheKey::from_hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap() }

    #[test]
    fn get_does_not_return_reserved_object_before_commit() {
        let mut state = MasterState::new_for_test();
        state.mount_segment("node-a", 1024 * 1024);
        state.put_start(&tenant(), &key(), 4096, 1).unwrap();
        assert!(state.get_replica_list(&tenant(), &key()).is_err());
    }

    #[test]
    fn committed_object_is_readable_with_lease() {
        let mut state = MasterState::new_for_test();
        state.mount_segment("node-a", 1024 * 1024);
        state.put_start(&tenant(), &key(), 4096, 1).unwrap();
        state.put_end(&tenant(), &key()).unwrap();
        let replicas = state.get_replica_list(&tenant(), &key()).unwrap();
        assert_eq!(replicas.replicas.len(), 1);
        assert!(replicas.lease.expires_at_ms > 0);
    }
}
```

Expected RED: `MasterState` undefined.

- [ ] **Step 2: Implement object model**

`crates/master/src/object.rs`:

```rust
use mooncache_common::{CacheKey, NodeId, TenantId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectStatus {
    Reserving,
    Writing,
    Committed,
    Revoked,
    Evicted,
}

#[derive(Debug, Clone)]
pub struct ReplicaDescriptor {
    pub node_id: String,
    pub offset: u64,
    pub len: u64,
}

#[derive(Debug, Clone)]
pub struct CacheObjectMeta {
    pub tenant_id: TenantId,
    pub cache_key: CacheKey,
    pub len: u64,
    pub status: ObjectStatus,
    pub replicas: Vec<ReplicaDescriptor>,
    pub hard_pinned: bool,
    pub soft_pinned_until_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct Lease {
    pub lease_id: String,
    pub expires_at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct ReplicaList {
    pub replicas: Vec<ReplicaDescriptor>,
    pub lease: Lease,
}
```

- [ ] **Step 3: Implement in-memory MasterState**

Use `HashMap<(TenantId, CacheKey), CacheObjectMeta>` and a simple segment allocator for first tests. Keep etcd persistence for a later task.

- [ ] **Step 4: Verify**

Run:

```bash
cargo test -p mooncache-master state
```

Expected: lifecycle tests pass.

---

### Task 5: Tenant Quota, Leases, and Eviction Rules

**Files:**
- Modify: `crates/master/src/quota.rs`
- Modify: `crates/master/src/lease.rs`
- Modify: `crates/master/src/eviction.rs`
- Modify: `crates/master/src/state.rs`

**Interfaces:**
- Consumes: `MasterState` object lifecycle.
- Produces: tenant quota reservation, lease-protected eviction, soft/hard pin behavior.

- [ ] **Step 1: Write quota and eviction tests**

```rust
#[test]
fn tenant_quota_blocks_write_when_eviction_cannot_reclaim() {
    let mut state = MasterState::new_for_test();
    state.set_tenant_quota("tenant-a", 4096, 0);
    state.mount_segment("node-a", 8192);
    let first = CacheKey::from_hex("1111111111111111111111111111111111111111111111111111111111111111").unwrap();
    let second = CacheKey::from_hex("2222222222222222222222222222222222222222222222222222222222222222").unwrap();
    state.put_start(&tenant(), &first, 4096, 1).unwrap();
    state.put_end(&tenant(), &first).unwrap();
    let err = state.put_start(&tenant(), &second, 4096, 1).unwrap_err();
    assert!(err.to_string().contains("quota exceeded"));
}

#[test]
fn eviction_skips_active_lease() {
    let mut state = MasterState::new_for_test();
    state.mount_segment("node-a", 8192);
    state.put_start(&tenant(), &key(), 4096, 1).unwrap();
    state.put_end(&tenant(), &key()).unwrap();
    let _lease = state.get_replica_list(&tenant(), &key()).unwrap().lease;
    let reclaimed = state.evict_for_tenant(&tenant(), 4096).unwrap();
    assert_eq!(reclaimed, 0);
}
```

- [ ] **Step 2: Implement quota model**

Add `TenantQuota { dram_bytes, ssd_bytes, used_dram_bytes, used_ssd_bytes }`. Reserve quota in `put_start`; release in revoke/remove/evict.

- [ ] **Step 3: Implement lease map**

Use default lease TTL of 5 seconds. Store leases by `(tenant, key)`. `get_replica_list` refreshes lease.

- [ ] **Step 4: Implement eviction candidate filter**

Eviction skips:

- non-committed objects;
- active leases;
- hard-pinned objects;
- soft-pinned objects unless no other candidate is available;
- objects in write state.

- [ ] **Step 5: Verify**

Run:

```bash
cargo test -p mooncache-master quota lease eviction
```

Expected: quota and eviction tests pass.

---

### Task 6: Store Node DRAM Object Chunks

**Files:**
- Create: `crates/store/Cargo.toml`
- Create: `crates/store/src/lib.rs`
- Create: `crates/store/src/chunk.rs`
- Create: `crates/store/src/memory.rs`
- Create: `crates/store/src/checksum.rs`

**Interfaces:**
- Consumes: `ReplicaDescriptor` from Master.
- Produces: `Store`, `write_chunk`, `read_chunk`, checksum verification.

- [ ] **Step 1: Write chunk read/write tests**

```rust
#[test]
fn reads_exact_bytes_written_to_chunk() {
    let mut store = MemoryStore::with_capacity(1024);
    let handle = store.allocate(5).unwrap();
    store.write_chunk(&handle, b"hello").unwrap();
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
```

- [ ] **Step 2: Implement MemoryStore**

Use a `Vec<u8>` arena and an allocation table. This first version favors correctness over the final allocator.

- [ ] **Step 3: Implement checksum**

Use SHA-256 per chunk. Store checksum in chunk metadata.

- [ ] **Step 4: Verify**

Run:

```bash
cargo test -p mooncache-store memory checksum
```

Expected: tests pass.

---

### Task 7: SSD Cold Tier and Promotion

**Files:**
- Modify: `crates/store/src/ssd.rs`
- Modify: `crates/store/src/lib.rs`
- Add tests in: `crates/store/src/ssd.rs`

**Interfaces:**
- Consumes: committed object bytes and metadata.
- Produces: `SsdStore`, `persist_object`, `read_object`, `promote_to_dram` hook.

- [ ] **Step 1: Write SSD persistence tests**

```rust
#[tokio::test]
async fn persists_and_reads_encrypted_object() {
    let dir = tempfile::tempdir().unwrap();
    let store = SsdStore::new_for_test(dir.path()).await.unwrap();
    store.persist_object("tenant-a", "abc", b"payload").await.unwrap();
    let bytes = store.read_object("tenant-a", "abc").await.unwrap();
    assert_eq!(bytes, b"payload");
    let raw = std::fs::read(dir.path().join("tenant-a").join("abc.mcobj")).unwrap();
    assert!(!raw.windows(b"payload".len()).any(|w| w == b"payload"));
}
```

- [ ] **Step 2: Implement SSD file layout**

Path: `<root>/<tenant_id>/<cache_key>.mcobj`. Encrypt payload before writing. Use atomic temp file + rename.

- [ ] **Step 3: Implement promotion hook**

When Gateway records SSD hit, call Store `promote_if_capacity` to allocate DRAM and copy bytes back.

- [ ] **Step 4: Verify**

Run:

```bash
cargo test -p mooncache-store ssd
```

Expected: SSD tests pass.

---

### Task 8: Vendor Adapter Trait and Mock Adapter

**Files:**
- Modify: `crates/gateway/Cargo.toml`
- Create: `crates/gateway/src/vendor.rs`

**Interfaces:**
- Consumes: `ResponsesRequest`.
- Produces: `VendorAdapter`, `VendorResponse`, `VendorStreamEvent`, mock adapter for tests.

- [ ] **Step 1: Write adapter tests**

```rust
#[tokio::test]
async fn mock_adapter_returns_configured_response() {
    let adapter = MockVendorAdapter::new_json(serde_json::json!({"id":"resp_1","output_text":"hello"}));
    let response = adapter.complete(test_request()).await.unwrap();
    assert_eq!(response.body["output_text"], "hello");
}

#[tokio::test]
async fn adapter_classifies_retryable_5xx() {
    let err = VendorError::HttpStatus { status: 503, body: "busy".into() };
    assert!(err.is_retryable_before_stream_start());
}
```

- [ ] **Step 2: Implement trait**

```rust
#[async_trait::async_trait]
pub trait VendorAdapter: Send + Sync {
    fn vendor_id(&self) -> &str;
    fn adapter_version(&self) -> &str;
    async fn resolve_model_version(&self, requested_model: &str) -> Result<String, VendorError>;
    async fn complete(&self, request: ResponsesRequest) -> Result<ResponsesResponse, VendorError>;
    async fn stream(&self, request: ResponsesRequest) -> Result<VendorEventStream, VendorError>;
}
```

- [ ] **Step 3: Implement mock adapter only**

Do not implement a real vendor until Gateway flow tests pass with mock. This prevents vendor integration from hiding cache correctness bugs.

- [ ] **Step 4: Verify**

Run:

```bash
cargo test -p mooncache-gateway vendor
```

Expected: adapter tests pass.

---

### Task 9: Gateway Non-Streaming Cache Flow

**Files:**
- Modify: `crates/gateway/src/cache_flow.rs`
- Modify: `crates/gateway/src/routes.rs`
- Add integration test: `tests/integration/cache_flow.rs`

**Interfaces:**
- Consumes: fingerprinting, MasterState, Store, VendorAdapter.
- Produces: `handle_response_request` for non-streaming requests.

- [ ] **Step 1: Write miss-then-hit integration test**

```rust
#[tokio::test]
async fn non_streaming_miss_writes_cache_and_next_request_hits() {
    let app = TestCluster::new().with_mock_vendor_json(serde_json::json!({"id":"resp_1","output_text":"hello"})).await;
    let body = serde_json::json!({"model":"gpt-test","input":"hello","temperature":0});

    let first = app.post_response(body.clone()).await;
    assert_eq!(first.header("x-cache-status"), "miss");
    assert_eq!(first.json()["output_text"], "hello");

    let second = app.post_response(body).await;
    assert_eq!(second.header("x-cache-status"), "hit");
    assert_eq!(app.vendor_call_count().await, 1);
}
```

- [ ] **Step 2: Implement test cluster harness**

Create an in-process Gateway + MasterState + MemoryStore + MockVendorAdapter harness for integration tests.

- [ ] **Step 3: Implement non-streaming flow**

Flow:

1. authenticate test tenant;
2. parse cache headers;
3. compute fingerprint;
4. check eligibility;
5. query Master;
6. read Store on hit;
7. call vendor on miss;
8. write committed object on successful eligible miss;
9. return cache headers.

- [ ] **Step 4: Verify**

Run:

```bash
cargo test --test cache_flow non_streaming_miss_writes_cache_and_next_request_hits
```

Expected: test passes and mock vendor call count is 1.

---

### Task 10: Streaming Capture and Replay

**Files:**
- Modify: `crates/gateway/src/streaming.rs`
- Modify: `crates/gateway/src/cache_flow.rs`
- Add integration test: `tests/integration/streaming_flow.rs`

**Interfaces:**
- Consumes: `SseEvent`, Store object layout.
- Produces: capture of raw SSE event sequence and aggregated JSON object.

- [ ] **Step 1: Write streaming replay test**

```rust
#[tokio::test]
async fn streaming_hit_replays_stored_sse_events_without_vendor_call() {
    let events = vec![
        SseEvent { event: Some("response.output_text.delta".into()), data: "{\"delta\":\"hel\"}".into() },
        SseEvent { event: Some("response.output_text.delta".into()), data: "{\"delta\":\"lo\"}".into() },
        SseEvent { event: Some("response.completed".into()), data: "{\"id\":\"resp_1\"}".into() },
    ];
    let app = TestCluster::new().with_mock_vendor_stream(events.clone()).await;
    let body = serde_json::json!({"model":"gpt-test","input":"hello","temperature":0,"stream":true});

    let first = app.post_response_stream(body.clone()).await;
    assert_eq!(first.header("x-cache-status"), "miss");
    assert_eq!(first.events(), events);

    let second = app.post_response_stream(body).await;
    assert_eq!(second.header("x-cache-status"), "hit");
    assert_eq!(second.events(), events);
    assert_eq!(app.vendor_call_count().await, 1);
}
```

- [ ] **Step 2: Implement SSE parser and serializer**

Preserve event order and data bytes. Do not reconstruct event schemas from final text for replay.

- [ ] **Step 3: Implement aggregation**

Build final aggregated response JSON from streaming completion metadata. For first implementation, require the mock adapter to provide final aggregate; real adapters can later supply vendor-specific aggregation.

- [ ] **Step 4: Verify**

Run:

```bash
cargo test --test streaming_flow streaming_hit_replays_stored_sse_events_without_vendor_call
```

Expected: test passes and vendor call count is 1.

---

### Task 11: Singleflight Coalescing

**Files:**
- Modify: `crates/gateway/src/singleflight.rs`
- Modify: `crates/gateway/src/cache_flow.rs`

**Interfaces:**
- Consumes: fingerprint and cache flow.
- Produces: `SingleflightGroup` with leader/waiter semantics.

- [ ] **Step 1: Write concurrent miss test**

```rust
#[tokio::test]
async fn identical_concurrent_misses_share_one_vendor_call() {
    let app = TestCluster::new().with_slow_mock_vendor_json(serde_json::json!({"id":"resp_1","output_text":"hello"})).await;
    let body = serde_json::json!({"model":"gpt-test","input":"hello","temperature":0});

    let (a, b, c) = tokio::join!(
        app.post_response(body.clone()),
        app.post_response(body.clone()),
        app.post_response(body.clone()),
    );

    assert_eq!(a.json()["output_text"], "hello");
    assert_eq!(b.json()["output_text"], "hello");
    assert_eq!(c.json()["output_text"], "hello");
    assert_eq!(app.vendor_call_count().await, 1);
}
```

- [ ] **Step 2: Implement `SingleflightGroup`**

Use `DashMap<CacheKey, SharedInFlight>` and `tokio::sync::watch` or `broadcast` for result fanout. Bound waiters per key.

- [ ] **Step 3: Add cache headers**

Leader response: `X-Cache-Coalesced: leader`. Waiter response: `X-Cache-Coalesced: waiter`.

- [ ] **Step 4: Verify**

Run:

```bash
cargo test -p mooncache-gateway singleflight
cargo test --test cache_flow identical_concurrent_misses_share_one_vendor_call
```

Expected: tests pass.

---

### Task 12: Admin API, RBAC, and Audit

**Files:**
- Create: `crates/admin-api/Cargo.toml`
- Create: `crates/admin-api/src/lib.rs`
- Create: `crates/admin-api/src/routes.rs`
- Create: `crates/admin-api/src/rbac.rs`
- Create: `crates/admin-api/src/audit.rs`
- Add integration test: `tests/integration/admin_api.rs`

**Interfaces:**
- Consumes: Master management operations.
- Produces: admin routes for tenants, cache operations, nodes, vendors, audit.

- [ ] **Step 1: Write RBAC tests**

```rust
#[test]
fn operator_can_drain_node_but_viewer_cannot() {
    assert!(Role::Operator.allows(AdminAction::DrainNode));
    assert!(!Role::Viewer.allows(AdminAction::DrainNode));
}

#[test]
fn admin_can_patch_tenant_policy() {
    assert!(Role::Admin.allows(AdminAction::PatchTenantPolicy));
    assert!(!Role::Operator.allows(AdminAction::PatchTenantPolicy));
}
```

- [ ] **Step 2: Implement RBAC enum**

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role { Viewer, Operator, Admin }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminAction {
    ReadMetrics,
    DrainNode,
    RemoveCacheObject,
    WarmupCache,
    PatchTenantPolicy,
    PatchVendorPolicy,
    ManageUsers,
}
```

- [ ] **Step 3: Implement audit event append**

Audit event fields: actor, role, action, resource, tenant scope, before/after summary, request ID, timestamp, result.

- [ ] **Step 4: Implement first routes**

Start with:

- `GET /admin/nodes`
- `POST /admin/nodes/{node_id}/drain`
- `POST /admin/cache/fingerprint/debug`
- `DELETE /admin/cache/objects/{tenant_id}/{cache_key}`

- [ ] **Step 5: Verify**

Run:

```bash
cargo test -p mooncache-admin-api
cargo test --test admin_api
```

Expected: RBAC and route tests pass.

---

### Task 13: Observability and Alert Signals

**Files:**
- Modify: `crates/gateway/src/cache_flow.rs`
- Modify: `crates/master/src/state.rs`
- Modify: `crates/store/src/lib.rs`
- Create: `crates/common/src/metrics.rs`

**Interfaces:**
- Consumes: all service flows.
- Produces: Prometheus-compatible metric names and structured tracing fields.

- [ ] **Step 1: Write metric naming tests**

```rust
#[test]
fn cache_status_metric_labels_are_stable() {
    assert_eq!(CacheMetric::RequestTotal.name(), "mooncache_gateway_requests_total");
    assert_eq!(CacheStatus::Hit.as_label(), "hit");
    assert_eq!(CacheStatus::Degraded.as_label(), "degraded");
}
```

- [ ] **Step 2: Implement metric constants**

Metric families:

- `mooncache_gateway_requests_total`
- `mooncache_gateway_request_latency_seconds`
- `mooncache_gateway_vendor_calls_avoided_total`
- `mooncache_gateway_singleflight_waiters_total`
- `mooncache_master_objects_total`
- `mooncache_master_evictions_total`
- `mooncache_store_dram_bytes`
- `mooncache_store_ssd_bytes`
- `mooncache_store_read_latency_seconds`
- `mooncache_admin_audit_events_total`

- [ ] **Step 3: Add tracing fields**

Required fields: request_id, tenant_id, cache_key redacted, cache_status, vendor_id, model_version, coalesced_role, writeback_status.

- [ ] **Step 4: Verify**

Run:

```bash
cargo test -p mooncache-common metrics
cargo test --workspace
```

Expected: tests pass.

---

### Task 14: React Control Panel Skeleton and Core Pages

**Files:**
- Create: `control-panel/package.json`
- Create: `control-panel/index.html`
- Create: `control-panel/src/main.tsx`
- Create: `control-panel/src/App.tsx`
- Create: `control-panel/src/api/client.ts`
- Create: `control-panel/src/auth/rbac.ts`
- Create page files listed in File Structure.

**Interfaces:**
- Consumes: Admin API.
- Produces: accessible operations console shell and route pages.

- [ ] **Step 1: Write RBAC UI tests**

`control-panel/src/auth/rbac.test.ts`:

```typescript
import { describe, expect, it } from 'vitest'
import { canPerform } from './rbac'

it('allows operators to drain nodes but blocks viewers', () => {
  expect(canPerform('operator', 'drain-node')).toBe(true)
  expect(canPerform('viewer', 'drain-node')).toBe(false)
})

it('allows only admins to edit tenant policy', () => {
  expect(canPerform('admin', 'edit-tenant-policy')).toBe(true)
  expect(canPerform('operator', 'edit-tenant-policy')).toBe(false)
})
```

- [ ] **Step 2: Create Vite package**

`control-panel/package.json`:

```json
{
  "scripts": {
    "dev": "vite",
    "build": "tsc && vite build",
    "test": "vitest run"
  },
  "dependencies": {
    "@tanstack/react-query": "latest",
    "@vitejs/plugin-react": "latest",
    "vite": "latest",
    "typescript": "latest",
    "react": "latest",
    "react-dom": "latest"
  },
  "devDependencies": {
    "vitest": "latest",
    "jsdom": "latest",
    "@testing-library/react": "latest",
    "@testing-library/jest-dom": "latest"
  }
}
```

- [ ] **Step 3: Implement RBAC utility**

```typescript
export type Role = 'viewer' | 'operator' | 'admin'
export type Action = 'read' | 'drain-node' | 'remove-cache-object' | 'warmup-cache' | 'edit-tenant-policy' | 'edit-vendor-policy'

const permissions: Record<Role, Set<Action>> = {
  viewer: new Set(['read']),
  operator: new Set(['read', 'drain-node', 'remove-cache-object', 'warmup-cache']),
  admin: new Set(['read', 'drain-node', 'remove-cache-object', 'warmup-cache', 'edit-tenant-policy', 'edit-vendor-policy']),
}

export function canPerform(role: Role, action: Action): boolean {
  return permissions[role].has(action)
}
```

- [ ] **Step 4: Implement accessible app shell**

Use semantic navigation, visible focus styles, and text labels for icon actions. Each page starts with one `h1`.

- [ ] **Step 5: Verify**

Run:

```bash
cd control-panel && npm test
cd control-panel && npm run build
```

Expected: tests and build pass.

---

### Task 15: Control Panel Data Pages and Operations

**Files:**
- Modify all `control-panel/src/pages/*.tsx`
- Modify: `control-panel/src/api/client.ts`

**Interfaces:**
- Consumes: Admin API routes.
- Produces: Overview, Cache Analytics, Nodes, Tenants, Vendors, Cache Operations, Alerts, Audit Log pages.

- [ ] **Step 1: Write page behavior tests**

Example for node drain:

```typescript
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { Nodes } from './Nodes'

it('requires operator permission before showing drain action', () => {
  render(<Nodes role="viewer" />)
  expect(screen.queryByRole('button', { name: /drain node/i })).toBeNull()
})

it('shows drain action for operator', () => {
  render(<Nodes role="operator" />)
  expect(screen.getByRole('button', { name: /drain node/i })).toBeInTheDocument()
})
```

- [ ] **Step 2: Implement API client**

Use a typed `fetchJson<T>` wrapper that maps non-2xx responses into user-visible errors without exposing stack traces.

- [ ] **Step 3: Implement Overview and Cache Analytics**

Panels must answer operator questions:

- Is the system healthy?
- Is cache value improving?
- Where is latency or pressure coming from?
- Which tenants or nodes need action?

- [ ] **Step 4: Implement write-operation modals**

Drain, purge, warmup, and policy changes require confirmation modals. Modals must trap focus, support Escape, and restore focus to trigger.

- [ ] **Step 5: Verify**

Run:

```bash
cd control-panel && npm test
cd control-panel && npm run build
```

Expected: tests and build pass.

---

### Task 16: App Binaries and Local Dev Topology

**Files:**
- Create: `apps/gateway/Cargo.toml`
- Create: `apps/gateway/src/main.rs`
- Create: `apps/master/Cargo.toml`
- Create: `apps/master/src/main.rs`
- Create: `apps/store-node/Cargo.toml`
- Create: `apps/store-node/src/main.rs`
- Create: `apps/admin-api/Cargo.toml`
- Create: `apps/admin-api/src/main.rs`
- Create: `docker-compose.yml`

**Interfaces:**
- Consumes: crates above.
- Produces: runnable local cluster.

- [ ] **Step 1: Write binary smoke tests through `--help`**

Use command tests that assert each binary parses config and exits cleanly with `--help`.

- [ ] **Step 2: Implement config structs**

Each service reads env vars and CLI flags:

- bind address;
- etcd URL;
- tenant config path;
- SSD root path;
- metrics bind address;
- vendor config path.

- [ ] **Step 3: Implement local docker-compose**

Services:

- etcd;
- master;
- one store-node;
- gateway;
- admin-api;
- control-panel static server.

- [ ] **Step 4: Verify**

Run:

```bash
cargo test --workspace
cargo run -p mooncache-master-app -- --help
cargo run -p mooncache-store-node-app -- --help
cargo run -p mooncache-gateway-app -- --help
cargo run -p mooncache-admin-api-app -- --help
```

Expected: all commands succeed.

---

### Task 17: End-to-End Verification and Load Smoke

**Files:**
- Add: `tests/integration/e2e_cluster.rs`
- Add: `tests/integration/load_smoke.rs`

**Interfaces:**
- Consumes: full local cluster harness.
- Produces: proof that cache hit, miss, streaming, admin, and degradation paths work together.

- [ ] **Step 1: Write E2E cache behavior test**

Test one tenant, deterministic Responses request, miss then hit, assert one vendor call.

- [ ] **Step 2: Write degradation test**

Stop or disable Master/Store in the test harness. Assert default request falls back to vendor with `X-Cache-Status: degraded`. Assert `X-Cache-Control: cache-only` returns cache-only miss error instead of calling vendor.

- [ ] **Step 3: Write load smoke test**

Simulate 1,000 identical concurrent deterministic requests against in-process Gateway. Assert vendor call count is 1 and all responses succeed.

- [ ] **Step 4: Verify**

Run:

```bash
cargo test --test e2e_cluster
cargo test --test load_smoke -- --ignored
```

Expected: E2E passes; load smoke passes when run explicitly.

---

## Final Verification Gate

Before claiming implementation complete, run:

```bash
cargo fmt --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
cd control-panel && npm test
cd control-panel && npm run build
```

Expected:

- Rust format clean.
- Clippy clean.
- Rust tests pass.
- Control panel tests pass.
- Control panel production build succeeds.

## Plan Self-Review

- Spec coverage: every confirmed design decision from `docs/superpowers/specs/2026-07-03-distributed-api-response-kvcache-design.md` maps to at least one task.
- Placeholder scan: no undefined future placeholders are used as deliverables.
- Type consistency: task interfaces consistently use `TenantId`, `CacheKey`, `ResponsesRequest`, `ResponsesResponse`, `SseEvent`, `MasterState`, `MemoryStore`, `VendorAdapter`, and RBAC role/action names.
- Scope control: implementation starts with mock vendor and in-process test harness before real vendor or distributed deployment complexity.
- Verification: every task has a targeted command and expected result.
