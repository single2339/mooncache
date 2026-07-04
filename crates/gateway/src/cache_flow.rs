use std::{borrow::Cow, sync::Arc, time::Instant};

use mooncache_common::{
    CacheError, CacheKey, CacheStatus as ObservedCacheStatus,
    CacheWriteStatus as ObservedCacheWriteStatus, GatewayMetrics, GatewayMetricsSnapshot,
    SingleflightRole, TenantId,
};
use mooncache_fingerprint::{
    classify_request, compute_cache_key, EligibilityDecision, FingerprintInput,
};
use mooncache_master::{MasterState, ReplicaDescriptor, ReplicaList};
use mooncache_protocol::{CacheControl, CacheStatus, ResponsesRequest};
use mooncache_store::{ChunkHandle, MemoryStore, StoreError};
use serde_json::Value;
use thiserror::Error;

use crate::clients::{LocalMasterClient, LocalStoreClient, MasterClient, StoreClient};
use crate::config::TenantConfigSet;
use crate::routes::GatewayResponse;
use crate::singleflight::{
    SingleflightGroup, SingleflightKey, SingleflightStart, SingleflightWriteMode,
};
use crate::streaming::{self, CachedStreamObject, CapturedStream};
use crate::{VendorAdapter, VendorError, VendorResponse};

const ENDPOINT_VERSION: &str = "responses-v1";
const TEST_API_KEY: &str = "test-api-key";
const TEST_TENANT_ID: &str = "test-tenant";
const REPLICA_COUNT: usize = 1;

pub struct GatewayState {
    master_client: Arc<dyn MasterClient>,
    store_client: Arc<dyn StoreClient>,
    vendor: Arc<dyn VendorAdapter>,
    tenant_id: TenantId,
    tenant_configs: Option<TenantConfigSet>,
    singleflight: SingleflightGroup,
    metrics: GatewayMetrics,
    cache_available: bool,
}

impl GatewayState {
    #[must_use]
    pub fn new_for_test<V>(master: MasterState, store: MemoryStore, vendor: Arc<V>) -> Self
    where
        V: VendorAdapter + 'static,
    {
        let tenant_id = TenantId::parse(TEST_TENANT_ID).expect("test tenant id is valid");
        Self {
            master_client: Arc::new(LocalMasterClient::new(master)),
            store_client: Arc::new(LocalStoreClient::new(store)),
            vendor,
            tenant_id,
            tenant_configs: None,
            singleflight: SingleflightGroup::default(),
            metrics: GatewayMetrics::default(),
            cache_available: true,
        }
    }

    #[must_use]
    pub fn new_with_tenant_config<V>(
        master: MasterState,
        store: MemoryStore,
        vendor: Arc<V>,
        tenant_configs: TenantConfigSet,
    ) -> Self
    where
        V: VendorAdapter + 'static,
    {
        let tenant_id = tenant_configs
            .tenants()
            .next()
            .map(|tenant| tenant.id.clone())
            .unwrap_or_else(|| TenantId::parse(TEST_TENANT_ID).expect("test tenant id is valid"));
        Self {
            master_client: Arc::new(LocalMasterClient::new(master)),
            store_client: Arc::new(LocalStoreClient::new(store)),
            vendor,
            tenant_id,
            tenant_configs: Some(tenant_configs),
            singleflight: SingleflightGroup::default(),
            metrics: GatewayMetrics::default(),
            cache_available: true,
        }
    }

    #[must_use]
    pub fn new_with_unavailable_cache_for_test<V>(vendor: Arc<V>) -> Self
    where
        V: VendorAdapter + 'static,
    {
        let tenant_id = TenantId::parse(TEST_TENANT_ID).expect("test tenant id is valid");
        Self {
            master_client: Arc::new(LocalMasterClient::new(MasterState::new_for_test())),
            store_client: Arc::new(LocalStoreClient::new(MemoryStore::with_capacity(0))),
            vendor,
            tenant_id,
            tenant_configs: None,
            singleflight: SingleflightGroup::default(),
            metrics: GatewayMetrics::default(),
            cache_available: false,
        }
    }

    /// Construct a GatewayState with arbitrary master and store clients (for remote/testing).
    #[must_use]
    pub fn new_with_clients<V>(
        master_client: Arc<dyn MasterClient>,
        store_client: Arc<dyn StoreClient>,
        vendor: Arc<V>,
    ) -> Self
    where
        V: VendorAdapter + 'static,
    {
        let tenant_id = TenantId::parse(TEST_TENANT_ID).expect("test tenant id is valid");
        Self {
            master_client,
            store_client,
            vendor,
            tenant_id,
            tenant_configs: None,
            singleflight: SingleflightGroup::default(),
            metrics: GatewayMetrics::default(),
            cache_available: true,
        }
    }

    fn authenticate(&self, authorization: Option<&str>) -> Option<TenantId> {
        let token = authorization?.strip_prefix("Bearer ")?;
        if let Some(configs) = &self.tenant_configs {
            return configs
                .tenant_for_bearer_token(token)
                .map(|tenant| tenant.id.clone());
        }
        (token == TEST_API_KEY).then(|| self.tenant_id.clone())
    }

    fn tenant_allows_vendor(&self, tenant_id: &TenantId, vendor_id: &str) -> bool {
        let Some(configs) = &self.tenant_configs else {
            return true;
        };
        configs
            .tenant(tenant_id.as_str())
            .is_some_and(|tenant| tenant.allowed_vendors.iter().any(|id| id == vendor_id))
    }

    async fn master_get_replica_list(
        &self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
    ) -> Result<ReplicaList, GatewayError> {
        self.master_client
            .get_replica_list(tenant_id, cache_key)
            .await
    }

    async fn master_put_start(
        &self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
        len: u64,
        replica_count: usize,
    ) -> Result<Vec<ReplicaDescriptor>, GatewayError> {
        self.master_client
            .put_start(tenant_id, cache_key, len, replica_count)
            .await
    }

    async fn master_put_end(
        &self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
    ) -> Result<(), GatewayError> {
        self.master_client.put_end(tenant_id, cache_key).await
    }

    async fn master_put_revoke(
        &self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
    ) -> Result<(), GatewayError> {
        self.master_client.put_revoke(tenant_id, cache_key).await
    }

    async fn store_read_chunk(
        &self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
        handle: &ChunkHandle,
    ) -> Result<crate::clients::StoreChunk, GatewayError> {
        self.store_client
            .read_chunk(tenant_id, cache_key, handle)
            .await
    }

    async fn store_write_preallocated_chunk(
        &self,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
        handle: &ChunkHandle,
        bytes: &[u8],
    ) -> Result<(), GatewayError> {
        self.store_client
            .write_preallocated_chunk(tenant_id, cache_key, handle, bytes)
            .await
    }

    fn cache_available(&self) -> bool {
        self.cache_available
    }

    #[must_use]
    pub fn metrics_snapshot(&self) -> GatewayMetricsSnapshot {
        self.metrics.snapshot()
    }
}

#[derive(Debug, Error)]
pub enum GatewayError {
    #[error(transparent)]
    Cache(#[from] CacheError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Vendor(#[from] VendorError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("gateway state lock was poisoned")]
    PoisonedLock,
    #[error("cache write failed ({write_error}); additionally failed to revoke reservation ({revoke_error})")]
    ReservationRevokeFailed {
        write_error: String,
        revoke_error: String,
    },
    #[error("singleflight leader failed: {0}")]
    SingleflightLeaderFailed(String),
    #[error("master upstream error: {0}")]
    MasterUpstream(String),
    #[error("store upstream error: {0}")]
    StoreUpstream(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CacheWriteStatus {
    Committed,
    Skipped,
    Failed,
}

pub async fn handle_response_request(
    state: &GatewayState,
    authorization: Option<&str>,
    cache_control: Option<&str>,
    body: Value,
) -> Result<GatewayResponse, GatewayError> {
    let record_simple = |status, write_status| {
        record_gateway_metrics(state, status, write_status);
        state.metrics.record_singleflight(SingleflightRole::None);
    };
    let Some(tenant_id) = state.authenticate(authorization) else {
        record_simple(CacheStatus::Bypass, CacheWriteStatus::Skipped);
        return Ok(GatewayResponse::error(
            401,
            "unauthorized",
            CacheStatus::Bypass,
            CacheWriteStatus::Skipped.as_header_value(),
            "none",
        ));
    };
    if !state.tenant_allows_vendor(&tenant_id, state.vendor.vendor_id()) {
        record_simple(CacheStatus::Bypass, CacheWriteStatus::Skipped);
        return Ok(GatewayResponse::error(
            403,
            "tenant is not allowed to use configured vendor",
            CacheStatus::Bypass,
            CacheWriteStatus::Skipped.as_header_value(),
            "none",
        ));
    }

    let cache_control = match CacheControl::parse(cache_control.unwrap_or_default()) {
        Ok(control) => control,
        Err(err) => {
            record_simple(CacheStatus::Bypass, CacheWriteStatus::Skipped);
            return Ok(GatewayResponse::error(
                400,
                err.to_string(),
                CacheStatus::Bypass,
                CacheWriteStatus::Skipped.as_header_value(),
                "none",
            ));
        }
    };

    let is_streaming = body.get("stream").and_then(Value::as_bool).unwrap_or(false);

    let vendor_request: ResponsesRequest = serde_json::from_value(body.clone())?;
    let resolved_model = state
        .vendor
        .resolve_model_version(&vendor_request.model)
        .await?;
    let force_replay = matches!(cache_control, CacheControl::ForceReplay);
    let eligibility = if state.vendor.model_cache_eligible(&vendor_request.model) {
        classify_request(&body, force_replay)
    } else {
        EligibilityDecision::Bypass {
            reason: "model is not cache eligible".to_owned(),
        }
    };
    let fingerprint_body = fingerprint_body_without_stream(&body);
    let cache_key = compute_cache_key(&FingerprintInput {
        tenant_id: &tenant_id,
        endpoint_version: ENDPOINT_VERSION,
        vendor_id: state.vendor.vendor_id(),
        resolved_model_version: &resolved_model,
        adapter_version: state.vendor.adapter_version(),
        cache_policy: fingerprint_policy(cache_control),
        body: &fingerprint_body,
    })?;

    if is_streaming {
        return streaming_response(
            state,
            tenant_id,
            cache_control,
            vendor_request,
            eligibility,
            cache_key,
        )
        .await;
    }

    if matches!(cache_control, CacheControl::Bypass) {
        return vendor_response(
            state,
            vendor_request,
            CacheStatus::Bypass,
            CacheWriteStatus::Skipped,
            None,
        )
        .await;
    }

    if let EligibilityDecision::Bypass { .. } = eligibility {
        return vendor_response(
            state,
            vendor_request,
            CacheStatus::Ineligible,
            CacheWriteStatus::Skipped,
            Some(cache_key),
        )
        .await;
    }

    if !state.cache_available() {
        if matches!(cache_control, CacheControl::CacheOnly) {
            record_simple(CacheStatus::CacheOnlyMiss, CacheWriteStatus::Skipped);
            return Ok(GatewayResponse::error(
                404,
                "cache object not found",
                CacheStatus::CacheOnlyMiss,
                CacheWriteStatus::Skipped.as_header_value(),
                "none",
            )
            .with_cache_key(&cache_key));
        }

        return vendor_response(
            state,
            vendor_request,
            CacheStatus::Degraded,
            CacheWriteStatus::Failed,
            Some(cache_key),
        )
        .await;
    }

    if !matches!(cache_control, CacheControl::WriteOnly) {
        if let Some(cached_body) = read_cached_body(state, &tenant_id, &cache_key).await? {
            record_simple(CacheStatus::Hit, CacheWriteStatus::Skipped);
            return Ok(GatewayResponse::ok(
                cached_body.value,
                CacheStatus::Hit,
                CacheWriteStatus::Skipped.as_header_value(),
                &cached_body.tier,
                Some(&cache_key),
            ));
        }
    }

    if matches!(cache_control, CacheControl::CacheOnly) {
        record_simple(CacheStatus::CacheOnlyMiss, CacheWriteStatus::Skipped);
        return Ok(GatewayResponse::error(
            404,
            "cache object not found",
            CacheStatus::CacheOnlyMiss,
            CacheWriteStatus::Skipped.as_header_value(),
            "none",
        )
        .with_cache_key(&cache_key));
    }

    let singleflight_key = SingleflightKey::new(
        tenant_id.clone(),
        cache_key.clone(),
        singleflight_write_mode(cache_control),
    );
    match state
        .singleflight
        .begin(singleflight_key)
        .map_err(|_| GatewayError::PoisonedLock)?
    {
        SingleflightStart::Leader(leader) => {
            state.metrics.record_singleflight(SingleflightRole::Leader);
            let result = cacheable_non_streaming_miss(
                state,
                tenant_id,
                cache_control,
                vendor_request,
                cache_key,
            )
            .await;
            match result {
                Ok(response) => {
                    state.singleflight.publish(leader, Ok(response.clone()));
                    Ok(response.with_cache_coalesced("leader"))
                }
                Err(err) => {
                    state.singleflight.publish(leader, Err(err.to_string()));
                    Err(err)
                }
            }
        }
        SingleflightStart::Waiter(waiter) => match waiter.wait().await {
            Ok(response) => {
                state.metrics.record_singleflight(SingleflightRole::Waiter);
                record_gateway_metrics(state, CacheStatus::Miss, CacheWriteStatus::Skipped);
                Ok(response
                    .with_cache_coalesced("waiter")
                    .with_cache_write(CacheWriteStatus::Skipped.as_header_value()))
            }
            Err(message) => Err(GatewayError::SingleflightLeaderFailed(message)),
        },
        SingleflightStart::OverCapacity => {
            state
                .metrics
                .record_singleflight(SingleflightRole::OverCapacity);
            cacheable_non_streaming_miss(state, tenant_id, cache_control, vendor_request, cache_key)
                .await
        }
    }
}

async fn cacheable_non_streaming_miss(
    state: &GatewayState,
    tenant_id: TenantId,
    cache_control: CacheControl,
    request: ResponsesRequest,
    cache_key: CacheKey,
) -> Result<GatewayResponse, GatewayError> {
    let vendor_response = complete_vendor(state, request).await?;
    let write_status = if matches!(cache_control, CacheControl::ReadOnly) {
        CacheWriteStatus::Skipped
    } else {
        write_cached_body(state, &tenant_id, &cache_key, &vendor_response.body)
            .await
            .unwrap_or(CacheWriteStatus::Failed)
    };
    record_gateway_metrics(state, CacheStatus::Miss, write_status);

    Ok(GatewayResponse::ok(
        vendor_response.body,
        CacheStatus::Miss,
        write_status.as_header_value(),
        "vendor",
        Some(&cache_key),
    ))
}

async fn streaming_response(
    state: &GatewayState,
    tenant_id: TenantId,
    cache_control: CacheControl,
    request: ResponsesRequest,
    eligibility: EligibilityDecision,
    cache_key: CacheKey,
) -> Result<GatewayResponse, GatewayError> {
    if matches!(cache_control, CacheControl::Bypass) {
        return vendor_stream_response(
            state,
            request,
            CacheStatus::Bypass,
            CacheWriteStatus::Skipped,
            None,
        )
        .await;
    }

    if let EligibilityDecision::Bypass { .. } = eligibility {
        return vendor_stream_response(
            state,
            request,
            CacheStatus::Ineligible,
            CacheWriteStatus::Skipped,
            Some(cache_key),
        )
        .await;
    }

    if !matches!(cache_control, CacheControl::WriteOnly) {
        if let Some(cached_stream) =
            read_cached_stream_object(state, &tenant_id, &cache_key).await?
        {
            record_gateway_metrics(state, CacheStatus::Hit, CacheWriteStatus::Skipped);
            state.metrics.record_singleflight(SingleflightRole::None);
            return Ok(GatewayResponse::ok_stream(
                cached_stream.value.final_body,
                cached_stream.value.events,
                CacheStatus::Hit,
                CacheWriteStatus::Skipped.as_header_value(),
                &cached_stream.tier,
                Some(&cache_key),
            ));
        }
    }

    if matches!(cache_control, CacheControl::CacheOnly) {
        record_gateway_metrics(state, CacheStatus::CacheOnlyMiss, CacheWriteStatus::Skipped);
        state.metrics.record_singleflight(SingleflightRole::None);
        return Ok(GatewayResponse::error(
            404,
            "cache object not found",
            CacheStatus::CacheOnlyMiss,
            CacheWriteStatus::Skipped.as_header_value(),
            "none",
        )
        .with_cache_key(&cache_key));
    }

    let started_at = Instant::now();
    let stream = state.vendor.stream(request).await?;
    let captured = streaming::collect_stream(stream).await?;
    state.metrics.record_vendor_call(started_at.elapsed());
    let write_status = if matches!(cache_control, CacheControl::ReadOnly) {
        CacheWriteStatus::Skipped
    } else {
        write_cached_stream_object(state, &tenant_id, &cache_key, &captured)
            .await
            .unwrap_or(CacheWriteStatus::Failed)
    };
    record_gateway_metrics(state, CacheStatus::Miss, write_status);
    state.metrics.record_singleflight(SingleflightRole::None);

    Ok(GatewayResponse::ok_stream(
        captured.final_body,
        captured.events,
        CacheStatus::Miss,
        write_status.as_header_value(),
        "vendor",
        Some(&cache_key),
    ))
}

async fn vendor_stream_response(
    state: &GatewayState,
    request: ResponsesRequest,
    cache_status: CacheStatus,
    write_status: CacheWriteStatus,
    cache_key: Option<CacheKey>,
) -> Result<GatewayResponse, GatewayError> {
    let started_at = Instant::now();
    let stream = state.vendor.stream(request).await?;
    let captured = streaming::collect_stream(stream).await?;
    state.metrics.record_vendor_call(started_at.elapsed());
    record_gateway_metrics(state, cache_status, write_status);
    state.metrics.record_singleflight(SingleflightRole::None);
    Ok(GatewayResponse::ok_stream(
        captured.final_body,
        captured.events,
        cache_status,
        write_status.as_header_value(),
        "vendor",
        cache_key.as_ref(),
    ))
}

async fn vendor_response(
    state: &GatewayState,
    request: ResponsesRequest,
    cache_status: CacheStatus,
    write_status: CacheWriteStatus,
    cache_key: Option<CacheKey>,
) -> Result<GatewayResponse, GatewayError> {
    let response = complete_vendor(state, request).await?;
    record_gateway_metrics(state, cache_status, write_status);
    state.metrics.record_singleflight(SingleflightRole::None);
    Ok(GatewayResponse::ok(
        response.body,
        cache_status,
        write_status.as_header_value(),
        "vendor",
        cache_key.as_ref(),
    ))
}

struct CachedPayload<T> {
    value: T,
    tier: String,
}

async fn read_cached_body(
    state: &GatewayState,
    tenant_id: &TenantId,
    cache_key: &CacheKey,
) -> Result<Option<CachedPayload<Value>>, GatewayError> {
    let Some(chunk) = read_cached_bytes(state, tenant_id, cache_key).await? else {
        return Ok(None);
    };
    Ok(Some(CachedPayload {
        value: streaming::cached_body_from_bytes(&chunk.bytes)?,
        tier: chunk.tier,
    }))
}

async fn read_cached_stream_object(
    state: &GatewayState,
    tenant_id: &TenantId,
    cache_key: &CacheKey,
) -> Result<Option<CachedPayload<CachedStreamObject>>, GatewayError> {
    let Some(chunk) = read_cached_bytes(state, tenant_id, cache_key).await? else {
        return Ok(None);
    };
    let Some(value) = streaming::stream_object_from_bytes(&chunk.bytes)? else {
        return Ok(None);
    };
    Ok(Some(CachedPayload {
        value,
        tier: chunk.tier,
    }))
}

async fn read_cached_bytes(
    state: &GatewayState,
    tenant_id: &TenantId,
    cache_key: &CacheKey,
) -> Result<Option<crate::clients::StoreChunk>, GatewayError> {
    let replica = match state.master_get_replica_list(tenant_id, cache_key).await {
        Ok(replica_list) => replica_list.replicas.into_iter().next(),
        Err(GatewayError::Cache(CacheError::NotFound)) => return Ok(None),
        Err(err) => return Err(err),
    };

    let Some(replica) = replica else {
        return Ok(None);
    };
    let handle = ChunkHandle::from_replica(&replica)?;
    state
        .store_read_chunk(tenant_id, cache_key, &handle)
        .await
        .map(Some)
}

async fn write_cached_body(
    state: &GatewayState,
    tenant_id: &TenantId,
    cache_key: &CacheKey,
    body: &Value,
) -> Result<CacheWriteStatus, GatewayError> {
    let bytes = serde_json::to_vec(body)?;
    write_cached_bytes(state, tenant_id, cache_key, &bytes).await
}

async fn write_cached_stream_object(
    state: &GatewayState,
    tenant_id: &TenantId,
    cache_key: &CacheKey,
    captured: &CapturedStream,
) -> Result<CacheWriteStatus, GatewayError> {
    let bytes = streaming::serialize_stream_object(captured)?;
    write_cached_bytes(state, tenant_id, cache_key, &bytes).await
}

async fn write_cached_bytes(
    state: &GatewayState,
    tenant_id: &TenantId,
    cache_key: &CacheKey,
    bytes: &[u8],
) -> Result<CacheWriteStatus, GatewayError> {
    let len = u64::try_from(bytes.len()).map_err(|_| {
        CacheError::Conflict("response body is too large to cache on this platform".to_owned())
    })?;

    let replicas = state
        .master_put_start(tenant_id, cache_key, len, REPLICA_COUNT)
        .await?;

    if let Err(err) = write_reserved_replicas(state, tenant_id, cache_key, &replicas, bytes).await {
        return Err(revoke_reservation_after_write_failure(state, tenant_id, cache_key, err).await);
    }

    if let Err(err) = state.master_put_end(tenant_id, cache_key).await {
        return Err(revoke_reservation_after_write_failure(state, tenant_id, cache_key, err).await);
    }
    Ok(CacheWriteStatus::Committed)
}

async fn write_reserved_replicas(
    state: &GatewayState,
    tenant_id: &TenantId,
    cache_key: &CacheKey,
    replicas: &[ReplicaDescriptor],
    bytes: &[u8],
) -> Result<(), GatewayError> {
    for replica in replicas {
        let handle = ChunkHandle::from_replica(replica)?;
        state
            .store_write_preallocated_chunk(tenant_id, cache_key, &handle, bytes)
            .await?;
    }
    Ok(())
}

async fn revoke_reservation_after_write_failure(
    state: &GatewayState,
    tenant_id: &TenantId,
    cache_key: &CacheKey,
    write_error: GatewayError,
) -> GatewayError {
    match state.master_put_revoke(tenant_id, cache_key).await {
        Ok(()) => write_error,
        Err(revoke_error) => GatewayError::ReservationRevokeFailed {
            write_error: write_error.to_string(),
            revoke_error: revoke_error.to_string(),
        },
    }
}

impl CacheWriteStatus {
    fn as_header_value(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::Skipped => "skipped",
            Self::Failed => "failed",
        }
    }

    fn as_metrics_status(self) -> ObservedCacheWriteStatus {
        match self {
            Self::Committed => ObservedCacheWriteStatus::Committed,
            Self::Skipped => ObservedCacheWriteStatus::Skipped,
            Self::Failed => ObservedCacheWriteStatus::Failed,
        }
    }
}

async fn complete_vendor(
    state: &GatewayState,
    request: ResponsesRequest,
) -> Result<VendorResponse, GatewayError> {
    let started_at = Instant::now();
    let response = state.vendor.complete(request).await;
    state.metrics.record_vendor_call(started_at.elapsed());
    response.map_err(GatewayError::from)
}

fn record_gateway_metrics(
    state: &GatewayState,
    cache_status: CacheStatus,
    write_status: CacheWriteStatus,
) {
    state
        .metrics
        .record_cache_status(observed_cache_status(cache_status));
    state.metrics.record_write(write_status.as_metrics_status());
}

fn observed_cache_status(status: CacheStatus) -> ObservedCacheStatus {
    match status {
        CacheStatus::Hit => ObservedCacheStatus::Hit,
        CacheStatus::Miss => ObservedCacheStatus::Miss,
        CacheStatus::Bypass => ObservedCacheStatus::Bypass,
        CacheStatus::Ineligible => ObservedCacheStatus::Ineligible,
        CacheStatus::CacheOnlyMiss => ObservedCacheStatus::CacheOnlyMiss,
        CacheStatus::Degraded => ObservedCacheStatus::Degraded,
    }
}

fn fingerprint_body_without_stream(body: &Value) -> Cow<'_, Value> {
    let Value::Object(object) = body else {
        return Cow::Borrowed(body);
    };

    if !object.contains_key("stream") {
        return Cow::Borrowed(body);
    }

    let mut normalized = object.clone();
    normalized.remove("stream");
    Cow::Owned(Value::Object(normalized))
}

fn singleflight_write_mode(cache_control: CacheControl) -> SingleflightWriteMode {
    if matches!(cache_control, CacheControl::ReadOnly) {
        SingleflightWriteMode::ReadOnly
    } else {
        SingleflightWriteMode::Writable
    }
}

fn fingerprint_policy(cache_control: CacheControl) -> &'static str {
    if matches!(cache_control, CacheControl::ForceReplay) {
        "force-replay"
    } else {
        "default"
    }
}

pub(crate) fn cache_status_header(cache_status: CacheStatus) -> &'static str {
    match cache_status {
        CacheStatus::Hit => "hit",
        CacheStatus::Miss => "miss",
        CacheStatus::Bypass => "bypass",
        CacheStatus::Ineligible => "ineligible",
        CacheStatus::CacheOnlyMiss => "cache-only-miss",
        CacheStatus::Degraded => "degraded",
    }
}
