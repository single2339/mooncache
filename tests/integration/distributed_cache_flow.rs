use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use mooncache_gateway::clients::{
    MasterClient, RemoteMasterClient, RemoteStoreClient, StoreClient,
};
use mooncache_gateway::{
    handle_response_request, GatewayRequest, GatewayState, MockVendorAdapter, VendorAdapter,
    VendorError, VendorEventStream, VendorResponse,
};
use mooncache_protocol::ResponsesRequest;
use parking_lot::Mutex;
use serde_json::{json, Value};
use tokio::net::TcpListener;

// ── Counting vendor adapter (same pattern as existing tests) ──────

struct CountingVendor {
    inner: MockVendorAdapter,
    calls: Arc<AtomicUsize>,
}

impl CountingVendor {
    fn new(inner: MockVendorAdapter, calls: Arc<AtomicUsize>) -> Self {
        Self { inner, calls }
    }
}

#[async_trait]
impl VendorAdapter for CountingVendor {
    fn vendor_id(&self) -> &str {
        self.inner.vendor_id()
    }

    fn adapter_version(&self) -> &str {
        self.inner.adapter_version()
    }

    async fn resolve_model_version(&self, requested_model: &str) -> Result<String, VendorError> {
        self.inner.resolve_model_version(requested_model).await
    }

    async fn complete(&self, request: ResponsesRequest) -> Result<VendorResponse, VendorError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.complete(request).await
    }

    async fn stream(&self, request: ResponsesRequest) -> Result<VendorEventStream, VendorError> {
        self.inner.stream(request).await
    }
}

// ── Test helpers ──────────────────────────────────────────────────

/// Start an axum server on a random port, returning (bound_address, join_handle).
async fn start_server(router: Router) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind should succeed");
    let addr = listener.local_addr().expect("local_addr should succeed");
    let handle = tokio::spawn(async move {
        axum::serve(listener, router)
            .await
            .expect("server should run");
    });
    (addr, handle)
}

/// Start the MoonCache master app on a random port.
/// Returns (master_url, join_handle).
async fn start_master_server() -> (String, tokio::task::JoinHandle<()>) {
    use mooncache_master::MasterState;

    let mut state = MasterState::new_for_test();
    state.mount_segment("store-0", 1_048_576);
    let _ = state.set_tenant_quota("test-tenant", 1_048_576, 0);
    let app_state = Arc::new(Mutex::new(state));

    let router = axum::Router::new()
        .route(
            "/healthz",
            axum::routing::get(|| async {
                axum::Json(json!({"ok": true, "service": "mooncache-master"}))
            }),
        )
        .route(
            "/metrics/snapshot",
            axum::routing::get({
                let state = Arc::clone(&app_state);
                move || {
                    let state = Arc::clone(&state);
                    async move {
                        let snapshot = state.lock().observability_snapshot();
                        axum::Json(serde_json::to_value(snapshot).unwrap())
                    }
                }
            }),
        )
        .route(
            "/objects/start",
            axum::routing::post({
                let state = Arc::clone(&app_state);
                move |axum::Json(body): axum::Json<Value>| {
                    let state = Arc::clone(&state);
                    async move {
                        let tenant_id = mooncache_common::TenantId::parse(
                            body["tenant_id"].as_str().unwrap_or(""),
                        );
                        let cache_key = mooncache_common::CacheKey::from_hex(
                            body["cache_key"].as_str().unwrap_or(""),
                        );
                        let len = body["len"].as_u64().unwrap_or(0);
                        let replica_count = body["replica_count"].as_u64().unwrap_or(1) as usize;

                        let tenant_id = match tenant_id {
                            Ok(id) => id,
                            Err(err) => {
                                return (
                                    axum::http::StatusCode::BAD_REQUEST,
                                    axum::Json(json!({"error": err.to_string()})),
                                );
                            }
                        };
                        let cache_key = match cache_key {
                            Ok(key) => key,
                            Err(err) => {
                                return (
                                    axum::http::StatusCode::BAD_REQUEST,
                                    axum::Json(json!({"error": err.to_string()})),
                                );
                            }
                        };
                        match state
                            .lock()
                            .put_start(&tenant_id, &cache_key, len, replica_count)
                        {
                            Ok(replicas) => (
                                axum::http::StatusCode::OK,
                                axum::Json(json!({"replicas": replicas})),
                            ),
                            Err(err) => (
                                axum::http::StatusCode::CONFLICT,
                                axum::Json(json!({"error": err.to_string()})),
                            ),
                        }
                    }
                }
            }),
        )
        .route(
            "/objects/end",
            axum::routing::post({
                let state = Arc::clone(&app_state);
                move |axum::Json(body): axum::Json<Value>| {
                    let state = Arc::clone(&state);
                    async move {
                        let tenant_id = match mooncache_common::TenantId::parse(
                            body["tenant_id"].as_str().unwrap_or(""),
                        ) {
                            Ok(id) => id,
                            Err(err) => {
                                return (
                                    axum::http::StatusCode::BAD_REQUEST,
                                    axum::Json(json!({"error": err.to_string()})),
                                );
                            }
                        };
                        let cache_key = match mooncache_common::CacheKey::from_hex(
                            body["cache_key"].as_str().unwrap_or(""),
                        ) {
                            Ok(key) => key,
                            Err(err) => {
                                return (
                                    axum::http::StatusCode::BAD_REQUEST,
                                    axum::Json(json!({"error": err.to_string()})),
                                );
                            }
                        };
                        match state.lock().put_end(&tenant_id, &cache_key) {
                            Ok(()) => (axum::http::StatusCode::OK, axum::Json(json!({"ok": true}))),
                            Err(err) => (
                                axum::http::StatusCode::CONFLICT,
                                axum::Json(json!({"error": err.to_string()})),
                            ),
                        }
                    }
                }
            }),
        )
        .route(
            "/objects/revoke",
            axum::routing::post({
                let state = Arc::clone(&app_state);
                move |axum::Json(body): axum::Json<Value>| {
                    let state = Arc::clone(&state);
                    async move {
                        let tenant_id = match mooncache_common::TenantId::parse(
                            body["tenant_id"].as_str().unwrap_or(""),
                        ) {
                            Ok(id) => id,
                            Err(err) => {
                                return (
                                    axum::http::StatusCode::BAD_REQUEST,
                                    axum::Json(json!({"error": err.to_string()})),
                                );
                            }
                        };
                        let cache_key = match mooncache_common::CacheKey::from_hex(
                            body["cache_key"].as_str().unwrap_or(""),
                        ) {
                            Ok(key) => key,
                            Err(err) => {
                                return (
                                    axum::http::StatusCode::BAD_REQUEST,
                                    axum::Json(json!({"error": err.to_string()})),
                                );
                            }
                        };
                        match state.lock().put_revoke(&tenant_id, &cache_key) {
                            Ok(()) => (axum::http::StatusCode::OK, axum::Json(json!({"ok": true}))),
                            Err(err) => (
                                axum::http::StatusCode::CONFLICT,
                                axum::Json(json!({"error": err.to_string()})),
                            ),
                        }
                    }
                }
            }),
        )
        .route(
            "/objects/replicas",
            axum::routing::get({
                let state = Arc::clone(&app_state);
                move |axum::extract::Query(params): axum::extract::Query<
                    HashMap<String, String>,
                >| {
                    let state = Arc::clone(&state);
                    async move {
                        let tenant_id = match mooncache_common::TenantId::parse(
                            params.get("tenant_id").map(String::as_str).unwrap_or(""),
                        ) {
                            Ok(id) => id,
                            Err(err) => {
                                return (
                                    axum::http::StatusCode::BAD_REQUEST,
                                    axum::Json(json!({"error": err.to_string()})),
                                );
                            }
                        };
                        let cache_key = match mooncache_common::CacheKey::from_hex(
                            params.get("cache_key").map(String::as_str).unwrap_or(""),
                        ) {
                            Ok(key) => key,
                            Err(err) => {
                                return (
                                    axum::http::StatusCode::BAD_REQUEST,
                                    axum::Json(json!({"error": err.to_string()})),
                                );
                            }
                        };
                        match state.lock().get_replica_list(&tenant_id, &cache_key) {
                            Ok(replica_list) => (
                                axum::http::StatusCode::OK,
                                axum::Json(serde_json::to_value(replica_list).unwrap()),
                            ),
                            Err(err) => (
                                axum::http::StatusCode::NOT_FOUND,
                                axum::Json(json!({"error": err.to_string()})),
                            ),
                        }
                    }
                }
            }),
        )
        .route(
            "/segments/mount",
            axum::routing::post({
                let state = Arc::clone(&app_state);
                move |axum::Json(body): axum::Json<Value>| {
                    let state = Arc::clone(&state);
                    async move {
                        let node_id = body["node_id"].as_str().unwrap_or("").to_string();
                        let len = body["len"].as_u64().unwrap_or(0);
                        state.lock().mount_segment(&node_id, len);
                        (axum::http::StatusCode::OK, axum::Json(json!({"ok": true})))
                    }
                }
            }),
        )
        .route(
            "/tenants/quota",
            axum::routing::post({
                let state = Arc::clone(&app_state);
                move |axum::Json(body): axum::Json<Value>| {
                    let state = Arc::clone(&state);
                    async move {
                        let tenant_id = body["tenant_id"].as_str().unwrap_or("");
                        let dram = body["dram_bytes"].as_u64().unwrap_or(0);
                        let ssd = body["ssd_bytes"].as_u64().unwrap_or(0);
                        match state.lock().set_tenant_quota(tenant_id, dram, ssd) {
                            Ok(()) => (axum::http::StatusCode::OK, axum::Json(json!({"ok": true}))),
                            Err(err) => (
                                axum::http::StatusCode::BAD_REQUEST,
                                axum::Json(json!({"error": err.to_string()})),
                            ),
                        }
                    }
                }
            }),
        );

    let (addr, handle) = start_server(router).await;
    (format!("http://{}", addr), handle)
}

/// Start the MoonCache store-node app on a random port.
/// Returns (store_url, join_handle).
async fn start_store_server() -> (String, tokio::task::JoinHandle<()>) {
    use mooncache_store::{ChunkHandle, MemoryStore};

    let store = Arc::new(Mutex::new(MemoryStore::with_capacity(1_048_576)));

    let router = axum::Router::new()
        .route(
            "/healthz",
            axum::routing::get(|| async {
                axum::Json(json!({"ok": true, "service": "mooncache-store-node"}))
            }),
        )
        .route(
            "/chunks/{offset}/{len}",
            axum::routing::get({
                let store = Arc::clone(&store);
                move |axum::extract::Path((offset, len)): axum::extract::Path<(usize, usize)>| {
                    let store = Arc::clone(&store);
                    async move {
                        let handle = ChunkHandle::new(offset, len);
                        let store = store.lock();
                        match store.read_chunk(&handle) {
                            Ok(data) => (
                                axum::http::StatusCode::OK,
                                axum::Json(json!({"offset": offset, "len": len, "data": data})),
                            ),
                            Err(err) => (
                                axum::http::StatusCode::NOT_FOUND,
                                axum::Json(json!({"error": err.to_string()})),
                            ),
                        }
                    }
                }
            }),
        )
        .route(
            "/chunks/preallocated",
            axum::routing::post({
                let store = Arc::clone(&store);
                move |axum::Json(body): axum::Json<Value>| {
                    let store = Arc::clone(&store);
                    async move {
                        let offset = body["offset"].as_u64().unwrap_or(0) as usize;
                        let len = body["len"].as_u64().unwrap_or(0) as usize;
                        let data: Vec<u8> =
                            serde_json::from_value(body["data"].clone()).unwrap_or_default();
                        let handle = ChunkHandle::new(offset, len);
                        let mut store = store.lock();
                        match store.write_preallocated_chunk(&handle, &data) {
                            Ok(()) => (
                                axum::http::StatusCode::OK,
                                axum::Json(json!({"ok": true, "offset": offset, "len": len})),
                            ),
                            Err(err) => (
                                axum::http::StatusCode::BAD_REQUEST,
                                axum::Json(json!({"error": err.to_string()})),
                            ),
                        }
                    }
                }
            }),
        );

    let (addr, handle) = start_server(router).await;
    (format!("http://{}", addr), handle)
}

// ── Tests ─────────────────────────────────────────────────────────

#[tokio::test]
async fn distributed_miss_writes_cache_and_next_request_hits() {
    // Start real master and store servers on random ports.
    let (master_url, _master_handle) = start_master_server().await;
    let (store_url, _store_handle) = start_store_server().await;

    // Create remote clients.
    let master_client: Arc<dyn MasterClient> =
        Arc::new(RemoteMasterClient::new(master_url.clone()));

    // Mount a segment and set quota via the remote master client.
    // We need to extract admin methods. Since RemoteMasterClient has mount_segment
    // and set_tenant_quota as inherent methods, we need to downcast.
    // For simplicity, call them directly via HTTP.
    {
        let client = reqwest::Client::new();
        let _: serde_json::Value = client
            .post(format!("{}/segments/mount", master_url))
            .json(&json!({"node_id": "store-0", "len": 1_048_576}))
            .send()
            .await
            .expect("mount should succeed")
            .json()
            .await
            .expect("mount response should parse");
        let _: serde_json::Value = client
            .post(format!("{}/tenants/quota", master_url))
            .json(&json!({"tenant_id": "test-tenant", "dram_bytes": 1_048_576, "ssd_bytes": 0}))
            .send()
            .await
            .expect("quota should succeed")
            .json()
            .await
            .expect("quota response should parse");
    }

    let mut node_urls = HashMap::new();
    node_urls.insert("store-0".to_string(), store_url.clone());
    let store_client: Arc<dyn StoreClient> = Arc::new(RemoteStoreClient::new(node_urls));

    // Set up the vendor with a counting wrapper.
    let vendor_calls = Arc::new(AtomicUsize::new(0));
    let vendor = Arc::new(CountingVendor::new(
        MockVendorAdapter::new_json(
            json!({"id": "resp_dist_1", "output_text": "distributed hello"}),
        ),
        Arc::clone(&vendor_calls),
    ));

    let state = Arc::new(GatewayState::new_with_clients(
        master_client,
        store_client,
        vendor,
    ));

    let request_body = json!({"model": "gpt-test", "input": "hello distributed", "temperature": 0});

    // First request: cache miss → vendor call → write to master/store.
    let miss_response = handle_response_request(
        &state,
        GatewayRequest {
            authorization: Some("Bearer test-api-key".to_string()),
            cache_control: None,
            body: request_body.clone(),
        },
    )
    .await
    .expect("first request should succeed");

    assert_eq!(miss_response.status_code, 200);
    assert_eq!(
        miss_response
            .headers
            .get("x-cache-status")
            .map(String::as_str),
        Some("miss")
    );
    assert_eq!(miss_response.body["output_text"], "distributed hello");
    assert_eq!(vendor_calls.load(Ordering::SeqCst), 1);

    // Second request: cache hit → no vendor call, reads from store.
    let hit_response = handle_response_request(
        &state,
        GatewayRequest {
            authorization: Some("Bearer test-api-key".to_string()),
            cache_control: None,
            body: request_body.clone(),
        },
    )
    .await
    .expect("second request should succeed");

    assert_eq!(hit_response.status_code, 200);
    assert_eq!(
        hit_response
            .headers
            .get("x-cache-status")
            .map(String::as_str),
        Some("hit")
    );
    assert_eq!(hit_response.body["output_text"], "distributed hello");
    // Vendor should not have been called again.
    assert_eq!(vendor_calls.load(Ordering::SeqCst), 1);

    // Drop server handles to stop servers.
    _master_handle.abort();
    _store_handle.abort();
}
