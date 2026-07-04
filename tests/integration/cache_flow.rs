use std::{
    future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use futures_util::FutureExt;
use mooncache_gateway::{
    handle_response_request, GatewayRequest, GatewayState, MockVendorAdapter, TenantConfigSet,
    VendorAdapter, VendorError, VendorEventStream, VendorResponse,
};
use mooncache_master::MasterState;
use mooncache_protocol::ResponsesRequest;
use mooncache_store::MemoryStore;
use serde_json::{json, Value};
use tokio::sync::Notify;

struct TestCluster {
    state: Arc<GatewayState>,
    vendor_calls: Arc<AtomicUsize>,
}

impl TestCluster {
    async fn new() -> Self {
        Self::with_mock_vendor_json(json!({"id":"resp_1","output_text":"hello"})).await
    }

    async fn with_mock_vendor_json(body: Value) -> Self {
        let mut master = MasterState::new_for_test();
        master.mount_segment("node-a", 1024 * 1024);
        master
            .set_tenant_quota("test-tenant", 1024 * 1024, 0)
            .expect("test tenant quota should be valid");

        let store = MemoryStore::with_capacity(1024 * 1024);
        let vendor_calls = Arc::new(AtomicUsize::new(0));
        let vendor = Arc::new(CountingVendor::new(
            MockVendorAdapter::new_json(body),
            Arc::clone(&vendor_calls),
        ));
        let state = Arc::new(GatewayState::new_for_test(master, store, vendor));

        Self {
            state,
            vendor_calls,
        }
    }

    async fn with_yielding_mock_vendor_json(body: Value) -> Self {
        let mut master = MasterState::new_for_test();
        master.mount_segment("node-a", 1024 * 1024);
        master
            .set_tenant_quota("test-tenant", 1024 * 1024, 0)
            .expect("test tenant quota should be valid");

        let store = MemoryStore::with_capacity(1024 * 1024);
        let vendor_calls = Arc::new(AtomicUsize::new(0));
        let vendor = Arc::new(CountingVendor::new_with_yields(
            MockVendorAdapter::new_json(body),
            Arc::clone(&vendor_calls),
            8,
        ));
        let state = Arc::new(GatewayState::new_for_test(master, store, vendor));

        Self {
            state,
            vendor_calls,
        }
    }

    async fn with_blocking_once_vendor_json(body: Value) -> (Self, Arc<Notify>) {
        let mut master = MasterState::new_for_test();
        master.mount_segment("node-a", 1024 * 1024);
        master
            .set_tenant_quota("test-tenant", 1024 * 1024, 0)
            .expect("test tenant quota should be valid");

        let store = MemoryStore::with_capacity(1024 * 1024);
        let vendor_calls = Arc::new(AtomicUsize::new(0));
        let first_call_started = Arc::new(Notify::new());
        let vendor = Arc::new(BlockingOnceVendor::new(
            MockVendorAdapter::new_json(body),
            Arc::clone(&vendor_calls),
            Arc::clone(&first_call_started),
        ));
        let state = Arc::new(GatewayState::new_for_test(master, store, vendor));

        (
            Self {
                state,
                vendor_calls,
            },
            first_call_started,
        )
    }

    async fn with_poisoned_store_allocation(vendor_body: Value, poison_body: Value) -> Self {
        let mut master = MasterState::new_for_test();
        master.mount_segment("node-a", 1024 * 1024);
        master
            .set_tenant_quota("test-tenant", 1024 * 1024, 0)
            .expect("test tenant quota should be valid");

        let vendor_bytes =
            serde_json::to_vec(&vendor_body).expect("test vendor body should serialize");
        let poison_bytes =
            serde_json::to_vec(&poison_body).expect("test poison body should serialize");
        assert_eq!(
            vendor_bytes.len(),
            poison_bytes.len(),
            "poison body must occupy the same handle shape the master will reserve"
        );

        let mut store = MemoryStore::with_capacity(1024 * 1024);
        let poison_handle = store
            .allocate(poison_bytes.len())
            .expect("poison allocation should fit");
        store
            .write_chunk(&poison_handle, &poison_bytes)
            .expect("poison chunk should be writable");

        let vendor_calls = Arc::new(AtomicUsize::new(0));
        let vendor = Arc::new(CountingVendor::new(
            MockVendorAdapter::new_json(vendor_body),
            Arc::clone(&vendor_calls),
        ));
        let state = Arc::new(GatewayState::new_for_test(master, store, vendor));

        Self {
            state,
            vendor_calls,
        }
    }

    async fn post_response(&self, body: Value) -> TestResponse {
        self.post_response_with_cache_control(body, None).await
    }

    async fn post_response_with_cache_control(
        &self,
        body: Value,
        cache_control: Option<&str>,
    ) -> TestResponse {
        let response = post_response_result_for_state(Arc::clone(&self.state), body, cache_control)
            .await
            .expect("gateway response should succeed");

        assert_eq!(response.status_code, 200);
        response
    }

    async fn vendor_call_count(&self) -> usize {
        self.vendor_calls.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
struct TestResponse {
    status_code: u16,
    headers: std::collections::BTreeMap<String, String>,
    body: Value,
}

impl TestResponse {
    fn header(&self, name: &str) -> &str {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
            .unwrap_or("")
    }

    fn json(&self) -> &Value {
        &self.body
    }
}

struct CountingVendor {
    inner: MockVendorAdapter,
    calls: Arc<AtomicUsize>,
    yields_before_response: usize,
}

impl CountingVendor {
    fn new(inner: MockVendorAdapter, calls: Arc<AtomicUsize>) -> Self {
        Self::new_with_yields(inner, calls, 0)
    }

    fn new_with_yields(
        inner: MockVendorAdapter,
        calls: Arc<AtomicUsize>,
        yields_before_response: usize,
    ) -> Self {
        Self {
            inner,
            calls,
            yields_before_response,
        }
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
        for _ in 0..self.yields_before_response {
            tokio::task::yield_now().await;
        }
        self.inner.complete(request).await
    }

    async fn stream(&self, request: ResponsesRequest) -> Result<VendorEventStream, VendorError> {
        self.inner.stream(request).await
    }
}

struct BlockingOnceVendor {
    inner: MockVendorAdapter,
    calls: Arc<AtomicUsize>,
    first_call_started: Arc<Notify>,
}

impl BlockingOnceVendor {
    fn new(
        inner: MockVendorAdapter,
        calls: Arc<AtomicUsize>,
        first_call_started: Arc<Notify>,
    ) -> Self {
        Self {
            inner,
            calls,
            first_call_started,
        }
    }
}

#[async_trait]
impl VendorAdapter for BlockingOnceVendor {
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
        let call_index = self.calls.fetch_add(1, Ordering::SeqCst);
        if call_index == 0 {
            self.first_call_started.notify_waiters();
            future::pending::<()>().await;
        }
        self.inner.complete(request).await
    }

    async fn stream(&self, request: ResponsesRequest) -> Result<VendorEventStream, VendorError> {
        self.inner.stream(request).await
    }
}

async fn post_response_result_for_state(
    state: Arc<GatewayState>,
    body: Value,
    cache_control: Option<&str>,
) -> Result<TestResponse, mooncache_gateway::GatewayError> {
    let response = handle_response_request(
        &state,
        GatewayRequest {
            authorization: Some("Bearer test-api-key".to_owned()),
            cache_control: cache_control.map(str::to_owned),
            body,
        },
    )
    .await?;

    Ok(TestResponse {
        status_code: response.status_code,
        headers: response.headers,
        body: response.body,
    })
}

#[tokio::test]
async fn configured_api_key_authenticates_configured_tenant() {
    let tenants = TenantConfigSet::parse_toml(
        r#"
        [[tenants]]
        id = "configured-tenant"
        name = "Configured Tenant"
        enabled = true
        api_key_sha256 = "e46ea83ec368dc44797a4b7da96ad92963dae141d417cd89fdb211b488422b0f"
        dram_quota_bytes = 1048576
        ssd_quota_bytes = 0
        request_rate_limit_per_minute = 1
        stream_concurrency_limit = 1
        vendor_spend_budget_usd = 1
        default_ttl_seconds = 60
        max_ttl_seconds = 60
        policy = "cache_first"
        allowed_vendors = ["mock"]
        "#,
    )
    .unwrap();
    let mut master = MasterState::new_for_test();
    master.mount_segment("node-a", 1024 * 1024);
    master
        .set_tenant_quota("configured-tenant", 1024 * 1024, 0)
        .expect("configured tenant quota should be valid");
    let store = MemoryStore::with_capacity(1024 * 1024);
    let vendor = Arc::new(MockVendorAdapter::new_json(
        json!({"id":"resp_configured","output_text":"configured"}),
    ));
    let state = Arc::new(GatewayState::new_with_tenant_config(
        master, store, vendor, tenants,
    ));

    let response = handle_response_request(
        &state,
        GatewayRequest {
            authorization: Some("Bearer demo-api-key-do-not-use".to_owned()),
            cache_control: None,
            body: json!({"model":"gpt-test","input":"hello","temperature":0}),
        },
    )
    .await
    .unwrap();

    assert_eq!(response.status_code, 200);
    assert_eq!(response.headers["x-cache-status"], "miss");
    assert_eq!(response.body["output_text"], "configured");
}

#[tokio::test]
async fn non_streaming_miss_writes_cache_and_next_request_hits() {
    let app = TestCluster::new().await;
    let body = json!({"model":"gpt-test","input":"hello","temperature":0});

    let first = app.post_response(body.clone()).await;
    assert_eq!(first.header("x-cache-status"), "miss");
    assert_eq!(first.json()["output_text"], "hello");
    assert_eq!(first.header("x-cache-coalesced"), "leader");

    let second = app.post_response(body).await;
    assert_eq!(second.header("x-cache-status"), "hit");
    assert_eq!(second.header("x-cache-coalesced"), "none");
    assert_eq!(second.json()["output_text"], "hello");
    assert_eq!(app.vendor_call_count().await, 1);
}

#[tokio::test]
async fn gateway_metrics_snapshot_tracks_cache_write_singleflight_and_vendor_flow() {
    let app =
        TestCluster::with_yielding_mock_vendor_json(json!({"id":"resp_1","output_text":"hello"}))
            .await;
    let body = json!({"model":"gpt-test","input":"hello","temperature":0});

    let (leader, waiter) = tokio::join!(
        app.post_response(body.clone()),
        app.post_response(body.clone())
    );
    let hit = app.post_response(body).await;

    assert_eq!(leader.header("x-cache-status"), "miss");
    assert_eq!(waiter.header("x-cache-coalesced"), "waiter");
    assert_eq!(waiter.header("x-cache-write"), "skipped");
    assert_eq!(hit.header("x-cache-status"), "hit");
    let metrics = app.state.metrics_snapshot();
    assert_eq!(metrics.cache_misses, 2);
    assert_eq!(metrics.cache_hits, 1);
    assert_eq!(metrics.write_committed, 1);
    assert_eq!(metrics.write_skipped, 2);
    assert_eq!(metrics.singleflight_leaders, 1);
    assert_eq!(metrics.singleflight_waiters, 1);
    assert_eq!(metrics.singleflight_none, 1);
    assert_eq!(metrics.vendor_calls, 1);
    assert!(metrics.vendor_latency_micros_total > 0);
}

#[tokio::test]
async fn identical_concurrent_misses_share_one_vendor_call() {
    let app =
        TestCluster::with_yielding_mock_vendor_json(json!({"id":"resp_1","output_text":"hello"}))
            .await;
    let body = json!({"model":"gpt-test","input":"hello","temperature":0});

    let (a, b, c) = tokio::join!(
        app.post_response(body.clone()),
        app.post_response(body.clone()),
        app.post_response(body),
    );

    assert_eq!(a.json(), &json!({"id":"resp_1","output_text":"hello"}));
    assert_eq!(b.json(), a.json());
    assert_eq!(c.json(), a.json());
    let mut roles = [
        a.header("x-cache-coalesced"),
        b.header("x-cache-coalesced"),
        c.header("x-cache-coalesced"),
    ];
    roles.sort_unstable();
    assert_eq!(roles, ["leader", "waiter", "waiter"]);
    assert_eq!(app.vendor_call_count().await, 1);
}

#[tokio::test]
async fn dropped_singleflight_leader_unblocks_waiter_and_allows_fresh_leader() {
    let (app, first_call_started) =
        TestCluster::with_blocking_once_vendor_json(json!({"id":"resp_2","output_text":"fresh"}))
            .await;
    let body = json!({"model":"gpt-test","input":"cancelled leader","temperature":0});

    let leader_state = Arc::clone(&app.state);
    let leader_body = body.clone();
    let leader = tokio::spawn(async move {
        post_response_result_for_state(leader_state, leader_body, None).await
    });
    first_call_started.notified().await;

    let waiter_state = Arc::clone(&app.state);
    let waiter_body = body.clone();
    let mut waiter = tokio::spawn(async move {
        post_response_result_for_state(waiter_state, waiter_body, None).await
    });
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }

    leader.abort();
    assert!(leader
        .await
        .expect_err("leader task should be aborted")
        .is_cancelled());
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    let Some(waiter_join) = (&mut waiter).now_or_never() else {
        waiter.abort();
        panic!("waiter should not hang after leader is dropped");
    };
    let waiter_result = waiter_join.expect("waiter task should not panic");
    assert!(
        matches!(
            waiter_result,
            Err(mooncache_gateway::GatewayError::SingleflightLeaderFailed(_))
        ),
        "waiter should observe abandoned leader, got {waiter_result:?}"
    );

    let fresh = app.post_response(body.clone()).await;
    assert_eq!(fresh.header("x-cache-status"), "miss");
    assert_eq!(fresh.header("x-cache-write"), "committed");
    assert_eq!(fresh.header("x-cache-coalesced"), "leader");
    assert_eq!(fresh.json(), &json!({"id":"resp_2","output_text":"fresh"}));
    assert_eq!(app.vendor_call_count().await, 2);

    let cached = app.post_response(body).await;
    assert_eq!(cached.header("x-cache-status"), "hit");
    assert_eq!(cached.json(), &json!({"id":"resp_2","output_text":"fresh"}));
    assert_eq!(app.vendor_call_count().await, 2);
}

#[tokio::test]
async fn concurrent_default_then_read_only_misses_do_not_coalesce_or_skip_write() {
    let app =
        TestCluster::with_yielding_mock_vendor_json(json!({"id":"resp_3","output_text":"mixed"}))
            .await;
    let body = json!({"model":"gpt-test","input":"default then read-only","temperature":0});

    let (default_response, read_only_response) = tokio::join!(
        app.post_response(body.clone()),
        app.post_response_with_cache_control(body.clone(), Some("read-only")),
    );

    assert_eq!(default_response.header("x-cache-status"), "miss");
    assert_eq!(default_response.header("x-cache-write"), "committed");
    assert_eq!(default_response.header("x-cache-coalesced"), "leader");
    assert_eq!(read_only_response.header("x-cache-status"), "miss");
    assert_eq!(read_only_response.header("x-cache-write"), "skipped");
    assert_eq!(read_only_response.header("x-cache-coalesced"), "leader");
    assert_eq!(app.vendor_call_count().await, 2);

    let cached = app.post_response(body).await;
    assert_eq!(cached.header("x-cache-status"), "hit");
    assert_eq!(cached.header("x-cache-coalesced"), "none");
    assert_eq!(cached.json(), &json!({"id":"resp_3","output_text":"mixed"}));
    assert_eq!(app.vendor_call_count().await, 2);
}

#[tokio::test]
async fn concurrent_read_only_then_default_misses_do_not_coalesce_or_skip_write() {
    let app =
        TestCluster::with_yielding_mock_vendor_json(json!({"id":"resp_4","output_text":"mixed"}))
            .await;
    let body = json!({"model":"gpt-test","input":"read-only then default","temperature":0});

    let (read_only_response, default_response) = tokio::join!(
        app.post_response_with_cache_control(body.clone(), Some("read-only")),
        app.post_response(body.clone()),
    );

    assert_eq!(read_only_response.header("x-cache-status"), "miss");
    assert_eq!(read_only_response.header("x-cache-write"), "skipped");
    assert_eq!(read_only_response.header("x-cache-coalesced"), "leader");
    assert_eq!(default_response.header("x-cache-status"), "miss");
    assert_eq!(default_response.header("x-cache-write"), "committed");
    assert_eq!(default_response.header("x-cache-coalesced"), "leader");
    assert_eq!(app.vendor_call_count().await, 2);

    let cached = app.post_response(body).await;
    assert_eq!(cached.header("x-cache-status"), "hit");
    assert_eq!(cached.header("x-cache-coalesced"), "none");
    assert_eq!(cached.json(), &json!({"id":"resp_4","output_text":"mixed"}));
    assert_eq!(app.vendor_call_count().await, 2);
}

#[tokio::test]
async fn writeback_uses_master_reserved_handle_when_store_allocator_has_diverged() {
    let app = TestCluster::with_poisoned_store_allocation(json!("OK"), json!("NO")).await;
    let body = json!({"model":"gpt-test","input":"hello","temperature":0});

    let first = app.post_response(body.clone()).await;
    assert_eq!(first.header("x-cache-status"), "miss");
    assert_eq!(first.header("x-cache-write"), "committed");
    assert_eq!(first.json(), &json!("OK"));

    let second = app.post_response(body).await;
    assert_eq!(second.header("x-cache-status"), "hit");
    assert_eq!(second.json(), &json!("OK"));
    assert_eq!(app.vendor_call_count().await, 1);
}

#[tokio::test]
async fn stochastic_request_without_force_replay_is_not_cached() {
    let app = TestCluster::new().await;
    let body = json!({"model":"gpt-test","input":"hello","temperature":0.7});

    let first = app.post_response(body.clone()).await;
    assert_eq!(first.header("x-cache-status"), "ineligible");
    assert_eq!(first.header("x-cache-write"), "skipped");

    let second = app.post_response(body).await;
    assert_eq!(second.header("x-cache-status"), "ineligible");
    assert_eq!(app.vendor_call_count().await, 2);
}

#[tokio::test]
async fn force_replay_allows_stochastic_request_to_cache_exact_response() {
    let app = TestCluster::new().await;
    let body = json!({"model":"gpt-test","input":"hello","temperature":0.7});

    let first = app
        .post_response_with_cache_control(body.clone(), Some("force-replay"))
        .await;
    assert_eq!(first.header("x-cache-status"), "miss");
    assert_eq!(first.header("x-cache-write"), "committed");

    let second = app
        .post_response_with_cache_control(body, Some("force-replay"))
        .await;
    assert_eq!(second.header("x-cache-status"), "hit");
    assert_eq!(app.vendor_call_count().await, 1);
}
