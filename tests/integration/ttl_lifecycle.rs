use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use futures_util::{stream, Stream};
use mooncache_gateway::{
    handle_response_request, GatewayRequest, GatewayState, TenantConfigSet, VendorAdapter,
    VendorError, VendorEventStream, VendorResponse,
};
use mooncache_master::MasterState;
use mooncache_protocol::{ResponsesRequest, SseEvent};
use mooncache_store::MemoryStore;
use serde_json::{json, Value};

const CONFIGURED_TENANT_ID: &str = "configured-tenant";
const CONFIGURED_API_KEY: &str = "demo-api-key-do-not-use";
const CONFIGURED_API_KEY_SHA256: &str =
    "e46ea83ec368dc44797a4b7da96ad92963dae141d417cd89fdb211b488422b0f";

#[tokio::test]
async fn cached_response_hits_before_ttl_and_misses_after_ttl_expires() {
    let app = TtlTestCluster::with_default_ttl_seconds(1);
    let request = json!({"model":"gpt-test","input":"ttl lifecycle","temperature":0});

    let initial = app.post_response(request.clone()).await;
    assert_eq!(initial.status_code, 200);
    assert_eq!(initial.header("x-cache-status"), "miss");
    assert_eq!(initial.header("x-cache-write"), "committed");
    assert_eq!(initial.header("x-cache-tier"), "vendor");
    assert_eq!(initial.body["output_text"], "fresh response 1");
    assert_eq!(app.vendor_call_count(), 1);

    let before_expiry = app.post_response(request.clone()).await;
    assert_eq!(before_expiry.status_code, 200);
    assert_eq!(before_expiry.header("x-cache-status"), "hit");
    assert_eq!(before_expiry.header("x-cache-write"), "skipped");
    assert_eq!(before_expiry.header("x-cache-tier"), "dram");
    assert_eq!(
        before_expiry.header("x-cache-key"),
        initial.header("x-cache-key")
    );
    assert_eq!(before_expiry.body["output_text"], "fresh response 1");
    assert_eq!(app.vendor_call_count(), 1);

    std::thread::sleep(Duration::from_millis(1_500));

    let after_expiry = app.post_response(request).await;
    assert_eq!(after_expiry.status_code, 200);
    assert_eq!(after_expiry.header("x-cache-status"), "miss");
    assert_eq!(after_expiry.header("x-cache-write"), "committed");
    assert_eq!(after_expiry.header("x-cache-tier"), "vendor");
    assert_eq!(
        after_expiry.header("x-cache-key"),
        initial.header("x-cache-key")
    );
    assert_eq!(after_expiry.body["output_text"], "fresh response 2");
    assert_eq!(app.vendor_call_count(), 2);
}

struct TtlTestCluster {
    state: Arc<GatewayState>,
    vendor_calls: Arc<AtomicUsize>,
}

impl TtlTestCluster {
    fn with_default_ttl_seconds(default_ttl_seconds: u64) -> Self {
        let mut master = MasterState::new_for_test();
        master.mount_segment("node-a", 1024 * 1024);
        master
            .set_tenant_quota(CONFIGURED_TENANT_ID, 1024 * 1024, 0)
            .expect("configured tenant quota should be valid");

        let tenants = TenantConfigSet::parse_toml(&format!(
            r#"
            [[tenants]]
            id = "{CONFIGURED_TENANT_ID}"
            name = "Configured Tenant"
            enabled = true
            api_key_sha256 = "{CONFIGURED_API_KEY_SHA256}"
            dram_quota_bytes = 1048576
            ssd_quota_bytes = 0
            request_rate_limit_per_minute = 1
            stream_concurrency_limit = 1
            vendor_spend_budget_usd = 1
            default_ttl_seconds = {default_ttl_seconds}
            max_ttl_seconds = {default_ttl_seconds}
            policy = "cache_first"
            allowed_vendors = ["mock"]
            "#
        ))
        .expect("test tenant config should parse");

        let vendor_calls = Arc::new(AtomicUsize::new(0));
        let vendor = Arc::new(SequencedVendor::new(Arc::clone(&vendor_calls)));
        let state = Arc::new(GatewayState::new_with_tenant_config(
            master,
            MemoryStore::with_capacity(1024 * 1024),
            vendor,
            tenants,
        ));

        Self {
            state,
            vendor_calls,
        }
    }

    async fn post_response(&self, body: Value) -> TestResponse {
        let response = handle_response_request(
            &self.state,
            GatewayRequest {
                authorization: Some(format!("Bearer {CONFIGURED_API_KEY}")),
                cache_control: None,
                body,
            },
        )
        .await
        .expect("gateway response should succeed");

        TestResponse {
            status_code: response.status_code,
            headers: response.headers,
            body: response.body,
        }
    }

    fn vendor_call_count(&self) -> usize {
        self.vendor_calls.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
struct TestResponse {
    status_code: u16,
    body: Value,
    headers: std::collections::BTreeMap<String, String>,
}

impl TestResponse {
    fn header(&self, name: &str) -> &str {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
            .unwrap_or("")
    }
}

struct SequencedVendor {
    calls: Arc<AtomicUsize>,
}

impl SequencedVendor {
    fn new(calls: Arc<AtomicUsize>) -> Self {
        Self { calls }
    }
}

#[async_trait]
impl VendorAdapter for SequencedVendor {
    fn vendor_id(&self) -> &str {
        "mock"
    }

    fn adapter_version(&self) -> &str {
        "mock-v1"
    }

    async fn resolve_model_version(&self, requested_model: &str) -> Result<String, VendorError> {
        Ok(requested_model.to_owned())
    }

    async fn complete(&self, _request: ResponsesRequest) -> Result<VendorResponse, VendorError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(VendorResponse {
            body: json!({
                "id": format!("resp_ttl_{call}"),
                "output_text": format!("fresh response {call}"),
            }),
        })
    }

    async fn stream(&self, _request: ResponsesRequest) -> Result<VendorEventStream, VendorError> {
        let call = self.calls.load(Ordering::SeqCst);
        let event = SseEvent {
            event: Some("response.completed".to_owned()),
            data: json!({
                "id": format!("resp_ttl_stream_{call}"),
                "output_text": format!("fresh stream response {call}"),
            })
            .to_string(),
        };
        let events: Pin<Box<dyn Stream<Item = Result<SseEvent, VendorError>> + Send + 'static>> =
            Box::pin(stream::once(async move { Ok(event) }));
        Ok(events)
    }
}
