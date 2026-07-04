use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use mooncache_gateway::{
    handle_response_request, GatewayRequest, GatewayState, MockVendorAdapter, VendorAdapter,
    VendorError, VendorEventStream, VendorResponse,
};
use mooncache_master::MasterState;
use mooncache_protocol::ResponsesRequest;
use mooncache_store::MemoryStore;
use serde_json::Value;

const TEST_API_KEY: &str = "test-api-key";
const TEST_TENANT_ID: &str = "test-tenant";

pub struct TestCluster {
    state: Arc<GatewayState>,
    vendor_calls: Arc<AtomicUsize>,
}

#[allow(dead_code)]
impl TestCluster {
    pub async fn with_vendor_body(body: Value) -> Self {
        Self::with_yielding_vendor_body(body, 0).await
    }

    pub async fn with_yielding_vendor_body(body: Value, yields_before_response: usize) -> Self {
        let mut master = MasterState::new_for_test();
        master.mount_segment("node-a", 1024 * 1024);
        master
            .set_tenant_quota(TEST_TENANT_ID, 1024 * 1024, 0)
            .expect("test tenant quota should be valid");

        let store = MemoryStore::with_capacity(1024 * 1024);
        let vendor_calls = Arc::new(AtomicUsize::new(0));
        let vendor = Arc::new(CountingVendor::new(
            MockVendorAdapter::new_json(body),
            Arc::clone(&vendor_calls),
            yields_before_response,
        ));
        let state = Arc::new(GatewayState::new_for_test(master, store, vendor));

        Self {
            state,
            vendor_calls,
        }
    }

    pub async fn with_unavailable_cache(body: Value) -> Self {
        let vendor_calls = Arc::new(AtomicUsize::new(0));
        let vendor = Arc::new(CountingVendor::new(
            MockVendorAdapter::new_json(body),
            Arc::clone(&vendor_calls),
            0,
        ));
        let state = Arc::new(GatewayState::new_with_unavailable_cache_for_test(vendor));

        Self {
            state,
            vendor_calls,
        }
    }

    pub fn state(&self) -> Arc<GatewayState> {
        Arc::clone(&self.state)
    }

    pub async fn post_response(&self, body: Value) -> TestResponse {
        self.post_response_with_cache_control(body, None).await
    }

    pub async fn post_response_with_cache_control(
        &self,
        body: Value,
        cache_control: Option<&str>,
    ) -> TestResponse {
        post_response_result_for_state(Arc::clone(&self.state), body, cache_control)
            .await
            .expect("gateway response should succeed")
    }

    pub fn vendor_call_count(&self) -> usize {
        self.vendor_calls.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
pub struct TestResponse {
    pub status_code: u16,
    pub body: Value,
    headers: std::collections::BTreeMap<String, String>,
}

impl TestResponse {
    pub fn header(&self, name: &str) -> &str {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
            .unwrap_or("")
    }
}

struct CountingVendor {
    inner: MockVendorAdapter,
    calls: Arc<AtomicUsize>,
    yields_before_response: usize,
}

impl CountingVendor {
    fn new(
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

pub async fn post_response_result_for_state(
    state: Arc<GatewayState>,
    body: Value,
    cache_control: Option<&str>,
) -> Result<TestResponse, mooncache_gateway::GatewayError> {
    let response = handle_response_request(
        &state,
        GatewayRequest {
            authorization: Some(format!("Bearer {TEST_API_KEY}")),
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
