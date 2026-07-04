use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use async_trait::async_trait;
use futures_util::stream;
use mooncache_gateway::{
    handle_response_request, GatewayRequest, GatewayState, VendorAdapter, VendorError,
    VendorEventStream, VendorResponse,
};
use mooncache_master::MasterState;
use mooncache_protocol::{ResponsesRequest, SseEvent};
use mooncache_store::MemoryStore;
use serde_json::{json, Value};

struct TestCluster {
    state: GatewayState,
    vendor_stream_calls: Arc<AtomicUsize>,
    vendor_complete_calls: Arc<AtomicUsize>,
}

impl TestCluster {
    async fn with_mock_vendor_stream(events: Vec<SseEvent>) -> Self {
        let mut master = MasterState::new_for_test();
        master.mount_segment("node-a", 1024 * 1024);
        master
            .set_tenant_quota("test-tenant", 1024 * 1024, 0)
            .expect("test tenant quota should be valid");

        let store = MemoryStore::with_capacity(1024 * 1024);
        let vendor_stream_calls = Arc::new(AtomicUsize::new(0));
        let vendor_complete_calls = Arc::new(AtomicUsize::new(0));
        let vendor = Arc::new(CountingStreamVendor::new(
            events,
            Arc::clone(&vendor_stream_calls),
            Arc::clone(&vendor_complete_calls),
        ));
        let state = GatewayState::new_for_test(master, store, vendor);

        Self {
            state,
            vendor_stream_calls,
            vendor_complete_calls,
        }
    }

    async fn post_response_stream(&self, body: Value) -> TestStreamResponse {
        let response = handle_response_request(
            &self.state,
            GatewayRequest {
                authorization: Some("Bearer test-api-key".to_owned()),
                cache_control: None,
                body,
            },
        )
        .await
        .expect("gateway response should succeed");

        assert_eq!(response.status_code, 200);
        TestStreamResponse {
            headers: response.headers,
            body: response.body,
            events: response
                .stream_events
                .expect("streaming response should include SSE events"),
        }
    }

    async fn post_response(&self, body: Value) -> TestResponse {
        let response = handle_response_request(
            &self.state,
            GatewayRequest {
                authorization: Some("Bearer test-api-key".to_owned()),
                cache_control: None,
                body,
            },
        )
        .await
        .expect("gateway response should succeed");

        assert_eq!(response.status_code, 200);
        assert!(
            response.stream_events.is_none(),
            "non-streaming response should not include SSE events"
        );
        TestResponse {
            headers: response.headers,
            body: response.body,
        }
    }

    async fn vendor_stream_call_count(&self) -> usize {
        self.vendor_stream_calls.load(Ordering::SeqCst)
    }

    async fn vendor_complete_call_count(&self) -> usize {
        self.vendor_complete_calls.load(Ordering::SeqCst)
    }
}

struct TestResponse {
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

struct TestStreamResponse {
    headers: std::collections::BTreeMap<String, String>,
    body: Value,
    events: Vec<SseEvent>,
}

impl TestStreamResponse {
    fn header(&self, name: &str) -> &str {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
            .unwrap_or("")
    }

    fn json(&self) -> &Value {
        &self.body
    }

    fn events(&self) -> &[SseEvent] {
        &self.events
    }
}

struct CountingStreamVendor {
    events: Vec<SseEvent>,
    stream_calls: Arc<AtomicUsize>,
    complete_calls: Arc<AtomicUsize>,
}

impl CountingStreamVendor {
    fn new(
        events: Vec<SseEvent>,
        stream_calls: Arc<AtomicUsize>,
        complete_calls: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            events,
            stream_calls,
            complete_calls,
        }
    }
}

#[async_trait]
impl VendorAdapter for CountingStreamVendor {
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
        self.complete_calls.fetch_add(1, Ordering::SeqCst);
        Ok(VendorResponse {
            body: json!({"id":"unexpected_complete"}),
        })
    }

    async fn stream(&self, request: ResponsesRequest) -> Result<VendorEventStream, VendorError> {
        assert_eq!(request.body["stream"], json!(true));
        self.stream_calls.fetch_add(1, Ordering::SeqCst);
        let events = self.events.clone().into_iter().map(Ok);
        Ok(Box::pin(stream::iter(events)))
    }
}

#[tokio::test]
async fn streaming_hit_replays_stored_sse_events_without_vendor_call() {
    let events = vec![
        SseEvent {
            event: Some("response.output_text.delta".into()),
            data: "{\"delta\":\"hel\"}".into(),
        },
        SseEvent {
            event: Some("response.output_text.delta".into()),
            data: "{\"delta\":\"lo\"}".into(),
        },
        SseEvent {
            event: Some("response.completed".into()),
            data: "{\"id\":\"resp_1\"}".into(),
        },
    ];
    let app = TestCluster::with_mock_vendor_stream(events.clone()).await;
    let body = json!({"model":"gpt-test","input":"hello","temperature":0,"stream":true});

    let first = app.post_response_stream(body.clone()).await;
    assert_eq!(first.header("x-cache-status"), "miss");
    assert_eq!(first.events(), events.as_slice());

    let second = app.post_response_stream(body).await;
    assert_eq!(second.header("x-cache-status"), "hit");
    assert_eq!(second.events(), events.as_slice());
    assert_eq!(app.vendor_stream_call_count().await, 1);
}

#[tokio::test]
async fn streaming_miss_seeds_equivalent_non_streaming_cache_hit() {
    let events = vec![SseEvent {
        event: Some("response.completed".into()),
        data: "{\"id\":\"resp_1\",\"output_text\":\"hello\"}".into(),
    }];
    let app = TestCluster::with_mock_vendor_stream(events.clone()).await;
    let streaming_body = json!({"model":"gpt-test","input":"hello","temperature":0,"stream":true});
    let non_streaming_body = json!({"model":"gpt-test","input":"hello","temperature":0});

    let first = app.post_response_stream(streaming_body).await;
    assert_eq!(first.header("x-cache-status"), "miss");
    assert_eq!(first.events(), events.as_slice());

    let second = app.post_response(non_streaming_body).await;
    assert_eq!(second.header("x-cache-status"), "hit");
    assert_eq!(second.json(), &json!({"id":"resp_1","output_text":"hello"}));
    assert_eq!(app.vendor_stream_call_count().await, 1);
    assert_eq!(app.vendor_complete_call_count().await, 0);
}

#[tokio::test]
async fn response_completed_envelope_caches_nested_response_without_mutating_sse() {
    let completed_data =
        "{\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"output_text\":\"hello\"}}";
    let events = vec![
        SseEvent {
            event: Some("response.output_text.delta".into()),
            data: "{\"delta\":\"hello\"}".into(),
        },
        SseEvent {
            event: Some("response.completed".into()),
            data: completed_data.into(),
        },
    ];
    let app = TestCluster::with_mock_vendor_stream(events.clone()).await;
    let body = json!({"model":"gpt-test","input":"hello","temperature":0,"stream":true});

    let first = app.post_response_stream(body.clone()).await;
    assert_eq!(first.header("x-cache-status"), "miss");
    assert_eq!(first.json(), &json!({"id":"resp_1","output_text":"hello"}));
    assert_eq!(first.events(), events.as_slice());

    let second = app.post_response_stream(body).await;
    assert_eq!(second.header("x-cache-status"), "hit");
    assert_eq!(second.json(), &json!({"id":"resp_1","output_text":"hello"}));
    assert_eq!(second.events(), events.as_slice());
    assert_eq!(app.vendor_stream_call_count().await, 1);
}
