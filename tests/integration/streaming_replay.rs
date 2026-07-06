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

const TEST_API_KEY: &str = "test-api-key";
const TEST_TENANT_ID: &str = "test-tenant";

struct StreamingReplayCluster {
    state: GatewayState,
    vendor_stream_calls: Arc<AtomicUsize>,
}

impl StreamingReplayCluster {
    async fn new(vendor_events_by_call: Vec<Vec<SseEvent>>) -> Self {
        let mut master = MasterState::new_for_test();
        master.mount_segment("node-a", 1024 * 1024);
        master
            .set_tenant_quota(TEST_TENANT_ID, 1024 * 1024, 0)
            .expect("test tenant quota should be valid");

        let store = MemoryStore::with_capacity(1024 * 1024);
        let vendor_stream_calls = Arc::new(AtomicUsize::new(0));
        let vendor = Arc::new(SequencedStreamingVendor {
            events_by_call: vendor_events_by_call,
            stream_calls: Arc::clone(&vendor_stream_calls),
        });
        let state = GatewayState::new_for_test(master, store, vendor);

        Self {
            state,
            vendor_stream_calls,
        }
    }

    async fn post_streaming_response(&self, body: Value) -> mooncache_gateway::GatewayResponse {
        let response = handle_response_request(
            &self.state,
            GatewayRequest {
                authorization: Some(format!("Bearer {TEST_API_KEY}")),
                cache_control: None,
                body,
            },
        )
        .await
        .expect("streaming gateway request should succeed");

        assert_eq!(response.status_code, 200);
        response
    }

    fn vendor_stream_call_count(&self) -> usize {
        self.vendor_stream_calls.load(Ordering::SeqCst)
    }
}

struct SequencedStreamingVendor {
    events_by_call: Vec<Vec<SseEvent>>,
    stream_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl VendorAdapter for SequencedStreamingVendor {
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
        Err(VendorError::InvalidResponse {
            message: "streaming replay requests must not use non-streaming completion".to_owned(),
        })
    }

    async fn stream(&self, request: ResponsesRequest) -> Result<VendorEventStream, VendorError> {
        assert_eq!(request.body["stream"], json!(true));
        let call_index = self.stream_calls.fetch_add(1, Ordering::SeqCst);
        let events = self
            .events_by_call
            .get(call_index)
            .cloned()
            .ok_or_else(|| VendorError::InvalidResponse {
                message: format!("unexpected vendor stream call {call_index}"),
            })?;

        Ok(Box::pin(stream::iter(events.into_iter().map(Ok))))
    }
}

#[tokio::test]
async fn repeated_identical_streaming_request_replays_first_cached_events_without_vendor_reentry() {
    let first_vendor_events = vec![
        SseEvent {
            event: Some("response.output_text.delta".to_owned()),
            data: json!({"delta":"hel"}).to_string(),
        },
        SseEvent {
            event: Some("response.output_text.delta".to_owned()),
            data: json!({"delta":"lo"}).to_string(),
        },
        SseEvent {
            event: Some("response.completed".to_owned()),
            data: json!({
                "type": "response.completed",
                "response": {"id":"resp_first","output_text":"hello"}
            })
            .to_string(),
        },
    ];
    let second_vendor_events = vec![SseEvent {
        event: Some("response.completed".to_owned()),
        data: json!({
            "type": "response.completed",
            "response": {"id":"resp_second","output_text":"goodbye"}
        })
        .to_string(),
    }];
    let app =
        StreamingReplayCluster::new(vec![first_vendor_events.clone(), second_vendor_events]).await;
    let request_body = json!({
        "model": "gpt-test",
        "input": "client-equivalent stream replay",
        "temperature": 0,
        "stream": true
    });

    let miss = app.post_streaming_response(request_body.clone()).await;
    assert_eq!(miss.headers["x-cache-status"], "miss");
    assert_eq!(miss.headers["x-cache-write"], "committed");
    assert_eq!(miss.headers["x-cache-tier"], "vendor");
    assert_eq!(
        miss.body,
        json!({"id":"resp_first","output_text":"hello"}),
        "streaming miss should expose the completed response body clients receive after the stream finishes"
    );
    assert_eq!(
        miss.stream_events.as_deref(),
        Some(first_vendor_events.as_slice()),
        "streaming miss should retain the vendor SSE event sequence for the client"
    );

    let hit = app.post_streaming_response(request_body).await;
    assert_eq!(hit.headers["x-cache-status"], "hit");
    assert_eq!(hit.headers["x-cache-write"], "skipped");
    assert_eq!(hit.headers["x-cache-tier"], "dram");
    assert_eq!(
        hit.body,
        json!({"id":"resp_first","output_text":"hello"}),
        "cache hit must replay the first response body, not a fresh vendor body"
    );
    assert_eq!(
        hit.stream_events.as_deref(),
        Some(first_vendor_events.as_slice()),
        "cache hit must replay the original SSE events in order for client equivalence"
    );
    assert_eq!(
        app.vendor_stream_call_count(),
        1,
        "the repeated identical streaming request must be served from cache without another vendor stream call"
    );
}
