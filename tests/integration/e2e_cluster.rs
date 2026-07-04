mod common;

use common::TestCluster;
use serde_json::json;

#[tokio::test]
async fn deterministic_responses_request_misses_then_hits_cache() {
    let app = TestCluster::with_vendor_body(json!({
        "id": "resp_e2e_1",
        "output_text": "cached hello"
    }))
    .await;
    let request = json!({"model":"gpt-test","input":"hello","temperature":0});

    let miss = app.post_response(request.clone()).await;
    assert_eq!(miss.status_code, 200);
    assert_eq!(miss.header("x-cache-status"), "miss");
    assert_eq!(miss.header("x-cache-write"), "committed");
    assert_eq!(miss.header("x-cache-tier"), "vendor");
    assert_eq!(miss.header("x-cache-coalesced"), "leader");
    assert!(!miss.header("x-cache-key").is_empty());
    assert_eq!(miss.body["output_text"], "cached hello");

    let hit = app.post_response(request).await;
    assert_eq!(hit.status_code, 200);
    assert_eq!(hit.header("x-cache-status"), "hit");
    assert_eq!(hit.header("x-cache-write"), "skipped");
    assert_eq!(hit.header("x-cache-tier"), "dram");
    assert_eq!(hit.header("x-cache-coalesced"), "none");
    assert_eq!(hit.header("x-cache-key"), miss.header("x-cache-key"));
    assert_eq!(hit.body, miss.body);
    assert_eq!(app.vendor_call_count(), 1);
}

#[tokio::test]
async fn unavailable_cache_degrades_default_and_cache_only_misses_without_vendor_call() {
    let app = TestCluster::with_unavailable_cache(json!({
        "id": "resp_degraded_1",
        "output_text": "vendor fallback"
    }))
    .await;
    let request = json!({"model":"gpt-test","input":"uncached","temperature":0});

    let degraded = app.post_response(request.clone()).await;
    assert_eq!(degraded.status_code, 200);
    assert_eq!(degraded.header("x-cache-status"), "degraded");
    assert_eq!(degraded.header("x-cache-write"), "failed");
    assert_eq!(degraded.header("x-cache-tier"), "vendor");
    assert_eq!(degraded.body["output_text"], "vendor fallback");
    assert_eq!(app.vendor_call_count(), 1);

    let cache_only = app
        .post_response_with_cache_control(request, Some("cache-only"))
        .await;
    assert_eq!(cache_only.status_code, 404);
    assert_eq!(cache_only.header("x-cache-status"), "cache-only-miss");
    assert_eq!(cache_only.header("x-cache-write"), "skipped");
    assert_eq!(cache_only.header("x-cache-tier"), "none");
    assert_eq!(cache_only.body, json!({"error":"cache object not found"}));
    assert_eq!(app.vendor_call_count(), 1);
}
