mod common;

use common::{post_response_result_for_state, TestCluster};
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "load smoke; run explicitly with cargo test --test load_smoke -- --ignored"]
async fn thousand_identical_concurrent_deterministic_requests_singleflight_to_one_vendor_call() {
    let app = TestCluster::with_yielding_vendor_body(
        json!({"id":"resp_load_1","output_text":"load cached"}),
        32,
    )
    .await;
    let request = json!({"model":"gpt-test","input":"load","temperature":0});

    let mut tasks = Vec::with_capacity(1_000);
    for _ in 0..1_000 {
        let state = app.state();
        let body = request.clone();
        tasks.push(tokio::spawn(async move {
            post_response_result_for_state(state, body, None).await
        }));
    }

    for task in tasks {
        let response = task
            .await
            .expect("load request task should not panic")
            .expect("gateway response should succeed");
        assert_eq!(response.status_code, 200);
        assert_eq!(response.body["output_text"], "load cached");
        assert!(
            matches!(response.header("x-cache-status"), "miss" | "hit"),
            "unexpected cache status: {}",
            response.header("x-cache-status")
        );
    }

    assert_eq!(app.vendor_call_count(), 1);
}
