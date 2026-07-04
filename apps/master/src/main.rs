use std::{env, process::ExitCode, sync::Arc};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::{delete, get, post},
    Json, Router,
};
use mooncache_common::{CacheError, CacheKey, TenantId};
use mooncache_master::MasterState;
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::net::TcpListener;

const SERVICE_NAME: &str = "mooncache-master";
const SERVICE_ENV_PREFIX: &str = "MOONCACHE_MASTER";
const DEFAULT_BIND_ADDR: &str = "0.0.0.0:8081";
const DEFAULT_METRICS_BIND_ADDR: &str = "0.0.0.0:9091";
const DEFAULT_ETCD_URL: &str = "http://127.0.0.1:2379";
const DEFAULT_TENANT_CONFIG_PATH: &str = "config/tenants.toml";
const DEFAULT_SSD_ROOT_PATH: &str = "/var/lib/mooncache/ssd";
const DEFAULT_VENDOR_CONFIG_PATH: &str = "config/vendors.toml";

// ---------------------------------------------------------------------------
// CLI config — untouched from original scaffold
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
struct AppConfig {
    bind_addr: String,
    etcd_url: String,
    tenant_config_path: String,
    ssd_root_path: String,
    metrics_bind_addr: String,
    vendor_config_path: String,
}

#[derive(Debug)]
enum ParsedConfig {
    Help,
    Run(AppConfig),
}

#[tokio::main]
async fn main() -> ExitCode {
    match parse_config(env::args().skip(1).collect()) {
        Ok(ParsedConfig::Help) => {
            print!("{}", usage());
            ExitCode::SUCCESS
        }
        Ok(ParsedConfig::Run(config)) => match run_server(config).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("error: {error}");
                ExitCode::FAILURE
            }
        },
        Err(error) => {
            eprintln!("error: {error}\n\n{}", usage());
            ExitCode::from(2)
        }
    }
}

fn parse_config(args: Vec<String>) -> Result<ParsedConfig, String> {
    let mut config = AppConfig::from_env();
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        if arg == "--help" || arg == "-h" {
            return Ok(ParsedConfig::Help);
        } else if arg == "--bind-addr" {
            config.bind_addr = next_value(&mut args, "--bind-addr")?;
        } else if let Some(value) = arg.strip_prefix("--bind-addr=") {
            config.bind_addr = required_value(value, "--bind-addr")?;
        } else if arg == "--etcd-url" {
            config.etcd_url = next_value(&mut args, "--etcd-url")?;
        } else if let Some(value) = arg.strip_prefix("--etcd-url=") {
            config.etcd_url = required_value(value, "--etcd-url")?;
        } else if arg == "--tenant-config" {
            config.tenant_config_path = next_value(&mut args, "--tenant-config")?;
        } else if let Some(value) = arg.strip_prefix("--tenant-config=") {
            config.tenant_config_path = required_value(value, "--tenant-config")?;
        } else if arg == "--ssd-root" {
            config.ssd_root_path = next_value(&mut args, "--ssd-root")?;
        } else if let Some(value) = arg.strip_prefix("--ssd-root=") {
            config.ssd_root_path = required_value(value, "--ssd-root")?;
        } else if arg == "--metrics-bind-addr" {
            config.metrics_bind_addr = next_value(&mut args, "--metrics-bind-addr")?;
        } else if let Some(value) = arg.strip_prefix("--metrics-bind-addr=") {
            config.metrics_bind_addr = required_value(value, "--metrics-bind-addr")?;
        } else if arg == "--vendor-config" {
            config.vendor_config_path = next_value(&mut args, "--vendor-config")?;
        } else if let Some(value) = arg.strip_prefix("--vendor-config=") {
            config.vendor_config_path = required_value(value, "--vendor-config")?;
        } else {
            return Err(format!("unknown argument `{arg}`"));
        }
    }

    Ok(ParsedConfig::Run(config))
}

impl AppConfig {
    fn from_env() -> Self {
        Self {
            bind_addr: env_value("BIND_ADDR", DEFAULT_BIND_ADDR),
            etcd_url: env_value("ETCD_URL", DEFAULT_ETCD_URL),
            tenant_config_path: env_value("TENANT_CONFIG", DEFAULT_TENANT_CONFIG_PATH),
            ssd_root_path: env_value("SSD_ROOT", DEFAULT_SSD_ROOT_PATH),
            metrics_bind_addr: env_value("METRICS_BIND_ADDR", DEFAULT_METRICS_BIND_ADDR),
            vendor_config_path: env_value("VENDOR_CONFIG", DEFAULT_VENDOR_CONFIG_PATH),
        }
    }
}

fn env_value(suffix: &str, default: &str) -> String {
    env::var(format!("{SERVICE_ENV_PREFIX}_{suffix}"))
        .or_else(|_| env::var(format!("MOONCACHE_{suffix}")))
        .unwrap_or_else(|_| default.to_string())
}

fn next_value(args: &mut std::vec::IntoIter<String>, flag: &str) -> Result<String, String> {
    match args.next() {
        Some(value) if !value.is_empty() && !value.starts_with("--") => Ok(value),
        _ => Err(format!("{flag} requires a value")),
    }
}

fn required_value(value: &str, flag: &str) -> Result<String, String> {
    if value.is_empty() {
        Err(format!("{flag} requires a value"))
    } else {
        Ok(value.to_string())
    }
}

fn usage() -> String {
    format!(
        "{SERVICE_NAME}\n\nUsage: cargo run -p mooncache-master-app -- [OPTIONS]\n\nOptions:\n  --bind-addr <ADDR>           API bind address [env: {SERVICE_ENV_PREFIX}_BIND_ADDR or MOONCACHE_BIND_ADDR] [default: {DEFAULT_BIND_ADDR}]\n  --etcd-url <URL>             Etcd endpoint URL [env: {SERVICE_ENV_PREFIX}_ETCD_URL or MOONCACHE_ETCD_URL] [default: {DEFAULT_ETCD_URL}]\n  --tenant-config <PATH>       Tenant config path [env: {SERVICE_ENV_PREFIX}_TENANT_CONFIG or MOONCACHE_TENANT_CONFIG] [default: {DEFAULT_TENANT_CONFIG_PATH}]\n  --ssd-root <PATH>            SSD root path [env: {SERVICE_ENV_PREFIX}_SSD_ROOT or MOONCACHE_SSD_ROOT] [default: {DEFAULT_SSD_ROOT_PATH}]\n  --metrics-bind-addr <ADDR>   Metrics bind address [env: {SERVICE_ENV_PREFIX}_METRICS_BIND_ADDR or MOONCACHE_METRICS_BIND_ADDR] [default: {DEFAULT_METRICS_BIND_ADDR}]\n  --vendor-config <PATH>       Vendor config path [env: {SERVICE_ENV_PREFIX}_VENDOR_CONFIG or MOONCACHE_VENDOR_CONFIG] [default: {DEFAULT_VENDOR_CONFIG_PATH}]\n  -h, --help                   Print help and exit\n"
    )
}

fn print_resolved_config(config: &AppConfig) {
    println!("{SERVICE_NAME} resolved config:");
    println!("  bind_addr={}", config.bind_addr);
    println!("  etcd_url={}", config.etcd_url);
    println!("  tenant_config_path={}", config.tenant_config_path);
    println!("  ssd_root_path={}", config.ssd_root_path);
    println!("  metrics_bind_addr={}", config.metrics_bind_addr);
    println!("  vendor_config_path={}", config.vendor_config_path);
    println!(
        "  core_crate={}",
        std::any::type_name::<mooncache_master::MasterState>()
    );
}

// ---------------------------------------------------------------------------
// server + router
// ---------------------------------------------------------------------------

async fn run_server(config: AppConfig) -> Result<(), String> {
    print_resolved_config(&config);
    let listener = TcpListener::bind(&config.bind_addr)
        .await
        .map_err(|error| format!("failed to bind {}: {error}", config.bind_addr))?;
    let local_addr = listener
        .local_addr()
        .map_err(|error| format!("failed to read bound address: {error}"))?;
    println!("{SERVICE_NAME} listening on {local_addr}");
    axum::serve(listener, build_router())
        .await
        .map_err(|error| format!("server error: {error}"))
}

type AppState = Arc<Mutex<MasterState>>;

fn build_router() -> Router {
    let mut state = MasterState::new_for_test();
    state.mount_segment("default", 1_048_576);
    build_router_with_state(Arc::new(Mutex::new(state)))
}

fn build_router_with_state(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics/snapshot", get(metrics_snapshot))
        .route("/objects/start", post(put_start))
        .route("/objects/end", post(put_end))
        .route("/objects/revoke", post(put_revoke))
        .route("/objects/replicas", get(get_replica_list))
        .route("/objects", delete(remove_object))
        .route("/tenants/quota", post(set_tenant_quota))
        .route("/segments/mount", post(mount_segment))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// request DTOs
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct PutStartRequest {
    tenant_id: String,
    cache_key: String,
    len: u64,
    replica_count: usize,
}

#[derive(Debug, Deserialize)]
struct PutEndRequest {
    tenant_id: String,
    cache_key: String,
}

#[derive(Debug, Deserialize)]
struct SetQuotaRequest {
    tenant_id: String,
    dram_bytes: u64,
    ssd_bytes: u64,
}

#[derive(Debug, Deserialize)]
struct MountSegmentRequest {
    node_id: String,
    len: u64,
}

#[derive(Debug, Deserialize)]
struct ReplicaListQuery {
    tenant_id: String,
    cache_key: String,
}

// ---------------------------------------------------------------------------
// route handlers
// ---------------------------------------------------------------------------

async fn healthz() -> Json<Value> {
    Json(json!({"ok": true, "service": SERVICE_NAME}))
}

async fn metrics_snapshot(State(state): State<AppState>) -> Json<Value> {
    let snapshot = state.lock().observability_snapshot();
    Json(json!(snapshot))
}

async fn put_start(
    State(state): State<AppState>,
    Json(body): Json<PutStartRequest>,
) -> (StatusCode, Json<Value>) {
    let tenant_id = match TenantId::parse(&body.tenant_id) {
        Ok(id) => id,
        Err(err) => return error_response(err),
    };
    let cache_key = match CacheKey::from_hex(&body.cache_key) {
        Ok(key) => key,
        Err(err) => return error_response(err),
    };
    match state
        .lock()
        .put_start(&tenant_id, &cache_key, body.len, body.replica_count)
    {
        Ok(replicas) => (StatusCode::OK, Json(json!({"replicas": replicas}))),
        Err(err) => error_response(err),
    }
}

async fn put_end(
    State(state): State<AppState>,
    Json(body): Json<PutEndRequest>,
) -> (StatusCode, Json<Value>) {
    let tenant_id = match TenantId::parse(&body.tenant_id) {
        Ok(id) => id,
        Err(err) => return error_response(err),
    };
    let cache_key = match CacheKey::from_hex(&body.cache_key) {
        Ok(key) => key,
        Err(err) => return error_response(err),
    };
    match state.lock().put_end(&tenant_id, &cache_key) {
        Ok(()) => (StatusCode::OK, Json(json!({"ok": true}))),
        Err(err) => error_response(err),
    }
}

async fn put_revoke(
    State(state): State<AppState>,
    Json(body): Json<PutEndRequest>,
) -> (StatusCode, Json<Value>) {
    let tenant_id = match TenantId::parse(&body.tenant_id) {
        Ok(id) => id,
        Err(err) => return error_response(err),
    };
    let cache_key = match CacheKey::from_hex(&body.cache_key) {
        Ok(key) => key,
        Err(err) => return error_response(err),
    };
    match state.lock().put_revoke(&tenant_id, &cache_key) {
        Ok(()) => (StatusCode::OK, Json(json!({"ok": true}))),
        Err(err) => error_response(err),
    }
}

async fn get_replica_list(
    State(state): State<AppState>,
    Query(query): Query<ReplicaListQuery>,
) -> (StatusCode, Json<Value>) {
    let tenant_id = match TenantId::parse(&query.tenant_id) {
        Ok(id) => id,
        Err(err) => return error_response(err),
    };
    let cache_key = match CacheKey::from_hex(&query.cache_key) {
        Ok(key) => key,
        Err(err) => return error_response(err),
    };
    match state.lock().get_replica_list(&tenant_id, &cache_key) {
        Ok(replica_list) => (StatusCode::OK, Json(json!(replica_list))),
        Err(err) => error_response(err),
    }
}

async fn remove_object(
    State(state): State<AppState>,
    Query(query): Query<ReplicaListQuery>,
) -> (StatusCode, Json<Value>) {
    let tenant_id = match TenantId::parse(&query.tenant_id) {
        Ok(id) => id,
        Err(err) => return error_response(err),
    };
    let cache_key = match CacheKey::from_hex(&query.cache_key) {
        Ok(key) => key,
        Err(err) => return error_response(err),
    };
    match state.lock().remove(&tenant_id, &cache_key) {
        Ok(()) => (StatusCode::OK, Json(json!({"ok": true}))),
        Err(err) => error_response(err),
    }
}

async fn set_tenant_quota(
    State(state): State<AppState>,
    Json(body): Json<SetQuotaRequest>,
) -> (StatusCode, Json<Value>) {
    match state
        .lock()
        .set_tenant_quota(&body.tenant_id, body.dram_bytes, body.ssd_bytes)
    {
        Ok(()) => (StatusCode::OK, Json(json!({"ok": true}))),
        Err(err) => error_response(err),
    }
}

async fn mount_segment(
    State(state): State<AppState>,
    Json(body): Json<MountSegmentRequest>,
) -> (StatusCode, Json<Value>) {
    state.lock().mount_segment(&body.node_id, body.len);
    (StatusCode::OK, Json(json!({"ok": true})))
}

fn error_response(err: CacheError) -> (StatusCode, Json<Value>) {
    let status = match &err {
        CacheError::NotFound => StatusCode::NOT_FOUND,
        CacheError::Conflict(_) => StatusCode::CONFLICT,
        CacheError::QuotaExceeded(_) => StatusCode::PAYLOAD_TOO_LARGE,
        CacheError::EmptyTenantId | CacheError::InvalidCacheKey | CacheError::InvalidId(_) => {
            StatusCode::BAD_REQUEST
        }
        CacheError::UpstreamUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
    };
    (status, Json(json!({"error": err.to_string()})))
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use axum::{
        body::{to_bytes, Body},
        http::{header, Request, StatusCode},
    };
    use serde_json::{json, Value};
    use tower::ServiceExt;

    // -----------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------

    const KEY_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const KEY_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn test_router() -> Router {
        let mut state = MasterState::new_for_test();
        state.mount_segment("node-0", 1_048_576);
        build_router_with_state(Arc::new(Mutex::new(state)))
    }

    fn test_router_with_quota(tenant: &str, dram_bytes: u64) -> Router {
        let mut state = MasterState::new_for_test();
        state.mount_segment("node-0", 1_048_576);
        let _ = state.set_tenant_quota(tenant, dram_bytes, 0);
        build_router_with_state(Arc::new(Mutex::new(state)))
    }

    async fn response_json(response: axum::response::Response) -> Value {
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        serde_json::from_slice(&bytes).expect("body should be JSON")
    }

    fn post_json(uri: &str, body: Value) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .expect("request should build")
    }

    fn get_uri(path_and_query: &str) -> Request<Body> {
        Request::builder()
            .uri(path_and_query)
            .body(Body::empty())
            .expect("request should build")
    }

    fn delete_uri(path_and_query: &str) -> Request<Body> {
        Request::builder()
            .method("DELETE")
            .uri(path_and_query)
            .body(Body::empty())
            .expect("request should build")
    }

    // -----------------------------------------------------------------------
    // CLI smoke tests (unchanged)
    // -----------------------------------------------------------------------

    #[test]
    fn cli_flags_override_defaults() {
        let parsed = parse_config(vec![
            "--bind-addr".to_string(),
            "127.0.0.1:18081".to_string(),
            "--etcd-url=http://etcd:2379".to_string(),
            "--tenant-config".to_string(),
            "local/tenants.toml".to_string(),
            "--ssd-root".to_string(),
            "local/ssd".to_string(),
            "--metrics-bind-addr".to_string(),
            "127.0.0.1:19091".to_string(),
            "--vendor-config=local/vendors.toml".to_string(),
        ])
        .expect("flags should parse");

        let ParsedConfig::Run(config) = parsed else {
            panic!("expected run config");
        };

        assert_eq!(config.bind_addr, "127.0.0.1:18081");
        assert_eq!(config.etcd_url, "http://etcd:2379");
        assert_eq!(config.tenant_config_path, "local/tenants.toml");
        assert_eq!(config.ssd_root_path, "local/ssd");
        assert_eq!(config.metrics_bind_addr, "127.0.0.1:19091");
        assert_eq!(config.vendor_config_path, "local/vendors.toml");
    }

    #[test]
    fn help_short_circuits_config_parsing() {
        let parsed = parse_config(vec!["--help".to_string()]).expect("help should parse");
        assert!(matches!(parsed, ParsedConfig::Help));
    }

    #[test]
    fn missing_flag_value_is_an_error() {
        let error =
            parse_config(vec!["--bind-addr".to_string()]).expect_err("missing value should fail");
        assert_eq!(error, "--bind-addr requires a value");
    }

    // -----------------------------------------------------------------------
    // healthz
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn healthz_returns_ok() {
        let app = test_router();

        let response = app
            .oneshot(get_uri("/healthz"))
            .await
            .expect("health request should complete");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["ok"], json!(true));
        assert_eq!(body["service"], json!(SERVICE_NAME));
    }

    // -----------------------------------------------------------------------
    // metrics/snapshot
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn metrics_snapshot_returns_zero_for_empty_state() {
        let app = test_router();

        let response = app
            .oneshot(get_uri("/metrics/snapshot"))
            .await
            .expect("snapshot request should complete");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["objects_total"], json!(0));
        assert_eq!(body["evictions_total"], json!(0));
    }

    #[tokio::test]
    async fn metrics_snapshot_counts_created_objects() {
        let app = test_router();

        // create an object
        let _ = app
            .clone()
            .oneshot(post_json(
                "/objects/start",
                json!({"tenant_id": "t1", "cache_key": KEY_A, "len": 4096, "replica_count": 1}),
            ))
            .await;

        let response = app
            .oneshot(get_uri("/metrics/snapshot"))
            .await
            .expect("snapshot request should complete");

        let body = response_json(response).await;
        assert_eq!(body["objects_total"], json!(1));
    }

    // -----------------------------------------------------------------------
    // PUT /objects/start
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn put_start_allocates_replicas() {
        let app = test_router();

        let response = app
            .oneshot(post_json(
                "/objects/start",
                json!({"tenant_id": "t1", "cache_key": KEY_A, "len": 4096, "replica_count": 1}),
            ))
            .await
            .expect("put_start should complete");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        let replicas = body["replicas"]
            .as_array()
            .expect("replicas should be an array");
        assert_eq!(replicas.len(), 1);
        assert_eq!(replicas[0]["node_id"], json!("node-0"));
        assert_eq!(replicas[0]["offset"], json!(0));
        assert_eq!(replicas[0]["len"], json!(4096));
    }

    #[tokio::test]
    async fn put_start_conflict_on_duplicate_key() {
        let app = test_router();

        let first = app
            .clone()
            .oneshot(post_json(
                "/objects/start",
                json!({"tenant_id": "t1", "cache_key": KEY_A, "len": 1024, "replica_count": 1}),
            ))
            .await
            .expect("first put_start should complete");
        assert_eq!(first.status(), StatusCode::OK);

        let second = app
            .oneshot(post_json(
                "/objects/start",
                json!({"tenant_id": "t1", "cache_key": KEY_A, "len": 1024, "replica_count": 1}),
            ))
            .await
            .expect("second put_start should complete");

        assert_eq!(second.status(), StatusCode::CONFLICT);
        let body = response_json(second).await;
        assert!(body["error"].as_str().unwrap().contains("already exists"));
    }

    #[tokio::test]
    async fn put_start_rejects_invalid_cache_key() {
        let app = test_router();

        let response = app
            .oneshot(post_json(
                "/objects/start",
                json!({"tenant_id": "t1", "cache_key": "bad-key", "len": 1024, "replica_count": 1}),
            ))
            .await
            .expect("put_start should complete");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    // -----------------------------------------------------------------------
    // POST /objects/end
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn put_end_commits_after_put_start() {
        let app = test_router();

        let _ = app
            .clone()
            .oneshot(post_json(
                "/objects/start",
                json!({"tenant_id": "t1", "cache_key": KEY_A, "len": 4096, "replica_count": 1}),
            ))
            .await
            .expect("put_start should complete");

        let response = app
            .oneshot(post_json(
                "/objects/end",
                json!({"tenant_id": "t1", "cache_key": KEY_A}),
            ))
            .await
            .expect("put_end should complete");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["ok"], json!(true));
    }

    #[tokio::test]
    async fn put_end_not_found_for_unknown_object() {
        let app = test_router();

        let response = app
            .oneshot(post_json(
                "/objects/end",
                json!({"tenant_id": "t1", "cache_key": KEY_A}),
            ))
            .await
            .expect("put_end should complete");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // -----------------------------------------------------------------------
    // GET /objects/replicas
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn get_replica_list_returns_committed_object_with_lease() {
        let app = test_router();

        let _ = app
            .clone()
            .oneshot(post_json(
                "/objects/start",
                json!({"tenant_id": "t1", "cache_key": KEY_A, "len": 4096, "replica_count": 1}),
            ))
            .await
            .expect("put_start should complete");
        let _ = app
            .clone()
            .oneshot(post_json(
                "/objects/end",
                json!({"tenant_id": "t1", "cache_key": KEY_A}),
            ))
            .await
            .expect("put_end should complete");

        let response = app
            .oneshot(get_uri(&format!(
                "/objects/replicas?tenant_id=t1&cache_key={KEY_A}"
            )))
            .await
            .expect("get_replica_list should complete");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["replicas"].as_array().unwrap().len(), 1);
        assert!(body["lease"]["expires_at_ms"].as_u64().unwrap() > 0);
    }

    #[tokio::test]
    async fn get_replica_list_not_found_for_unknown_key() {
        let app = test_router();

        let response = app
            .oneshot(get_uri(&format!(
                "/objects/replicas?tenant_id=t1&cache_key={KEY_A}"
            )))
            .await
            .expect("get_replica_list should complete");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = response_json(response).await;
        assert!(body["error"].as_str().unwrap().contains("not found"));
    }

    #[tokio::test]
    async fn get_replica_list_not_found_for_reserved_object_before_commit() {
        let app = test_router();

        let _ = app
            .clone()
            .oneshot(post_json(
                "/objects/start",
                json!({"tenant_id": "t1", "cache_key": KEY_A, "len": 4096, "replica_count": 1}),
            ))
            .await
            .expect("put_start should complete");

        let response = app
            .oneshot(get_uri(&format!(
                "/objects/replicas?tenant_id=t1&cache_key={KEY_A}"
            )))
            .await
            .expect("get_replica_list should complete");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // -----------------------------------------------------------------------
    // POST /objects/revoke
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn put_revoke_releases_object() {
        let app = test_router();

        let _ = app
            .clone()
            .oneshot(post_json(
                "/objects/start",
                json!({"tenant_id": "t1", "cache_key": KEY_A, "len": 4096, "replica_count": 1}),
            ))
            .await
            .expect("put_start should complete");

        let response = app
            .oneshot(post_json(
                "/objects/revoke",
                json!({"tenant_id": "t1", "cache_key": KEY_A}),
            ))
            .await
            .expect("put_revoke should complete");

        assert_eq!(response.status(), StatusCode::OK);
    }

    // -----------------------------------------------------------------------
    // DELETE /objects
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn remove_object_deletes_committed_object() {
        let app = test_router();

        let _ = app
            .clone()
            .oneshot(post_json(
                "/objects/start",
                json!({"tenant_id": "t1", "cache_key": KEY_A, "len": 4096, "replica_count": 1}),
            ))
            .await
            .expect("put_start should complete");
        let _ = app
            .clone()
            .oneshot(post_json(
                "/objects/end",
                json!({"tenant_id": "t1", "cache_key": KEY_A}),
            ))
            .await
            .expect("put_end should complete");

        let response = app
            .oneshot(delete_uri(&format!(
                "/objects?tenant_id=t1&cache_key={KEY_A}"
            )))
            .await
            .expect("remove should complete");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn remove_object_not_found() {
        let app = test_router();

        let response = app
            .oneshot(delete_uri(&format!(
                "/objects?tenant_id=t1&cache_key={KEY_A}"
            )))
            .await
            .expect("remove should complete");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // -----------------------------------------------------------------------
    // POST /tenants/quota
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn set_tenant_quota_sets_limit() {
        let app = test_router();

        let response = app
            .oneshot(post_json(
                "/tenants/quota",
                json!({"tenant_id": "t1", "dram_bytes": 8192, "ssd_bytes": 0}),
            ))
            .await
            .expect("set_quota should complete");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["ok"], json!(true));
    }

    #[tokio::test]
    async fn set_tenant_quota_rejects_empty_tenant_id() {
        let app = test_router();

        let response = app
            .oneshot(post_json(
                "/tenants/quota",
                json!({"tenant_id": "", "dram_bytes": 4096, "ssd_bytes": 0}),
            ))
            .await
            .expect("set_quota should complete");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn tenant_quota_blocks_write_when_exceeded() {
        let app = test_router_with_quota("t1", 4096);

        // fill quota with first object
        let first = app
            .clone()
            .oneshot(post_json(
                "/objects/start",
                json!({"tenant_id": "t1", "cache_key": KEY_A, "len": 4096, "replica_count": 1}),
            ))
            .await
            .expect("first put_start should complete");
        assert_eq!(first.status(), StatusCode::OK);
        let _ = app
            .clone()
            .oneshot(post_json(
                "/objects/end",
                json!({"tenant_id": "t1", "cache_key": KEY_A}),
            ))
            .await
            .expect("first put_end should complete");

        // second object should be blocked by quota
        let second = app
            .oneshot(post_json(
                "/objects/start",
                json!({"tenant_id": "t1", "cache_key": KEY_B, "len": 4096, "replica_count": 1}),
            ))
            .await
            .expect("second put_start should complete");

        assert_eq!(second.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = response_json(second).await;
        assert!(body["error"].as_str().unwrap().contains("quota exceeded"));
    }

    // -----------------------------------------------------------------------
    // POST /segments/mount
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn mount_segment_acknowledges_mount() {
        let app = test_router();

        let response = app
            .oneshot(post_json(
                "/segments/mount",
                json!({"node_id": "node-9", "len": 1_048_576}),
            ))
            .await
            .expect("mount should complete");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["ok"], json!(true));
    }

    // -----------------------------------------------------------------------
    // e2e: put_start → put_end → get_replica_list → revoke
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn full_object_lifecycle() {
        let app = test_router_with_quota("t1", 1_048_576);

        // allocate
        let resp = app
            .clone()
            .oneshot(post_json(
                "/objects/start",
                json!({"tenant_id": "t1", "cache_key": KEY_A, "len": 65536, "replica_count": 1}),
            ))
            .await
            .expect("put_start");
        assert_eq!(resp.status(), StatusCode::OK);

        // commit
        let resp = app
            .clone()
            .oneshot(post_json(
                "/objects/end",
                json!({"tenant_id": "t1", "cache_key": KEY_A}),
            ))
            .await
            .expect("put_end");
        assert_eq!(resp.status(), StatusCode::OK);

        // read
        let resp = app
            .clone()
            .oneshot(get_uri(&format!(
                "/objects/replicas?tenant_id=t1&cache_key={KEY_A}"
            )))
            .await
            .expect("get_replica_list");
        assert_eq!(resp.status(), StatusCode::OK);

        // revoke
        let resp = app
            .clone()
            .oneshot(post_json(
                "/objects/revoke",
                json!({"tenant_id": "t1", "cache_key": KEY_A}),
            ))
            .await
            .expect("put_revoke");
        assert_eq!(resp.status(), StatusCode::OK);

        // should be gone
        let resp = app
            .oneshot(get_uri(&format!(
                "/objects/replicas?tenant_id=t1&cache_key={KEY_A}"
            )))
            .await
            .expect("get_replica_list after revoke");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
