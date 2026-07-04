use std::{
    env,
    process::ExitCode,
    sync::{Arc, Mutex},
};

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use mooncache_admin_api::{
    AdminAction, AdminError, AdminRequestContext, AdminService, CacheFingerprintDebugRequest, Role,
};
use mooncache_common::{CacheError, CacheKey, RequestId, TenantId};
use mooncache_master::MasterState;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::net::TcpListener;

const SERVICE_NAME: &str = "mooncache-admin-api";
const SERVICE_ENV_PREFIX: &str = "MOONCACHE_ADMIN_API";
const DEFAULT_BIND_ADDR: &str = "0.0.0.0:8083";
const DEFAULT_METRICS_BIND_ADDR: &str = "0.0.0.0:9093";
const DEFAULT_ETCD_URL: &str = "http://127.0.0.1:2379";
const DEFAULT_TENANT_CONFIG_PATH: &str = "config/tenants.toml";
const DEFAULT_SSD_ROOT_PATH: &str = "/var/lib/mooncache/ssd";
const DEFAULT_VENDOR_CONFIG_PATH: &str = "config/vendors.toml";

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
        "{SERVICE_NAME}\n\nUsage: cargo run -p mooncache-admin-api-app -- [OPTIONS]\n\nOptions:\n  --bind-addr <ADDR>           API bind address [env: {SERVICE_ENV_PREFIX}_BIND_ADDR or MOONCACHE_BIND_ADDR] [default: {DEFAULT_BIND_ADDR}]\n  --etcd-url <URL>             Etcd endpoint URL [env: {SERVICE_ENV_PREFIX}_ETCD_URL or MOONCACHE_ETCD_URL] [default: {DEFAULT_ETCD_URL}]\n  --tenant-config <PATH>       Tenant config path [env: {SERVICE_ENV_PREFIX}_TENANT_CONFIG or MOONCACHE_TENANT_CONFIG] [default: {DEFAULT_TENANT_CONFIG_PATH}]\n  --ssd-root <PATH>            SSD root path [env: {SERVICE_ENV_PREFIX}_SSD_ROOT or MOONCACHE_SSD_ROOT] [default: {DEFAULT_SSD_ROOT_PATH}]\n  --metrics-bind-addr <ADDR>   Metrics bind address [env: {SERVICE_ENV_PREFIX}_METRICS_BIND_ADDR or MOONCACHE_METRICS_BIND_ADDR] [default: {DEFAULT_METRICS_BIND_ADDR}]\n  --vendor-config <PATH>       Vendor config path [env: {SERVICE_ENV_PREFIX}_VENDOR_CONFIG or MOONCACHE_VENDOR_CONFIG] [default: {DEFAULT_VENDOR_CONFIG_PATH}]\n  -h, --help                   Print help and exit\n"
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
        std::any::type_name::<mooncache_admin_api::AdminService>()
    );
}

#[derive(Clone)]
struct AdminHttpState {
    service: Arc<AdminService>,
    master: Arc<Mutex<MasterState>>,
}

#[derive(Debug, Deserialize)]
struct FingerprintDebugHttpRequest {
    tenant_id: String,
    endpoint_version: String,
    vendor_id: String,
    resolved_model_version: String,
    adapter_version: String,
    cache_policy: String,
    body: Value,
}

#[derive(Debug, Deserialize)]
struct PurgeHttpRequest {
    tenant_id: String,
    cache_key: String,
}

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

fn build_router() -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/admin/metrics", get(admin_metrics))
        .route("/admin/audit-events", get(audit_events))
        .route("/admin/nodes", get(nodes))
        .route("/admin/nodes/{node_id}/drain", post(drain_node))
        .route("/admin/cache/fingerprint/debug", post(fingerprint_debug))
        .route("/admin/cache/purge", post(cache_purge))
        .route("/admin/cache/warmup", post(cache_warmup))
        .with_state(build_admin_state())
}

fn build_admin_state() -> AdminHttpState {
    let mut master = MasterState::new_for_test();
    master.mount_segment("local-store", 1_048_576);
    AdminHttpState {
        service: Arc::new(AdminService::new_for_test(["local-store"])),
        master: Arc::new(Mutex::new(master)),
    }
}

async fn healthz() -> Json<Value> {
    Json(json!({"ok": true, "service": SERVICE_NAME}))
}

async fn admin_metrics(State(state): State<AdminHttpState>, headers: HeaderMap) -> Response {
    let context = match admin_context(&headers) {
        Ok(context) => context,
        Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
    };
    match state.service.metrics_snapshot(&context) {
        Ok(snapshot) => Json(snapshot).into_response(),
        Err(error) => admin_error_response(error),
    }
}

async fn audit_events(State(state): State<AdminHttpState>, headers: HeaderMap) -> Response {
    let context = match admin_context(&headers) {
        Ok(context) => context,
        Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
    };
    match state.service.audit_events(&context) {
        Ok(events) => Json(events).into_response(),
        Err(error) => admin_error_response(error),
    }
}

async fn nodes(State(state): State<AdminHttpState>, headers: HeaderMap) -> Response {
    let context = match admin_context(&headers) {
        Ok(context) => context,
        Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
    };
    match state.service.list_nodes(&context) {
        Ok(nodes) => Json(
            nodes
                .into_iter()
                .map(|node| json!({"node_id": node.node_id, "draining": node.draining}))
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(error) => admin_error_response(error),
    }
}

async fn drain_node(
    State(state): State<AdminHttpState>,
    headers: HeaderMap,
    Path(node_id): Path<String>,
) -> Response {
    let context = match admin_context(&headers) {
        Ok(context) => context,
        Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
    };
    match state.service.drain_node(&context, &node_id) {
        Ok(node) => {
            Json(json!({"node_id": node.node_id, "draining": node.draining})).into_response()
        }
        Err(error) => admin_error_response(error),
    }
}

async fn fingerprint_debug(
    State(state): State<AdminHttpState>,
    headers: HeaderMap,
    Json(request): Json<FingerprintDebugHttpRequest>,
) -> Response {
    let context = match admin_context(&headers) {
        Ok(context) => context,
        Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
    };
    let tenant_id = match TenantId::parse(request.tenant_id) {
        Ok(tenant_id) => tenant_id,
        Err(error) => return cache_error_response(error),
    };
    let request = CacheFingerprintDebugRequest {
        tenant_id,
        endpoint_version: request.endpoint_version,
        vendor_id: request.vendor_id,
        resolved_model_version: request.resolved_model_version,
        adapter_version: request.adapter_version,
        cache_policy: request.cache_policy,
        body: request.body,
    };
    match state.service.debug_cache_fingerprint(&context, request) {
        Ok(response) => Json(json!({"cache_key": response.cache_key.as_str()})).into_response(),
        Err(error) => admin_error_response(error),
    }
}

async fn cache_purge(
    State(state): State<AdminHttpState>,
    headers: HeaderMap,
    Json(request): Json<PurgeHttpRequest>,
) -> Response {
    let context = match admin_context(&headers) {
        Ok(context) => context,
        Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
    };
    let tenant_id = match TenantId::parse(request.tenant_id) {
        Ok(tenant_id) => tenant_id,
        Err(error) => return cache_error_response(error),
    };
    let cache_key = match CacheKey::from_hex(request.cache_key) {
        Ok(cache_key) => cache_key,
        Err(error) => return cache_error_response(error),
    };
    let mut master = match state.master.lock() {
        Ok(master) => master,
        Err(_) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "master state is unavailable",
            );
        }
    };

    match state
        .service
        .delete_cache_object(&context, &mut master, &tenant_id, &cache_key)
    {
        Ok(()) => Json(json!({"ok": true})).into_response(),
        Err(error) => admin_error_response(error),
    }
}

async fn cache_warmup(headers: HeaderMap, Json(_request): Json<Value>) -> Response {
    let context = match admin_context(&headers) {
        Ok(context) => context,
        Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
    };
    if !context.role.allows(AdminAction::WarmupCache) {
        return json_error(
            StatusCode::FORBIDDEN,
            "forbidden: WarmupCache requires elevated role",
        );
    }
    Json(json!({
        "ok": true,
        "status": "not_configured",
        "detail": "local warmup backend is not configured; no upstream warmup was performed"
    }))
    .into_response()
}

fn admin_context(headers: &HeaderMap) -> Result<AdminRequestContext, String> {
    let actor =
        optional_header(headers, "x-admin-actor")?.unwrap_or_else(|| "local-admin".to_owned());
    let role = optional_header(headers, "x-admin-role")?
        .map(|value| parse_role(&value))
        .transpose()?
        .unwrap_or(Role::Admin);
    Ok(AdminRequestContext {
        actor,
        role,
        request_id: RequestId::new(),
    })
}

fn parse_role(value: &str) -> Result<Role, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "admin" => Ok(Role::Admin),
        "operator" => Ok(Role::Operator),
        "viewer" => Ok(Role::Viewer),
        "no-access" | "no_access" => Ok(Role::NoAccess),
        other => Err(format!("invalid x-admin-role `{other}`")),
    }
}

fn optional_header(headers: &HeaderMap, name: &str) -> Result<Option<String>, String> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| format!("header `{name}` must be valid UTF-8"))
        })
        .transpose()
}

fn admin_error_response(error: AdminError) -> Response {
    match error {
        AdminError::Forbidden { .. } => json_error(StatusCode::FORBIDDEN, error.to_string()),
        AdminError::NotFound { .. } => json_error(StatusCode::NOT_FOUND, error.to_string()),
        AdminError::Cache(error) => cache_error_response(error),
        AdminError::Audit(_) | AdminError::StateUnavailable => {
            json_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
        }
    }
}

fn cache_error_response(error: CacheError) -> Response {
    let status = match error {
        CacheError::EmptyTenantId | CacheError::InvalidCacheKey | CacheError::InvalidId(_) => {
            StatusCode::BAD_REQUEST
        }
        CacheError::NotFound => StatusCode::NOT_FOUND,
        CacheError::Conflict(_) => StatusCode::CONFLICT,
        CacheError::QuotaExceeded(_) | CacheError::UpstreamUnavailable(_) => {
            StatusCode::SERVICE_UNAVAILABLE
        }
    };
    json_error(status, error.to_string())
}

fn json_error(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        Json(json!({
            "ok": false,
            "error": message.into()
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::{
        body::{to_bytes, Body},
        http::{header, Request, StatusCode},
    };
    use serde_json::{json, Value};
    use tower::ServiceExt;

    #[test]
    fn cli_flags_override_defaults() {
        let parsed = parse_config(vec![
            "--bind-addr".to_string(),
            "127.0.0.1:18083".to_string(),
            "--etcd-url=http://etcd:2379".to_string(),
            "--tenant-config".to_string(),
            "local/tenants.toml".to_string(),
            "--ssd-root".to_string(),
            "local/ssd".to_string(),
            "--metrics-bind-addr".to_string(),
            "127.0.0.1:19093".to_string(),
            "--vendor-config=local/vendors.toml".to_string(),
        ])
        .expect("flags should parse");

        let ParsedConfig::Run(config) = parsed else {
            panic!("expected run config");
        };

        assert_eq!(config.bind_addr, "127.0.0.1:18083");
        assert_eq!(config.etcd_url, "http://etcd:2379");
        assert_eq!(config.tenant_config_path, "local/tenants.toml");
        assert_eq!(config.ssd_root_path, "local/ssd");
        assert_eq!(config.metrics_bind_addr, "127.0.0.1:19093");
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

    #[tokio::test]
    async fn http_router_maps_admin_service_endpoints() {
        let app = build_router();

        let health = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("health request should complete");
        assert_eq!(health.status(), StatusCode::OK);
        let health_body = response_json(health).await;
        assert_eq!(health_body["ok"], json!(true));
        assert_eq!(health_body["service"], json!(SERVICE_NAME));

        let nodes = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/nodes")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("nodes request should complete");
        assert_eq!(nodes.status(), StatusCode::OK);
        let nodes_body = response_json(nodes).await;
        assert_eq!(nodes_body[0]["node_id"], json!("local-store"));
        assert_eq!(nodes_body[0]["draining"], json!(false));

        let debug = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/cache/fingerprint/debug")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-admin-role", "viewer")
                    .body(Body::from(
                        json!({
                            "tenant_id": "test-tenant",
                            "endpoint_version": "responses-v1",
                            "vendor_id": "mock",
                            "resolved_model_version": "gpt-test",
                            "adapter_version": "mock-v1",
                            "cache_policy": "default",
                            "body": {"model": "gpt-test", "input": "hello"}
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("debug request should complete");
        assert_eq!(debug.status(), StatusCode::OK);
        let debug_body = response_json(debug).await;
        assert_eq!(debug_body["cache_key"].as_str().unwrap().len(), 64);

        let drain = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/nodes/local-store/drain")
                    .header("x-admin-role", "operator")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("drain request should complete");
        assert_eq!(drain.status(), StatusCode::OK);
        let drain_body = response_json(drain).await;
        assert_eq!(drain_body["node_id"], json!("local-store"));
        assert_eq!(drain_body["draining"], json!(true));

        let metrics = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/metrics")
                    .header("x-admin-role", "viewer")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("metrics request should complete");
        assert_eq!(metrics.status(), StatusCode::OK);
        let metrics_body = response_json(metrics).await;
        assert!(metrics_body["audit_events_total"].as_u64().unwrap() >= 1);

        let audit = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/audit-events")
                    .header("x-admin-role", "viewer")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("audit request should complete");
        assert_eq!(audit.status(), StatusCode::OK);
        assert!(!response_json(audit).await.as_array().unwrap().is_empty());

        let warmup = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/cache/warmup")
                    .header("x-admin-role", "operator")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json!({"tenant_id": "test-tenant"}).to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("warmup request should complete");
        assert_eq!(warmup.status(), StatusCode::OK);
        let warmup_body = response_json(warmup).await;
        assert_eq!(warmup_body["ok"], json!(true));
        assert_eq!(warmup_body["status"], json!("not_configured"));

        let purge = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/cache/purge")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "tenant_id": "test-tenant",
                            "cache_key": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("purge request should complete");
        assert_eq!(purge.status(), StatusCode::NOT_FOUND);
        let purge_body = response_json(purge).await;
        assert_eq!(purge_body["ok"], json!(false));
    }

    async fn response_json(response: axum::response::Response) -> Value {
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        serde_json::from_slice(&bytes).expect("body should be JSON")
    }
}
