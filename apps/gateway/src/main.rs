use std::{
    collections::{BTreeMap, HashMap},
    env,
    process::ExitCode,
    sync::Arc,
};

use axum::{
    extract::State,
    http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
#[cfg(test)]
use mooncache_gateway::MockVendorAdapter;
use mooncache_gateway::{
    clients::{RemoteMasterClient, RemoteStoreClient},
    handle_response_request, GatewayError, GatewayRequest, GatewayResponse, GatewayState,
    OpenAiResponsesAdapter, TenantConfigSet, VendorConfigSet,
};
#[cfg(test)]
use mooncache_master::MasterState;
#[cfg(test)]
use mooncache_store::MemoryStore;
use serde_json::{json, Value};
use tokio::net::TcpListener;

const SERVICE_NAME: &str = "mooncache-gateway";
const SERVICE_ENV_PREFIX: &str = "MOONCACHE_GATEWAY";
const DEFAULT_BIND_ADDR: &str = "0.0.0.0:8080";
const DEFAULT_METRICS_BIND_ADDR: &str = "0.0.0.0:9090";
const DEFAULT_ETCD_URL: &str = "http://127.0.0.1:2379";
const DEFAULT_MASTER_URL: &str = "http://127.0.0.1:8081";
const DEFAULT_STORE_NODES: &str = "default=http://127.0.0.1:8082";
const DEFAULT_TENANT_CONFIG_PATH: &str = "config/tenants.toml";
const DEFAULT_SSD_ROOT_PATH: &str = "/var/lib/mooncache/ssd";
const DEFAULT_VENDOR_CONFIG_PATH: &str = "config/vendors.toml";

#[derive(Debug, PartialEq, Eq)]
struct AppConfig {
    bind_addr: String,
    etcd_url: String,
    master_url: String,
    store_node_urls: BTreeMap<String, String>,
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
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        return Ok(ParsedConfig::Help);
    }
    let mut config = AppConfig::from_env()?;
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
        } else if arg == "--master-url" {
            config.master_url = next_value(&mut args, "--master-url")?;
        } else if let Some(value) = arg.strip_prefix("--master-url=") {
            config.master_url = required_value(value, "--master-url")?;
        } else if arg == "--store-nodes" {
            config.store_node_urls =
                parse_store_node_urls(&next_value(&mut args, "--store-nodes")?)?;
        } else if let Some(value) = arg.strip_prefix("--store-nodes=") {
            config.store_node_urls =
                parse_store_node_urls(required_value(value, "--store-nodes")?.as_str())?;
        } else if arg == "--store-node" {
            insert_store_node(
                &mut config.store_node_urls,
                &next_value(&mut args, "--store-node")?,
            )?;
        } else if let Some(value) = arg.strip_prefix("--store-node=") {
            insert_store_node(
                &mut config.store_node_urls,
                required_value(value, "--store-node")?.as_str(),
            )?;
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
    fn from_env() -> Result<Self, String> {
        Ok(Self {
            bind_addr: env_value("BIND_ADDR", DEFAULT_BIND_ADDR),
            etcd_url: env_value("ETCD_URL", DEFAULT_ETCD_URL),
            master_url: env_value("MASTER_URL", DEFAULT_MASTER_URL),
            store_node_urls: parse_store_node_urls(&env_value("STORE_NODES", DEFAULT_STORE_NODES))?,
            tenant_config_path: env_value("TENANT_CONFIG", DEFAULT_TENANT_CONFIG_PATH),
            ssd_root_path: env_value("SSD_ROOT", DEFAULT_SSD_ROOT_PATH),
            metrics_bind_addr: env_value("METRICS_BIND_ADDR", DEFAULT_METRICS_BIND_ADDR),
            vendor_config_path: env_value("VENDOR_CONFIG", DEFAULT_VENDOR_CONFIG_PATH),
        })
    }
}

fn env_value(suffix: &str, default: &str) -> String {
    env::var(format!("{SERVICE_ENV_PREFIX}_{suffix}"))
        .or_else(|_| env::var(format!("MOONCACHE_{suffix}")))
        .unwrap_or_else(|_| default.to_string())
}

fn parse_store_node_urls(raw: &str) -> Result<BTreeMap<String, String>, String> {
    let mut nodes = BTreeMap::new();
    for item in raw.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        insert_store_node(&mut nodes, item)?;
    }
    if nodes.is_empty() {
        return Err("store node list must not be empty".to_owned());
    }
    Ok(nodes)
}

fn insert_store_node(nodes: &mut BTreeMap<String, String>, item: &str) -> Result<(), String> {
    let (node_id, url) = item
        .split_once('=')
        .ok_or_else(|| format!("store node `{item}` must use NODE_ID=URL"))?;
    let node_id = node_id.trim();
    let url = url.trim();
    if node_id.is_empty() || url.is_empty() {
        return Err(format!(
            "store node `{item}` must use non-empty NODE_ID=URL"
        ));
    }
    nodes.insert(node_id.to_owned(), url.trim_end_matches('/').to_owned());
    Ok(())
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
        "{SERVICE_NAME}\n\nUsage: cargo run -p mooncache-gateway-app -- [OPTIONS]\n\nOptions:\n  --bind-addr <ADDR>           API bind address [env: {SERVICE_ENV_PREFIX}_BIND_ADDR or MOONCACHE_BIND_ADDR] [default: {DEFAULT_BIND_ADDR}]\n  --etcd-url <URL>             Etcd endpoint URL [env: {SERVICE_ENV_PREFIX}_ETCD_URL or MOONCACHE_ETCD_URL] [default: {DEFAULT_ETCD_URL}]\n  --master-url <URL>           Master API URL [env: {SERVICE_ENV_PREFIX}_MASTER_URL or MOONCACHE_MASTER_URL] [default: {DEFAULT_MASTER_URL}]\n  --store-nodes <NODE=URL,..>  Store node URLs [env: {SERVICE_ENV_PREFIX}_STORE_NODES or MOONCACHE_STORE_NODES] [default: {DEFAULT_STORE_NODES}]\n  --store-node <NODE=URL>      Add or replace one store node URL; may repeat\n  --tenant-config <PATH>       Tenant config path [env: {SERVICE_ENV_PREFIX}_TENANT_CONFIG or MOONCACHE_TENANT_CONFIG] [default: {DEFAULT_TENANT_CONFIG_PATH}]\n  --ssd-root <PATH>            SSD root path [env: {SERVICE_ENV_PREFIX}_SSD_ROOT or MOONCACHE_SSD_ROOT] [default: {DEFAULT_SSD_ROOT_PATH}]\n  --metrics-bind-addr <ADDR>   Metrics bind address [env: {SERVICE_ENV_PREFIX}_METRICS_BIND_ADDR or MOONCACHE_METRICS_BIND_ADDR] [default: {DEFAULT_METRICS_BIND_ADDR}]\n  --vendor-config <PATH>       Vendor config path [env: {SERVICE_ENV_PREFIX}_VENDOR_CONFIG or MOONCACHE_VENDOR_CONFIG] [default: {DEFAULT_VENDOR_CONFIG_PATH}]\n  -h, --help                   Print help\n"
    )
}

fn print_resolved_config(config: &AppConfig) {
    println!("{SERVICE_NAME} resolved config:");
    println!("  bind_addr={}", config.bind_addr);
    println!("  etcd_url={}", config.etcd_url);
    println!("  master_url={}", config.master_url);
    println!("  store_node_urls={:?}", config.store_node_urls);
    println!("  tenant_config_path={}", config.tenant_config_path);
    println!("  ssd_root_path={}", config.ssd_root_path);
    println!("  metrics_bind_addr={}", config.metrics_bind_addr);
    println!("  vendor_config_path={}", config.vendor_config_path);
    println!(
        "  core_crate={}",
        std::any::type_name::<mooncache_gateway::GatewayState>()
    );
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
    axum::serve(listener, build_router_from_config(&config)?)
        .await
        .map_err(|error| format!("server error: {error}"))
}

#[cfg(test)]
fn build_router() -> Router {
    let state = Arc::new(build_gateway_state());
    router_with_state(state)
}

fn build_router_from_config(config: &AppConfig) -> Result<Router, String> {
    let state = Arc::new(build_gateway_state_from_config(config)?);
    Ok(router_with_state(state))
}

fn router_with_state(state: Arc<GatewayState>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics/snapshot", get(metrics_snapshot))
        .route("/v1/responses", post(responses))
        .with_state(state)
}

#[cfg(test)]
fn build_gateway_state() -> GatewayState {
    let mut master = MasterState::new_for_test();
    master.mount_segment("local-store", 1_048_576);
    master
        .set_tenant_quota("test-tenant", 1_048_576, 0)
        .expect("local tenant quota should be valid");
    let store = MemoryStore::with_capacity(1_048_576);
    let vendor = Arc::new(MockVendorAdapter::new_json(json!({
        "id": "resp_local_test",
        "output_text": "hello from mooncache local gateway"
    })));
    GatewayState::new_for_test(master, store, vendor)
}

fn build_gateway_state_from_config(config: &AppConfig) -> Result<GatewayState, String> {
    let tenants = TenantConfigSet::load(&config.tenant_config_path)
        .map_err(|error| format!("failed to load tenant config: {error}"))?;
    let vendors = VendorConfigSet::load(&config.vendor_config_path)
        .map_err(|error| format!("failed to load vendor config: {error}"))?;
    let vendor_config = vendors
        .vendors()
        .next()
        .ok_or_else(|| "vendor config must contain at least one vendor".to_owned())?;
    if vendor_config.adapter != "openai-responses" {
        return Err(format!(
            "unsupported vendor adapter `{}` for vendor `{}`",
            vendor_config.adapter, vendor_config.id
        ));
    }
    let api_key = env::var(&vendor_config.api_key_env).map_err(|_| {
        format!(
            "vendor `{}` api key env `{}` is not set",
            vendor_config.id, vendor_config.api_key_env
        )
    })?;
    let vendor = Arc::new(
        OpenAiResponsesAdapter::new(
            vendor_config.base_url.clone(),
            api_key,
            vendor_config.timeout_ms,
            vendor_config.headers.clone(),
            vendor_config.models.clone(),
        )
        .map_err(|error| format!("failed to build vendor `{}`: {error}", vendor_config.id))?,
    );

    let master_client = Arc::new(RemoteMasterClient::new(config.master_url.clone()));
    let store_client = Arc::new(RemoteStoreClient::new(HashMap::from_iter(
        config.store_node_urls.clone(),
    )));
    Ok(GatewayState::new_with_clients_and_tenant_config(
        master_client,
        store_client,
        vendor,
        tenants,
    ))
}

async fn healthz() -> Json<Value> {
    Json(json!({"ok": true, "service": SERVICE_NAME}))
}

async fn metrics_snapshot(State(state): State<Arc<GatewayState>>) -> Json<Value> {
    Json(json!(state.metrics_snapshot()))
}

async fn responses(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let authorization = match optional_header(&headers, header::AUTHORIZATION.as_str()) {
        Ok(value) => value,
        Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
    };
    let cache_control = match optional_header(&headers, "x-cache-control") {
        Ok(Some(value)) => Some(value),
        Ok(None) => match optional_header(&headers, header::CACHE_CONTROL.as_str()) {
            Ok(value) => value,
            Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
        },
        Err(message) => return json_error(StatusCode::BAD_REQUEST, message),
    };

    match handle_response_request(
        &state,
        GatewayRequest {
            authorization,
            cache_control,
            body,
        },
    )
    .await
    {
        Ok(response) => gateway_http_response(response),
        Err(error) => gateway_error_response(error),
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

fn gateway_http_response(response: GatewayResponse) -> Response {
    let status =
        StatusCode::from_u16(response.status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut http_response = (status, Json(response.body)).into_response();
    for (name, value) in response.headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            http_response.headers_mut().insert(name, value);
        }
    }
    http_response
}

fn gateway_error_response(error: GatewayError) -> Response {
    let status = match error {
        GatewayError::Json(_) => StatusCode::BAD_REQUEST,
        GatewayError::Vendor(_) => StatusCode::BAD_GATEWAY,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
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

    use std::collections::BTreeMap;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    use axum::{
        body::{to_bytes, Body},
        http::{header, Request, StatusCode},
    };
    use serde_json::{json, Value};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    async fn serve_one_vendor_response() -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                let n = socket.read(&mut buffer).await.unwrap();
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..n]);
                if request.windows(4).any(|window| window == b"\r\n\r\n")
                    && String::from_utf8_lossy(&request).contains("\"input\":\"from config\"")
                {
                    break;
                }
            }
            let response_body =
                "{\"id\":\"resp_config\",\"output_text\":\"from configured vendor\"}";
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8(request).unwrap()
        });
        (format!("http://{addr}/v1"), handle)
    }

    struct RemoteTestCluster {
        master_url: String,
        store_url: String,
        master_start_count: Arc<AtomicUsize>,
        store_write_count: Arc<AtomicUsize>,
        master_handle: tokio::task::JoinHandle<()>,
        store_handle: tokio::task::JoinHandle<()>,
    }

    async fn serve_remote_master_and_store() -> RemoteTestCluster {
        let committed_len = Arc::new(Mutex::new(None::<u64>));
        let pending_len = Arc::new(Mutex::new(None::<u64>));
        let stored_bytes = Arc::new(Mutex::new(Vec::<u8>::new()));
        let master_start_count = Arc::new(AtomicUsize::new(0));
        let store_write_count = Arc::new(AtomicUsize::new(0));

        let master = Router::new()
            .route(
                "/objects/replicas",
                get({
                    let committed_len = Arc::clone(&committed_len);
                    move || {
                        let committed_len = Arc::clone(&committed_len);
                        async move {
                            match *committed_len.lock().expect("committed length lock should work") {
                                Some(len) => (
                                    StatusCode::OK,
                                    Json(json!({"replicas": [{"node_id": "default", "offset": 0, "len": len}]})),
                                ),
                                None => (
                                    StatusCode::NOT_FOUND,
                                    Json(json!({"error": "cache object not found"})),
                                ),
                            }
                        }
                    }
                }),
            )
            .route(
                "/objects/start",
                post({
                    let master_start_count = Arc::clone(&master_start_count);
                    let pending_len = Arc::clone(&pending_len);
                    move |Json(body): Json<Value>| {
                        let master_start_count = Arc::clone(&master_start_count);
                        let pending_len = Arc::clone(&pending_len);
                        async move {
                            master_start_count.fetch_add(1, Ordering::SeqCst);
                            let len = body["len"].as_u64().expect("start request should include len");
                            *pending_len.lock().expect("pending length lock should work") = Some(len);
                            (
                                StatusCode::OK,
                                Json(json!({"replicas": [{"node_id": "default", "offset": 0, "len": len}]})),
                            )
                        }
                    }
                }),
            )
            .route(
                "/objects/end",
                post({
                    let committed_len = Arc::clone(&committed_len);
                    let pending_len = Arc::clone(&pending_len);
                    move |_body: Json<Value>| {
                        let committed_len = Arc::clone(&committed_len);
                        let pending_len = Arc::clone(&pending_len);
                        async move {
                            *committed_len.lock().expect("committed length lock should work") =
                                *pending_len.lock().expect("pending length lock should work");
                            (StatusCode::OK, Json(json!({"ok": true})))
                        }
                    }
                }),
            )
            .route(
                "/objects/revoke",
                post(|| async { (StatusCode::OK, Json(json!({"ok": true}))) }),
            );
        let master_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let master_addr = master_listener.local_addr().unwrap();
        let master_handle = tokio::spawn(async move {
            axum::serve(master_listener, master).await.unwrap();
        });

        let store = Router::new()
            .route(
                "/chunks/preallocated",
                post({
                    let stored_bytes = Arc::clone(&stored_bytes);
                    let store_write_count = Arc::clone(&store_write_count);
                    move |Json(body): Json<Value>| {
                        let stored_bytes = Arc::clone(&stored_bytes);
                        let store_write_count = Arc::clone(&store_write_count);
                        async move {
                            store_write_count.fetch_add(1, Ordering::SeqCst);
                            let data: Vec<u8> = serde_json::from_value(body["data"].clone()).unwrap();
                            *stored_bytes.lock().expect("stored bytes lock should work") = data;
                            (StatusCode::OK, Json(json!({"ok": true})))
                        }
                    }
                }),
            )
            .route(
                "/chunks/{offset}/{len}",
                get({
                    let stored_bytes = Arc::clone(&stored_bytes);
                    move || {
                        let stored_bytes = Arc::clone(&stored_bytes);
                        async move {
                            let bytes = stored_bytes.lock().expect("stored bytes lock should work").clone();
                            (
                                StatusCode::OK,
                                Json(json!({"offset": 0, "len": bytes.len(), "tier": "dram", "data": bytes})),
                            )
                        }
                    }
                }),
            );
        let store_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let store_addr = store_listener.local_addr().unwrap();
        let store_handle = tokio::spawn(async move {
            axum::serve(store_listener, store).await.unwrap();
        });

        RemoteTestCluster {
            master_url: format!("http://{master_addr}"),
            store_url: format!("http://{store_addr}"),
            master_start_count,
            store_write_count,
            master_handle,
            store_handle,
        }
    }
    use tower::ServiceExt;

    #[test]
    fn cli_flags_override_defaults() {
        let parsed = parse_config(vec![
            "--bind-addr".to_string(),
            "127.0.0.1:18080".to_string(),
            "--etcd-url=http://etcd:2379".to_string(),
            "--master-url".to_string(),
            "http://master:8081".to_string(),
            "--store-nodes=node-a=http://store-a:8082".to_string(),
            "--store-node".to_string(),
            "node-b=http://store-b:8082".to_string(),
            "--tenant-config".to_string(),
            "local/tenants.toml".to_string(),
            "--ssd-root".to_string(),
            "local/ssd".to_string(),
            "--metrics-bind-addr".to_string(),
            "127.0.0.1:19090".to_string(),
            "--vendor-config=local/vendors.toml".to_string(),
        ])
        .expect("flags should parse");

        let ParsedConfig::Run(config) = parsed else {
            panic!("expected run config");
        };

        assert_eq!(config.bind_addr, "127.0.0.1:18080");
        assert_eq!(config.etcd_url, "http://etcd:2379");
        assert_eq!(config.master_url, "http://master:8081");
        assert_eq!(
            config.store_node_urls,
            BTreeMap::from([
                ("node-a".to_owned(), "http://store-a:8082".to_owned()),
                ("node-b".to_owned(), "http://store-b:8082".to_owned()),
            ])
        );
        assert_eq!(config.tenant_config_path, "local/tenants.toml");
        assert_eq!(config.ssd_root_path, "local/ssd");
        assert_eq!(config.metrics_bind_addr, "127.0.0.1:19090");
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

    #[test]
    fn invalid_store_nodes_is_an_error() {
        let error = parse_config(vec![
            "--store-nodes".to_string(),
            "missing-equals".to_string(),
        ])
        .expect_err("malformed store nodes should fail");

        assert_eq!(error, "store node `missing-equals` must use NODE_ID=URL");
    }

    #[tokio::test]
    async fn http_router_serves_health_and_responses() {
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

        let cacheable_body = json!({"model": "gpt-test", "input": "cache me"});
        let miss = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/responses")
                    .header(header::AUTHORIZATION, "Bearer test-api-key")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(cacheable_body.to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("cache miss request should complete");
        assert_eq!(miss.status(), StatusCode::OK);
        assert_eq!(miss.headers().get("x-cache-status").unwrap(), "miss");
        assert_eq!(miss.headers().get("x-cache-write").unwrap(), "committed");

        let hit = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/responses")
                    .header(header::AUTHORIZATION, "Bearer test-api-key")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(cacheable_body.to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("cache hit request should complete");
        assert_eq!(hit.status(), StatusCode::OK);
        assert_eq!(hit.headers().get("x-cache-status").unwrap(), "hit");
        assert_eq!(hit.headers().get("x-cache-write").unwrap(), "skipped");

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/responses")
                    .header(header::AUTHORIZATION, "Bearer test-api-key")
                    .header("x-cache-control", "bypass")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({"model": "gpt-test", "input": "hello"}).to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("response request should complete");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("x-cache-status").unwrap(), "bypass");
        assert_eq!(response.headers().get("x-cache-write").unwrap(), "skipped");
        let body = response_json(response).await;
        assert_eq!(body["id"], json!("resp_local_test"));
    }

    #[tokio::test]
    async fn config_router_authenticates_tenant_and_calls_configured_vendor() {
        let (vendor_base_url, captured_request) = serve_one_vendor_response().await;
        std::env::set_var("MOONCACHE_TEST_VENDOR_KEY", "configured-vendor-token");
        let tempdir = tempfile::tempdir().unwrap();
        let tenant_config = tempdir.path().join("tenants.toml");
        let vendor_config = tempdir.path().join("vendors.toml");
        std::fs::write(
            &tenant_config,
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
            allowed_vendors = ["openai"]
            "#,
        )
        .unwrap();
        std::fs::write(
            &vendor_config,
            format!(
                r#"
                [[vendors]]
                id = "openai"
                adapter = "openai-responses"
                adapter_version = "openai-responses-v1"
                base_url = "{vendor_base_url}"
                api_key_env = "MOONCACHE_TEST_VENDOR_KEY"
                timeout_ms = 1000

                [[vendors.models]]
                requested = "gpt-test"
                resolved = "gpt-test"
                cache_eligible = true
                "#
            ),
        )
        .unwrap();
        let cluster = serve_remote_master_and_store().await;
        let config = AppConfig {
            bind_addr: "127.0.0.1:0".to_owned(),
            etcd_url: DEFAULT_ETCD_URL.to_owned(),
            master_url: cluster.master_url.clone(),
            store_node_urls: BTreeMap::from([("default".to_owned(), cluster.store_url.clone())]),
            tenant_config_path: tenant_config.display().to_string(),
            ssd_root_path: tempdir.path().join("ssd").display().to_string(),
            metrics_bind_addr: "127.0.0.1:0".to_owned(),
            vendor_config_path: vendor_config.display().to_string(),
        };
        let app = build_router_from_config(&config).expect("config router should build");

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/responses")
                    .header(header::AUTHORIZATION, "Bearer demo-api-key-do-not-use")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({"model": "gpt-test", "input": "from config", "temperature": 0})
                            .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("configured response request should complete");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("x-cache-status").unwrap(), "miss");
        let body = response_json(response).await;
        assert_eq!(body["output_text"], json!("from configured vendor"));
        let vendor_request = captured_request.await.unwrap();
        assert!(vendor_request.contains("authorization: Bearer configured-vendor-token"));
        assert_eq!(
            cluster.master_start_count.load(Ordering::SeqCst),
            1,
            "configured gateway must reserve through remote master, not a local MasterState"
        );
        assert_eq!(
            cluster.store_write_count.load(Ordering::SeqCst),
            1,
            "configured gateway must write through remote store, not a local MemoryStore"
        );
        cluster.master_handle.abort();
        cluster.store_handle.abort();
    }
    async fn response_json(response: axum::response::Response) -> Value {
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        serde_json::from_slice(&bytes).expect("body should be JSON")
    }
}
