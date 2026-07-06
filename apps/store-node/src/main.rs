use std::{collections::HashMap, env, path::Path, process::ExitCode, sync::Arc};

use axum::{
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use mooncache_common::StoreCapacitySnapshot;
use mooncache_store::{ChunkHandle, MemoryStore, SsdError, SsdStore, StoreError};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::net::TcpListener;

const SERVICE_NAME: &str = "mooncache-store-node";
const SERVICE_ENV_PREFIX: &str = "MOONCACHE_STORE_NODE";
const DEFAULT_BIND_ADDR: &str = "0.0.0.0:8082";
const DEFAULT_METRICS_BIND_ADDR: &str = "0.0.0.0:9092";
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

type AppState = Arc<StoreNodeState>;

struct StoreNodeState {
    dram: Mutex<MemoryStore>,
    ssd: Option<SsdStore>,
    ssd_objects: Mutex<HashMap<(String, String), usize>>,
    ssd_bytes_capacity: u64,
}

// ── DTOs ────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct WriteChunkRequest {
    tenant_id: Option<String>,
    cache_key: Option<String>,
    len: usize,
    data: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct PreallocatedWriteRequest {
    tenant_id: Option<String>,
    cache_key: Option<String>,
    offset: usize,
    len: usize,
    data: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct ReadChunkQuery {
    tenant_id: Option<String>,
    cache_key: Option<String>,
}

#[derive(Debug, Serialize)]
struct WriteChunkResponse {
    ok: bool,
    offset: usize,
    len: usize,
}

#[derive(Debug, Serialize)]
struct ReadChunkResponse {
    offset: usize,
    len: usize,
    tier: &'static str,
    data: Vec<u8>,
}

// ── Entrypoint ───────────────────────────────────────────────────

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

impl StoreNodeState {
    #[cfg(test)]
    fn memory_only(dram_capacity: usize) -> Self {
        Self {
            dram: Mutex::new(MemoryStore::with_capacity(dram_capacity)),
            ssd: None,
            ssd_objects: Mutex::new(HashMap::new()),
            ssd_bytes_capacity: 0,
        }
    }

    async fn with_ssd(
        dram_capacity: usize,
        ssd_root: impl AsRef<Path>,
        ssd_bytes_capacity: u64,
        ssd_key: &[u8],
    ) -> Result<Self, SsdError> {
        let ssd = SsdStore::new_with_key(ssd_root, ssd_key).await?;
        Ok(Self {
            dram: Mutex::new(MemoryStore::with_capacity(dram_capacity)),
            ssd: Some(ssd),
            ssd_objects: Mutex::new(HashMap::new()),
            ssd_bytes_capacity,
        })
    }

    fn capacity_snapshot(&self) -> StoreCapacitySnapshot {
        let dram = self.dram.lock().capacity_snapshot();
        StoreCapacitySnapshot {
            dram_bytes_used: dram.dram_bytes_used,
            dram_bytes_capacity: dram.dram_bytes_capacity,
            ssd_bytes_used: self.ssd_bytes_used(),
            ssd_bytes_capacity: self.ssd_bytes_capacity,
        }
    }

    async fn allocate_and_write(
        &self,
        len: usize,
        data: &[u8],
        tenant_id: Option<&str>,
        cache_key: Option<&str>,
    ) -> Result<ChunkHandle, StoreNodeError> {
        let handle = {
            let mut dram = self.dram.lock();
            let handle = dram.allocate(len)?;
            dram.write_chunk(&handle, data)?;
            handle
        };

        if let (Some(ssd), Some(tenant_id), Some(cache_key)) = (&self.ssd, tenant_id, cache_key) {
            ssd.persist_object(tenant_id, cache_key, data).await?;
            self.record_ssd_object(tenant_id, cache_key, data.len());
        }

        Ok(handle)
    }

    async fn write_preallocated(
        &self,
        handle: &ChunkHandle,
        bytes: &[u8],
        tenant_id: Option<&str>,
        cache_key: Option<&str>,
    ) -> Result<(), StoreNodeError> {
        {
            let mut dram = self.dram.lock();
            dram.write_preallocated_chunk(handle, bytes)?;
        }

        if let (Some(ssd), Some(tenant_id), Some(cache_key)) = (&self.ssd, tenant_id, cache_key) {
            ssd.persist_object(tenant_id, cache_key, bytes).await?;
            self.record_ssd_object(tenant_id, cache_key, bytes.len());
        }
        Ok(())
    }

    async fn read_chunk(
        &self,
        handle: &ChunkHandle,
        tenant_id: Option<&str>,
        cache_key: Option<&str>,
    ) -> Result<ReadChunkResponse, StoreNodeError> {
        let dram_result = {
            let dram = self.dram.lock();
            dram.read_chunk(handle)
        };
        match dram_result {
            Ok(data) => Ok(ReadChunkResponse {
                offset: handle.offset(),
                len: handle.len(),
                tier: "dram",
                data,
            }),
            Err(err @ (StoreError::InvalidHandle { .. } | StoreError::UnwrittenChunk))
                if self.can_promote_from_ssd(tenant_id, cache_key) =>
            {
                let (Some(ssd), Some(tenant_id), Some(cache_key)) =
                    (&self.ssd, tenant_id, cache_key)
                else {
                    return Err(err.into());
                };
                let data = ssd.promote_to_dram(tenant_id, cache_key).await?;
                {
                    let mut dram = self.dram.lock();
                    dram.write_preallocated_chunk(handle, &data)?;
                }
                Ok(ReadChunkResponse {
                    offset: handle.offset(),
                    len: handle.len(),
                    tier: "ssd",
                    data,
                })
            }
            Err(err) => Err(err.into()),
        }
    }

    fn can_promote_from_ssd(&self, tenant_id: Option<&str>, cache_key: Option<&str>) -> bool {
        self.ssd.is_some() && tenant_id.is_some() && cache_key.is_some()
    }

    fn record_ssd_object(&self, tenant_id: &str, cache_key: &str, len: usize) {
        self.ssd_objects
            .lock()
            .insert((tenant_id.to_owned(), cache_key.to_owned()), len);
    }

    fn ssd_bytes_used(&self) -> u64 {
        self.ssd_objects
            .lock()
            .values()
            .fold(0_u64, |total, len| total.saturating_add(usize_to_u64(*len)))
    }
}

enum StoreNodeError {
    Store(StoreError),
    Ssd(SsdError),
}

impl From<StoreError> for StoreNodeError {
    fn from(value: StoreError) -> Self {
        Self::Store(value)
    }
}

impl From<SsdError> for StoreNodeError {
    fn from(value: SsdError) -> Self {
        Self::Ssd(value)
    }
}
fn usage() -> String {
    format!(
        "{SERVICE_NAME}\n\nUsage: cargo run -p mooncache-store-node-app -- [OPTIONS]\n\nOptions:\n  --bind-addr <ADDR>           API bind address [env: {SERVICE_ENV_PREFIX}_BIND_ADDR or MOONCACHE_BIND_ADDR] [default: {DEFAULT_BIND_ADDR}]\n  --etcd-url <URL>             Etcd endpoint URL [env: {SERVICE_ENV_PREFIX}_ETCD_URL or MOONCACHE_ETCD_URL] [default: {DEFAULT_ETCD_URL}]\n  --tenant-config <PATH>       Tenant config path [env: {SERVICE_ENV_PREFIX}_TENANT_CONFIG or MOONCACHE_TENANT_CONFIG] [default: {DEFAULT_TENANT_CONFIG_PATH}]\n  --ssd-root <PATH>            SSD root path [env: {SERVICE_ENV_PREFIX}_SSD_ROOT or MOONCACHE_SSD_ROOT] [default: {DEFAULT_SSD_ROOT_PATH}]\n  --metrics-bind-addr <ADDR>   Metrics bind address [env: {SERVICE_ENV_PREFIX}_METRICS_BIND_ADDR or MOONCACHE_METRICS_BIND_ADDR] [default: {DEFAULT_METRICS_BIND_ADDR}]\n  --vendor-config <PATH>       Vendor config path [env: {SERVICE_ENV_PREFIX}_VENDOR_CONFIG or MOONCACHE_VENDOR_CONFIG] [default: {DEFAULT_VENDOR_CONFIG_PATH}]\n  -h, --help                   Print help and exit\n"
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
        std::any::type_name::<mooncache_store::MemoryStore>()
    );
}

// ── Server ───────────────────────────────────────────────────────

async fn run_server(config: AppConfig) -> Result<(), String> {
    print_resolved_config(&config);
    let listener = TcpListener::bind(&config.bind_addr)
        .await
        .map_err(|error| format!("failed to bind {}: {error}", config.bind_addr))?;
    let local_addr = listener
        .local_addr()
        .map_err(|error| format!("failed to read bound address: {error}"))?;
    println!("{SERVICE_NAME} listening on {local_addr}");
    axum::serve(
        listener,
        build_router_with_ssd_root(&config.ssd_root_path).await?,
    )
    .await
    .map_err(|error| format!("server error: {error}"))
}

#[cfg(test)]
fn build_router() -> Router {
    build_router_with_state(Arc::new(StoreNodeState::memory_only(1_048_576)))
}

async fn build_router_with_ssd_root(ssd_root: impl AsRef<Path>) -> Result<Router, String> {
    let ssd_key = ssd_key_from_env()?;
    let state = StoreNodeState::with_ssd(1_048_576, ssd_root, 1_048_576, &ssd_key)
        .await
        .map_err(|error| format!("failed to initialize SSD tier: {error}"))?;
    Ok(build_router_with_state(Arc::new(state)))
}

fn ssd_key_from_env() -> Result<[u8; 32], String> {
    let value = env::var(format!("{SERVICE_ENV_PREFIX}_SSD_KEY"))
        .or_else(|_| env::var("MOONCACHE_SSD_KEY"))
        .map_err(|_| {
            format!(
                "{SERVICE_ENV_PREFIX}_SSD_KEY or MOONCACHE_SSD_KEY must be set to 64 lowercase hex characters"
            )
        })?;
    parse_32_byte_hex(&value)
}

fn parse_32_byte_hex(value: &str) -> Result<[u8; 32], String> {
    if value.len() != 64 {
        return Err("SSD key must be 64 lowercase hex characters".to_string());
    }
    let mut out = [0_u8; 32];
    let bytes = value.as_bytes();
    for (index, slot) in out.iter_mut().enumerate() {
        let high = hex_nibble(bytes[index * 2])?;
        let low = hex_nibble(bytes[index * 2 + 1])?;
        *slot = (high << 4) | low;
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err("SSD key must be 64 lowercase hex characters".to_string()),
    }
}

fn build_router_with_state(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics/snapshot", get(metrics_snapshot))
        .route("/chunks/{offset}/{len}", get(get_chunk))
        .route("/chunks", axum::routing::post(post_chunk))
        .route(
            "/chunks/preallocated",
            axum::routing::post(post_chunk_preallocated),
        )
        .with_state(state)
}

// ── Handlers ─────────────────────────────────────────────────────

async fn healthz() -> Json<Value> {
    Json(json!({"ok": true, "service": SERVICE_NAME}))
}

async fn metrics_snapshot(State(state): State<AppState>) -> Json<Value> {
    let snapshot = state.capacity_snapshot();
    Json(json!(snapshot))
}

async fn post_chunk(
    State(state): State<AppState>,
    Json(req): Json<WriteChunkRequest>,
) -> Result<Json<WriteChunkResponse>, (StatusCode, Json<Value>)> {
    let handle = state
        .allocate_and_write(
            req.len,
            &req.data,
            req.tenant_id.as_deref(),
            req.cache_key.as_deref(),
        )
        .await
        .map_err(store_node_error_to_response)?;
    Ok(Json(WriteChunkResponse {
        ok: true,
        offset: handle.offset(),
        len: handle.len(),
    }))
}

async fn post_chunk_preallocated(
    State(state): State<AppState>,
    Json(req): Json<PreallocatedWriteRequest>,
) -> Result<Json<WriteChunkResponse>, (StatusCode, Json<Value>)> {
    let handle = ChunkHandle::new(req.offset, req.len);
    state
        .write_preallocated(
            &handle,
            &req.data,
            req.tenant_id.as_deref(),
            req.cache_key.as_deref(),
        )
        .await
        .map_err(store_node_error_to_response)?;
    Ok(Json(WriteChunkResponse {
        ok: true,
        offset: handle.offset(),
        len: handle.len(),
    }))
}

async fn get_chunk(
    State(state): State<AppState>,
    AxumPath((offset, len)): AxumPath<(usize, usize)>,
    Query(query): Query<ReadChunkQuery>,
) -> Result<Json<ReadChunkResponse>, (StatusCode, Json<Value>)> {
    let handle = ChunkHandle::new(offset, len);
    state
        .read_chunk(
            &handle,
            query.tenant_id.as_deref(),
            query.cache_key.as_deref(),
        )
        .await
        .map(Json)
        .map_err(store_node_error_to_response)
}

// ── Error mapping ────────────────────────────────────────────────

fn store_error_to_response(error: StoreError) -> (StatusCode, Json<Value>) {
    let code = match &error {
        StoreError::EmptyChunk | StoreError::LengthMismatch { .. } => StatusCode::BAD_REQUEST,
        StoreError::InsufficientCapacity { .. } => StatusCode::INSUFFICIENT_STORAGE,
        StoreError::InvalidHandle { .. } | StoreError::UnwrittenChunk => StatusCode::NOT_FOUND,
        StoreError::ChecksumMismatch { .. } => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (code, Json(json!({"error": error.to_string()})))
}

fn store_node_error_to_response(error: StoreNodeError) -> (StatusCode, Json<Value>) {
    match error {
        StoreNodeError::Store(error) => store_error_to_response(error),
        StoreNodeError::Ssd(error) => ssd_error_to_response(error),
    }
}

fn ssd_error_to_response(error: SsdError) -> (StatusCode, Json<Value>) {
    let code = match &error {
        SsdError::InvalidPathSegment { .. } => StatusCode::BAD_REQUEST,
        SsdError::NotFound { .. } => StatusCode::NOT_FOUND,
        SsdError::CorruptObject { .. }
        | SsdError::Encryption
        | SsdError::Decryption
        | SsdError::Io { .. } => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (code, Json(json!({"error": error.to_string()})))
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

// ── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use serde_json::{json, Value};
    use tower::ServiceExt;

    const TEST_SSD_KEY: &[u8; 32] = b"mooncache-store-node-test-key!!!";

    #[test]
    fn parses_32_byte_ssd_key_hex() {
        let key =
            parse_32_byte_hex("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
                .expect("valid hex key should parse");

        assert_eq!(key[0], 0);
        assert_eq!(key[31], 31);
    }

    #[test]
    fn rejects_missing_or_uppercase_ssd_key_hex() {
        assert!(parse_32_byte_hex("abc").is_err());
        assert!(parse_32_byte_hex(
            "000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F"
        )
        .is_err());
    }

    // ── CLI tests (preserved from original) ──────────────────────

    #[test]
    fn cli_flags_override_defaults() {
        let parsed = parse_config(vec![
            "--bind-addr".to_string(),
            "127.0.0.1:18082".to_string(),
            "--etcd-url=http://etcd:2379".to_string(),
            "--tenant-config".to_string(),
            "local/tenants.toml".to_string(),
            "--ssd-root".to_string(),
            "local/ssd".to_string(),
            "--metrics-bind-addr".to_string(),
            "127.0.0.1:19092".to_string(),
            "--vendor-config=local/vendors.toml".to_string(),
        ])
        .expect("flags should parse");

        let ParsedConfig::Run(config) = parsed else {
            panic!("expected run config");
        };

        assert_eq!(config.bind_addr, "127.0.0.1:18082");
        assert_eq!(config.etcd_url, "http://etcd:2379");
        assert_eq!(config.tenant_config_path, "local/tenants.toml");
        assert_eq!(config.ssd_root_path, "local/ssd");
        assert_eq!(config.metrics_bind_addr, "127.0.0.1:19092");
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

    // ── HTTP endpoint tests ─────────────────────────────────────

    #[tokio::test]
    async fn healthz_returns_ok_and_service_name() {
        let app = build_router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("health request should complete");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["ok"], json!(true));
        assert_eq!(body["service"], json!(SERVICE_NAME));
    }

    #[tokio::test]
    async fn metrics_snapshot_returns_zero_initially() {
        let app = build_router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics/snapshot")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("snapshot request should complete");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["dram_bytes_used"], json!(0));
        assert_eq!(body["dram_bytes_capacity"], json!(1_048_576));
        assert_eq!(body["ssd_bytes_used"], json!(0));
        assert_eq!(body["ssd_bytes_capacity"], json!(0));
    }

    #[tokio::test]
    async fn write_chunk_then_read_chunk_roundtrip() {
        let app = build_router();
        let payload = b"hello store node";

        // Write
        let write_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/chunks")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"len": payload.len(), "data": payload.to_vec()}).to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("write should complete");

        assert_eq!(write_resp.status(), StatusCode::OK);
        let write_body = response_json(write_resp).await;
        assert_eq!(write_body["ok"], json!(true));
        assert_eq!(write_body["offset"], json!(0));
        assert_eq!(write_body["len"], json!(payload.len()));

        // Read
        let read_resp = app
            .oneshot(
                Request::builder()
                    .uri(format!("/chunks/0/{}", payload.len()))
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("read should complete");

        assert_eq!(read_resp.status(), StatusCode::OK);
        let read_body = response_json(read_resp).await;
        assert_eq!(read_body["offset"], json!(0));
        assert_eq!(read_body["len"], json!(payload.len()));
        let returned: Vec<u8> =
            serde_json::from_value(read_body["data"].clone()).expect("data should be bytes");
        assert_eq!(returned, payload.to_vec());
    }

    #[tokio::test]
    async fn read_missing_chunk_returns_not_found() {
        let app = build_router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/chunks/999/5")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("read should complete");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn read_unwritten_chunk_returns_not_found() {
        let app = build_router();

        // Allocate a slot by writing a different chunk first
        let app = app;
        let _write = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/chunks")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"len": 3, "data": [1, 2, 3]}).to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("write should complete");

        // Read at offset 4 (past allocated area) — not found
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/chunks/4/2")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("read should complete");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn write_empty_chunk_returns_bad_request() {
        let app = build_router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/chunks")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"len": 0, "data": []}).to_string()))
                    .expect("request should build"),
            )
            .await
            .expect("write should complete");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn write_length_mismatch_returns_bad_request() {
        let app = build_router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/chunks")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"len": 10, "data": [1, 2, 3]}).to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("write should complete");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn metrics_reflects_written_data() {
        let app = build_router();
        let payload = b"capacity check";

        let _write = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/chunks")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"len": payload.len(), "data": payload.to_vec()}).to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("write should complete");

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics/snapshot")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("snapshot should complete");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["dram_bytes_used"], json!(payload.len()));
        assert_eq!(body["dram_bytes_capacity"], json!(1_048_576));
    }

    #[tokio::test]
    async fn preallocated_write_with_object_identity_updates_ssd_metrics() {
        let dir = tempfile::tempdir().unwrap();
        let app = build_router_with_ssd_for_test(dir.path(), 1_048_576).await;
        let payload = b"ssd mirror";

        let write_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/chunks/preallocated")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "tenant_id": "tenant-a",
                            "cache_key": "abcdef",
                            "offset": 0,
                            "len": payload.len(),
                            "data": payload.to_vec()
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("write should complete");

        assert_eq!(write_resp.status(), StatusCode::OK);

        let metrics_resp = app
            .oneshot(
                Request::builder()
                    .uri("/metrics/snapshot")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("snapshot should complete");
        let metrics = response_json(metrics_resp).await;
        assert_eq!(metrics["dram_bytes_used"], json!(payload.len()));
        assert_eq!(metrics["ssd_bytes_used"], json!(payload.len()));
        assert_eq!(metrics["ssd_bytes_capacity"], json!(1_048_576));
    }

    #[tokio::test]
    async fn read_promotes_from_ssd_when_dram_chunk_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let app = build_router_with_ssd_for_test(dir.path(), 1_048_576).await;
        let payload = b"promote from ssd";

        let _write = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/chunks/preallocated")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "tenant_id": "tenant-a",
                            "cache_key": "abcdef",
                            "offset": 64,
                            "len": payload.len(),
                            "data": payload.to_vec()
                        })
                        .to_string(),
                    ))
                    .expect("request should build"),
            )
            .await
            .expect("write should complete");

        let cold_app = build_router_with_existing_ssd_for_test(dir.path(), 1_048_576).await;
        let read_resp = cold_app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/chunks/64/{}?tenant_id=tenant-a&cache_key=abcdef",
                        payload.len()
                    ))
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("read should complete");

        assert_eq!(read_resp.status(), StatusCode::OK);
        let body = response_json(read_resp).await;
        assert_eq!(body["tier"], json!("ssd"));
        let returned: Vec<u8> =
            serde_json::from_value(body["data"].clone()).expect("data should be bytes");
        assert_eq!(returned, payload.to_vec());

        let promoted_resp = cold_app
            .oneshot(
                Request::builder()
                    .uri(format!("/chunks/64/{}", payload.len()))
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("promoted read should complete");
        let promoted = response_json(promoted_resp).await;
        assert_eq!(promoted["tier"], json!("dram"));
    }

    // ── Helpers ──────────────────────────────────────────────────

    async fn build_router_with_ssd_for_test(root: &Path, ssd_bytes_capacity: u64) -> Router {
        let state = StoreNodeState::with_ssd(1_048_576, root, ssd_bytes_capacity, TEST_SSD_KEY)
            .await
            .expect("SSD state should build");
        build_router_with_state(Arc::new(state))
    }

    async fn build_router_with_existing_ssd_for_test(
        root: &Path,
        ssd_bytes_capacity: u64,
    ) -> Router {
        let state = StoreNodeState::with_ssd(1_048_576, root, ssd_bytes_capacity, TEST_SSD_KEY)
            .await
            .expect("SSD state should build");
        build_router_with_state(Arc::new(state))
    }

    async fn response_json(response: axum::response::Response) -> Value {
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        serde_json::from_slice(&bytes).expect("body should be JSON")
    }
}
