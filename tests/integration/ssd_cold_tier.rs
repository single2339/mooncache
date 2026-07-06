use std::{
    fs,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

use reqwest::Client;
use serde_json::{json, Value};

const TEST_SSD_KEY_HEX: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
const TENANT_ID: &str = "tenant-a";

struct StoreNodeProcess {
    child: Child,
    base_url: String,
    _stdout: BufReader<std::process::ChildStdout>,
}

impl StoreNodeProcess {
    fn base_url(&self) -> &str {
        &self.base_url
    }
}

impl Drop for StoreNodeProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test]
async fn chunk_written_with_object_identity_is_promoted_from_ssd_after_dram_restart() {
    let ssd_root = tempfile::tempdir().expect("SSD root tempdir should be created");
    let client = Client::new();
    let payload = b"cold tier survives dram restart";
    let cache_key = "cold-restart-key";

    let node = start_store_node(ssd_root.path());
    let write_body: Value = client
        .post(format!("{}/chunks", node.base_url()))
        .json(&json!({
            "tenant_id": TENANT_ID,
            "cache_key": cache_key,
            "len": payload.len(),
            "data": payload.to_vec(),
        }))
        .send()
        .await
        .expect("DRAM write request should complete")
        .json()
        .await
        .expect("DRAM write response should be JSON");
    assert_eq!(write_body["ok"], json!(true));
    assert_eq!(write_body["offset"], json!(0));
    assert_eq!(write_body["len"], json!(payload.len()));
    drop(node);

    let cold_node = start_store_node(ssd_root.path());
    let read_response = client
        .get(format!(
            "{}/chunks/0/{}?tenant_id={}&cache_key={}",
            cold_node.base_url(),
            payload.len(),
            TENANT_ID,
            cache_key,
        ))
        .send()
        .await
        .expect("cold read request should complete");

    assert_eq!(
        read_response.status(),
        reqwest::StatusCode::OK,
        "DRAM loss should not lose an object written with identity; the store must promote it from SSD"
    );
    let read_body: Value = read_response
        .json()
        .await
        .expect("cold read response should be JSON");
    assert_eq!(read_body["tier"], json!("ssd"));
    let returned: Vec<u8> = serde_json::from_value(read_body["data"].clone())
        .expect("cold read data should be encoded bytes");
    assert_eq!(returned, payload.to_vec());

    let promoted_response = client
        .get(format!(
            "{}/chunks/0/{}",
            cold_node.base_url(),
            payload.len()
        ))
        .send()
        .await
        .expect("promoted DRAM read request should complete");
    assert_eq!(promoted_response.status(), reqwest::StatusCode::OK);
    let promoted_body: Value = promoted_response
        .json()
        .await
        .expect("promoted read response should be JSON");
    assert_eq!(promoted_body["tier"], json!("dram"));
}

#[tokio::test]
async fn corrupted_ssd_object_returns_error_without_replaying_payload() {
    let ssd_root = tempfile::tempdir().expect("SSD root tempdir should be created");
    let client = Client::new();
    let payload = b"checksummed cold payload";
    let cache_key = "corrupt-key";

    let node = start_store_node(ssd_root.path());
    let write_response = client
        .post(format!("{}/chunks/preallocated", node.base_url()))
        .json(&json!({
            "tenant_id": TENANT_ID,
            "cache_key": cache_key,
            "offset": 128,
            "len": payload.len(),
            "data": payload.to_vec(),
        }))
        .send()
        .await
        .expect("SSD mirror write request should complete");
    assert_eq!(write_response.status(), reqwest::StatusCode::OK);
    drop(node);

    let corrupted_path = corrupt_only_ssd_object(ssd_root.path(), TENANT_ID);
    assert!(
        corrupted_path.starts_with(ssd_root.path()),
        "test must only corrupt the isolated SSD fixture"
    );

    let cold_node = start_store_node(ssd_root.path());
    let read_response = client
        .get(format!(
            "{}/chunks/128/{}?tenant_id={}&cache_key={}",
            cold_node.base_url(),
            payload.len(),
            TENANT_ID,
            cache_key,
        ))
        .send()
        .await
        .expect("corrupt cold read request should complete");

    assert_eq!(
        read_response.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        "corrupt SSD bytes must be rejected instead of replayed"
    );
    let body: Value = read_response
        .json()
        .await
        .expect("corrupt read response should be JSON");
    assert!(
        body.get("data").is_none(),
        "corrupt cold object must not return cached payload bytes"
    );
    let error = body["error"]
        .as_str()
        .expect("corrupt read should expose an error message");
    assert!(
        error.contains("decrypt") || error.contains("corrupt"),
        "corrupt cold object should surface validation failure, got {error:?}"
    );
}

#[allow(clippy::zombie_processes)]
fn start_store_node(ssd_root: &Path) -> StoreNodeProcess {
    let mut child = Command::new(env!("CARGO_BIN_EXE_mooncache-store-node-app"))
        .arg("--bind-addr")
        .arg("127.0.0.1:0")
        .arg("--ssd-root")
        .arg(ssd_root)
        .env("MOONCACHE_STORE_NODE_SSD_KEY", TEST_SSD_KEY_HEX)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("store-node process should start");

    let stdout = child
        .stdout
        .take()
        .expect("store-node stdout should be piped");
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();

    for _ in 0..64 {
        line.clear();
        let bytes = reader
            .read_line(&mut line)
            .expect("store-node stdout should be readable");
        if bytes == 0 {
            let status = child.wait().expect("child status should be readable");
            panic!("store-node exited before reporting its bound address: {status:?}");
        }

        if let Some(addr) = line
            .trim()
            .strip_prefix("mooncache-store-node listening on ")
        {
            return StoreNodeProcess {
                child,
                base_url: format!("http://{addr}"),
                _stdout: reader,
            };
        }
    }

    panic!("store-node did not report its bound address");
}

fn corrupt_only_ssd_object(root: &Path, tenant_id: &str) -> PathBuf {
    let tenant_dir = root.join(tenant_id);
    let mut object_paths: Vec<PathBuf> = fs::read_dir(&tenant_dir)
        .unwrap_or_else(|error| panic!("tenant SSD dir {tenant_dir:?} should exist: {error}"))
        .map(|entry| entry.expect("SSD dir entry should be readable").path())
        .filter(|path| path.is_file())
        .collect();
    object_paths.sort();
    assert_eq!(
        object_paths.len(),
        1,
        "test fixture should create exactly one SSD object file"
    );

    let object_path = object_paths.remove(0);
    let mut bytes = fs::read(&object_path).expect("SSD object bytes should be readable");
    assert!(
        bytes.len() > 1,
        "SSD object should contain enough bytes to corrupt"
    );
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    fs::write(&object_path, bytes).expect("SSD object bytes should be corruptible");
    object_path
}
