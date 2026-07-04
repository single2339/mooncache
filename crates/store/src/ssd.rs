use std::{
    io,
    path::{Path, PathBuf},
};

use chacha20poly1305::{
    aead::{Aead, AeadCore, KeyInit, OsRng, Payload},
    ChaCha20Poly1305,
};
use thiserror::Error;
use tokio::{fs, io::AsyncWriteExt};

const FILE_MAGIC: &[u8] = b"MCSSD1\0";
const NONCE_LEN: usize = 12;
const OBJECT_EXTENSION: &str = "mcobj";

pub type SsdResult<T> = Result<T, SsdError>;

#[derive(Debug, Error)]
pub enum SsdError {
    #[error("invalid {kind} path segment: {value:?}")]
    InvalidPathSegment { kind: &'static str, value: String },
    #[error("SSD object not found for tenant {tenant_id:?} and cache key {cache_key:?}")]
    NotFound {
        tenant_id: String,
        cache_key: String,
    },
    #[error("SSD object is corrupt: {reason}")]
    CorruptObject { reason: &'static str },
    #[error("failed to encrypt SSD object")]
    Encryption,
    #[error("failed to decrypt SSD object")]
    Decryption,
    #[error("SSD io error while {op} {path}: {source}")]
    Io {
        op: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

pub struct SsdStore {
    root: PathBuf,
    cipher: ChaCha20Poly1305,
}

impl SsdStore {
    pub async fn new_for_test(root: impl AsRef<Path>) -> SsdResult<Self> {
        let key = ChaCha20Poly1305::generate_key(&mut OsRng);
        Self::new_with_key(root, key.as_slice()).await
    }

    pub async fn new_with_key(root: impl AsRef<Path>, key: &[u8]) -> SsdResult<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)
            .await
            .map_err(|source| io_error("creating SSD root", &root, source))?;
        let cipher = ChaCha20Poly1305::new_from_slice(key).map_err(|_| SsdError::Encryption)?;
        Ok(Self { root, cipher })
    }

    pub async fn persist_object(
        &self,
        tenant_id: &str,
        cache_key: &str,
        bytes: &[u8],
    ) -> SsdResult<()> {
        let object_path = self.object_path(tenant_id, cache_key)?;
        let tenant_dir = object_path
            .parent()
            .ok_or(SsdError::CorruptObject {
                reason: "object path has no parent directory",
            })?
            .to_path_buf();
        fs::create_dir_all(&tenant_dir)
            .await
            .map_err(|source| io_error("creating tenant SSD directory", &tenant_dir, source))?;

        let nonce = ChaCha20Poly1305::generate_nonce(&mut OsRng);
        let aad = aad(tenant_id, cache_key);
        let ciphertext = self
            .cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: bytes,
                    aad: aad.as_bytes(),
                },
            )
            .map_err(|_| SsdError::Encryption)?;

        let mut file_bytes = Vec::with_capacity(FILE_MAGIC.len() + NONCE_LEN + ciphertext.len());
        file_bytes.extend_from_slice(FILE_MAGIC);
        file_bytes.extend_from_slice(&nonce);
        file_bytes.extend_from_slice(&ciphertext);

        let tmp_path = self.temp_path(&tenant_dir, cache_key)?;
        let write_result = async {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp_path)
                .await
                .map_err(|source| io_error("creating SSD temp object", &tmp_path, source))?;
            file.write_all(&file_bytes)
                .await
                .map_err(|source| io_error("writing SSD temp object", &tmp_path, source))?;
            file.flush()
                .await
                .map_err(|source| io_error("flushing SSD temp object", &tmp_path, source))?;
            file.sync_all()
                .await
                .map_err(|source| io_error("syncing SSD temp object", &tmp_path, source))?;
            drop(file);
            fs::rename(&tmp_path, &object_path)
                .await
                .map_err(|source| io_error("renaming SSD object into place", &object_path, source))
        }
        .await;

        if write_result.is_err() {
            let _ = fs::remove_file(&tmp_path).await;
        }

        write_result
    }

    pub async fn read_object(&self, tenant_id: &str, cache_key: &str) -> SsdResult<Vec<u8>> {
        let object_path = self.object_path(tenant_id, cache_key)?;
        let file_bytes = match fs::read(&object_path).await {
            Ok(bytes) => bytes,
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                return Err(SsdError::NotFound {
                    tenant_id: tenant_id.to_owned(),
                    cache_key: cache_key.to_owned(),
                });
            }
            Err(source) => return Err(io_error("reading SSD object", &object_path, source)),
        };

        let encrypted = file_bytes
            .strip_prefix(FILE_MAGIC)
            .ok_or(SsdError::CorruptObject {
                reason: "missing SSD file magic",
            })?;
        if encrypted.len() < NONCE_LEN {
            return Err(SsdError::CorruptObject {
                reason: "missing SSD object nonce",
            });
        }

        let (nonce, ciphertext) = encrypted.split_at(NONCE_LEN);
        let aad = aad(tenant_id, cache_key);
        self.cipher
            .decrypt(
                nonce.into(),
                Payload {
                    msg: ciphertext,
                    aad: aad.as_bytes(),
                },
            )
            .map_err(|_| SsdError::Decryption)
    }

    pub async fn promote_to_dram(&self, tenant_id: &str, cache_key: &str) -> SsdResult<Vec<u8>> {
        self.read_object(tenant_id, cache_key).await
    }

    fn object_path(&self, tenant_id: &str, cache_key: &str) -> SsdResult<PathBuf> {
        let tenant_id = validate_path_segment("tenant_id", tenant_id)?;
        let cache_key = validate_path_segment("cache_key", cache_key)?;
        Ok(self
            .root
            .join(tenant_id)
            .join(format!("{cache_key}.{OBJECT_EXTENSION}")))
    }

    fn temp_path(&self, tenant_dir: &Path, cache_key: &str) -> SsdResult<PathBuf> {
        let mut suffix = [0_u8; 8];
        chacha20poly1305::aead::rand_core::RngCore::fill_bytes(&mut OsRng, &mut suffix);
        Ok(tenant_dir.join(format!(".{cache_key}.{}.tmp", hex_lower(&suffix))))
    }
}

fn validate_path_segment<'a>(kind: &'static str, value: &'a str) -> SsdResult<&'a str> {
    let is_valid = !value.is_empty()
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));

    if is_valid {
        Ok(value)
    } else {
        Err(SsdError::InvalidPathSegment {
            kind,
            value: value.to_owned(),
        })
    }
}

fn aad(tenant_id: &str, cache_key: &str) -> String {
    format!("{tenant_id}\0{cache_key}")
}

fn io_error(op: &'static str, path: &Path, source: io::Error) -> SsdError {
    SsdError::Io {
        op,
        path: path.to_path_buf(),
        source,
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{SsdError, SsdStore};

    #[tokio::test]
    async fn persists_and_reads_encrypted_object() {
        let dir = tempfile::tempdir().unwrap();
        let store = SsdStore::new_for_test(dir.path()).await.unwrap();
        store
            .persist_object("tenant-a", "abc", b"payload")
            .await
            .unwrap();

        let bytes = store.read_object("tenant-a", "abc").await.unwrap();
        assert_eq!(bytes, b"payload");

        let raw = std::fs::read(dir.path().join("tenant-a").join("abc.mcobj")).unwrap();
        assert!(!raw.windows(b"payload".len()).any(|w| w == b"payload"));
    }

    #[tokio::test]
    async fn promotion_hook_returns_bytes_for_dram_caller() {
        let dir = tempfile::tempdir().unwrap();
        let store = SsdStore::new_for_test(dir.path()).await.unwrap();
        store
            .persist_object("tenant-a", "promote-key", b"promote me")
            .await
            .unwrap();

        let bytes = store
            .promote_to_dram("tenant-a", "promote-key")
            .await
            .unwrap();

        assert_eq!(bytes, b"promote me");
    }

    #[tokio::test]
    async fn rejects_path_unsafe_tenant_and_key_segments() {
        let dir = tempfile::tempdir().unwrap();
        let store = SsdStore::new_for_test(dir.path()).await.unwrap();

        let tenant_error = store
            .persist_object("../tenant", "abc", b"payload")
            .await
            .unwrap_err();
        assert!(matches!(
            tenant_error,
            SsdError::InvalidPathSegment {
                kind: "tenant_id",
                ..
            }
        ));

        let key_error = store
            .persist_object("tenant-a", "../abc", b"payload")
            .await
            .unwrap_err();
        assert!(matches!(
            key_error,
            SsdError::InvalidPathSegment {
                kind: "cache_key",
                ..
            }
        ));

        assert!(!dir.path().join("tenant").exists());
        assert!(!dir.path().join("abc.mcobj").exists());
    }
}
