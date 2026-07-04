use mooncache_common::{CacheError, CacheResult};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TenantQuota {
    pub dram_bytes: u64,
    pub ssd_bytes: u64,
    pub used_dram_bytes: u64,
    pub used_ssd_bytes: u64,
}

impl TenantQuota {
    pub fn new(dram_bytes: u64, ssd_bytes: u64) -> Self {
        Self {
            dram_bytes,
            ssd_bytes,
            used_dram_bytes: 0,
            used_ssd_bytes: 0,
        }
    }

    pub(crate) fn with_usage(dram_bytes: u64, ssd_bytes: u64, used_dram_bytes: u64) -> Self {
        Self {
            dram_bytes,
            ssd_bytes,
            used_dram_bytes,
            used_ssd_bytes: 0,
        }
    }

    pub(crate) fn reserve_dram(&mut self, bytes: u64) -> CacheResult<()> {
        let next_used = self
            .used_dram_bytes
            .checked_add(bytes)
            .ok_or_else(|| CacheError::QuotaExceeded("DRAM quota accounting overflow".into()))?;

        if next_used > self.dram_bytes {
            return Err(CacheError::QuotaExceeded(format!(
                "DRAM quota exceeded: requested {next_used} bytes, limit {} bytes",
                self.dram_bytes
            )));
        }

        self.used_dram_bytes = next_used;
        Ok(())
    }

    pub(crate) fn release_dram(&mut self, bytes: u64) {
        self.used_dram_bytes = self.used_dram_bytes.saturating_sub(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_dram_rejects_usage_above_limit() {
        let mut quota = TenantQuota::new(4096, 0);
        quota.reserve_dram(4096).unwrap();

        let err = quota.reserve_dram(1).unwrap_err();

        assert!(err.to_string().contains("quota exceeded"));
    }

    #[test]
    fn release_dram_frees_reserved_bytes() {
        let mut quota = TenantQuota::new(4096, 0);
        quota.reserve_dram(4096).unwrap();
        quota.release_dram(4096);

        quota.reserve_dram(4096).unwrap();
        assert_eq!(quota.used_dram_bytes, 4096);
    }
}
