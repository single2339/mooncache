use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheMetric {
    RequestTotal,
    RequestLatencySeconds,
    VendorCallsAvoidedTotal,
    SingleflightWaitersTotal,
    MasterObjectsTotal,
    MasterEvictionsTotal,
    StoreDramBytes,
    StoreSsdBytes,
    StoreReadLatencySeconds,
    AdminAuditEventsTotal,
}

impl CacheMetric {
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::RequestTotal => "mooncache_gateway_requests_total",
            Self::RequestLatencySeconds => "mooncache_gateway_request_latency_seconds",
            Self::VendorCallsAvoidedTotal => "mooncache_gateway_vendor_calls_avoided_total",
            Self::SingleflightWaitersTotal => "mooncache_gateway_singleflight_waiters_total",
            Self::MasterObjectsTotal => "mooncache_master_objects_total",
            Self::MasterEvictionsTotal => "mooncache_master_evictions_total",
            Self::StoreDramBytes => "mooncache_store_dram_bytes",
            Self::StoreSsdBytes => "mooncache_store_ssd_bytes",
            Self::StoreReadLatencySeconds => "mooncache_store_read_latency_seconds",
            Self::AdminAuditEventsTotal => "mooncache_admin_audit_events_total",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CacheStatus {
    Hit,
    Miss,
    Bypass,
    Ineligible,
    CacheOnlyMiss,
    Degraded,
}

impl CacheStatus {
    #[must_use]
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Bypass => "bypass",
            Self::Ineligible => "ineligible",
            Self::CacheOnlyMiss => "cache_only_miss",
            Self::Degraded => "degraded",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CacheWriteStatus {
    Committed,
    Failed,
    Skipped,
}

impl CacheWriteStatus {
    #[must_use]
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SingleflightRole {
    Leader,
    Waiter,
    None,
    OverCapacity,
}

impl SingleflightRole {
    #[must_use]
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Leader => "leader",
            Self::Waiter => "waiter",
            Self::None => "none",
            Self::OverCapacity => "over_capacity",
        }
    }
}

#[derive(Debug, Default)]
pub struct GatewayMetrics {
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_bypasses: AtomicU64,
    cache_ineligible: AtomicU64,
    cache_only_misses: AtomicU64,
    cache_degraded: AtomicU64,
    write_committed: AtomicU64,
    write_failed: AtomicU64,
    write_skipped: AtomicU64,
    singleflight_leaders: AtomicU64,
    singleflight_waiters: AtomicU64,
    singleflight_none: AtomicU64,
    singleflight_over_capacity: AtomicU64,
    vendor_calls: AtomicU64,
    vendor_latency_micros_total: AtomicU64,
}

impl GatewayMetrics {
    pub fn record_cache_status(&self, status: CacheStatus) {
        match status {
            CacheStatus::Hit => self.cache_hits.fetch_add(1, Ordering::Relaxed),
            CacheStatus::Miss => self.cache_misses.fetch_add(1, Ordering::Relaxed),
            CacheStatus::Bypass => self.cache_bypasses.fetch_add(1, Ordering::Relaxed),
            CacheStatus::Ineligible => self.cache_ineligible.fetch_add(1, Ordering::Relaxed),
            CacheStatus::CacheOnlyMiss => self.cache_only_misses.fetch_add(1, Ordering::Relaxed),
            CacheStatus::Degraded => self.cache_degraded.fetch_add(1, Ordering::Relaxed),
        };
    }

    pub fn record_write(&self, status: CacheWriteStatus) {
        match status {
            CacheWriteStatus::Committed => self.write_committed.fetch_add(1, Ordering::Relaxed),
            CacheWriteStatus::Failed => self.write_failed.fetch_add(1, Ordering::Relaxed),
            CacheWriteStatus::Skipped => self.write_skipped.fetch_add(1, Ordering::Relaxed),
        };
    }

    pub fn record_singleflight(&self, role: SingleflightRole) {
        match role {
            SingleflightRole::Leader => self.singleflight_leaders.fetch_add(1, Ordering::Relaxed),
            SingleflightRole::Waiter => self.singleflight_waiters.fetch_add(1, Ordering::Relaxed),
            SingleflightRole::None => self.singleflight_none.fetch_add(1, Ordering::Relaxed),
            SingleflightRole::OverCapacity => self
                .singleflight_over_capacity
                .fetch_add(1, Ordering::Relaxed),
        };
    }

    pub fn record_vendor_call(&self, latency: Duration) {
        self.vendor_calls.fetch_add(1, Ordering::Relaxed);
        let micros = u64::try_from(latency.as_micros())
            .unwrap_or(u64::MAX)
            .max(1);
        self.vendor_latency_micros_total
            .fetch_add(micros, Ordering::Relaxed);
    }

    #[must_use]
    pub fn snapshot(&self) -> GatewayMetricsSnapshot {
        GatewayMetricsSnapshot {
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            cache_bypasses: self.cache_bypasses.load(Ordering::Relaxed),
            cache_ineligible: self.cache_ineligible.load(Ordering::Relaxed),
            cache_only_misses: self.cache_only_misses.load(Ordering::Relaxed),
            cache_degraded: self.cache_degraded.load(Ordering::Relaxed),
            write_committed: self.write_committed.load(Ordering::Relaxed),
            write_failed: self.write_failed.load(Ordering::Relaxed),
            write_skipped: self.write_skipped.load(Ordering::Relaxed),
            singleflight_leaders: self.singleflight_leaders.load(Ordering::Relaxed),
            singleflight_waiters: self.singleflight_waiters.load(Ordering::Relaxed),
            singleflight_none: self.singleflight_none.load(Ordering::Relaxed),
            singleflight_over_capacity: self.singleflight_over_capacity.load(Ordering::Relaxed),
            vendor_calls: self.vendor_calls.load(Ordering::Relaxed),
            vendor_latency_micros_total: self.vendor_latency_micros_total.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayMetricsSnapshot {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_bypasses: u64,
    pub cache_ineligible: u64,
    pub cache_only_misses: u64,
    pub cache_degraded: u64,
    pub write_committed: u64,
    pub write_failed: u64,
    pub write_skipped: u64,
    pub singleflight_leaders: u64,
    pub singleflight_waiters: u64,
    pub singleflight_none: u64,
    pub singleflight_over_capacity: u64,
    pub vendor_calls: u64,
    pub vendor_latency_micros_total: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdminActionResult {
    Success,
    Denied,
    Failed,
}

impl AdminActionResult {
    #[must_use]
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Denied => "denied",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Default)]
pub struct AdminMetrics {
    audit_events_total: AtomicU64,
    audit_success_total: AtomicU64,
    audit_denied_total: AtomicU64,
    audit_failed_total: AtomicU64,
    action_counts: Mutex<BTreeMap<String, u64>>,
}

impl AdminMetrics {
    pub fn record_action(&self, action: &str, result: AdminActionResult) {
        self.audit_events_total.fetch_add(1, Ordering::Relaxed);
        match result {
            AdminActionResult::Success => self.audit_success_total.fetch_add(1, Ordering::Relaxed),
            AdminActionResult::Denied => self.audit_denied_total.fetch_add(1, Ordering::Relaxed),
            AdminActionResult::Failed => self.audit_failed_total.fetch_add(1, Ordering::Relaxed),
        };

        let mut counts = self
            .action_counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *counts.entry(action.to_owned()).or_default() += 1;
    }

    #[must_use]
    pub fn snapshot(&self) -> AdminMetricsSnapshot {
        AdminMetricsSnapshot {
            audit_events_total: self.audit_events_total.load(Ordering::Relaxed),
            audit_success_total: self.audit_success_total.load(Ordering::Relaxed),
            audit_denied_total: self.audit_denied_total.load(Ordering::Relaxed),
            audit_failed_total: self.audit_failed_total.load(Ordering::Relaxed),
            action_counts: self
                .action_counts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminMetricsSnapshot {
    pub audit_events_total: u64,
    pub audit_success_total: u64,
    pub audit_denied_total: u64,
    pub audit_failed_total: u64,
    pub action_counts: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreCapacitySnapshot {
    pub dram_bytes_used: u64,
    pub dram_bytes_capacity: u64,
    pub ssd_bytes_used: u64,
    pub ssd_bytes_capacity: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MasterMetricsSnapshot {
    pub objects_total: u64,
    pub evictions_total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayTraceFields {
    pub request_id: String,
    pub tenant_id: String,
    pub cache_key_redacted: String,
    pub cache_status: CacheStatus,
    pub vendor_id: String,
    pub model_version: String,
    pub coalesced_role: SingleflightRole,
    pub writeback_status: CacheWriteStatus,
}

impl GatewayTraceFields {
    #[must_use]
    pub fn as_label_pairs(&self) -> BTreeMap<&'static str, String> {
        BTreeMap::from([
            ("request_id", self.request_id.clone()),
            ("tenant_id", self.tenant_id.clone()),
            ("cache_key", self.cache_key_redacted.clone()),
            ("cache_status", self.cache_status.as_label().to_owned()),
            ("vendor_id", self.vendor_id.clone()),
            ("model_version", self.model_version.clone()),
            ("coalesced_role", self.coalesced_role.as_label().to_owned()),
            (
                "writeback_status",
                self.writeback_status.as_label().to_owned(),
            ),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn cache_status_metric_labels_are_stable() {
        assert_eq!(
            CacheMetric::RequestTotal.name(),
            "mooncache_gateway_requests_total"
        );
        assert_eq!(CacheStatus::Hit.as_label(), "hit");
        assert_eq!(CacheStatus::Degraded.as_label(), "degraded");
        assert_eq!(
            CacheMetric::StoreDramBytes.name(),
            "mooncache_store_dram_bytes"
        );
        assert_eq!(
            CacheMetric::AdminAuditEventsTotal.name(),
            "mooncache_admin_audit_events_total"
        );
    }

    #[test]
    fn gateway_metrics_snapshot_counts_cache_write_singleflight_and_vendor_events() {
        let metrics = GatewayMetrics::default();

        metrics.record_cache_status(CacheStatus::Hit);
        metrics.record_cache_status(CacheStatus::Miss);
        metrics.record_cache_status(CacheStatus::Miss);
        metrics.record_write(CacheWriteStatus::Committed);
        metrics.record_write(CacheWriteStatus::Failed);
        metrics.record_write(CacheWriteStatus::Skipped);
        metrics.record_singleflight(SingleflightRole::Leader);
        metrics.record_singleflight(SingleflightRole::Waiter);
        metrics.record_singleflight(SingleflightRole::None);
        metrics.record_vendor_call(Duration::from_micros(15));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.cache_hits, 1);
        assert_eq!(snapshot.cache_misses, 2);
        assert_eq!(snapshot.write_committed, 1);
        assert_eq!(snapshot.write_failed, 1);
        assert_eq!(snapshot.write_skipped, 1);
        assert_eq!(snapshot.singleflight_leaders, 1);
        assert_eq!(snapshot.singleflight_waiters, 1);
        assert_eq!(snapshot.singleflight_none, 1);
        assert_eq!(snapshot.vendor_calls, 1);
        assert_eq!(snapshot.vendor_latency_micros_total, 15);
    }

    #[test]
    fn admin_metrics_snapshot_counts_actions_by_result() {
        let metrics = AdminMetrics::default();

        metrics.record_action("drain_node", AdminActionResult::Success);
        metrics.record_action("drain_node", AdminActionResult::Denied);
        metrics.record_action("read_audit_log", AdminActionResult::Success);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.audit_events_total, 3);
        assert_eq!(snapshot.audit_success_total, 2);
        assert_eq!(snapshot.audit_denied_total, 1);
        assert_eq!(snapshot.action_counts.get("drain_node"), Some(&2));
        assert_eq!(snapshot.action_counts.get("read_audit_log"), Some(&1));
    }

    #[test]
    fn admin_metrics_recovers_action_counts_after_lock_poisoning() {
        let metrics = AdminMetrics::default();
        metrics.record_action("drain_node", AdminActionResult::Success);

        let _ = std::panic::catch_unwind(|| {
            let _held = metrics
                .action_counts
                .lock()
                .expect("action-count lock should be available before poisoning");
            panic!("poison action-count lock for recovery test");
        });

        metrics.record_action("read_metrics", AdminActionResult::Denied);
        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.audit_events_total, 2);
        assert_eq!(snapshot.audit_success_total, 1);
        assert_eq!(snapshot.audit_denied_total, 1);
        assert_eq!(snapshot.action_counts.get("drain_node"), Some(&1));
        assert_eq!(snapshot.action_counts.get("read_metrics"), Some(&1));
    }

    #[test]
    fn gateway_trace_fields_expose_required_labels_without_payloads() {
        let fields = GatewayTraceFields {
            request_id: "req-1".to_owned(),
            tenant_id: "tenant-a".to_owned(),
            cache_key_redacted: "01234567…cdef".to_owned(),
            cache_status: CacheStatus::Hit,
            vendor_id: "mock".to_owned(),
            model_version: "gpt-test-2026-07-04".to_owned(),
            coalesced_role: SingleflightRole::Waiter,
            writeback_status: CacheWriteStatus::Skipped,
        };

        let pairs = fields.as_label_pairs();
        assert_eq!(pairs.get("request_id"), Some(&"req-1".to_owned()));
        assert_eq!(pairs.get("tenant_id"), Some(&"tenant-a".to_owned()));
        assert_eq!(pairs.get("cache_key"), Some(&"01234567…cdef".to_owned()));
        assert_eq!(pairs.get("cache_status"), Some(&"hit".to_owned()));
        assert_eq!(pairs.get("vendor_id"), Some(&"mock".to_owned()));
        assert_eq!(
            pairs.get("model_version"),
            Some(&"gpt-test-2026-07-04".to_owned())
        );
        assert_eq!(pairs.get("coalesced_role"), Some(&"waiter".to_owned()));
        assert_eq!(pairs.get("writeback_status"), Some(&"skipped".to_owned()));
    }
}
