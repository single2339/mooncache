use std::{
    collections::BTreeMap,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use mooncache_common::{
    AdminActionResult, AdminMetrics, AdminMetricsSnapshot, CacheError, CacheKey, RequestId,
    TenantId,
};
use mooncache_fingerprint::{compute_cache_key, FingerprintInput};
use mooncache_master::MasterState;
use serde_json::Value;
use thiserror::Error;

use crate::{AdminAction, AuditError, AuditEvent, AuditResult, InMemoryAuditSink, Role};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminRequestContext {
    pub actor: String,
    pub role: Role,
    pub request_id: RequestId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeView {
    pub node_id: String,
    pub draining: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheFingerprintDebugRequest {
    pub tenant_id: TenantId,
    pub endpoint_version: String,
    pub vendor_id: String,
    pub resolved_model_version: String,
    pub adapter_version: String,
    pub cache_policy: String,
    pub body: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheFingerprintDebugResponse {
    pub cache_key: CacheKey,
}

#[derive(Debug, Error)]
pub enum AdminError {
    #[error("forbidden: {action:?} requires elevated role")]
    Forbidden { action: AdminAction },
    #[error("not found: {resource}")]
    NotFound { resource: String },
    #[error(transparent)]
    Cache(#[from] CacheError),
    #[error(transparent)]
    Audit(#[from] AuditError),
    #[error("admin service state is unavailable")]
    StateUnavailable,
}

#[derive(Debug, Default)]
pub struct AdminService {
    nodes: Mutex<BTreeMap<String, NodeState>>,
    audit: InMemoryAuditSink,
    metrics: AdminMetrics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NodeState {
    draining: bool,
}

#[derive(Debug)]
struct AuditRecord {
    action: AdminAction,
    resource: String,
    tenant_scope: Option<String>,
    before_summary: Option<String>,
    after_summary: Option<String>,
    result: AuditResult,
}

impl AdminService {
    pub fn new_for_test<I, S>(node_ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let nodes = node_ids
            .into_iter()
            .map(|node_id| (node_id.into(), NodeState { draining: false }))
            .collect();
        Self {
            nodes: Mutex::new(nodes),
            audit: InMemoryAuditSink::default(),
            metrics: AdminMetrics::default(),
        }
    }

    pub fn metrics_snapshot(
        &self,
        context: &AdminRequestContext,
    ) -> Result<AdminMetricsSnapshot, AdminError> {
        let action = AdminAction::ReadMetrics;
        let resource = "metrics".to_owned();
        if !context.role.allows(action) {
            self.record_audit(
                context,
                AuditRecord {
                    action,
                    resource,
                    tenant_scope: None,
                    before_summary: None,
                    after_summary: None,
                    result: AuditResult::Denied,
                },
            )?;
            return Err(AdminError::Forbidden { action });
        }

        self.record_audit(
            context,
            AuditRecord {
                action,
                resource,
                tenant_scope: None,
                before_summary: None,
                after_summary: Some("snapshot_returned".to_owned()),
                result: AuditResult::Success,
            },
        )?;
        Ok(self.metrics.snapshot())
    }

    pub fn list_nodes(&self, context: &AdminRequestContext) -> Result<Vec<NodeView>, AdminError> {
        authorize(context, AdminAction::ReadNodes)?;
        self.nodes
            .lock()
            .map_err(|_| AdminError::StateUnavailable)
            .map(|nodes| {
                nodes
                    .iter()
                    .map(|(node_id, node)| NodeView {
                        node_id: node_id.clone(),
                        draining: node.draining,
                    })
                    .collect()
            })
    }

    pub fn drain_node(
        &self,
        context: &AdminRequestContext,
        node_id: &str,
    ) -> Result<NodeView, AdminError> {
        let action = AdminAction::DrainNode;
        let resource = format!("node:{node_id}");
        if !context.role.allows(action) {
            self.record_audit(
                context,
                AuditRecord {
                    action,
                    resource,
                    tenant_scope: None,
                    before_summary: None,
                    after_summary: None,
                    result: AuditResult::Denied,
                },
            )?;
            return Err(AdminError::Forbidden { action });
        }

        let mut nodes = self
            .nodes
            .lock()
            .map_err(|_| AdminError::StateUnavailable)?;
        let Some(node) = nodes.get_mut(node_id) else {
            let err = AdminError::NotFound {
                resource: format!("node:{node_id}"),
            };
            drop(nodes);
            self.record_audit(
                context,
                AuditRecord {
                    action,
                    resource: format!("node:{node_id}"),
                    tenant_scope: None,
                    before_summary: None,
                    after_summary: None,
                    result: AuditResult::Failed(err.to_string()),
                },
            )?;
            return Err(err);
        };

        let before = format!("draining={}", node.draining);
        node.draining = true;
        let after = format!("draining={}", node.draining);
        let view = NodeView {
            node_id: node_id.to_owned(),
            draining: node.draining,
        };
        drop(nodes);

        self.record_audit(
            context,
            AuditRecord {
                action,
                resource: format!("node:{node_id}"),
                tenant_scope: None,
                before_summary: Some(before),
                after_summary: Some(after),
                result: AuditResult::Success,
            },
        )?;
        Ok(view)
    }

    pub fn debug_cache_fingerprint(
        &self,
        context: &AdminRequestContext,
        request: CacheFingerprintDebugRequest,
    ) -> Result<CacheFingerprintDebugResponse, AdminError> {
        let action = AdminAction::DebugCacheFingerprint;
        let tenant_scope = Some(request.tenant_id.as_str().to_owned());
        let base_resource = format!("cache_fingerprint:{}", request.tenant_id.as_str());
        let request_summary = fingerprint_debug_summary(&request);
        if !context.role.allows(action) {
            self.record_audit(
                context,
                AuditRecord {
                    action,
                    resource: base_resource,
                    tenant_scope,
                    before_summary: Some(request_summary),
                    after_summary: None,
                    result: AuditResult::Denied,
                },
            )?;
            return Err(AdminError::Forbidden { action });
        }

        let cache_key = match compute_cache_key(&FingerprintInput {
            tenant_id: &request.tenant_id,
            endpoint_version: &request.endpoint_version,
            vendor_id: &request.vendor_id,
            resolved_model_version: &request.resolved_model_version,
            adapter_version: &request.adapter_version,
            cache_policy: &request.cache_policy,
            body: &request.body,
        }) {
            Ok(cache_key) => cache_key,
            Err(err) => {
                let message = err.to_string();
                self.record_audit(
                    context,
                    AuditRecord {
                        action,
                        resource: base_resource,
                        tenant_scope,
                        before_summary: Some(request_summary),
                        after_summary: None,
                        result: AuditResult::Failed(message),
                    },
                )?;
                return Err(AdminError::Cache(err));
            }
        };
        self.record_audit(
            context,
            AuditRecord {
                action,
                resource: format!("{base_resource}/{}", cache_key.redacted()),
                tenant_scope,
                before_summary: Some(request_summary),
                after_summary: Some(format!("cache_key={}", cache_key.redacted())),
                result: AuditResult::Success,
            },
        )?;
        Ok(CacheFingerprintDebugResponse { cache_key })
    }

    pub fn delete_cache_object(
        &self,
        context: &AdminRequestContext,
        master: &mut MasterState,
        tenant_id: &TenantId,
        cache_key: &CacheKey,
    ) -> Result<(), AdminError> {
        let action = AdminAction::RemoveCacheObject;
        let resource = format!(
            "cache_object:{}/{}",
            tenant_id.as_str(),
            cache_key.redacted()
        );
        let tenant_scope = Some(tenant_id.as_str().to_owned());
        if !context.role.allows(action) {
            self.record_audit(
                context,
                AuditRecord {
                    action,
                    resource,
                    tenant_scope,
                    before_summary: None,
                    after_summary: None,
                    result: AuditResult::Denied,
                },
            )?;
            return Err(AdminError::Forbidden { action });
        }

        match master.remove(tenant_id, cache_key) {
            Ok(()) => {
                self.record_audit(
                    context,
                    AuditRecord {
                        action,
                        resource,
                        tenant_scope,
                        before_summary: Some("delete requested".to_owned()),
                        after_summary: Some("deleted".to_owned()),
                        result: AuditResult::Success,
                    },
                )?;
                Ok(())
            }
            Err(err) => {
                self.record_audit(
                    context,
                    AuditRecord {
                        action,
                        resource,
                        tenant_scope,
                        before_summary: Some("delete requested".to_owned()),
                        after_summary: None,
                        result: AuditResult::Failed(err.to_string()),
                    },
                )?;
                Err(AdminError::Cache(err))
            }
        }
    }

    pub fn audit_events(
        &self,
        context: &AdminRequestContext,
    ) -> Result<Vec<AuditEvent>, AdminError> {
        let action = AdminAction::ReadAuditLog;
        let resource = "audit_log".to_owned();
        if !context.role.allows(action) {
            self.record_audit(
                context,
                AuditRecord {
                    action,
                    resource,
                    tenant_scope: None,
                    before_summary: None,
                    after_summary: None,
                    result: AuditResult::Denied,
                },
            )?;
            return Err(AdminError::Forbidden { action });
        }

        let events = self.audit.list().map_err(AdminError::Audit)?;
        self.record_audit(
            context,
            AuditRecord {
                action,
                resource,
                tenant_scope: None,
                before_summary: None,
                after_summary: Some(format!("listed_events={}", events.len())),
                result: AuditResult::Success,
            },
        )?;
        Ok(events)
    }

    fn record_audit(
        &self,
        context: &AdminRequestContext,
        record: AuditRecord,
    ) -> Result<(), AdminError> {
        let action = record.action;
        let result = admin_action_result(&record.result);
        self.audit.append(AuditEvent {
            actor: context.actor.clone(),
            role: context.role,
            action: record.action,
            resource: record.resource,
            tenant_scope: record.tenant_scope,
            before_summary: record.before_summary,
            after_summary: record.after_summary,
            request_id: context.request_id.clone(),
            timestamp_ms: now_ms(),
            result: record.result,
        })?;
        self.metrics
            .record_action(admin_action_label(action), result);
        Ok(())
    }
}

fn fingerprint_debug_summary(request: &CacheFingerprintDebugRequest) -> String {
    format!(
        "endpoint_version={} vendor_id={} resolved_model_version={} adapter_version={} cache_policy={} body=redacted",
        request.endpoint_version,
        request.vendor_id,
        request.resolved_model_version,
        request.adapter_version,
        request.cache_policy
    )
}

fn authorize(context: &AdminRequestContext, action: AdminAction) -> Result<(), AdminError> {
    if context.role.allows(action) {
        Ok(())
    } else {
        Err(AdminError::Forbidden { action })
    }
}

fn admin_action_result(result: &AuditResult) -> AdminActionResult {
    match result {
        AuditResult::Success => AdminActionResult::Success,
        AuditResult::Denied => AdminActionResult::Denied,
        AuditResult::Failed(_) => AdminActionResult::Failed,
    }
}

fn admin_action_label(action: AdminAction) -> &'static str {
    match action {
        AdminAction::ReadMetrics => "read_metrics",
        AdminAction::ReadNodes => "read_nodes",
        AdminAction::ReadAuditLog => "read_audit_log",
        AdminAction::DebugCacheFingerprint => "debug_cache_fingerprint",
        AdminAction::DrainNode => "drain_node",
        AdminAction::RemoveCacheObject => "remove_cache_object",
        AdminAction::WarmupCache => "warmup_cache",
        AdminAction::PatchTenantPolicy => "patch_tenant_policy",
        AdminAction::PatchVendorPolicy => "patch_vendor_policy",
        AdminAction::ManageUsers => "manage_users",
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}
