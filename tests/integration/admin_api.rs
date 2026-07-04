use mooncache_admin_api::{
    AdminAction, AdminRequestContext, AdminService, AuditEvent, AuditResult,
    CacheFingerprintDebugRequest, InMemoryAuditSink, Role,
};
use mooncache_common::{CacheKey, RequestId, TenantId};
use mooncache_master::MasterState;
use serde_json::json;

#[test]
fn operator_can_drain_node_but_viewer_cannot() {
    assert!(Role::Operator.allows(AdminAction::DrainNode));
    assert!(!Role::Viewer.allows(AdminAction::DrainNode));
}

#[test]
fn admin_can_patch_tenant_policy_but_operator_cannot() {
    assert!(Role::Admin.allows(AdminAction::PatchTenantPolicy));
    assert!(!Role::Operator.allows(AdminAction::PatchTenantPolicy));
}

#[test]
fn viewer_operator_and_admin_can_read_audit_log() {
    assert!(Role::Viewer.allows(AdminAction::ReadAuditLog));
    assert!(Role::Operator.allows(AdminAction::ReadAuditLog));
    assert!(Role::Admin.allows(AdminAction::ReadAuditLog));
    assert!(!Role::NoAccess.allows(AdminAction::ReadAuditLog));
}

#[test]
fn in_memory_audit_sink_appends_and_lists_events() {
    let sink = InMemoryAuditSink::default();
    let request_id = RequestId::new();

    sink.append(AuditEvent {
        actor: "alice".to_owned(),
        role: Role::Operator,
        action: AdminAction::DrainNode,
        resource: "node:node-a".to_owned(),
        tenant_scope: None,
        before_summary: Some("draining=false".to_owned()),
        after_summary: Some("draining=true".to_owned()),
        request_id,
        timestamp_ms: 42,
        result: AuditResult::Success,
    })
    .expect("audit append should succeed");

    let events = sink.list().expect("audit list should succeed");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].actor, "alice");
    assert_eq!(events[0].action, AdminAction::DrainNode);
    assert_eq!(events[0].result, AuditResult::Success);
}

#[test]
fn service_drains_node_for_operator_and_audits_write() {
    let service = AdminService::new_for_test(["node-a"]);
    let context = operator_context("ops@example.com");

    let drained = service
        .drain_node(&context, "node-a")
        .expect("operator should drain nodes");

    assert_eq!(drained.node_id, "node-a");
    assert!(drained.draining);

    let events = service
        .audit_events(&context)
        .expect("audit list should succeed");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].actor, "ops@example.com");
    assert_eq!(events[0].role, Role::Operator);
    assert_eq!(events[0].action, AdminAction::DrainNode);
    assert_eq!(events[0].resource, "node:node-a");
    assert_eq!(events[0].result, AuditResult::Success);
}

#[test]
fn service_rejects_viewer_drain_and_audits_denial() {
    let service = AdminService::new_for_test(["node-a"]);
    let context = viewer_context("reader@example.com");

    let err = service
        .drain_node(&context, "node-a")
        .expect_err("viewer should not drain nodes");

    assert_eq!(
        err.to_string(),
        "forbidden: DrainNode requires elevated role"
    );
    let nodes = service
        .list_nodes(&context)
        .expect("viewer should list nodes");
    assert_eq!(nodes[0].node_id, "node-a");
    assert!(!nodes[0].draining);

    let events = service
        .audit_events(&context)
        .expect("audit list should succeed");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].result, AuditResult::Denied);
}

#[test]
fn service_debugs_cache_fingerprint_for_viewer() {
    let service = AdminService::new_for_test(["node-a"]);
    let context = viewer_context("reader@example.com");
    let tenant_id = TenantId::parse("tenant-a").expect("valid tenant");

    let left = service
        .debug_cache_fingerprint(
            &context,
            fingerprint_request(
                tenant_id.clone(),
                json!({"input": "hello", "model": "gpt-x"}),
            ),
        )
        .expect("viewer should debug fingerprints");
    let right = service
        .debug_cache_fingerprint(
            &context,
            fingerprint_request(tenant_id, json!({"model": "gpt-x", "input": "hello"})),
        )
        .expect("viewer should debug fingerprints");

    assert_eq!(left.cache_key, right.cache_key);
    assert_eq!(left.cache_key.as_str().len(), 64);

    let events = service
        .audit_events(&context)
        .expect("audit list should succeed");
    assert_eq!(events.len(), 2);
    assert!(events.iter().all(|event| {
        event.action == AdminAction::DebugCacheFingerprint
            && event.tenant_scope.as_deref() == Some("tenant-a")
            && event.resource.starts_with("cache_fingerprint:tenant-a/")
            && event.result == AuditResult::Success
    }));
    let audit_text = format!("{events:?}");
    assert!(!audit_text.contains("hello"));
    assert!(!audit_text.contains(left.cache_key.as_str()));
}
#[test]
fn service_rejects_no_access_fingerprint_debug_and_audits_denial() {
    let service = AdminService::new_for_test(["node-a"]);
    let denied_context = no_access_context("anonymous@example.com");
    let tenant_id = TenantId::parse("tenant-a").expect("valid tenant");

    let err = service
        .debug_cache_fingerprint(
            &denied_context,
            fingerprint_request(
                tenant_id,
                json!({"input": "secret prompt", "model": "gpt-x"}),
            ),
        )
        .expect_err("no-access context should not debug fingerprints");

    assert_eq!(
        err.to_string(),
        "forbidden: DebugCacheFingerprint requires elevated role"
    );
    let audit_reader = viewer_context("auditor@example.com");
    let events = service
        .audit_events(&audit_reader)
        .expect("viewer should read audit log");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].actor, "anonymous@example.com");
    assert_eq!(events[0].role, Role::NoAccess);
    assert_eq!(events[0].action, AdminAction::DebugCacheFingerprint);
    assert_eq!(events[0].tenant_scope.as_deref(), Some("tenant-a"));
    assert_eq!(events[0].result, AuditResult::Denied);
    let audit_text = format!("{events:?}");
    assert!(!audit_text.contains("secret prompt"));
}

#[test]
fn viewer_can_read_audit_log_and_listing_is_audited() {
    let service = AdminService::new_for_test(["node-a"]);
    let context = viewer_context("reader@example.com");

    let first = service
        .audit_events(&context)
        .expect("viewer should read audit log");
    assert!(first.is_empty());

    let second = service
        .audit_events(&context)
        .expect("viewer should read audit log");
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].actor, "reader@example.com");
    assert_eq!(second[0].role, Role::Viewer);
    assert_eq!(second[0].action, AdminAction::ReadAuditLog);
    assert_eq!(second[0].resource, "audit_log");
    assert_eq!(second[0].result, AuditResult::Success);
    assert_eq!(second[0].after_summary.as_deref(), Some("listed_events=0"));
}

#[test]
fn no_access_context_cannot_read_audit_log_and_denial_is_audited() {
    let service = AdminService::new_for_test(["node-a"]);
    let denied_context = no_access_context("anonymous@example.com");

    let err = service
        .audit_events(&denied_context)
        .expect_err("no-access context should not read audit logs");

    assert_eq!(
        err.to_string(),
        "forbidden: ReadAuditLog requires elevated role"
    );
    let audit_reader = viewer_context("auditor@example.com");
    let events = service
        .audit_events(&audit_reader)
        .expect("viewer should read audit log");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].actor, "anonymous@example.com");
    assert_eq!(events[0].role, Role::NoAccess);
    assert_eq!(events[0].action, AdminAction::ReadAuditLog);
    assert_eq!(events[0].resource, "audit_log");
    assert_eq!(events[0].result, AuditResult::Denied);
}

#[test]
fn service_deletes_cache_object_for_operator_and_audits_write() {
    let service = AdminService::new_for_test(["node-a"]);
    let context = operator_context("ops@example.com");
    let tenant_id = TenantId::parse("tenant-a").expect("valid tenant");
    let cache_key =
        CacheKey::from_hex("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
            .expect("valid cache key");
    let mut master = MasterState::new_for_test();
    master.mount_segment("node-a", 1024);
    master
        .set_tenant_quota("tenant-a", 1024, 0)
        .expect("quota should be valid");
    master
        .put_start(&tenant_id, &cache_key, 128, 1)
        .expect("object reservation should succeed");
    master
        .put_end(&tenant_id, &cache_key)
        .expect("object commit should succeed");

    service
        .delete_cache_object(&context, &mut master, &tenant_id, &cache_key)
        .expect("operator should delete cache objects");

    let err = master
        .get_replica_list(&tenant_id, &cache_key)
        .expect_err("deleted object should not be found");
    assert_eq!(err.to_string(), "not found");

    let events = service
        .audit_events(&context)
        .expect("audit list should succeed");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].action, AdminAction::RemoveCacheObject);
    assert_eq!(events[0].tenant_scope.as_deref(), Some("tenant-a"));
    assert_eq!(events[0].result, AuditResult::Success);
}

#[test]
fn admin_metrics_snapshot_authorizes_and_audits_read_metrics() {
    let service = AdminService::new_for_test(["node-a"]);
    let operator = operator_context("ops@example.com");
    let viewer = viewer_context("reader@example.com");
    let denied = no_access_context("anonymous@example.com");

    service
        .drain_node(&operator, "node-a")
        .expect("operator should drain nodes");
    let _ = service
        .drain_node(&viewer, "node-a")
        .expect_err("viewer should not drain nodes");

    let metrics = service
        .metrics_snapshot(&viewer)
        .expect("viewer should read admin metrics");
    assert_eq!(metrics.audit_events_total, 3);
    assert_eq!(metrics.audit_success_total, 2);
    assert_eq!(metrics.audit_denied_total, 1);
    assert_eq!(metrics.action_counts.get("drain_node"), Some(&2));
    assert_eq!(metrics.action_counts.get("read_metrics"), Some(&1));

    let err = service
        .metrics_snapshot(&denied)
        .expect_err("no-access context should not read admin metrics");
    assert_eq!(
        err.to_string(),
        "forbidden: ReadMetrics requires elevated role"
    );

    let metrics = service
        .metrics_snapshot(&viewer)
        .expect("viewer should read admin metrics after denial");
    assert_eq!(metrics.audit_events_total, 5);
    assert_eq!(metrics.audit_success_total, 3);
    assert_eq!(metrics.audit_denied_total, 2);
    assert_eq!(metrics.action_counts.get("drain_node"), Some(&2));
    assert_eq!(metrics.action_counts.get("read_metrics"), Some(&3));
}

fn no_access_context(actor: &str) -> AdminRequestContext {
    AdminRequestContext {
        actor: actor.to_owned(),
        role: Role::NoAccess,
        request_id: RequestId::new(),
    }
}

fn viewer_context(actor: &str) -> AdminRequestContext {
    AdminRequestContext {
        actor: actor.to_owned(),
        role: Role::Viewer,
        request_id: RequestId::new(),
    }
}

fn operator_context(actor: &str) -> AdminRequestContext {
    AdminRequestContext {
        actor: actor.to_owned(),
        role: Role::Operator,
        request_id: RequestId::new(),
    }
}

fn fingerprint_request(
    tenant_id: TenantId,
    body: serde_json::Value,
) -> CacheFingerprintDebugRequest {
    CacheFingerprintDebugRequest {
        tenant_id,
        endpoint_version: "responses-v1".to_owned(),
        vendor_id: "openai".to_owned(),
        resolved_model_version: "gpt-x-2026-07-03".to_owned(),
        adapter_version: "adapter-v1".to_owned(),
        cache_policy: "default".to_owned(),
        body,
    }
}
