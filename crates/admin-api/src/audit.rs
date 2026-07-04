use std::sync::{Arc, Mutex};

use mooncache_common::RequestId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{AdminAction, Role};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditResult {
    Success,
    Denied,
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEvent {
    pub actor: String,
    pub role: Role,
    pub action: AdminAction,
    pub resource: String,
    pub tenant_scope: Option<String>,
    pub before_summary: Option<String>,
    pub after_summary: Option<String>,
    pub request_id: RequestId,
    pub timestamp_ms: u64,
    pub result: AuditResult,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuditError {
    #[error("audit sink is unavailable")]
    Unavailable,
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryAuditSink {
    events: Arc<Mutex<Vec<AuditEvent>>>,
}

impl InMemoryAuditSink {
    pub fn append(&self, event: AuditEvent) -> Result<(), AuditError> {
        self.events
            .lock()
            .map_err(|_| AuditError::Unavailable)?
            .push(event);
        Ok(())
    }

    pub fn list(&self) -> Result<Vec<AuditEvent>, AuditError> {
        self.events
            .lock()
            .map(|events| events.clone())
            .map_err(|_| AuditError::Unavailable)
    }
}
