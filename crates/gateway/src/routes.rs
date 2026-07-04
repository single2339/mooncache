use std::collections::BTreeMap;

use mooncache_common::CacheKey;
use mooncache_protocol::{CacheStatus, SseEvent};
use serde_json::{json, Value};

use crate::cache_flow::{cache_status_header, GatewayError, GatewayState};

#[derive(Debug, Clone)]
pub struct GatewayRequest {
    pub authorization: Option<String>,
    pub cache_control: Option<String>,
    pub body: Value,
}

#[derive(Debug, Clone)]
pub struct GatewayResponse {
    pub status_code: u16,
    pub headers: BTreeMap<String, String>,
    pub body: Value,
    pub stream_events: Option<Vec<SseEvent>>,
}

impl GatewayResponse {
    pub(crate) fn ok(
        body: Value,
        cache_status: CacheStatus,
        cache_write: &str,
        cache_tier: &str,
        cache_key: Option<&CacheKey>,
    ) -> Self {
        let mut response =
            Self::with_cache_headers(200, body, cache_status, cache_write, cache_tier);
        if let Some(cache_key) = cache_key {
            response = response.with_cache_key(cache_key);
        }
        response
    }

    pub(crate) fn ok_stream(
        body: Value,
        events: Vec<SseEvent>,
        cache_status: CacheStatus,
        cache_write: &str,
        cache_tier: &str,
        cache_key: Option<&CacheKey>,
    ) -> Self {
        let mut response =
            Self::with_cache_headers(200, body, cache_status, cache_write, cache_tier);
        response.stream_events = Some(events);
        if let Some(cache_key) = cache_key {
            response = response.with_cache_key(cache_key);
        }
        response
    }

    pub(crate) fn error(
        status_code: u16,
        message: impl Into<String>,
        cache_status: CacheStatus,
        cache_write: &str,
        cache_tier: &str,
    ) -> Self {
        Self::with_cache_headers(
            status_code,
            json!({ "error": message.into() }),
            cache_status,
            cache_write,
            cache_tier,
        )
    }

    pub(crate) fn with_cache_key(mut self, cache_key: &CacheKey) -> Self {
        self.headers
            .insert("x-cache-key".to_owned(), cache_key.redacted());
        self
    }

    pub(crate) fn with_cache_coalesced(mut self, role: &str) -> Self {
        self.headers
            .insert("x-cache-coalesced".to_owned(), role.to_owned());
        self
    }

    pub(crate) fn with_cache_write(mut self, cache_write: &str) -> Self {
        self.headers
            .insert("x-cache-write".to_owned(), cache_write.to_owned());
        self
    }

    fn with_cache_headers(
        status_code: u16,
        body: Value,
        cache_status: CacheStatus,
        cache_write: &str,
        cache_tier: &str,
    ) -> Self {
        let mut headers = BTreeMap::new();
        headers.insert(
            "x-cache-status".to_owned(),
            cache_status_header(cache_status).to_owned(),
        );
        headers.insert("x-cache-write".to_owned(), cache_write.to_owned());
        headers.insert("x-cache-tier".to_owned(), cache_tier.to_owned());
        headers.insert("x-cache-coalesced".to_owned(), "none".to_owned());

        Self {
            status_code,
            headers,
            body,
            stream_events: None,
        }
    }
}

pub async fn handle_response_request(
    state: &GatewayState,
    request: GatewayRequest,
) -> Result<GatewayResponse, GatewayError> {
    crate::cache_flow::handle_response_request(
        state,
        request.authorization.as_deref(),
        request.cache_control.as_deref(),
        request.body,
    )
    .await
}
