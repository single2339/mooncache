use std::pin::Pin;

use async_trait::async_trait;
use futures_util::{stream, Stream};
use mooncache_protocol::{ResponsesRequest, SseEvent};
use serde_json::Value;
use thiserror::Error;

pub type VendorResponse = mooncache_protocol::ResponsesResponse;
pub type VendorStreamEvent = SseEvent;
pub type VendorEventStream =
    Pin<Box<dyn Stream<Item = Result<VendorStreamEvent, VendorError>> + Send + 'static>>;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum VendorError {
    #[error("vendor returned HTTP {status}: {body}")]
    HttpStatus { status: u16, body: String },
    #[error("vendor transport error: {message}")]
    Transport { message: String },
    #[error("vendor response was invalid: {message}")]
    InvalidResponse { message: String },
}

impl VendorError {
    #[must_use]
    pub fn is_retryable_before_stream_start(&self) -> bool {
        matches!(
            self,
            Self::Transport { .. }
                | Self::HttpStatus {
                    status: 429 | 500..=599,
                    ..
                }
        )
    }
}

#[async_trait]
pub trait VendorAdapter: Send + Sync {
    fn vendor_id(&self) -> &str;
    fn adapter_version(&self) -> &str;
    async fn resolve_model_version(&self, requested_model: &str) -> Result<String, VendorError>;
    async fn complete(&self, request: ResponsesRequest) -> Result<VendorResponse, VendorError>;
    async fn stream(&self, request: ResponsesRequest) -> Result<VendorEventStream, VendorError>;
}

#[derive(Debug, Clone)]
pub struct MockVendorAdapter {
    body: Value,
}

impl MockVendorAdapter {
    #[must_use]
    pub fn new_json(body: Value) -> Self {
        Self { body }
    }
}

#[async_trait]
impl VendorAdapter for MockVendorAdapter {
    fn vendor_id(&self) -> &str {
        "mock"
    }

    fn adapter_version(&self) -> &str {
        "mock-v1"
    }

    async fn resolve_model_version(&self, requested_model: &str) -> Result<String, VendorError> {
        Ok(requested_model.to_owned())
    }

    async fn complete(&self, _request: ResponsesRequest) -> Result<VendorResponse, VendorError> {
        Ok(VendorResponse {
            body: self.body.clone(),
        })
    }

    async fn stream(&self, _request: ResponsesRequest) -> Result<VendorEventStream, VendorError> {
        let event = SseEvent {
            event: Some("response.completed".to_owned()),
            data: self.body.to_string(),
        };
        Ok(Box::pin(stream::once(async move { Ok(event) })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_request() -> ResponsesRequest {
        ResponsesRequest {
            model: "mock-model".to_owned(),
            body: json!({"input": "hello"}),
        }
    }

    #[tokio::test]
    async fn mock_adapter_returns_configured_response() {
        let adapter = MockVendorAdapter::new_json(json!({"id":"resp_1","output_text":"hello"}));
        let response = adapter.complete(test_request()).await.unwrap();
        assert_eq!(response.body["output_text"], "hello");
    }

    #[tokio::test]
    async fn adapter_classifies_retryable_5xx() {
        let err = VendorError::HttpStatus {
            status: 503,
            body: "busy".into(),
        };
        assert!(err.is_retryable_before_stream_start());
    }

    #[tokio::test]
    async fn adapter_classifies_transport_error_retryable() {
        let err = VendorError::Transport {
            message: "connection reset".into(),
        };
        assert!(err.is_retryable_before_stream_start());
    }

    #[tokio::test]
    async fn adapter_classifies_429_retryable() {
        let err = VendorError::HttpStatus {
            status: 429,
            body: "rate limited".into(),
        };
        assert!(err.is_retryable_before_stream_start());
    }

    #[tokio::test]
    async fn adapter_classifies_ordinary_4xx_non_retryable() {
        for status in [400, 404] {
            let err = VendorError::HttpStatus {
                status,
                body: "client error".into(),
            };
            assert!(!err.is_retryable_before_stream_start());
        }
    }
}
