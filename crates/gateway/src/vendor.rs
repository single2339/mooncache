use std::{collections::BTreeMap, pin::Pin, time::Duration};

use async_trait::async_trait;
use futures_util::{stream, Stream};
use mooncache_protocol::{ResponsesRequest, SseEvent};
use reqwest::Client;
use serde_json::{json, Value};
use thiserror::Error;

use crate::config::VendorModelConfig;

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
    fn model_cache_eligible(&self, _requested_model: &str) -> bool {
        true
    }
    async fn resolve_model_version(&self, requested_model: &str) -> Result<String, VendorError>;
    async fn complete(&self, request: ResponsesRequest) -> Result<VendorResponse, VendorError>;
    async fn stream(&self, request: ResponsesRequest) -> Result<VendorEventStream, VendorError>;
}

#[derive(Debug, Clone)]
pub struct OpenAiResponsesAdapter {
    client: Client,
    base_url: String,
    api_key: String,
    headers: BTreeMap<String, String>,
    models: BTreeMap<String, VendorModelConfig>,
}

impl OpenAiResponsesAdapter {
    pub fn new(
        base_url: impl Into<String>,
        api_key: String,
        timeout_ms: u64,
        headers: BTreeMap<String, String>,
        models: Vec<VendorModelConfig>,
    ) -> Result<Self, VendorError> {
        let base_url = base_url.into().trim_end_matches('/').to_owned();
        if base_url.is_empty() {
            return Err(VendorError::InvalidResponse {
                message: "vendor base_url must not be empty".into(),
            });
        }
        if api_key.trim().is_empty() {
            return Err(VendorError::InvalidResponse {
                message: "vendor api key must not be empty".into(),
            });
        }
        let client = Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .map_err(|error| VendorError::Transport {
                message: error.to_string(),
            })?;
        Ok(Self {
            client,
            base_url,
            api_key,
            headers,
            models: models
                .into_iter()
                .map(|model| (model.requested.clone(), model))
                .collect(),
        })
    }

    fn responses_url(&self) -> String {
        format!("{}/responses", self.base_url)
    }

    async fn post_json(&self, body: Value) -> Result<reqwest::Response, VendorError> {
        let mut request = self
            .client
            .post(self.responses_url())
            .bearer_auth(&self.api_key)
            .json(&body);
        for (name, value) in &self.headers {
            request = request.header(name, value);
        }
        request
            .send()
            .await
            .map_err(|error| VendorError::Transport {
                message: error.to_string(),
            })
    }
}

#[async_trait]
impl VendorAdapter for OpenAiResponsesAdapter {
    fn vendor_id(&self) -> &str {
        "openai"
    }

    fn adapter_version(&self) -> &str {
        "openai-responses-v1"
    }

    fn model_cache_eligible(&self, requested_model: &str) -> bool {
        self.models
            .get(requested_model)
            .is_none_or(|model| model.cache_eligible)
    }

    async fn resolve_model_version(&self, requested_model: &str) -> Result<String, VendorError> {
        Ok(self.models.get(requested_model).map_or_else(
            || requested_model.to_owned(),
            |model| model.resolved.clone(),
        ))
    }

    async fn complete(&self, request: ResponsesRequest) -> Result<VendorResponse, VendorError> {
        let response = self.post_json(json!(request)).await?;
        let status = response.status();
        if !status.is_success() {
            let body = read_error_body(response).await;
            return Err(VendorError::HttpStatus {
                status: status.as_u16(),
                body,
            });
        }
        let body =
            response
                .json::<Value>()
                .await
                .map_err(|error| VendorError::InvalidResponse {
                    message: error.to_string(),
                })?;
        Ok(VendorResponse { body })
    }

    async fn stream(&self, request: ResponsesRequest) -> Result<VendorEventStream, VendorError> {
        let mut body = json!(request);
        if let Value::Object(object) = &mut body {
            object.insert("stream".to_owned(), Value::Bool(true));
        }
        let response = self.post_json(body).await?;
        let status = response.status();
        if !status.is_success() {
            let body = read_error_body(response).await;
            return Err(VendorError::HttpStatus {
                status: status.as_u16(),
                body,
            });
        }
        let text = response
            .text()
            .await
            .map_err(|error| VendorError::Transport {
                message: error.to_string(),
            })?;
        let events = parse_sse_events(&text)?;
        Ok(Box::pin(stream::iter(events.into_iter().map(Ok))))
    }
}

async fn read_error_body(response: reqwest::Response) -> String {
    response
        .text()
        .await
        .unwrap_or_else(|error| format!("[failed to read error body: {error}]"))
}

fn parse_sse_events(text: &str) -> Result<Vec<SseEvent>, VendorError> {
    let mut events = Vec::new();
    let mut event_name: Option<String> = None;
    let mut data_lines: Vec<String> = Vec::new();

    for line in text.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            flush_sse_event(&mut events, &mut event_name, &mut data_lines);
            continue;
        }
        if line.starts_with(':') {
            continue;
        }
        if let Some(value) = line.strip_prefix("event:") {
            event_name = Some(value.trim_start().to_owned());
        } else if let Some(value) = line.strip_prefix("data:") {
            data_lines.push(value.trim_start().to_owned());
        }
    }
    flush_sse_event(&mut events, &mut event_name, &mut data_lines);

    if events.is_empty() {
        return Err(VendorError::InvalidResponse {
            message: "vendor stream did not contain SSE events".into(),
        });
    }
    Ok(events)
}

fn flush_sse_event(
    events: &mut Vec<SseEvent>,
    event_name: &mut Option<String>,
    data_lines: &mut Vec<String>,
) {
    if data_lines.is_empty() {
        *event_name = None;
        return;
    }
    events.push(SseEvent {
        event: event_name.take(),
        data: data_lines.join("\n"),
    });
    data_lines.clear();
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
    use futures_util::StreamExt;
    use serde_json::json;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    async fn serve_one_response(
        response: &'static str,
    ) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                let n = socket.read(&mut buffer).await.unwrap();
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..n]);
                if request.windows(4).any(|window| window == b"\r\n\r\n")
                    && String::from_utf8_lossy(&request).contains("\"input\":\"hello\"")
                {
                    break;
                }
            }
            socket.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8(request).unwrap()
        });
        (format!("http://{addr}/v1"), handle)
    }

    const JSON_RESPONSE: &str = concat!(
        "HTTP/1.1 200 OK\r\n",
        "content-type: application/json\r\n",
        "content-length: 37\r\n",
        "\r\n",
        "{\"id\":\"resp_1\",\"output_text\":\"hello\"}"
    );

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
    async fn openai_adapter_posts_responses_json_with_bearer_auth() {
        let (base_url, captured_request) = serve_one_response(JSON_RESPONSE).await;
        let adapter = OpenAiResponsesAdapter::new(
            base_url,
            "test-token".to_owned(),
            1_000,
            BTreeMap::new(),
            vec![],
        )
        .unwrap();

        let response = adapter.complete(test_request()).await.unwrap();
        let request = captured_request.await.unwrap();

        assert_eq!(response.body["output_text"], "hello");
        assert!(request.starts_with("POST /v1/responses HTTP/1.1"));
        assert!(request.contains("authorization: Bearer test-token"));
        assert!(request.contains("\"model\":\"mock-model\""));
        assert!(request.contains("\"input\":\"hello\""));
    }

    #[tokio::test]
    async fn openai_adapter_sends_configured_headers_and_resolves_model_aliases() {
        let (base_url, captured_request) = serve_one_response(JSON_RESPONSE).await;
        let adapter = OpenAiResponsesAdapter::new(
            base_url,
            "test-token".to_owned(),
            1_000,
            [("OpenAI-Beta".to_owned(), "responses=v1".to_owned())].into(),
            vec![crate::config::VendorModelConfig {
                requested: "mock-model".to_owned(),
                resolved: "mock-model-2026-07-04".to_owned(),
                cache_eligible: false,
            }],
        )
        .unwrap();

        let response = adapter.complete(test_request()).await.unwrap();
        let request = captured_request.await.unwrap();

        assert_eq!(response.body["output_text"], "hello");
        assert!(request.contains("openai-beta: responses=v1"));
        assert_eq!(
            adapter.resolve_model_version("mock-model").await.unwrap(),
            "mock-model-2026-07-04"
        );
        assert!(!adapter.model_cache_eligible("mock-model"));
    }

    #[tokio::test]
    async fn openai_adapter_stream_posts_stream_true_and_parses_sse() {
        let (base_url, captured_request) = serve_one_response(concat!(
            "HTTP/1.1 200 OK\r\n",
            "content-type: text/event-stream\r\n",
            "connection: close\r\n",
            "\r\n",
            "event: response.completed\n",
            "data: {\"id\":\"resp_1\"}\n",
            "\n"
        ))
        .await;
        let adapter = OpenAiResponsesAdapter::new(
            base_url,
            "test-token".to_owned(),
            1_000,
            BTreeMap::new(),
            vec![],
        )
        .unwrap();

        let events: Vec<_> = adapter
            .stream(test_request())
            .await
            .unwrap()
            .collect()
            .await;
        let request = captured_request.await.unwrap();

        let event = events.into_iter().next().unwrap().unwrap();
        assert_eq!(event.event.as_deref(), Some("response.completed"));
        assert_eq!(event.data, "{\"id\":\"resp_1\"}");
        assert!(request.contains("\"stream\":true"));
    }

    #[tokio::test]
    async fn openai_adapter_maps_non_success_status_to_vendor_error() {
        let (base_url, captured_request) = serve_one_response(concat!(
            "HTTP/1.1 429 Too Many Requests\r\n",
            "content-type: text/plain\r\n",
            "content-length: 10\r\n",
            "\r\n",
            "rate limit"
        ))
        .await;
        let adapter = OpenAiResponsesAdapter::new(
            base_url,
            "test-token".to_owned(),
            1_000,
            BTreeMap::new(),
            vec![],
        )
        .unwrap();

        let err = adapter.complete(test_request()).await.unwrap_err();
        let _ = captured_request.await.unwrap();

        assert_eq!(
            err,
            VendorError::HttpStatus {
                status: 429,
                body: "rate limit".to_owned()
            }
        );
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
