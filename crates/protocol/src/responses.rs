use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResponsesRequest {
    pub model: String,
    #[serde(flatten)]
    pub body: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResponsesResponse {
    #[serde(flatten)]
    pub body: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SseEvent {
    pub event: Option<String>,
    pub data: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn responses_request_keeps_openai_fields_flexible() {
        let request: ResponsesRequest = serde_json::from_value(json!({
            "model": "gpt-4.1-mini",
            "input": "hello",
            "temperature": 0,
            "metadata": {"purpose": "cache-test"}
        }))
        .unwrap();

        assert_eq!(request.model, "gpt-4.1-mini");
        assert_eq!(request.body["input"], json!("hello"));
        assert_eq!(request.body["metadata"]["purpose"], json!("cache-test"));
    }

    #[test]
    fn responses_response_accepts_arbitrary_json_shape() {
        let body = json!({
            "id": "resp_123",
            "output": [{"type": "message", "content": [{"type": "output_text", "text": "hi"}]}]
        });

        let response: ResponsesResponse = serde_json::from_value(body.clone()).unwrap();

        assert_eq!(response.body, body);
    }

    #[test]
    fn sse_event_allows_optional_event_name() {
        let event = SseEvent {
            event: None,
            data: "[DONE]".to_owned(),
        };

        assert_eq!(event.event, None);
        assert_eq!(event.data, "[DONE]");
    }
}
