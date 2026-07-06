use futures_util::StreamExt;
use mooncache_protocol::SseEvent;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{VendorError, VendorEventStream};

const STREAMING_OBJECT_TYPE: &str = "mooncache.streaming-response.v1";

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CapturedStream {
    pub(crate) events: Vec<SseEvent>,
    pub(crate) final_body: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct CachedStreamObject {
    mooncache_object: String,
    #[serde(default)]
    pub(crate) expires_at_ms: Option<u64>,
    pub(crate) events: Vec<SseEvent>,
    pub(crate) final_body: Value,
}

pub(crate) async fn collect_stream(
    mut stream: VendorEventStream,
) -> Result<CapturedStream, VendorError> {
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event?);
    }

    let final_body = aggregate_final_body(&events)?;
    Ok(CapturedStream { events, final_body })
}

pub(crate) fn serialize_stream_object_with_expiry(
    captured: &CapturedStream,
    expires_at_ms: Option<u64>,
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&CachedStreamObject {
        mooncache_object: STREAMING_OBJECT_TYPE.to_owned(),
        expires_at_ms,
        events: captured.events.clone(),
        final_body: captured.final_body.clone(),
    })
}

pub(crate) fn cached_body_from_bytes(bytes: &[u8]) -> Result<Value, serde_json::Error> {
    let value: Value = serde_json::from_slice(bytes)?;
    if is_streaming_object(&value) {
        let object: CachedStreamObject = serde_json::from_value(value)?;
        Ok(object.final_body)
    } else {
        Ok(value)
    }
}

pub(crate) fn stream_object_from_bytes(
    bytes: &[u8],
) -> Result<Option<CachedStreamObject>, serde_json::Error> {
    let value: Value = serde_json::from_slice(bytes)?;
    if is_streaming_object(&value) {
        serde_json::from_value(value).map(Some)
    } else {
        Ok(None)
    }
}

fn is_streaming_object(value: &Value) -> bool {
    value
        .get("mooncache_object")
        .and_then(Value::as_str)
        .is_some_and(|object_type| object_type == STREAMING_OBJECT_TYPE)
}

fn aggregate_final_body(events: &[SseEvent]) -> Result<Value, VendorError> {
    let completed = events
        .iter()
        .rev()
        .find(|event| event.event.as_deref() == Some("response.completed"))
        .ok_or_else(|| VendorError::InvalidResponse {
            message: "stream completed without response.completed event".to_owned(),
        })?;

    let completed_data: Value =
        serde_json::from_str(&completed.data).map_err(|err| VendorError::InvalidResponse {
            message: format!("response.completed data was not valid JSON: {err}"),
        })?;

    Ok(match completed_data {
        Value::Object(mut object) => object.remove("response").unwrap_or(Value::Object(object)),
        value => value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn stream_object_roundtrips_events_and_final_body() {
        let captured = CapturedStream {
            events: vec![
                SseEvent {
                    event: Some("response.output_text.delta".to_owned()),
                    data: "{\"delta\":\"hel\"}".to_owned(),
                },
                SseEvent {
                    event: Some("response.completed".to_owned()),
                    data: "{\"id\":\"resp_1\"}".to_owned(),
                },
            ],
            final_body: json!({"id":"resp_1"}),
        };

        let bytes = serialize_stream_object_with_expiry(&captured, None).unwrap();
        let object = stream_object_from_bytes(&bytes).unwrap().unwrap();

        assert_eq!(object.events, captured.events);
        assert_eq!(object.final_body, captured.final_body);
    }

    #[test]
    fn cached_body_from_stream_object_returns_final_body() {
        let captured = CapturedStream {
            events: vec![SseEvent {
                event: Some("response.completed".to_owned()),
                data: "{\"id\":\"resp_1\"}".to_owned(),
            }],
            final_body: json!({"id":"resp_1"}),
        };

        let bytes = serialize_stream_object_with_expiry(&captured, None).unwrap();

        assert_eq!(
            cached_body_from_bytes(&bytes).unwrap(),
            json!({"id":"resp_1"})
        );
    }
}
