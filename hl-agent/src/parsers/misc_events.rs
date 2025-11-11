use crate::parsers::{
    drain_complete_lines, line_preview, parse_iso8601_to_millis, trim_line_bytes, Parser,
};
use crate::sorter_client::proto::DataRecord;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{self, Map, Value};
use std::path::Path;
use tracing::warn;

const LINE_PREVIEW_LIMIT: usize = 256;

#[derive(Default)]
pub struct MiscEventsParser {
    buffer: Vec<u8>,
}

#[derive(Serialize)]
struct MiscEvent {
    time: String,
    hash: String,
    inner: Value,
}

#[derive(Deserialize)]
struct NodeMiscEventBatch {
    #[serde(alias = "blockNumber", alias = "block_number", alias = "block_height")]
    block_number: u64,
    #[serde(default, deserialize_with = "deserialize_misc_events")]
    events: Vec<RawMiscEvent>,
}

#[derive(Deserialize)]
struct RawMiscEvent {
    time: String,
    hash: String,
    #[serde(flatten)]
    payload: Map<String, Value>,
}

impl Parser for MiscEventsParser {
    fn parse(&mut self, _file_path: &Path, data: &[u8]) -> Result<Vec<DataRecord>> {
        self.buffer.extend_from_slice(data);
        let lines = drain_complete_lines(&mut self.buffer);
        let mut records = Vec::with_capacity(lines.len());

        for (line_idx, raw_line) in lines.into_iter().enumerate() {
            let line = trim_line_bytes(raw_line);
            if line.is_empty() {
                continue;
            }

            match serde_json::from_slice::<NodeMiscEventBatch>(&line) {
                Ok(batch) => {
                    for event in batch.events {
                        records.push(node_misc_event_to_record(event, Some(batch.block_number))?);
                    }
                }
                Err(err) => {
                    warn!(
                        error = %err,
                        line_idx,
                        preview = %line_preview(&line, LINE_PREVIEW_LIMIT),
                        "skipping unrecognized misc event line format"
                    );
                }
            }
        }

        Ok(records)
    }

    fn backlog_len(&self) -> usize {
        self.buffer.len()
    }
}

fn node_misc_event_to_record(
    event: RawMiscEvent,
    block_height: Option<u64>,
) -> Result<DataRecord> {
    let user = extract_user_from_payload(&event.payload);

    let event = MiscEvent {
        time: event.time.clone(),
        hash: event.hash.clone(),
        inner: Value::Object(event.payload),
    };

    let partition_key = if user.is_empty() {
        "system".to_string()
    } else {
        user.clone()
    };

    let payload =
        serde_json::to_vec(&event).context("failed to encode misc event payload to JSON")?;

    // Parse timestamp
    let timestamp = parse_iso8601_to_millis(&event.time).unwrap_or(0);

    Ok(DataRecord {
        block_height,
        tx_hash: if event.hash.is_empty() {
            None
        } else {
            Some(event.hash)
        },
        timestamp,
        topic: "hl.misc_events".to_string(),
        partition_key,
        payload,
    })
}

fn extract_user_from_payload(payload: &Map<String, Value>) -> String {
    if let Some(user_value) = payload.get("user").and_then(Value::as_str) {
        return user_value.to_string();
    }

    if let Some(Value::Object(inner_map)) = payload.get("inner") {
        for event_data in inner_map.values() {
            let user = extract_user_from_event(event_data);
            if !user.is_empty() {
                return user;
            }
        }
    }

    for value in payload.values() {
        let user = extract_user_from_event(value);
        if !user.is_empty() {
            return user;
        }
    }

    String::new()
}

fn extract_user_from_event(event_data: &Value) -> String {
    if let Value::Object(map) = event_data {
        map.get("user")
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .or_else(|| {
                map.get("users").and_then(|value| {
                    value
                        .as_array()
                        .and_then(|arr| arr.first())
                        .and_then(Value::as_str)
                        .map(ToString::to_string)
                })
            })
            .or_else(|| {
                map.get("delta").and_then(|value| {
                    value
                        .as_object()
                        .and_then(|delta| delta.get("user"))
                        .and_then(Value::as_str)
                        .map(ToString::to_string)
                })
            })
            .unwrap_or_default()
    } else {
        String::new()
    }
}

fn deserialize_misc_events<'de, D>(deserializer: D) -> Result<Vec<RawMiscEvent>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    match value {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) => items
            .into_iter()
            .map(|item| serde_json::from_value(item).map_err(serde::de::Error::custom))
            .collect(),
        Some(Value::Object(map)) => map
            .into_iter()
            .map(|(_, item)| serde_json::from_value(item).map_err(serde::de::Error::custom))
            .collect(),
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected misc event list or map, got {other:?}"
        ))),
    }
}
