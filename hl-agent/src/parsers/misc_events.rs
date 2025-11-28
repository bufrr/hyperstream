use crate::parsers::batch::BatchEnvelope;
use crate::parsers::{
    line_preview, normalize_tx_hash, parse_iso8601_to_millis, BufferedLineParser, LineParser,
    LINE_PREVIEW_LIMIT,
};
use crate::sorter_client::proto::DataRecord;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{self, Map, Value};
use std::path::Path;
use tracing::warn;

/// Converts miscellaneous JSON events into the `hl.misc_events` stream.
///
/// The parser expects the `node_misc_events` batch format, extracts user identifiers when
/// available, and serializes a compact payload `{time, hash, inner}` for sorter.
pub type MiscEventsParser = BufferedLineParser<MiscEventsLineParser>;

/// Creates a new MiscEventsParser with default configuration.
pub fn new_misc_events_parser() -> MiscEventsParser {
    BufferedLineParser::new(MiscEventsLineParser)
}

/// Inner line parser for misc events - handles parsing individual lines.
pub struct MiscEventsLineParser;

impl Default for MiscEventsLineParser {
    fn default() -> Self {
        Self
    }
}

#[derive(Serialize)]
struct MiscEvent {
    time: String,
    hash: String,
    inner: Value,
}

#[derive(Deserialize)]
struct RawMiscEvent {
    time: String,
    hash: String,
    #[serde(flatten)]
    payload: Map<String, Value>,
}

impl LineParser for MiscEventsLineParser {
    fn parse_line(&mut self, _file_path: &Path, line: &[u8]) -> Result<Vec<DataRecord>> {
        match serde_json::from_slice::<BatchEnvelope<RawMiscEvent>>(line) {
            Ok(batch) => {
                let mut records = Vec::with_capacity(batch.events.len());
                for event in batch.events {
                    records.push(node_misc_event_to_record(event, Some(batch.block_number))?);
                }
                Ok(records)
            }
            Err(err) => {
                warn!(
                    error = %err,
                    preview = %line_preview(line, LINE_PREVIEW_LIMIT),
                    "skipping unrecognized misc event line format"
                );
                Ok(Vec::new())
            }
        }
    }
}

fn node_misc_event_to_record(event: RawMiscEvent, block_height: Option<u64>) -> Result<DataRecord> {
    let RawMiscEvent {
        time,
        hash,
        mut payload,
    } = event;
    let user = extract_user_from_payload(&payload);
    let inner_value = payload
        .remove("inner")
        .unwrap_or_else(|| Value::Object(payload.clone()));

    let event = MiscEvent {
        time,
        hash,
        inner: inner_value,
    };

    let _ = user; // User extracted but no longer needed for partition_key

    let payload =
        serde_json::to_vec(&event).context("failed to encode misc event payload to JSON")?;

    // Parse timestamp
    let timestamp = parse_iso8601_to_millis(&event.time).unwrap_or(0);

    Ok(DataRecord {
        block_height,
        // Zero-hash misc events are emitted by system modules; drop their hashes for tx metadata.
        tx_hash: normalize_tx_hash(&event.hash),
        timestamp,
        topic: "hl.misc_events".to_string(),
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
