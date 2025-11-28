use crate::parsers::{
    line_preview, normalize_tx_hash, parse_iso8601_to_millis, schemas::BatchEnvelope,
    BufferedLineParser, LineParser, LINE_PREVIEW_LIMIT,
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
    let inner_value = payload
        .remove("inner")
        .unwrap_or_else(|| Value::Object(payload.clone()));

    let event = MiscEvent {
        time,
        hash,
        inner: inner_value,
    };

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
