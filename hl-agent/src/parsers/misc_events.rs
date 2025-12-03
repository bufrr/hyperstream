use crate::metrics::PARSE_ERRORS_TOTAL;
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
        let parsed = match serde_json::from_slice::<BatchEnvelope<RawMiscEvent>>(line) {
            Ok(batch) => batch,
            Err(err) => {
                PARSE_ERRORS_TOTAL
                    .with_label_values(&["misc_events", "decode_failed"])
                    .inc();
                warn!(
                    error = %err,
                    preview = %line_preview(line, LINE_PREVIEW_LIMIT),
                    "skipping unrecognized misc event line format"
                );
                return Ok(Vec::new());
            }
        };

        let mut records = Vec::with_capacity(parsed.events.len());
        for event in parsed.events {
            records.push(node_misc_event_to_record(event, Some(parsed.block_number))?);
        }
        Ok(records)
    }

    fn parser_type(&self) -> &'static str {
        "misc_events"
    }
}

fn node_misc_event_to_record(event: RawMiscEvent, block_height: Option<u64>) -> Result<DataRecord> {
    let RawMiscEvent {
        time,
        hash,
        payload,
    } = event;
    let mut payload_value = Value::Object(payload);
    let inner_value = payload_value
        .as_object_mut()
        .and_then(|map| map.remove("inner"))
        .unwrap_or_else(|| payload_value.clone());

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn parses_misc_events_batch_line() {
        let mut parser = MiscEventsLineParser::default();
        let line = br#"{"block_number":1,"block_time":"2025-12-02T12:00:00.000000000","events":[{"time":"2025-12-02T12:00:00.000000000","hash":"0xabc","inner":{"Foo":"bar"}},{"time":"2025-12-02T12:00:01.000000000","hash":"0x0000000000000000000000000000000000000000","inner":{"Baz":1}}]}"#;

        let _: serde_json::Value = serde_json::from_slice(line).expect("raw Value should parse");
        let records = parser
            .parse_line(Path::new("misc_events.ndjson"), line)
            .expect("line should parse");
        let parsed: BatchEnvelope<RawMiscEvent> =
            serde_json::from_slice(line).expect("serde_json should parse test line");
        assert_eq!(parsed.events.len(), 2);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].topic, "hl.misc_events");
        assert_eq!(records[0].block_height, Some(1));
        // Zero hashes should be normalized to None
        assert!(records[1].tx_hash.is_none());
    }
}
