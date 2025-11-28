//! Generic batch envelope for `_by_block` file formats.
//!
//! Many Hyperliquid node files wrap events in a batch structure with metadata:
//! ```json
//! {
//!   "block_number": 807847463,
//!   "block_time": "2025-11-25T08:33:18.111517886",
//!   "events": [...]
//! }
//! ```
//!
//! This module provides a generic `BatchEnvelope<T>` to eliminate duplicate struct definitions.

use crate::parsers::utils::deserialize_flexible_events;
use serde::de::DeserializeOwned;
use serde::Deserialize;

/// Generic batch envelope for `_by_block` file formats.
///
/// Wraps events with block metadata. The `events` field supports flexible
/// deserialization (arrays, objects, or null).
#[derive(Debug, Deserialize)]
#[serde(bound(deserialize = "T: DeserializeOwned"))]
pub struct BatchEnvelope<T> {
    /// Block height at which events occurred
    #[serde(alias = "blockNumber", alias = "block_height", alias = "blockHeight")]
    pub block_number: u64,

    /// Optional block timestamp (ISO8601 format)
    #[serde(alias = "blockTime", alias = "block_time", default)]
    pub block_time: Option<String>,

    /// Optional round/height (unused but present in some formats)
    #[serde(default, alias = "round", alias = "height")]
    #[allow(dead_code)]
    _round: Option<u64>,

    /// Events in this batch (supports array or object formats)
    #[serde(default, deserialize_with = "deserialize_batch_events")]
    pub events: Vec<T>,
}

/// Deserialize events field that may be null, array, or object-map.
fn deserialize_batch_events<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: DeserializeOwned,
{
    deserialize_flexible_events(deserializer, "batch events")
}

/// Batch envelope specifically for fill events which use tuple format: `(user, fill)`.
#[derive(Debug, Deserialize)]
#[serde(bound(deserialize = "F: DeserializeOwned"))]
pub struct FillBatchEnvelope<F> {
    /// Block height at which fills occurred
    #[serde(alias = "blockNumber", alias = "block_height", alias = "blockHeight")]
    pub block_number: u64,

    /// Optional block timestamp (ISO8601 format)
    #[serde(alias = "blockTime", alias = "block_time", default)]
    #[allow(dead_code)]
    pub block_time: Option<String>,

    /// Optional round/height (unused but present in some formats)
    #[serde(default, alias = "round", alias = "height")]
    #[allow(dead_code)]
    _round: Option<u64>,

    /// Fill events as tuples: `(user_address, fill_data)`
    #[serde(default)]
    pub events: Vec<(String, F)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde_json::json;

    #[derive(Debug, Deserialize, PartialEq)]
    struct TestEvent {
        id: u64,
        name: String,
    }

    #[test]
    fn batch_envelope_parses_array_events() {
        let json = json!({
            "block_number": 12345,
            "events": [
                {"id": 1, "name": "first"},
                {"id": 2, "name": "second"}
            ]
        });

        let batch: BatchEnvelope<TestEvent> = serde_json::from_value(json).unwrap();
        assert_eq!(batch.block_number, 12345);
        assert_eq!(batch.events.len(), 2);
        assert_eq!(batch.events[0].id, 1);
    }

    #[test]
    fn batch_envelope_parses_object_events() {
        let json = json!({
            "blockNumber": 67890,
            "events": {
                "first": {"id": 1, "name": "first"},
                "second": {"id": 2, "name": "second"}
            }
        });

        let batch: BatchEnvelope<TestEvent> = serde_json::from_value(json).unwrap();
        assert_eq!(batch.block_number, 67890);
        assert_eq!(batch.events.len(), 2);
    }

    #[test]
    fn batch_envelope_handles_null_events() {
        let json = json!({
            "block_height": 11111,
            "events": null
        });

        let batch: BatchEnvelope<TestEvent> = serde_json::from_value(json).unwrap();
        assert_eq!(batch.block_number, 11111);
        assert!(batch.events.is_empty());
    }

    #[test]
    fn fill_batch_envelope_parses_tuple_events() {
        #[derive(Debug, Deserialize)]
        struct TestFill {
            coin: String,
        }

        let json = json!({
            "block_number": 99999,
            "events": [
                ["0xuser1", {"coin": "ETH"}],
                ["0xuser2", {"coin": "BTC"}]
            ]
        });

        let batch: FillBatchEnvelope<TestFill> = serde_json::from_value(json).unwrap();
        assert_eq!(batch.block_number, 99999);
        assert_eq!(batch.events.len(), 2);
        assert_eq!(batch.events[0].0, "0xuser1");
        assert_eq!(batch.events[0].1.coin, "ETH");
    }
}
