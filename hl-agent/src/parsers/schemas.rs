//! Shared output schemas for Hyperliquid data topics.
//!
//! These schemas define the structure of data records sent to downstream systems.
//! Both file parsers and WebSocket parsers use these shared schemas to ensure
//! consistent output formats.

use crate::parsers::deserialize_flexible_events;
use crate::sorter_client::proto::DataRecord;
use anyhow::Result;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    pub _block_time: Option<String>,

    /// Optional round/height (unused but present in some formats)
    #[serde(default, alias = "round", alias = "height")]
    _round: Option<u64>,

    /// Fill events as tuples: `(user_address, fill_data)`
    #[serde(default)]
    pub events: Vec<(String, F)>,
}

/// Output schema for `hl.blocks` topic.
///
/// Represents a block on the Hyperliquid chain with metadata.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    /// Block height on the chain
    pub height: u64,
    /// Block timestamp in milliseconds since Unix epoch
    #[serde(rename = "blockTime")]
    pub block_time: u64,
    /// Block hash (may be empty if not available from source)
    pub hash: String,
    /// Block proposer address
    pub proposer: String,
    /// Number of transactions in the block
    #[serde(rename = "numTxs")]
    pub num_txs: u64,
    /// CometBFT consensus round number (optional, different from height)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub round: Option<u64>,
}

impl Block {
    /// Convert this block to a DataRecord for the `hl.blocks` topic.
    pub fn to_data_record(&self) -> Result<DataRecord> {
        let payload = serde_json::to_vec(self)?;
        Ok(DataRecord {
            block_height: Some(self.height),
            tx_hash: None,
            timestamp: self.block_time,
            topic: "hl.blocks".to_string(),
            payload,
        })
    }
}

/// Output schema for `hl.transactions` topic.
///
/// Represents a transaction on the Hyperliquid chain.
#[derive(Debug, Clone, Serialize)]
pub struct Transaction {
    /// Transaction timestamp in milliseconds since Unix epoch
    pub time: u64,
    /// User address who submitted the transaction
    pub user: String,
    /// Transaction hash (may be empty if not available)
    pub hash: String,
    /// Transaction action payload (dynamic JSON)
    pub action: Value,
    /// Block height containing this transaction
    pub block: u64,
    /// Error message if transaction failed (null if successful)
    pub error: Option<String>,
}

impl Transaction {}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[test]
    fn block_serialization() {
        let block = Block {
            height: 12345,
            block_time: 1700000000000,
            hash: "0xabc123".to_string(),
            proposer: "0xdef456".to_string(),
            num_txs: 10,
            round: Some(99999),
        };

        let json = serde_json::to_string(&block).unwrap();
        assert!(json.contains("\"height\":12345"));
        assert!(json.contains("\"blockTime\":1700000000000"));
        assert!(json.contains("\"numTxs\":10"));
        assert!(json.contains("\"round\":99999"));
    }

    #[test]
    fn block_without_round() {
        let block = Block {
            height: 12345,
            block_time: 1700000000000,
            hash: "".to_string(),
            proposer: "".to_string(),
            num_txs: 0,
            round: None,
        };

        let json = serde_json::to_string(&block).unwrap();
        assert!(!json.contains("round"));
    }

    #[test]
    fn transaction_serialization() {
        let tx = Transaction {
            time: 1700000000000,
            user: "0xuser123".to_string(),
            hash: "0xtxhash".to_string(),
            action: json!({"type": "order"}),
            block: 12345,
            error: None,
        };

        let json = serde_json::to_string(&tx).unwrap();
        assert!(json.contains("\"user\":\"0xuser123\""));
        assert!(json.contains("\"block\":12345"));
    }

    #[test]
    fn block_to_data_record() {
        let block = Block {
            height: 100,
            block_time: 1700000000000,
            hash: "0xhash".to_string(),
            proposer: "0xproposer".to_string(),
            num_txs: 5,
            round: None,
        };

        let record = block.to_data_record().unwrap();
        assert_eq!(record.topic, "hl.blocks");
        assert_eq!(record.block_height, Some(100));
        assert_eq!(record.timestamp, 1700000000000);
    }
}
