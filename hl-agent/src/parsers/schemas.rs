//! Shared output schemas for Hyperliquid data topics.
//!
//! These schemas define the structure of data records sent to downstream systems.
//! Both file parsers and WebSocket parsers use these shared schemas to ensure
//! consistent output formats.

use crate::sorter_client::proto::DataRecord;
use anyhow::Result;
use serde::Serialize;
use serde_json::Value;

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
            partition_key: self.height.to_string(),
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

impl Transaction {
    /// Convert this transaction to a DataRecord for the `hl.transactions` topic.
    #[allow(dead_code)]
    pub fn to_data_record(&self) -> Result<DataRecord> {
        let payload = serde_json::to_vec(self)?;
        let tx_hash = if self.hash.is_empty() {
            None
        } else {
            Some(self.hash.clone())
        };

        let partition_key = if self.user.is_empty() {
            "unknown".to_string()
        } else {
            self.user.clone()
        };

        Ok(DataRecord {
            block_height: Some(self.block),
            tx_hash,
            timestamp: self.time,
            topic: "hl.transactions".to_string(),
            partition_key,
            payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
        assert_eq!(record.partition_key, "100");
        assert_eq!(record.block_height, Some(100));
        assert_eq!(record.timestamp, 1700000000000);
    }

    #[test]
    fn transaction_to_data_record() {
        let tx = Transaction {
            time: 1700000000000,
            user: "0xuser".to_string(),
            hash: "0xhash".to_string(),
            action: json!({}),
            block: 200,
            error: Some("failed".to_string()),
        };

        let record = tx.to_data_record().unwrap();
        assert_eq!(record.topic, "hl.transactions");
        assert_eq!(record.partition_key, "0xuser");
        assert_eq!(record.tx_hash, Some("0xhash".to_string()));
    }
}
