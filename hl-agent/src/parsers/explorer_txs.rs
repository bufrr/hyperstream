//! Parser for explorerTxs WebSocket data
//!
//! Converts ExplorerTx messages into hl.transactions DataRecords

use crate::sorter_client::proto::DataRecord;
use crate::sources::explorer_ws::ExplorerTx;
use anyhow::Result;
use serde::Serialize;
use serde_json::Value;

/// Output schema for hl.transactions topic (from Explorer WS)
#[derive(Debug, Serialize)]
pub struct Transaction {
    pub time: u64, // milliseconds
    pub user: String,
    pub action: Value, // Keep as dynamic JSON
    pub block: u64,
    pub hash: String,
    pub error: Option<String>,
}

impl From<ExplorerTx> for Transaction {
    fn from(et: ExplorerTx) -> Self {
        Self {
            time: et.time,
            user: et.user,
            action: et.action,
            block: et.block,
            hash: et.hash,
            error: et.error,
        }
    }
}

pub struct ExplorerTxsParser;

impl ExplorerTxsParser {
    pub fn new() -> Self {
        Self
    }

    /// Convert ExplorerTx to DataRecord
    pub fn parse_tx(&self, tx: ExplorerTx) -> Result<DataRecord> {
        let output_tx = Transaction::from(tx);

        let block_height = Some(output_tx.block);
        let tx_hash = Some(output_tx.hash.clone());
        let timestamp = output_tx.time;
        let topic = "hl.transactions".to_string();
        let partition_key = output_tx.user.clone();
        let payload = serde_json::to_vec(&output_tx)?;

        Ok(DataRecord {
            block_height,
            tx_hash,
            timestamp,
            topic,
            partition_key,
            payload,
        })
    }
}

impl Default for ExplorerTxsParser {
    fn default() -> Self {
        Self::new()
    }
}

// Note: This parser doesn't implement the Parser trait because it works
// with structured ExplorerTx objects, not raw byte chunks.

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_tx_conversion() {
        let explorer_tx = ExplorerTx {
            time: 1763973059106,
            user: "0xc64cc00b46101bd40aa1c3121195e85c0b0918d8".to_string(),
            action: json!({"type": "order", "orders": []}),
            block: 467380970,
            hash: "0x043dbdb186dd8fb105b7041bdbaaea010101229721d0ae83a806690445d1699b".to_string(),
            error: None,
        };

        let parser = ExplorerTxsParser::new();
        let record = parser.parse_tx(explorer_tx).unwrap();

        assert_eq!(record.metadata.as_ref().unwrap().topic, "hl.transactions");
        assert_eq!(
            record.metadata.as_ref().unwrap().partition_key,
            "0xc64cc00b46101bd40aa1c3121195e85c0b0918d8"
        );
        assert_eq!(
            record.metadata.as_ref().unwrap().timestamp_ms,
            1763973059106
        );

        let tx: Transaction = serde_json::from_slice(&record.payload).unwrap();
        assert_eq!(tx.user, "0xc64cc00b46101bd40aa1c3121195e85c0b0918d8");
        assert_eq!(tx.block, 467380970);
        assert!(tx.error.is_none());
    }

    #[test]
    fn test_tx_with_error() {
        let explorer_tx = ExplorerTx {
            time: 1763973059106,
            user: "0xtest".to_string(),
            action: json!({"type": "cancel"}),
            block: 123,
            hash: "0xabc".to_string(),
            error: Some("Insufficient balance".to_string()),
        };

        let parser = ExplorerTxsParser::new();
        let record = parser.parse_tx(explorer_tx).unwrap();

        let tx: Transaction = serde_json::from_slice(&record.payload).unwrap();
        assert_eq!(tx.error, Some("Insufficient balance".to_string()));
    }

    #[test]
    fn test_tx_serialization() {
        let tx = Transaction {
            time: 1234567890,
            user: "0xuser".to_string(),
            action: json!({"type": "order"}),
            block: 999,
            hash: "0xhash".to_string(),
            error: None,
        };

        let json = serde_json::to_string(&tx).unwrap();
        assert!(json.contains("\"user\":\"0xuser\""));
        assert!(json.contains("\"block\":999"));
        assert!(json.contains("\"error\":null"));
    }
}
