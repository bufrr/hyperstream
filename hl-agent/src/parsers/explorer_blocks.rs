//! Parser for explorerBlock WebSocket data
//!
//! Converts ExplorerBlock messages into hl.blocks DataRecords

use crate::sorter_client::proto::DataRecord;
use crate::sources::explorer_ws::ExplorerBlock;
use anyhow::Result;
use serde::Serialize;

/// Output schema for hl.blocks topic (from Explorer WS)
#[derive(Debug, Serialize)]
pub struct Block {
    pub height: u64,
    pub time: u64, // milliseconds (renamed from blockTime for consistency)
    pub hash: String,
    pub proposer: String,
    #[serde(rename = "numTxs")]
    pub num_txs: u32,
}

impl From<ExplorerBlock> for Block {
    fn from(eb: ExplorerBlock) -> Self {
        Self {
            height: eb.height,
            time: eb.block_time, // Map blockTime to time
            hash: eb.hash,
            proposer: eb.proposer,
            num_txs: eb.num_txs,
        }
    }
}

pub struct ExplorerBlocksParser;

impl ExplorerBlocksParser {
    pub fn new() -> Self {
        Self
    }

    /// Convert ExplorerBlock to DataRecord
    pub fn parse_block(&self, block: ExplorerBlock) -> Result<DataRecord> {
        let output_block = Block::from(block);

        let block_height = output_block.height;
        let topic = "hl.blocks".to_string();
        let partition_key = format!("{}", output_block.height);
        let payload = serde_json::to_vec(&output_block)?;
        let timestamp = output_block.time;

        Ok(DataRecord {
            block_height: Some(block_height),
            tx_hash: None,
            timestamp,
            topic,
            partition_key,
            payload,
        })
    }
}

impl Default for ExplorerBlocksParser {
    fn default() -> Self {
        Self::new()
    }
}

// Note: This parser doesn't implement the Parser trait because it works
// with structured ExplorerBlock objects, not raw byte chunks.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_conversion() {
        let explorer_block = ExplorerBlock {
            height: 467380970,
            time: 1763973059106,
            hash: "0xabc123def456".to_string(),
            proposer: "0x1234567890abcdef".to_string(),
            num_txs: 42,
        };

        let parser = ExplorerBlocksParser::new();
        let record = parser.parse_block(explorer_block).unwrap();

        assert_eq!(record.metadata.as_ref().unwrap().topic, "hl.blocks");
        assert_eq!(record.metadata.as_ref().unwrap().partition_key, "467380970");
        assert_eq!(
            record.metadata.as_ref().unwrap().timestamp_ms,
            1763973059106
        );

        let block: Block = serde_json::from_slice(&record.payload).unwrap();
        assert_eq!(block.height, 467380970);
        assert_eq!(block.hash, "0xabc123def456");
        assert_eq!(block.num_txs, 42);
    }

    #[test]
    fn test_block_serialization() {
        let block = Block {
            height: 123456,
            time: 1234567890,
            hash: "0xabc".to_string(),
            proposer: "0xdef".to_string(),
            num_txs: 5,
        };

        let json = serde_json::to_string(&block).unwrap();
        assert!(json.contains("\"height\":123456"));
        assert!(json.contains("\"hash\":\"0xabc\""));
        assert!(json.contains("\"numTxs\":5"));
    }
}
