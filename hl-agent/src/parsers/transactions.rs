use crate::parsers::{deserialize_string_or_number, deserialize_u64_from_any, Parser};
use crate::sorter_client::proto::DataRecord;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;
use tracing::warn;

#[derive(Default)]
pub struct TransactionsParser {
    buffer: Vec<u8>,
    emitted: bool,
}

#[derive(Serialize)]
struct AlliumTransaction {
    time: u64,
    user: String,
    action: Value,
    block: u64,
    hash: String,
    error: Option<Value>,
}

#[derive(Deserialize, Default)]
struct NodeTransaction {
    #[serde(
        default,
        alias = "timestamp",
        alias = "blockTime",
        deserialize_with = "deserialize_u64_from_any"
    )]
    time: u64,
    #[serde(
        default,
        alias = "userAddress",
        alias = "address",
        deserialize_with = "deserialize_string_or_number"
    )]
    user: String,
    #[serde(default)]
    action: Value,
    #[serde(
        default,
        alias = "block",
        alias = "blockHeight",
        alias = "height",
        deserialize_with = "deserialize_u64_from_any"
    )]
    block: u64,
    #[serde(
        default,
        alias = "txHash",
        alias = "tx_hash",
        deserialize_with = "deserialize_string_or_number"
    )]
    hash: String,
    #[serde(default, alias = "err")]
    error: Option<Value>,
}

impl Parser for TransactionsParser {
    fn parse(&mut self, _file_path: &Path, data: &[u8]) -> Result<Vec<DataRecord>> {
        if self.emitted {
            // replica_cmds files are immutable per block; nothing left to emit.
            return Ok(Vec::new());
        }

        self.buffer.extend_from_slice(data);
        if self.buffer.is_empty() {
            return Ok(Vec::new());
        }

        match serde_json::from_slice::<NodeTransaction>(&self.buffer) {
            Ok(node_transaction) => {
                let tx_hash = if node_transaction.hash.is_empty() {
                    None
                } else {
                    Some(node_transaction.hash.clone())
                };
                let partition_key = tx_hash.clone().unwrap_or_else(|| "unknown".to_string());
                let block_height = if node_transaction.block == 0 {
                    None
                } else {
                    Some(node_transaction.block)
                };

                let allium_transaction = AlliumTransaction {
                    time: node_transaction.time,
                    user: node_transaction.user,
                    action: node_transaction.action,
                    block: node_transaction.block,
                    hash: node_transaction.hash,
                    error: node_transaction.error,
                };

                let payload = serde_json::to_vec(&allium_transaction)
                    .context("failed to encode transactions payload to JSON")?;
                self.emitted = true;
                self.buffer.clear();

                Ok(vec![DataRecord {
                    block_height,
                    tx_hash,
                    timestamp: allium_transaction.time,
                    topic: "hl.transactions".to_string(),
                    partition_key,
                    payload,
                }])
            }
            Err(err) if err.is_eof() => Ok(Vec::new()),
            Err(err) => {
                warn!(error = %err, "failed to parse replica_cmds JSON payload");
                self.buffer.clear();
                Ok(Vec::new())
            }
        }
    }

    fn backlog_len(&self) -> usize {
        self.buffer.len()
    }
}
