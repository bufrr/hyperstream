use crate::parsers::{
    deserialize_option_u64_from_any, deserialize_string_or_number, deserialize_u64_from_any,
    drain_complete_lines, trim_line_bytes, Parser,
};
use crate::sorter_client::proto::DataRecord;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tracing::warn;

#[derive(Default)]
pub struct TradesParser {
    buffer: Vec<u8>,
}

#[derive(Serialize)]
struct Trade {
    coin: String,
    side: String,
    px: String,
    sz: String,
    time: u64,
    hash: String,
    tid: u64,
    users: Vec<String>,
}

#[derive(Deserialize, Default)]
struct NodeTrade {
    #[serde(default)]
    coin: String,
    #[serde(default)]
    side: String,
    #[serde(default, deserialize_with = "deserialize_string_or_number")]
    px: String,
    #[serde(default, deserialize_with = "deserialize_string_or_number")]
    sz: String,
    #[serde(
        default,
        alias = "timestamp",
        deserialize_with = "deserialize_u64_from_any"
    )]
    time: u64,
    #[serde(
        default,
        alias = "txHash",
        alias = "tx_hash",
        deserialize_with = "deserialize_string_or_number"
    )]
    hash: String,
    #[serde(
        default,
        alias = "tradeId",
        alias = "trade_id",
        deserialize_with = "deserialize_u64_from_any"
    )]
    tid: u64,
    #[serde(default)]
    users: Vec<String>,
    #[serde(
        default,
        alias = "height",
        alias = "block",
        alias = "blockHeight",
        deserialize_with = "deserialize_option_u64_from_any"
    )]
    block_height: Option<u64>,
}

impl Parser for TradesParser {
    fn parse(&mut self, _file_path: &Path, data: &[u8]) -> Result<Vec<DataRecord>> {
        self.buffer.extend_from_slice(data);
        let lines = drain_complete_lines(&mut self.buffer);

        let mut records = Vec::with_capacity(lines.len());

        for raw_line in lines {
            let line = trim_line_bytes(raw_line);
            if line.is_empty() {
                continue;
            }

            match serde_json::from_slice::<NodeTrade>(&line) {
                Ok(node_trade) => {
                    let block_height = node_trade.block_height;
                    let tx_hash = if node_trade.hash.is_empty() {
                        None
                    } else {
                        Some(node_trade.hash.clone())
                    };

                    let trade = Trade {
                        coin: node_trade.coin,
                        side: node_trade.side,
                        px: node_trade.px,
                        sz: node_trade.sz,
                        time: node_trade.time,
                        hash: node_trade.hash,
                        tid: node_trade.tid,
                        users: node_trade.users,
                    };

                    let partition_key = if trade.coin.is_empty() {
                        "unknown".to_string()
                    } else {
                        trade.coin.clone()
                    };

                    let payload = serde_json::to_vec(&trade)
                        .context("failed to encode trade payload to JSON")?;

                    records.push(DataRecord {
                        block_height,
                        tx_hash,
                        timestamp: trade.time,
                        topic: "hl.trades".to_string(),
                        partition_key,
                        payload,
                    });
                }
                Err(err) => {
                    warn!(error = %err, "failed to parse trade line as JSON");
                }
            }
        }

        Ok(records)
    }

    fn backlog_len(&self) -> usize {
        self.buffer.len()
    }
}
