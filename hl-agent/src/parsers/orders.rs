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
pub struct OrdersParser {
    buffer: Vec<u8>,
}

#[derive(Serialize)]
struct AlliumOrder {
    coin: String,
    side: String,
    #[serde(rename = "limitPx")]
    limit_px: String,
    sz: String,
    oid: u64,
    timestamp: u64,
    user: String,
    #[serde(rename = "orderType")]
    order_type: String,
    status: String,
}

#[derive(Deserialize, Default)]
struct NodeOrder {
    #[serde(default)]
    coin: String,
    #[serde(default)]
    side: String,
    #[serde(
        default,
        alias = "limitPx",
        alias = "limit_px",
        deserialize_with = "deserialize_string_or_number"
    )]
    limit_px: String,
    #[serde(default, deserialize_with = "deserialize_string_or_number")]
    sz: String,
    #[serde(
        default,
        alias = "oid",
        alias = "orderId",
        alias = "order_id",
        deserialize_with = "deserialize_u64_from_any"
    )]
    oid: u64,
    #[serde(
        default,
        alias = "timestamp",
        alias = "time",
        alias = "recvTime",
        deserialize_with = "deserialize_u64_from_any"
    )]
    timestamp: u64,
    #[serde(
        default,
        alias = "userAddress",
        alias = "address",
        deserialize_with = "deserialize_string_or_number"
    )]
    user: String,
    #[serde(default, alias = "orderType", alias = "order_type")]
    order_type: String,
    #[serde(default)]
    status: String,
    #[serde(
        default,
        alias = "height",
        alias = "block",
        alias = "blockHeight",
        deserialize_with = "deserialize_option_u64_from_any"
    )]
    block_height: Option<u64>,
    #[serde(
        default,
        alias = "hash",
        alias = "txHash",
        alias = "tx_hash",
        deserialize_with = "deserialize_string_or_number"
    )]
    hash: String,
}

impl Parser for OrdersParser {
    fn parse(&mut self, _file_path: &Path, data: &[u8]) -> Result<Vec<DataRecord>> {
        self.buffer.extend_from_slice(data);
        let lines = drain_complete_lines(&mut self.buffer);
        let mut records = Vec::with_capacity(lines.len());

        for raw_line in lines {
            let line = trim_line_bytes(raw_line);
            if line.is_empty() {
                continue;
            }

            match serde_json::from_slice::<NodeOrder>(&line) {
                Ok(node_order) => {
                    let block_height = node_order.block_height;
                    let tx_hash = if node_order.hash.is_empty() {
                        None
                    } else {
                        Some(node_order.hash.clone())
                    };

                    let allium_order = AlliumOrder {
                        coin: node_order.coin,
                        side: node_order.side,
                        limit_px: node_order.limit_px,
                        sz: node_order.sz,
                        oid: node_order.oid,
                        timestamp: node_order.timestamp,
                        user: node_order.user,
                        order_type: node_order.order_type,
                        status: node_order.status,
                    };

                    let partition_key = if allium_order.coin.is_empty() {
                        "unknown".to_string()
                    } else {
                        allium_order.coin.clone()
                    };

                    let payload = serde_json::to_vec(&allium_order)
                        .context("failed to encode order payload to JSON")?;

                    records.push(DataRecord {
                        block_height,
                        tx_hash,
                        timestamp: allium_order.timestamp,
                        topic: "hl.orders".to_string(),
                        partition_key,
                        payload,
                    });
                }
                Err(err) => {
                    warn!(error = %err, "failed to parse order line as JSON");
                }
            }
        }

        Ok(records)
    }

    fn backlog_len(&self) -> usize {
        self.buffer.len()
    }
}
