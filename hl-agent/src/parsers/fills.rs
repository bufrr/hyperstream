use crate::parsers::{
    deserialize_option_u64_from_any, deserialize_string_or_number, deserialize_u64_from_any,
    drain_complete_lines, partition_key_or_unknown, trim_line_bytes, Parser,
};
use crate::sorter_client::proto::DataRecord;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;
use tracing::warn;

#[derive(Default)]
pub struct FillsParser {
    buffer: Vec<u8>,
}

#[derive(Serialize)]
struct Fill {
    user: String,
    coin: String,
    px: String,
    sz: String,
    side: String,
    time: u64,
    #[serde(rename = "startPosition")]
    start_position: String,
    dir: String,
    #[serde(rename = "closedPnl")]
    closed_pnl: String,
    hash: String,
    oid: u64,
    crossed: bool,
    fee: String,
    tid: u64,
    #[serde(rename = "feeToken")]
    fee_token: String,
    liquidation: Option<Value>,
}

#[derive(Deserialize, Default)]
struct NodeFill {
    #[serde(default)]
    user: String,
    #[serde(default)]
    coin: String,
    #[serde(default, deserialize_with = "deserialize_string_or_number")]
    px: String,
    #[serde(default, deserialize_with = "deserialize_string_or_number")]
    sz: String,
    #[serde(default)]
    side: String,
    #[serde(
        default,
        alias = "timestamp",
        alias = "time",
        alias = "tradeTime",
        deserialize_with = "deserialize_u64_from_any"
    )]
    time: u64,
    #[serde(
        default,
        alias = "startPosition",
        alias = "start_position",
        deserialize_with = "deserialize_string_or_number"
    )]
    start_position: String,
    #[serde(default)]
    dir: String,
    #[serde(
        default,
        alias = "closedPnl",
        alias = "closed_pnl",
        deserialize_with = "deserialize_string_or_number"
    )]
    closed_pnl: String,
    #[serde(
        default,
        alias = "oid",
        alias = "orderId",
        alias = "order_id",
        deserialize_with = "deserialize_u64_from_any"
    )]
    oid: u64,
    #[serde(default, alias = "isCrossed", alias = "is_crossed")]
    crossed: bool,
    #[serde(default, deserialize_with = "deserialize_string_or_number")]
    fee: String,
    #[serde(
        default,
        alias = "tradeId",
        alias = "trade_id",
        deserialize_with = "deserialize_u64_from_any"
    )]
    tid: u64,
    #[serde(default, alias = "feeToken", alias = "fee_token")]
    fee_token: String,
    #[serde(default)]
    liquidation: Option<Value>,
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

#[derive(Deserialize)]
struct NodeFillBatch {
    #[serde(alias = "blockNumber", alias = "block_height", alias = "blockHeight")]
    block_number: u64,
    events: Vec<(String, NodeFill)>,
}

impl Parser for FillsParser {
    fn parse(&mut self, _file_path: &Path, data: &[u8]) -> Result<Vec<DataRecord>> {
        self.buffer.extend_from_slice(data);
        let lines = drain_complete_lines(&mut self.buffer);
        let mut records = Vec::with_capacity(lines.len());

        for raw_line in lines {
            let line = trim_line_bytes(raw_line);
            if line.is_empty() {
                continue;
            }

            if let Ok(batch) = serde_json::from_slice::<NodeFillBatch>(&line) {
                for (user, mut fill) in batch.events {
                    fill.user = user;
                    fill.block_height = Some(batch.block_number);
                    records.push(node_fill_to_record(fill)?);
                }
                continue;
            }

            match serde_json::from_slice::<NodeFill>(&line) {
                Ok(fill) => {
                    records.push(node_fill_to_record(fill)?);
                }
                Err(err) => {
                    warn!(error = %err, "failed to parse fill line as JSON");
                }
            }
        }

        Ok(records)
    }

    fn backlog_len(&self) -> usize {
        self.buffer.len()
    }
}

fn node_fill_to_record(node_fill: NodeFill) -> Result<DataRecord> {
    let block_height = node_fill.block_height;
    let tx_hash = if node_fill.hash.is_empty() {
        None
    } else {
        Some(node_fill.hash.clone())
    };

    let fill = Fill {
        user: node_fill.user,
        coin: node_fill.coin,
        px: node_fill.px,
        sz: node_fill.sz,
        side: node_fill.side,
        time: node_fill.time,
        start_position: node_fill.start_position,
        dir: node_fill.dir,
        closed_pnl: node_fill.closed_pnl,
        hash: node_fill.hash,
        oid: node_fill.oid,
        crossed: node_fill.crossed,
        fee: node_fill.fee,
        tid: node_fill.tid,
        fee_token: node_fill.fee_token,
        liquidation: node_fill.liquidation,
    };

    let user_for_partition = partition_key_or_unknown(&fill.user);
    let coin_for_partition = partition_key_or_unknown(&fill.coin);
    let partition_key = format!("{user_for_partition}-{coin_for_partition}");

    let payload =
        serde_json::to_vec(&fill).context("failed to encode fill payload to JSON")?;

    Ok(DataRecord {
        block_height,
        tx_hash,
        timestamp: fill.time,
        topic: "hl.fills".to_string(),
        partition_key,
        payload,
    })
}
