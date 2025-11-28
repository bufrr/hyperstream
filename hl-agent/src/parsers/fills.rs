use super::fill_types::{parse_fill_line, FillLine, NodeFill};
use crate::parsers::{line_preview, normalize_tx_hash, BufferedLineParser, LineParser, LINE_PREVIEW_LIMIT};
use crate::sorter_client::proto::DataRecord;
use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;
use std::path::Path;
use tracing::warn;

/// Converts `node_fills_by_block` JSON lines into tuple payloads for the `hl.fills` topic.
///
/// The parser accepts both single fill lines and aggregated batch lines, buffers partial reads, and
/// outputs `[user, fill_details]` tuples using the schema sorter expects.
pub type FillsParser = BufferedLineParser<FillsLineParser>;

/// Creates a new FillsParser with default configuration.
pub fn new_fills_parser() -> FillsParser {
    BufferedLineParser::new(FillsLineParser)
}

/// Inner line parser for fills - handles parsing individual lines.
pub struct FillsLineParser;

impl Default for FillsLineParser {
    fn default() -> Self {
        Self
    }
}

// Output schema for hl.fills: tuple of [user, fillDetails]
// Allium format: ["0x...", {coin, px, sz, ...}]
#[derive(Serialize)]
struct FillDetails {
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

impl LineParser for FillsLineParser {
    fn parse_line(&mut self, _file_path: &Path, line: &[u8]) -> Result<Vec<DataRecord>> {
        let mut records = Vec::new();

        match parse_fill_line(line) {
            Ok(FillLine::Batch(batch)) => {
                let block_number = batch.block_number;
                for (user, mut fill) in batch.events {
                    fill.user = user;
                    fill.block_height = Some(block_number);
                    records.push(node_fill_to_record(fill)?);
                }
            }
            Ok(FillLine::Single(fill)) => {
                records.push(node_fill_to_record(*fill)?);
            }
            Err(err) => {
                warn!(
                    error = %err,
                    preview = %line_preview(line, LINE_PREVIEW_LIMIT),
                    "failed to parse fill line as JSON"
                );
            }
        }

        Ok(records)
    }
}

fn node_fill_to_record(node_fill: NodeFill) -> Result<DataRecord> {
    let block_height = node_fill.block_height;
    // System-triggered fills use a zero hash; drop it so sorter doesn't fabricate tx ids.
    let tx_hash = normalize_tx_hash(&node_fill.hash);

    // Extract user for tuple format
    let user = node_fill.user.clone();
    let timestamp = node_fill.time;

    // Build fill details (without user)
    let fill_details = FillDetails {
        coin: node_fill.coin.clone(),
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

    // Serialize as tuple: [user, fillDetails]
    // Allium format: ["0x...", {coin: "ETH", px: "100", ...}]
    let fill_tuple = (user, fill_details);
    let payload =
        serde_json::to_vec(&fill_tuple).context("failed to encode fill payload to JSON")?;

    Ok(DataRecord {
        block_height,
        tx_hash,
        timestamp,
        topic: "hl.fills".to_string(),
        payload,
    })
}
