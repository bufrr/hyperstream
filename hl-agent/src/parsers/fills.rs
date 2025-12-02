use crate::parsers::schemas::FillBatchEnvelope;
use crate::parsers::{
    deserialize_option_u64_from_any, deserialize_string_or_number, deserialize_u64_from_any,
    line_preview, normalize_tx_hash, BufferedLineParser, LineParser, LINE_PREVIEW_LIMIT,
};
use crate::sorter_client::proto::DataRecord;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
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

#[derive(Debug, Deserialize, Default)]
pub(crate) struct NodeFill {
    #[serde(default)]
    pub(crate) user: String,
    #[serde(default)]
    pub(crate) coin: String,
    #[serde(default, deserialize_with = "deserialize_string_or_number")]
    pub(crate) px: String,
    #[serde(default, deserialize_with = "deserialize_string_or_number")]
    pub(crate) sz: String,
    #[serde(default)]
    pub(crate) side: String,
    #[serde(
        default,
        alias = "timestamp",
        alias = "time",
        alias = "tradeTime",
        deserialize_with = "deserialize_u64_from_any"
    )]
    pub(crate) time: u64,
    #[serde(
        default,
        alias = "startPosition",
        alias = "start_position",
        deserialize_with = "deserialize_string_or_number"
    )]
    pub(crate) start_position: String,
    #[serde(default)]
    pub(crate) dir: String,
    #[serde(
        default,
        alias = "closedPnl",
        alias = "closed_pnl",
        deserialize_with = "deserialize_string_or_number"
    )]
    pub(crate) closed_pnl: String,
    #[serde(
        default,
        alias = "oid",
        alias = "orderId",
        alias = "order_id",
        deserialize_with = "deserialize_u64_from_any"
    )]
    pub(crate) oid: u64,
    #[serde(default, alias = "isCrossed", alias = "is_crossed")]
    pub(crate) crossed: bool,
    #[serde(default, deserialize_with = "deserialize_string_or_number")]
    pub(crate) fee: String,
    #[serde(
        default,
        alias = "tradeId",
        alias = "trade_id",
        deserialize_with = "deserialize_u64_from_any"
    )]
    pub(crate) tid: u64,
    #[serde(default, alias = "feeToken", alias = "fee_token")]
    pub(crate) fee_token: String,
    #[serde(default)]
    pub(crate) liquidation: Option<Value>,
    #[serde(
        default,
        alias = "height",
        alias = "block",
        alias = "blockHeight",
        deserialize_with = "deserialize_option_u64_from_any"
    )]
    pub(crate) block_height: Option<u64>,
    #[serde(
        default,
        alias = "hash",
        alias = "txHash",
        alias = "tx_hash",
        deserialize_with = "deserialize_string_or_number"
    )]
    pub(crate) hash: String,
}

#[derive(Debug)]
pub(crate) enum FillLine {
    Batch(FillBatchEnvelope<NodeFill>),
    Single(Box<NodeFill>),
}

#[derive(Debug)]
pub(crate) struct FillLineParseError {
    batch_err: serde_json::Error,
    single_err: serde_json::Error,
}

impl fmt::Display for FillLineParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "batch parse failed: {}; single fill parse failed: {}",
            self.batch_err, self.single_err
        )
    }
}

pub(crate) fn parse_fill_line(line: &[u8]) -> Result<FillLine, FillLineParseError> {
    match serde_json::from_slice::<FillBatchEnvelope<NodeFill>>(line) {
        Ok(batch) => Ok(FillLine::Batch(batch)),
        Err(batch_err) => match serde_json::from_slice::<NodeFill>(line) {
            Ok(fill) => Ok(FillLine::Single(Box::new(fill))),
            Err(single_err) => Err(FillLineParseError {
                batch_err,
                single_err,
            }),
        },
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

    fn parser_type(&self) -> &'static str {
        "fills"
    }
}

fn node_fill_to_record(node_fill: NodeFill) -> Result<DataRecord> {
    let NodeFill {
        user,
        coin,
        px,
        sz,
        side,
        time,
        start_position,
        dir,
        closed_pnl,
        hash,
        oid,
        crossed,
        fee,
        tid,
        fee_token,
        liquidation,
        block_height,
        ..
    } = node_fill;

    // System-triggered fills use a zero hash; drop it so sorter doesn't fabricate tx ids.
    let tx_hash = normalize_tx_hash(&hash);
    let timestamp = time;

    // Build fill details (without user)
    let fill_details = FillDetails {
        coin,
        px,
        sz,
        side,
        time,
        start_position,
        dir,
        closed_pnl,
        hash,
        oid,
        crossed,
        fee,
        tid,
        fee_token,
        liquidation,
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
