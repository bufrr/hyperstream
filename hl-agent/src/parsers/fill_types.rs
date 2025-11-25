use super::{
    deserialize_option_u64_from_any, deserialize_string_or_number, deserialize_u64_from_any,
};
use serde::Deserialize;
use serde_json::Value;
use std::fmt;

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

#[derive(Debug, Deserialize)]
pub(crate) struct NodeFillBatch {
    #[serde(alias = "blockNumber", alias = "block_height", alias = "blockHeight")]
    pub(crate) block_number: u64,
    #[serde(default, alias = "round", alias = "height")]
    pub(crate) _round: Option<u64>,
    #[serde(default)]
    pub(crate) events: Vec<(String, NodeFill)>,
}

#[derive(Debug)]
pub(crate) enum FillLine {
    Batch(NodeFillBatch),
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
    match serde_json::from_slice::<NodeFillBatch>(line) {
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
