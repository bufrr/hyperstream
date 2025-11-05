use crate::sorter_client::proto::DataRecord;
use anyhow::{anyhow, Result};
use std::path::Path;

pub mod blocks;
pub mod fills;
pub mod misc_events;
pub mod orders;
pub mod trades;
pub mod transactions;

pub use blocks::BlocksParser;
pub use fills::FillsParser;
pub use misc_events::MiscEventsParser;
pub use orders::OrdersParser;
pub use trades::TradesParser;
pub use transactions::TransactionsParser;

pub trait Parser: Send {
    fn parse(&mut self, file_path: &Path, data: &[u8]) -> Result<Vec<DataRecord>>;
    fn backlog_len(&self) -> usize;
}

pub fn route_parser(file_path: &Path) -> Result<Box<dyn Parser>> {
    if path_contains(file_path, "periodic_abci_states") {
        return Ok(Box::new(BlocksParser::default()));
    }
    if path_contains(file_path, "replica_cmds") {
        return Ok(Box::new(TransactionsParser::default()));
    }
    if path_contains(file_path, "node_trades") {
        return Ok(Box::new(TradesParser::default()));
    }
    if path_contains(file_path, "node_order_statuses")
        || path_contains(file_path, "node_fills_by_block")
        || path_contains(file_path, "node_order_statuses_by_block")
    {
        return Ok(Box::new(FillsParser::default()));
    }
    if path_contains(file_path, "node_raw_book_diffs")
        || path_contains(file_path, "node_raw_book_diffs_by_block")
    {
        return Ok(Box::new(OrdersParser::default()));
    }
    if path_contains(file_path, "misc_events") || path_contains(file_path, "misc_events_by_block") {
        return Ok(Box::new(MiscEventsParser::default()));
    }

    Err(anyhow!(
        "no parser registered for path {}",
        file_path.display()
    ))
}

fn path_contains(path: &Path, needle: &str) -> bool {
    path.iter()
        .filter_map(|c| c.to_str())
        .any(|component| component == needle)
}

pub(crate) fn drain_complete_lines(buffer: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let mut lines = Vec::new();
    let mut start = 0usize;

    for (idx, byte) in buffer.iter().enumerate() {
        if *byte == b'\n' {
            let line = buffer[start..idx].to_vec();
            lines.push(line);
            start = idx + 1;
        }
    }

    if start > 0 {
        buffer.drain(0..start);
    }

    lines
}

pub(crate) fn trim_line_bytes(mut line: Vec<u8>) -> Vec<u8> {
    while line.last().map(|b| *b == b'\r' || *b == b' ') == Some(true) {
        line.pop();
    }
    line
}

pub(crate) fn deserialize_string_or_number<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    match value {
        None | Some(serde_json::Value::Null) => Ok(String::new()),
        Some(serde_json::Value::String(s)) => Ok(s),
        Some(serde_json::Value::Number(n)) => Ok(n.to_string()),
        Some(serde_json::Value::Bool(b)) => Ok(b.to_string()),
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected string or number, got {other:?}"
        ))),
    }
}

pub(crate) fn deserialize_u64_from_any<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    match value {
        None | Some(serde_json::Value::Null) => Ok(0),
        Some(serde_json::Value::Number(n)) => n
            .as_u64()
            .ok_or_else(|| serde::de::Error::custom("expected u64-compatible number")),
        Some(serde_json::Value::String(s)) => s
            .parse::<u64>()
            .map_err(|_| serde::de::Error::custom(format!("failed to parse u64 from {s:?}"))),
        Some(serde_json::Value::Bool(true)) => Ok(1),
        Some(serde_json::Value::Bool(false)) => Ok(0),
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected number, got {other:?}"
        ))),
    }
}

pub(crate) fn deserialize_option_u64_from_any<'de, D>(
    deserializer: D,
) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Number(n)) => n
            .as_u64()
            .ok_or_else(|| serde::de::Error::custom("expected u64-compatible number"))
            .map(Some),
        Some(serde_json::Value::String(s)) => {
            if s.trim().is_empty() {
                Ok(None)
            } else {
                s.parse::<u64>().map(Some).map_err(|_| {
                    serde::de::Error::custom(format!("failed to parse u64 from {s:?}"))
                })
            }
        }
        Some(serde_json::Value::Bool(true)) => Ok(Some(1)),
        Some(serde_json::Value::Bool(false)) => Ok(Some(0)),
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected number, got {other:?}"
        ))),
    }
}
