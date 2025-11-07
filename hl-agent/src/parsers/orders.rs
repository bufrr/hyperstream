use crate::parsers::{drain_complete_lines, trim_line_bytes, Parser};
use crate::sorter_client::proto::DataRecord;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{self, Value};
use std::path::Path;
use tracing::warn;

const LINE_PREVIEW_LIMIT: usize = 256;

#[derive(Default)]
pub struct OrdersParser {
    buffer: Vec<u8>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Order {
    coin: String,
    side: String,
    limit_px: String,
    sz: String,
    oid: u64,
    user: String,
    book_diff: Value,
}

#[derive(Deserialize)]
struct NodeOrderBatch {
    #[serde(alias = "blockNumber", alias = "block_number", alias = "block_height")]
    block_number: u64,
    #[serde(default, deserialize_with = "deserialize_node_book_diffs")]
    events: Vec<NodeBookDiff>,
}

#[derive(Deserialize)]
struct NodeBookDiff {
    user: String,
    oid: u64,
    coin: String,
    side: String,
    px: String,
    #[serde(alias = "bookDiff", alias = "book_diff")]
    raw_book_diff: Value,
}

impl Parser for OrdersParser {
    fn parse(&mut self, _file_path: &Path, data: &[u8]) -> Result<Vec<DataRecord>> {
        self.buffer.extend_from_slice(data);
        let lines = drain_complete_lines(&mut self.buffer);
        let mut records = Vec::with_capacity(lines.len());

        for (line_idx, raw_line) in lines.into_iter().enumerate() {
            let line = trim_line_bytes(raw_line);
            if line.is_empty() {
                continue;
            }

            match serde_json::from_slice::<NodeOrderBatch>(&line) {
                Ok(batch) => {
                    for book_diff in batch.events {
                        records.push(node_book_diff_to_record(
                            book_diff,
                            Some(batch.block_number),
                        )?);
                    }
                }
                Err(err) => {
                    warn!(
                        error = %err,
                        line_idx,
                        preview = %line_preview(&line),
                        "skipping unrecognized order book diff line format"
                    );
                }
            }
        }

        Ok(records)
    }

    fn backlog_len(&self) -> usize {
        self.buffer.len()
    }
}

fn node_book_diff_to_record(
    book_diff: NodeBookDiff,
    block_height: Option<u64>,
) -> Result<DataRecord> {
    let NodeBookDiff {
        user,
        oid,
        coin,
        side,
        px,
        raw_book_diff,
    } = book_diff;

    // Extract size from raw_book_diff
    let sz = extract_size_from_book_diff(&raw_book_diff);

    let partition_key = format!("{user}-{coin}-{oid}");

    let order = Order {
        coin,
        side,
        limit_px: px,
        sz,
        oid,
        user,
        book_diff: raw_book_diff,
    };

    let payload =
        serde_json::to_vec(&order).context("failed to encode order payload to JSON")?;

    Ok(DataRecord {
        block_height,
        tx_hash: None,
        timestamp: 0, // Order book diffs don't have timestamps
        topic: "hl.orders".to_string(),
        partition_key,
        payload,
    })
}

fn extract_size_from_book_diff(book_diff: &Value) -> String {
    // Try to extract sz from {"new": {"sz": "..."}} or {"change": {"sz": "..."}}
    if let Value::Object(map) = book_diff {
        if let Some(Value::Object(inner)) = map.get("new").or_else(|| map.get("change")) {
            if let Some(Value::String(sz)) = inner.get("sz") {
                return sz.clone();
            }
        }
    }
    "0.0".to_string()
}

fn deserialize_node_book_diffs<'de, D>(deserializer: D) -> Result<Vec<NodeBookDiff>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    match value {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(arr)) => arr
            .into_iter()
            .map(|item| serde_json::from_value(item).map_err(serde::de::Error::custom))
            .collect(),
        Some(Value::Object(map)) => map
            .into_iter()
            .map(|(_, item)| serde_json::from_value(item).map_err(serde::de::Error::custom))
            .collect(),
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected events array or object, got {other:?}"
        ))),
    }
}

fn line_preview(line: &[u8]) -> String {
    let text = String::from_utf8_lossy(line);
    let mut preview = String::new();
    let mut consumed = 0usize;
    for ch in text.chars() {
        if consumed >= LINE_PREVIEW_LIMIT {
            preview.push('…');
            return preview;
        }
        preview.push(ch);
        consumed += 1;
    }
    preview
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_order_batch_accepts_object_events() {
        let json = r#"
        {
            "block_number": 10,
            "events": {
                "first": {
                    "user": "alice",
                    "oid": 1,
                    "coin": "BTC",
                    "side": "bid",
                    "px": "100.0",
                    "raw_book_diff": {"new": {"sz": "0.5"}}
                }
            }
        }
        "#;

        let batch: NodeOrderBatch = serde_json::from_str(json).expect("should parse");
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.events[0].user, "alice");
        assert_eq!(batch.events[0].coin, "BTC");
    }
}
