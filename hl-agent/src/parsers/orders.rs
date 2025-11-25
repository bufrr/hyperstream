use crate::parsers::{
    drain_complete_lines, line_preview, normalize_tx_hash, parse_iso8601_to_millis,
    partition_key_or_unknown, trim_line_bytes, Parser,
};
use crate::sorter_client::proto::DataRecord;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;
use tracing::warn;

const LINE_PREVIEW_LIMIT: usize = 256;

/// Parses `node_order_statuses` JSON lines and produces `hl.orders` status snapshots.
///
/// The parser handles both single-event lines and batch envelopes, normalizes hashes, and flattens
/// the order fields that downstream consumers expect.
pub struct OrdersParser {
    buffer: Vec<u8>,
}

impl Default for OrdersParser {
    fn default() -> Self {
        Self { buffer: Vec::new() }
    }
}

#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
struct OrderStatusRecord {
    user: String,
    hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    builder: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    coin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    side: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit_px: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sz: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    oid: Option<Value>,
}

#[derive(Default)]
struct OrderFieldValues {
    coin: Option<String>,
    side: Option<String>,
    limit_px: Option<String>,
    sz: Option<String>,
    oid: Option<Value>,
}

#[derive(Deserialize)]
struct NodeOrderBatch {
    #[serde(alias = "blockNumber", alias = "block_number", alias = "block_height")]
    block_number: u64,
    #[serde(alias = "blockTime", alias = "block_time", default)]
    block_time: Option<String>,
    #[serde(default, alias = "round", alias = "height")]
    _round: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_node_order_events")]
    events: Vec<NodeOrderStatus>,
}

#[derive(Deserialize)]
struct NodeOrderStatus {
    #[serde(default)]
    user: String,
    #[serde(default, deserialize_with = "deserialize_option_string")]
    hash: Option<String>,
    #[serde(default)]
    builder: Option<Value>,
    #[serde(default)]
    time: Option<String>,
    #[serde(default)]
    status: Value,
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

            if let Ok(batch) = serde_json::from_slice::<NodeOrderBatch>(&line) {
                let block_number = batch.block_number;
                for event in batch.events {
                    records.push(order_status_to_record(
                        event,
                        Some(block_number),
                        batch.block_time.as_deref(),
                    )?);
                }
                continue;
            }

            match serde_json::from_slice::<NodeOrderStatus>(&line) {
                Ok(event) => {
                    records.push(order_status_to_record(event, None, None)?);
                }
                Err(err) => {
                    warn!(
                        error = %err,
                        line_idx,
                        preview = %line_preview(&line, LINE_PREVIEW_LIMIT),
                        "skipping unrecognized order status line format"
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

fn order_status_to_record(
    event: NodeOrderStatus,
    block_height: Option<u64>,
    block_time: Option<&str>,
) -> Result<DataRecord> {
    let NodeOrderStatus {
        user,
        hash,
        builder,
        time,
        status,
    } = event;

    // System-generated order updates surface as zero-hash entries; skip them for tx metadata.
    let tx_hash = hash.as_deref().and_then(normalize_tx_hash);
    let flattened_order = extract_order_fields(&status);

    let order_record = OrderStatusRecord {
        user: user.clone(),
        hash: hash.clone(),
        builder,
        time: time.clone(),
        status: Some(status.clone()),
        coin: flattened_order.coin.clone(),
        side: flattened_order.side.clone(),
        limit_px: flattened_order.limit_px.clone(),
        sz: flattened_order.sz.clone(),
        oid: flattened_order.oid.clone(),
    };

    let payload =
        serde_json::to_vec(&order_record).context("failed to encode order payload to JSON")?;

    let timestamp = time
        .as_deref()
        .and_then(parse_iso8601_to_millis)
        .or_else(|| block_time.and_then(parse_iso8601_to_millis))
        .unwrap_or(0);

    let (coin, oid) = extract_order_identity(&status);
    let partition_key = format!(
        "{}-{}-{}",
        partition_key_or_unknown(&user),
        partition_key_or_unknown(&coin),
        partition_key_or_unknown(&oid)
    );

    Ok(DataRecord {
        block_height,
        tx_hash,
        timestamp,
        topic: "hl.orders".to_string(),
        partition_key,
        payload,
    })
}

fn extract_order_identity(status: &Value) -> (String, String) {
    if let Some(order_value) = status
        .as_object()
        .and_then(|map| map.get("order"))
        .or_else(|| status.get("order"))
    {
        if let Some(order) = order_value.as_object() {
            let coin = order
                .get("coin")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let oid = order
                .get("oid")
                .and_then(value_to_string)
                .unwrap_or_default();
            return (coin, oid);
        }
    }
    (String::new(), String::new())
}

fn extract_order_fields(status: &Value) -> OrderFieldValues {
    let order_object = status
        .as_object()
        .and_then(|obj| obj.get("order").and_then(Value::as_object))
        .or_else(|| status.as_object());

    let mut values = OrderFieldValues::default();
    if let Some(order) = order_object {
        values.coin = order.get("coin").and_then(value_to_string);
        values.side = order.get("side").and_then(value_to_string);
        values.limit_px = order
            .get("limitPx")
            .or_else(|| order.get("limit_px"))
            .and_then(value_to_string);
        values.sz = order.get("sz").and_then(value_to_string);
        values.oid = order.get("oid").cloned();
    }
    values
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => {
            if s.trim().is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        }
        Value::Number(num) => num
            .as_u64()
            .map(|value| value.to_string())
            .or_else(|| num.as_i64().map(|value| value.to_string()))
            .or_else(|| num.as_f64().map(|value| value.to_string())),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

fn deserialize_node_order_events<'de, D>(deserializer: D) -> Result<Vec<NodeOrderStatus>, D::Error>
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
            "expected events list or object, got {other:?}"
        ))),
    }
}

fn deserialize_option_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
        Some(Value::Number(num)) => Ok(Some(num.to_string())),
        Some(Value::Bool(b)) => Ok(Some(b.to_string())),
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected string, got {other:?}"
        ))),
    }
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
                    "hash": null,
                    "status": {
                        "order": {
                            "coin": "BTC",
                            "oid": 42
                        }
                    }
                }
            }
        }
        "#;

        let batch: NodeOrderBatch = serde_json::from_str(json).expect("should parse");
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.events[0].user, "alice");
    }
}
