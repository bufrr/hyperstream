use crate::parsers::utils::deserialize_option_string;
use crate::parsers::{
    line_preview, normalize_tx_hash, parse_iso8601_to_millis, schemas::BatchEnvelope,
    BufferedLineParser, LineParser, LINE_PREVIEW_LIMIT,
};
use crate::sorter_client::proto::DataRecord;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value};
use std::path::Path;
use tracing::warn;

/// Parses `node_order_statuses` JSON lines and produces `hl.orders` status snapshots.
///
/// The parser handles both single-event lines and batch envelopes, normalizes hashes, and flattens
/// the order fields that downstream consumers expect.
pub type OrdersParser = BufferedLineParser<OrdersLineParser>;

/// Line-level parser for order statuses.
#[derive(Default)]
pub struct OrdersLineParser;

#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
struct OrderStatusRecord {
    user: String,
    hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    builder: Option<Value>,
    /// Time in milliseconds (converted from ISO8601 for Allium compatibility)
    time: u64,
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

impl LineParser for OrdersLineParser {
    fn parse_line(&mut self, _file_path: &Path, line: &[u8]) -> Result<Vec<DataRecord>> {
        let mut records = Vec::new();

        if let Ok(batch) = sonic_rs::from_slice::<BatchEnvelope<NodeOrderStatus>>(line) {
            let block_number = batch.block_number;
            for event in batch.events {
                records.push(order_status_to_record(
                    event,
                    Some(block_number),
                    batch.block_time.as_deref(),
                )?);
            }
            return Ok(records);
        }

        match sonic_rs::from_slice::<NodeOrderStatus>(line) {
            Ok(event) => {
                records.push(order_status_to_record(event, None, None)?);
            }
            Err(err) => {
                warn!(
                    error = %err,
                    preview = %line_preview(line, LINE_PREVIEW_LIMIT),
                    "skipping unrecognized order status line format"
                );
            }
        }

        Ok(records)
    }

    fn parser_type(&self) -> &'static str {
        "orders"
    }
}

impl Default for BufferedLineParser<OrdersLineParser> {
    fn default() -> Self {
        BufferedLineParser::new(OrdersLineParser)
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

    // Convert ISO8601 time to milliseconds for Allium compatibility
    let timestamp = time
        .as_deref()
        .and_then(parse_iso8601_to_millis)
        .or_else(|| block_time.and_then(parse_iso8601_to_millis))
        .unwrap_or(0);

    let order_record = OrderStatusRecord {
        user: user.clone(),
        hash: hash.clone(),
        builder,
        time: timestamp, // Now using milliseconds instead of ISO8601 string
        status: Some(status.clone()),
        coin: flattened_order.coin.clone(),
        side: flattened_order.side.clone(),
        limit_px: flattened_order.limit_px.clone(),
        sz: flattened_order.sz.clone(),
        oid: flattened_order.oid.clone(),
    };

    let payload =
        sonic_rs::to_vec(&order_record).context("failed to encode order payload to JSON")?;

    Ok(DataRecord {
        block_height,
        tx_hash,
        timestamp,
        topic: "hl.orders".to_string(),
        payload,
    })
}

fn extract_order_fields(status: &Value) -> OrderFieldValues {
    let order_object = status
        .as_object()
        .and_then(|obj| {
            obj.get(&"order".to_string())
                .and_then(|value| value.as_object())
        })
        .or_else(|| status.as_object());

    let mut values = OrderFieldValues::default();
    if let Some(order) = order_object {
        values.coin = order.get(&"coin".to_string()).and_then(value_to_string);
        values.side = order.get(&"side".to_string()).and_then(value_to_string);
        values.limit_px = order
            .get(&"limitPx".to_string())
            .or_else(|| order.get(&"limit_px".to_string()))
            .and_then(value_to_string);
        values.sz = order.get(&"sz".to_string()).and_then(value_to_string);
        values.oid = order.get(&"oid".to_string()).cloned();
    }
    values
}

fn value_to_string(value: &Value) -> Option<String> {
    if let Some(s) = value.as_str() {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    } else if let Some(num) = value.as_u64() {
        Some(num.to_string())
    } else if let Some(num) = value.as_i64() {
        Some(num.to_string())
    } else if let Some(num) = value.as_f64() {
        Some(num.to_string())
    } else if let Some(b) = value.as_bool() {
        Some(b.to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_envelope_accepts_object_events() {
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

        let batch: BatchEnvelope<NodeOrderStatus> = sonic_rs::from_str(json).expect("should parse");
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.events[0].user, "alice");
    }
}
