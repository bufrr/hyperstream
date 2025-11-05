use crate::parsers::{
    deserialize_option_u64_from_any, deserialize_string_or_number, deserialize_u64_from_any,
    drain_complete_lines, trim_line_bytes, Parser,
};
use crate::sorter_client::proto::DataRecord;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;
use tracing::warn;

#[derive(Default)]
pub struct MiscEventsParser {
    buffer: Vec<u8>,
}

#[derive(Serialize)]
struct AlliumMiscEvent {
    #[serde(rename = "eventType")]
    event_type: String,
    user: String,
    time: u64,
    data: Value,
}

#[derive(Deserialize, Default)]
struct NodeMiscEvent {
    #[serde(default, alias = "eventType", alias = "event_type")]
    event_type: String,
    #[serde(
        default,
        alias = "userAddress",
        alias = "address",
        deserialize_with = "deserialize_string_or_number"
    )]
    user: String,
    #[serde(
        default,
        alias = "time",
        alias = "timestamp",
        alias = "eventTime",
        deserialize_with = "deserialize_u64_from_any"
    )]
    time: u64,
    #[serde(default)]
    data: Value,
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

impl Parser for MiscEventsParser {
    fn parse(&mut self, _file_path: &Path, data: &[u8]) -> Result<Vec<DataRecord>> {
        self.buffer.extend_from_slice(data);
        let lines = drain_complete_lines(&mut self.buffer);
        let mut records = Vec::with_capacity(lines.len());

        for raw_line in lines {
            let line = trim_line_bytes(raw_line);
            if line.is_empty() {
                continue;
            }

            match serde_json::from_slice::<NodeMiscEvent>(&line) {
                Ok(node_event) => {
                    let block_height = node_event.block_height;
                    let tx_hash = if node_event.hash.is_empty() {
                        None
                    } else {
                        Some(node_event.hash.clone())
                    };

                    let allium_event = AlliumMiscEvent {
                        event_type: node_event.event_type,
                        user: node_event.user,
                        time: node_event.time,
                        data: node_event.data,
                    };

                    let partition_key = if allium_event.event_type.is_empty() {
                        "unknown".to_string()
                    } else {
                        allium_event.event_type.clone()
                    };

                    let payload = serde_json::to_vec(&allium_event)
                        .context("failed to encode misc event payload to JSON")?;

                    records.push(DataRecord {
                        block_height,
                        tx_hash,
                        timestamp: allium_event.time,
                        topic: "hl.misc_events".to_string(),
                        partition_key,
                        payload,
                    });
                }
                Err(err) => {
                    warn!(error = %err, "failed to parse misc event line as JSON");
                }
            }
        }

        Ok(records)
    }

    fn backlog_len(&self) -> usize {
        self.buffer.len()
    }
}
