use crate::parsers::{parse_iso8601_to_millis, Parser};
use crate::sorter_client::proto::DataRecord;
use anyhow::{Context, Result};
use rmp_serde::decode::Error as RmpError;
use serde::{Deserialize, Serialize};
use std::io::{self, Cursor};
use std::path::Path;
use tracing::warn;

#[derive(Default)]
pub struct BlocksParser {
    buffer: Vec<u8>,
}

// Output schema for downstream
#[derive(Serialize)]
struct Block {
    height: u64,
    #[serde(rename = "blockTime")]
    block_time: u64,
    hash: String,
    proposer: String,
    #[serde(rename = "numTxs")]
    num_txs: u32,
}

// Real Hyperliquid node data structure
#[derive(Deserialize)]
struct NodeBlock {
    exchange: ExchangeState,
}

#[derive(Deserialize)]
struct ExchangeState {
    context: ExchangeContext,
}

#[derive(Deserialize)]
struct ExchangeContext {
    height: u64,
    time: String, // ISO 8601
    #[serde(default)]
    tx_index: u32,
    // Ignore all other fields (hardfork, next_oid, perp_dexs, etc.)
    #[serde(flatten)]
    _extra: serde_json::Value,
}

impl Parser for BlocksParser {
    fn parse(&mut self, _file_path: &Path, data: &[u8]) -> Result<Vec<DataRecord>> {
        self.buffer.extend_from_slice(data);

        if self.buffer.is_empty() {
            return Ok(Vec::new());
        }

        let mut cursor = 0usize;
        let mut records = Vec::new();

        while cursor < self.buffer.len() {
            let slice = &self.buffer[cursor..];
            let mut reader = Cursor::new(slice);
            let mut deserializer = rmp_serde::Deserializer::new(&mut reader);

            match NodeBlock::deserialize(&mut deserializer) {
                Ok(node_block) => {
                    let consumed = reader.position() as usize;
                    if consumed == 0 {
                        break;
                    }

                    let ctx = &node_block.exchange.context;

                    // Parse ISO 8601 time to milliseconds
                    let block_time = parse_iso8601_to_millis(&ctx.time).unwrap_or(0);

                    let block = Block {
                        height: ctx.height,
                        block_time,
                        hash: String::new(), // Not in source data
                        proposer: String::new(), // Not in source data
                        num_txs: ctx.tx_index,
                    };

                    let payload = serde_json::to_vec(&block)
                        .context("failed to encode block payload as JSON")?;

                    let partition_key = if block.height == 0 {
                        "unknown".to_string()
                    } else {
                        block.height.to_string()
                    };

                    records.push(DataRecord {
                        block_height: if block.height == 0 {
                            None
                        } else {
                            Some(block.height)
                        },
                        tx_hash: None,
                        timestamp: block.block_time,
                        topic: "hl.blocks".to_string(),
                        partition_key,
                        payload,
                    });

                    cursor += consumed;
                }
                Err(err) if is_incomplete_msgpack(&err) => {
                    break;
                }
                Err(err) => {
                    warn!(error = ?err, cursor, "failed to parse MessagePack block payload");
                    cursor += 1;
                }
            }
        }

        if cursor > 0 {
            self.buffer.drain(0..cursor);
        }

        Ok(records)
    }

    fn backlog_len(&self) -> usize {
        self.buffer.len()
    }
}

fn is_incomplete_msgpack(err: &RmpError) -> bool {
    match err {
        RmpError::InvalidMarkerRead(io_err) => io_err.kind() == io::ErrorKind::UnexpectedEof,
        RmpError::InvalidDataRead(io_err) => io_err.kind() == io::ErrorKind::UnexpectedEof,
        _ => false,
    }
}
