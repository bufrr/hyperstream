use crate::parsers::Parser;
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

#[derive(Serialize)]
struct AlliumBlock {
    height: u64,
    #[serde(rename = "blockTime")]
    block_time: u64,
    hash: String,
    proposer: String,
    #[serde(rename = "numTxs")]
    num_txs: u32,
}

#[derive(Deserialize, Default)]
struct NodeBlock {
    #[serde(default)]
    height: u64,
    #[serde(default, alias = "blockTime", alias = "block_time")]
    block_time: u64,
    #[serde(default)]
    hash: String,
    #[serde(default)]
    proposer: String,
    #[serde(default, alias = "numTxs", alias = "num_txs")]
    num_txs: u32,
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

                    let allium_block = AlliumBlock {
                        height: node_block.height,
                        block_time: node_block.block_time,
                        hash: node_block.hash,
                        proposer: node_block.proposer,
                        num_txs: node_block.num_txs,
                    };

                    let payload = serde_json::to_vec(&allium_block)
                        .context("failed to encode block payload as JSON")?;

                    let partition_key = if allium_block.height == 0 {
                        "unknown".to_string()
                    } else {
                        allium_block.height.to_string()
                    };

                    records.push(DataRecord {
                        block_height: if allium_block.height == 0 {
                            None
                        } else {
                            Some(allium_block.height)
                        },
                        tx_hash: None,
                        timestamp: allium_block.block_time,
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
