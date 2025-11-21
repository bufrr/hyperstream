use crate::parsers::{
    drain_complete_lines, line_preview, parse_iso8601_to_millis, trim_line_bytes,
};
use crate::sorter_client::proto::DataRecord;
use anyhow::{Context, Result};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Arc, OnceLock};
use tracing::warn;

const REPLICA_LINE_PREVIEW: usize = 256;

pub(crate) type SharedProposerCache = Arc<DashMap<u64, String>>;

static GLOBAL_PROPOSER_CACHE: OnceLock<SharedProposerCache> = OnceLock::new();

pub(crate) fn proposer_cache() -> SharedProposerCache {
    GLOBAL_PROPOSER_CACHE
        .get_or_init(|| Arc::new(DashMap::new()))
        .clone()
}

/// Parses `replica_cmds` JSONL files and emits canonical `hl.blocks` records.
///
/// Input lines are the same structures produced by `periodic_abci_states` where each line contains
/// an `abci_block`. The parser caches proposers for later use by the transaction parser and emits
/// one `DataRecord` per block on the `hl.blocks` topic.
pub struct BlocksParser {
    buffer: Vec<u8>,
    proposer_cache: SharedProposerCache,
}

impl Default for BlocksParser {
    fn default() -> Self {
        Self {
            buffer: Vec::new(),
            proposer_cache: proposer_cache(),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BlockRecord {
    height: u64, // Using round as height (no mapping available)
    block_time: u64,
    hash: String, // Always empty (not available in replica_cmds)
    proposer: String,
    #[serde(rename = "numTxs")]
    num_txs: u64,
    round: u64,
}

#[derive(Debug, Deserialize, Default)]
struct ReplicaCmd {
    #[serde(default)]
    abci_block: Option<ReplicaAbciBlock>,
}

#[derive(Debug, Deserialize, Default)]
struct ReplicaAbciBlock {
    #[serde(default)]
    round: u64,
    #[serde(default)]
    proposer: String,
    #[serde(default)]
    time: String,
    #[serde(default)]
    signed_action_bundles: Vec<serde_json::Value>, // We only need the count
}

impl crate::parsers::Parser for BlocksParser {
    fn parse(&mut self, _file_path: &Path, data: &[u8]) -> Result<Vec<DataRecord>> {
        self.buffer.extend_from_slice(data);
        if self.buffer.is_empty() {
            return Ok(Vec::new());
        }

        let lines = drain_complete_lines(&mut self.buffer);
        let mut records = Vec::new();

        for raw_line in lines {
            let line = trim_line_bytes(raw_line);
            if line.is_empty() {
                continue;
            }

            match serde_json::from_slice::<ReplicaCmd>(&line) {
                Ok(cmd) => {
                    if let Some(block) = cmd.abci_block {
                        // Cache proposer for transactions parser
                        if block.round != 0 && !block.proposer.is_empty() {
                            self.proposer_cache
                                .insert(block.round, block.proposer.clone());
                        }

                        if let Some(record) = abci_block_to_record(block) {
                            match block_record_to_data_record(record) {
                                Ok(data_record) => records.push(data_record),
                                Err(err) => {
                                    warn!(
                                        error = %err,
                                        "failed to convert abci_block to data record"
                                    );
                                }
                            }
                        }
                    }
                }
                Err(err) => {
                    warn!(
                        error = %err,
                        preview = %line_preview(&line, REPLICA_LINE_PREVIEW),
                        "failed to parse replica_cmds line"
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

fn abci_block_to_record(block: ReplicaAbciBlock) -> Option<BlockRecord> {
    if block.round == 0 {
        return None;
    }

    let timestamp = parse_abci_block_time(&block.time);
    let num_txs = block.signed_action_bundles.len() as u64;

    // Use round as height since we don't have height mapping
    Some(BlockRecord {
        height: block.round,
        block_time: timestamp,
        hash: String::new(), // Not available in replica_cmds
        proposer: block.proposer,
        num_txs,
        round: block.round,
    })
}

fn block_record_to_data_record(block_payload: BlockRecord) -> Result<DataRecord> {
    let block_height = block_payload.height;
    let timestamp = block_payload.block_time;
    let payload =
        serde_json::to_vec(&block_payload).context("failed to encode hl.blocks payload")?;

    Ok(DataRecord {
        block_height: Some(block_height),
        tx_hash: None,
        timestamp,
        topic: "hl.blocks".to_string(),
        partition_key: block_height.to_string(),
        payload,
    })
}

fn parse_abci_block_time(time: &str) -> u64 {
    if time.trim().is_empty() {
        return 0;
    }

    match parse_iso8601_to_millis(time) {
        Some(value) => value,
        None => {
            warn!(%time, "failed to parse abci_block time");
            0
        }
    }
}
