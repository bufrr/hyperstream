use crate::parsers::block_merger::{BlockMerger, ReplicaBlockData};
use crate::parsers::utils::extract_starting_block;
use crate::parsers::{
    drain_complete_lines, line_preview, parse_iso8601_to_millis, trim_line_bytes,
    LINE_PREVIEW_LIMIT,
};
use crate::sorter_client::proto::DataRecord;
use anyhow::Result;
use dashmap::DashMap;
use serde::Deserialize;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use tracing::warn;

pub(crate) type SharedProposerCache = Arc<DashMap<u64, String>>;

static GLOBAL_PROPOSER_CACHE: OnceLock<SharedProposerCache> = OnceLock::new();

pub(crate) fn proposer_cache() -> SharedProposerCache {
    GLOBAL_PROPOSER_CACHE
        .get_or_init(|| Arc::new(DashMap::new()))
        .clone()
}

/// Parses `replica_cmds` JSONL files and adds blocks to the BlockMerger.
///
/// Input lines are the same structures produced by `periodic_abci_states` where each line contains
/// an `abci_block`. The parser caches proposers for later use by the transaction parser.
///
/// Block height is calculated as: `starting_block (from filename) + line_number`
/// Block hash comes from the BlockMerger which pairs replica data with Explorer WebSocket hashes.
///
/// Note: This parser adds blocks to the global BlockMerger instead of emitting DataRecords directly.
/// The BlockMerger's flush task is responsible for emitting merged blocks.
pub struct BlocksParser {
    buffer: Vec<u8>,
    proposer_cache: SharedProposerCache,
    /// Starting block number extracted from filename (e.g., 808750000 from ".../808750000")
    starting_block: Option<u64>,
    /// Current line count within the file (0-indexed, first line = block at starting_block)
    line_count: u64,
    /// Reference to the global block merger
    merger: Arc<BlockMerger>,
}

impl Default for BlocksParser {
    fn default() -> Self {
        Self {
            buffer: Vec::new(),
            proposer_cache: proposer_cache(),
            starting_block: None,
            line_count: 0,
            merger: BlockMerger::global(),
        }
    }
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
    fn parse(&mut self, file_path: &Path, data: &[u8]) -> Result<Vec<DataRecord>> {
        // Extract starting block from filename on first call
        if self.starting_block.is_none() {
            self.starting_block = extract_starting_block(file_path);
            if self.starting_block.is_none() {
                warn!(
                    file_path = %file_path.display(),
                    "could not extract starting block from filename, falling back to round"
                );
            }
        }

        self.buffer.extend_from_slice(data);
        if self.buffer.is_empty() {
            return Ok(Vec::new());
        }

        let lines = drain_complete_lines(&mut self.buffer);
        let mut records = Vec::new();

        for raw_line in lines {
            let line = trim_line_bytes(raw_line);
            if line.is_empty() {
                self.line_count += 1;
                continue;
            }

            // Calculate block height: starting_block + line_count
            let calculated_height = self.starting_block.map(|start| start + self.line_count);

            match serde_json::from_slice::<ReplicaCmd>(&line) {
                Ok(cmd) => {
                    if let Some(block) = cmd.abci_block {
                        // Use calculated height, fall back to round if filename parsing failed
                        let height = calculated_height.unwrap_or(block.round);

                        // Cache proposer for transactions parser using the correct height
                        if height != 0 && !block.proposer.is_empty() {
                            self.proposer_cache.insert(height, block.proposer.clone());
                        }

                        // Convert to ReplicaBlockData and process through merger
                        if let Some(replica_data) = self.abci_block_to_replica_data(block, height) {
                            // Process block - looks up hash from cache and returns merged block
                            let merged = self.merger.process_file_block_blocking(replica_data);
                            match merged.to_data_record() {
                                Ok(data_record) => records.push(data_record),
                                Err(err) => {
                                    warn!(
                                        error = %err,
                                        height,
                                        "failed to convert merged block to data record"
                                    );
                                }
                            }
                        }
                    }
                }
                Err(err) => {
                    warn!(
                        error = %err,
                        preview = %line_preview(&line, LINE_PREVIEW_LIMIT),
                        "failed to parse replica_cmds line"
                    );
                }
            }

            self.line_count += 1;
        }
        Ok(records)
    }

    fn backlog_len(&self) -> usize {
        self.buffer.len()
    }
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

impl BlocksParser {
    /// Convert ABCI block to ReplicaBlockData for the merger
    fn abci_block_to_replica_data(
        &self,
        block: ReplicaAbciBlock,
        height: u64,
    ) -> Option<ReplicaBlockData> {
        // Skip if height is 0 (invalid block)
        if height == 0 {
            return None;
        }

        let block_time = parse_abci_block_time(&block.time);
        let num_txs = block.signed_action_bundles.len() as u64;

        Some(ReplicaBlockData {
            height,
            block_time,
            proposer: block.proposer,
            num_txs,
            round: block.round,
        })
    }
}
