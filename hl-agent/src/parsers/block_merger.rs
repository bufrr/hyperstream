//! Block merger for combining replica_cmds file data with Explorer hashes stored in HashStore.

use super::hash_store::HashStore;
use super::schemas::Block;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::runtime::{Builder, Handle};
use tokio::task;
use tracing::{info, warn};

/// Final merged block record ready for output.
/// This is a type alias for the shared Block schema.
pub type MergedBlock = Block;

impl MergedBlock {
    /// Create a merged block from replica data and optional hash
    pub fn from_replica_data(data: ReplicaBlockData, hash: Option<String>) -> Self {
        Self {
            height: data.height,
            block_time: data.block_time,
            hash: hash.unwrap_or_default(),
            proposer: data.proposer,
            num_txs: data.num_txs,
            round: Some(data.round),
        }
    }
}

/// Block data from replica_cmds file parser (everything except hash)
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaBlockData {
    pub height: u64,
    pub block_time: u64,
    pub proposer: String,
    #[serde(rename = "numTxs")]
    pub num_txs: u64,
    pub round: u64,
}

#[derive(Default)]
struct MergerStats {
    hash_hits: AtomicU64,
    hash_misses: AtomicU64,
    blocks_processed: AtomicU64,
    validation_failures: AtomicU64,
}

/// Global block merger for enriching file blocks with Explorer hashes stored in HashStore.
pub struct BlockMerger {
    hash_store: Arc<HashStore>,
    stats: MergerStats,
}

static GLOBAL_BLOCK_MERGER: OnceLock<Arc<BlockMerger>> = OnceLock::new();

impl BlockMerger {
    /// Create a new BlockMerger wired to the global HashStore.
    pub fn new() -> Self {
        Self {
            hash_store: HashStore::global(),
            stats: MergerStats::default(),
        }
    }

    /// Create a BlockMerger using the provided HashStore (primarily for tests).
    #[allow(dead_code)]
    pub fn with_store(hash_store: Arc<HashStore>) -> Self {
        Self {
            hash_store,
            stats: MergerStats::default(),
        }
    }

    /// Get or initialize the global block merger.
    pub fn global() -> Arc<BlockMerger> {
        GLOBAL_BLOCK_MERGER
            .get_or_init(|| Arc::new(BlockMerger::new()))
            .clone()
    }

    /// Process a block from the file parser asynchronously, fetching the hash if available.
    pub async fn process_file_block(&self, data: ReplicaBlockData) -> MergedBlock {
        self.stats.blocks_processed.fetch_add(1, Ordering::Relaxed);

        let hash = match self.hash_store.get_block_data(data.height).await {
            Some(redis_data) => {
                let block_time_match = redis_data.block_time == data.block_time;
                let proposer_match =
                    redis_data.proposer.to_lowercase() == data.proposer.to_lowercase();

                if block_time_match && proposer_match {
                    self.stats.hash_hits.fetch_add(1, Ordering::Relaxed);
                    Some(redis_data.hash)
                } else {
                    self.stats.hash_misses.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .validation_failures
                        .fetch_add(1, Ordering::Relaxed);
                    warn!(
                        height = data.height,
                        expected_block_time = data.block_time,
                        actual_block_time = redis_data.block_time,
                        expected_proposer = %data.proposer,
                        actual_proposer = %redis_data.proposer,
                        "Redis block data validation failed; ignoring hash"
                    );
                    None
                }
            }
            None => {
                self.stats.hash_misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        };

        MergedBlock::from_replica_data(data, hash)
    }

    /// Blocking helper for synchronous callers (e.g., parsers running inside Tokio tasks).
    pub fn process_file_block_blocking(&self, data: ReplicaBlockData) -> MergedBlock {
        if let Ok(handle) = Handle::try_current() {
            task::block_in_place(|| handle.block_on(self.process_file_block(data)))
        } else {
            Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build runtime for block merger")
                .block_on(self.process_file_block(data))
        }
    }

    /// Log statistics summary.
    pub fn log_stats(&self) {
        let hits = self.stats.hash_hits.load(Ordering::Relaxed);
        let misses = self.stats.hash_misses.load(Ordering::Relaxed);
        let total = self.stats.blocks_processed.load(Ordering::Relaxed);
        let validation_failures = self.stats.validation_failures.load(Ordering::Relaxed);
        let hit_rate = if total > 0 {
            (hits as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        info!(
            cache_hits = hits,
            cache_misses = misses,
            hit_rate = format!("{hit_rate:.1}%"),
            blocks_processed = total,
            validation_failures,
            hash_store_cache = self.hash_store.cache_len(),
            "BlockMerger stats"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsers::hash_store::{RedisBlockData, DEFAULT_HASH_STORE_CACHE_SIZE};

    const TEST_REDIS_URL: &str = "redis://127.0.0.1:6379";

    async fn test_store() -> Arc<HashStore> {
        Arc::new(
            HashStore::new(
                TEST_REDIS_URL,
                DEFAULT_HASH_STORE_CACHE_SIZE,
                true, // cache-only for tests
            )
            .await
            .expect("hash store init"),
        )
    }

    fn sample_block(height: u64) -> ReplicaBlockData {
        ReplicaBlockData {
            height,
            block_time: 1_700_000_000_000,
            proposer: "proposer".to_string(),
            num_txs: 4,
            round: height,
        }
    }

    fn redis_data(block_time: u64, proposer: &str, hash: &str) -> RedisBlockData {
        RedisBlockData {
            block_time,
            proposer: proposer.to_string(),
            hash: hash.to_string(),
        }
    }

    #[tokio::test]
    async fn test_hash_hit() {
        let store = test_store().await;
        let block = sample_block(100);
        store.cache_insert_for_test(100, redis_data(block.block_time, "PROPOSER", "0xabc123"));
        let merger = BlockMerger::with_store(store);

        let merged = merger.process_file_block(block).await;
        assert_eq!(merged.hash, "0xabc123");
    }

    #[tokio::test]
    async fn test_hash_miss() {
        let store = test_store().await;
        let merger = BlockMerger::with_store(store);

        let merged = merger.process_file_block(sample_block(200)).await;
        assert_eq!(merged.hash, "");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_blocking_helper() {
        let store = test_store().await;
        let block = sample_block(300);
        store.cache_insert_for_test(
            300,
            redis_data(block.block_time, &block.proposer, "0xdeadbeef"),
        );
        let merger = BlockMerger::with_store(store);

        let merged = merger.process_file_block_blocking(block);
        assert_eq!(merged.hash, "0xdeadbeef");
    }

    #[tokio::test]
    async fn test_validation_failure() {
        let store = test_store().await;
        // Intentionally set a mismatched block time.
        store.cache_insert_for_test(400, redis_data(1_800_000_000_000, "proposer", "0xfeedface"));
        let merger = BlockMerger::with_store(store);

        let merged = merger.process_file_block(sample_block(400)).await;
        assert_eq!(merged.hash, "");
        assert_eq!(merger.stats.validation_failures.load(Ordering::Relaxed), 1);
    }
}
