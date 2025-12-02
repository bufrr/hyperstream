//! Block merger for combining replica_cmds file data with Explorer hashes stored in HashStore.

use super::hash_store::HashStore;
use super::schemas::Block;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
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

/// Block data returned by the Hyperliquid Explorer API.
#[derive(Clone, Debug)]
struct ExplorerBlockData {
    hash: String,
    block_time: u64,
    proposer: String,
}

#[derive(Deserialize)]
struct ExplorerBlockDetails {
    hash: String,
    #[serde(rename = "blockTime")]
    block_time: u64,
    proposer: String,
}

#[derive(Deserialize)]
struct ExplorerBlockResponse {
    #[serde(rename = "blockDetails")]
    block_details: Option<ExplorerBlockDetails>,
}

static EXPLORER_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

fn get_or_build_client() -> Result<&'static reqwest::Client, String> {
    if let Some(client) = EXPLORER_CLIENT.get() {
        return Ok(client);
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|err| format!("failed to build explorer client: {err}"))?;

    let _ = EXPLORER_CLIENT.set(client);
    EXPLORER_CLIENT
        .get()
        .ok_or_else(|| "failed to initialize explorer client".to_string())
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

/// Single attempt to fetch block from Explorer API
async fn fetch_block_from_explorer_once(height: u64) -> Result<ExplorerBlockData, String> {
    if cfg!(test) {
        return Err("Explorer API fallback disabled in tests".to_string());
    }

    let client = get_or_build_client()?;

    let response = client
        .post("https://rpc.hyperliquid.xyz/explorer")
        .json(&serde_json::json!({
            "height": height,
            "type": "blockDetails",
        }))
        .send()
        .await
        .map_err(|err| format!("Explorer API request error: {err}"))?;

    let status = response.status();
    if !status.is_success() {
        return Err(format!("Explorer API returned HTTP {status}"));
    }

    let payload: ExplorerBlockResponse = response
        .json()
        .await
        .map_err(|err| format!("failed to parse Explorer API response: {err}"))?;

    let details = payload
        .block_details
        .ok_or_else(|| "Explorer API response missing blockDetails".to_string())?;

    Ok(ExplorerBlockData {
        hash: details.hash,
        block_time: details.block_time,
        proposer: details.proposer,
    })
}

/// Fetch block from Explorer API with 3 retry attempts and exponential backoff
async fn fetch_block_from_explorer(height: u64) -> Result<ExplorerBlockData, String> {
    const MAX_RETRIES: u32 = 3;
    const BASE_DELAY_MS: u64 = 500; // Start with 500ms

    let mut last_error = String::new();

    for attempt in 1..=MAX_RETRIES {
        match fetch_block_from_explorer_once(height).await {
            Ok(data) => {
                if attempt > 1 {
                    info!(
                        height = height,
                        attempt = attempt,
                        "Explorer API succeeded after retry"
                    );
                }
                return Ok(data);
            }
            Err(err) => {
                last_error = err.clone();

                if attempt < MAX_RETRIES {
                    // Exponential backoff: 500ms, 1000ms, 2000ms
                    let delay_ms = BASE_DELAY_MS * (1 << (attempt - 1));
                    warn!(
                        height = height,
                        attempt = attempt,
                        max_retries = MAX_RETRIES,
                        retry_after_ms = delay_ms,
                        error = %err,
                        "Explorer API attempt failed; will retry"
                    );
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                } else {
                    warn!(
                        height = height,
                        attempts = MAX_RETRIES,
                        error = %err,
                        "Explorer API failed after all retry attempts"
                    );
                }
            }
        }
    }

    Err(last_error)
}

impl Default for BlockMerger {
    fn default() -> Self {
        Self {
            hash_store: HashStore::global(),
            stats: MergerStats::default(),
        }
    }
}

impl BlockMerger {
    /// Create a new BlockMerger wired to the global HashStore.
    pub fn new() -> Self {
        Self {
            hash_store: HashStore::global(),
            stats: MergerStats::default(),
        }
    }

    /// Create a BlockMerger using the provided HashStore (primarily for tests).
    #[cfg(test)]
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
    /// Returns None if Redis data exists but validation fails (block_time or proposer mismatch).
    pub async fn process_file_block(&self, data: ReplicaBlockData) -> Option<MergedBlock> {
        self.stats.blocks_processed.fetch_add(1, Ordering::Relaxed);

        match self.hash_store.get_block_data(data.height).await {
            Some(redis_data) => {
                let block_time_match = redis_data.block_time == data.block_time;
                let proposer_match =
                    redis_data.proposer.to_lowercase() == data.proposer.to_lowercase();

                if block_time_match && proposer_match {
                    self.stats.hash_hits.fetch_add(1, Ordering::Relaxed);
                    crate::metrics::REDIS_CACHE_TOTAL
                        .with_label_values(&["hit"])
                        .inc();
                    Some(MergedBlock::from_replica_data(data, Some(redis_data.hash)))
                } else {
                    self.stats.hash_misses.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .validation_failures
                        .fetch_add(1, Ordering::Relaxed);

                    // Record validation failure reason
                    let reason = match (!block_time_match, !proposer_match) {
                        (true, true) => "both_mismatch",
                        (true, false) => "block_time_mismatch",
                        (false, true) => "proposer_mismatch",
                        _ => "unknown",
                    };
                    crate::metrics::BLOCK_VALIDATION_FAILURES_TOTAL
                        .with_label_values(&[reason])
                        .inc();
                    crate::metrics::REDIS_CACHE_TOTAL
                        .with_label_values(&["miss"])
                        .inc();

                    warn!(
                        height = data.height,
                        expected_block_time = data.block_time,
                        actual_block_time = redis_data.block_time,
                        expected_proposer = %data.proposer,
                        actual_proposer = %redis_data.proposer,
                        "Redis block data validation failed; dropping block"
                    );
                    None
                }
            }
            None => {
                // No Redis data - try Explorer API fallback before emitting without hash
                self.stats.hash_misses.fetch_add(1, Ordering::Relaxed);
                crate::metrics::REDIS_CACHE_TOTAL
                    .with_label_values(&["miss"])
                    .inc();
                warn!(
                    height = data.height,
                    "No Redis data for block; trying Explorer API fallback (will emit without hash on failure)"
                );

                match fetch_block_from_explorer(data.height).await {
                    Ok(explorer_data) => {
                        let block_time_match = explorer_data.block_time == data.block_time;
                        let proposer_match =
                            explorer_data.proposer.to_lowercase() == data.proposer.to_lowercase();

                        if block_time_match && proposer_match {
                            crate::metrics::EXPLORER_API_FALLBACK_TOTAL
                                .with_label_values(&["success"])
                                .inc();
                            Some(MergedBlock::from_replica_data(
                                data,
                                Some(explorer_data.hash),
                            ))
                        } else {
                            self.stats
                                .validation_failures
                                .fetch_add(1, Ordering::Relaxed);

                            // Record validation failure reason
                            let reason = match (!block_time_match, !proposer_match) {
                                (true, true) => "both_mismatch",
                                (true, false) => "block_time_mismatch",
                                (false, true) => "proposer_mismatch",
                                _ => "unknown",
                            };
                            crate::metrics::BLOCK_VALIDATION_FAILURES_TOTAL
                                .with_label_values(&[reason])
                                .inc();
                            crate::metrics::EXPLORER_API_FALLBACK_TOTAL
                                .with_label_values(&["validation_failed"])
                                .inc();

                            warn!(
                                height = data.height,
                                expected_block_time = data.block_time,
                                actual_block_time = explorer_data.block_time,
                                expected_proposer = %data.proposer,
                                actual_proposer = %explorer_data.proposer,
                                "Explorer block data validation failed; dropping block"
                            );
                            None
                        }
                    }
                    Err(err) => {
                        self.stats
                            .validation_failures
                            .fetch_add(1, Ordering::Relaxed);
                        crate::metrics::BLOCK_VALIDATION_FAILURES_TOTAL
                            .with_label_values(&["no_redis_data"])
                            .inc();
                        crate::metrics::EXPLORER_API_FALLBACK_TOTAL
                            .with_label_values(&["api_error"])
                            .inc();

                        warn!(
                            height = data.height,
                            error = %err,
                            "No Redis data for block; Explorer API fallback failed; emitting without hash (validation failure counted)"
                        );
                        Some(MergedBlock::from_replica_data(data, None))
                    }
                }
            }
        }
    }

    /// Blocking helper for synchronous callers (e.g., parsers running inside Tokio tasks).
    /// Returns None if Redis data exists but validation fails.
    pub fn process_file_block_blocking(&self, data: ReplicaBlockData) -> Option<MergedBlock> {
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

        let merged = merger
            .process_file_block(block)
            .await
            .expect("block should be emitted");
        assert_eq!(merged.hash, "0xabc123");
    }

    #[tokio::test]
    async fn test_hash_miss() {
        let store = test_store().await;
        let merger = BlockMerger::with_store(store);

        // No Redis data - block should still be emitted without hash
        let merged = merger
            .process_file_block(sample_block(200))
            .await
            .expect("block should be emitted");
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

        let merged = merger
            .process_file_block_blocking(block)
            .expect("block should be emitted");
        assert_eq!(merged.hash, "0xdeadbeef");
    }

    #[tokio::test]
    async fn test_validation_failure_drops_block() {
        let store = test_store().await;
        // Intentionally set a mismatched block time.
        store.cache_insert_for_test(400, redis_data(1_800_000_000_000, "proposer", "0xfeedface"));
        let merger = BlockMerger::with_store(store);

        // Block should NOT be emitted due to validation failure
        let merged = merger.process_file_block(sample_block(400)).await;
        assert!(
            merged.is_none(),
            "block should be dropped on validation failure"
        );
        assert_eq!(merger.stats.validation_failures.load(Ordering::Relaxed), 1);
    }
}
