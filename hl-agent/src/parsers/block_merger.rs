//! Block merger for combining replica_cmds file data with hashes stored in HashStore.

use super::hash_store::{BlockLookupResult, HashStore, RedisBlockData};
use super::schemas::Block;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use tokio::runtime::{Builder, Handle};
use tokio::task;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

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

#[derive(Serialize)]
struct ExplorerBlockRequest {
    #[serde(rename = "type")]
    request_type: String,
    #[serde(rename = "blockNumber")]
    block_number: u64,
}

#[derive(Deserialize, Debug)]
struct ExplorerBlockResponse {
    hash: Option<String>,
    #[serde(rename = "blockTime")]
    block_time: Option<u64>,
    proposer: Option<String>,
}

const EXPLORER_API_URL: &str = "https://api.hyperliquid.xyz/info";
const EXPLORER_REQUEST_TYPE: &str = "blockDetails";
const EXPLORER_RETRY_DELAYS_MS: [u64; 3] = [500, 1000, 2000];

#[derive(Default)]
struct MergerStats {
    hash_hits: AtomicU64,
    hash_misses: AtomicU64,
    blocks_processed: AtomicU64,
    validation_failures: AtomicU64,
}

pub struct BlockMerger {
    hash_store: Arc<HashStore>,
    stats: MergerStats,
    explorer_client: reqwest::Client,
}

static GLOBAL_BLOCK_MERGER: OnceLock<Arc<BlockMerger>> = OnceLock::new();

fn build_explorer_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("failed to create reqwest client")
}

impl Default for BlockMerger {
    fn default() -> Self {
        Self {
            hash_store: HashStore::global(),
            stats: MergerStats::default(),
            explorer_client: build_explorer_client(),
        }
    }
}

impl BlockMerger {
    /// Create a new BlockMerger wired to the global HashStore.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a BlockMerger using the provided HashStore (primarily for tests).
    #[cfg(test)]
    pub fn with_store(hash_store: Arc<HashStore>) -> Self {
        Self {
            hash_store,
            stats: MergerStats::default(),
            explorer_client: build_explorer_client(),
        }
    }

    /// Get or initialize the global block merger.
    pub fn global() -> Arc<BlockMerger> {
        GLOBAL_BLOCK_MERGER
            .get_or_init(|| Arc::new(BlockMerger::new()))
            .clone()
    }

    fn validation_reason(
        data: &ReplicaBlockData,
        block_time: u64,
        proposer: &str,
    ) -> Option<&'static str> {
        let block_time_match = block_time == data.block_time;
        let proposer_match = proposer.eq_ignore_ascii_case(&data.proposer);

        match (block_time_match, proposer_match) {
            (true, true) => None,
            (false, true) => Some("block_time_mismatch"),
            (true, false) => Some("proposer_mismatch"),
            (false, false) => Some("both_mismatch"),
        }
    }

    async fn store_explorer_data(&self, height: u64, data: &RedisBlockData) {
        if let Err(err) = self.hash_store.set_block_data(height, data).await {
            warn!(height, %err, "Failed to store Explorer data in Redis");
        }
    }

    fn observe_duration(label: &str, duration: Duration) {
        crate::metrics::BLOCK_MERGER_DURATION
            .with_label_values(&[label])
            .observe(duration.as_secs_f64());
    }

    fn inc_redis_cache(label: &str) {
        crate::metrics::REDIS_CACHE_TOTAL
            .with_label_values(&[label])
            .inc();
    }

    fn inc_validation_failure(reason: &str) {
        crate::metrics::BLOCK_VALIDATION_FAILURES_TOTAL
            .with_label_values(&[reason])
            .inc();
    }

    fn inc_explorer_fallback(result: &str) {
        crate::metrics::EXPLORER_API_FALLBACK_TOTAL
            .with_label_values(&[result])
            .inc();
    }

    /// Fetch block details from Explorer API with exponential backoff retry.
    async fn fetch_from_explorer(&self, height: u64) -> Option<RedisBlockData> {
        let mut last_duration = Duration::from_secs(0);

        for (attempt, delay_ms) in EXPLORER_RETRY_DELAYS_MS.iter().enumerate() {
            let attempt_start = Instant::now();
            let request_body = ExplorerBlockRequest {
                request_type: EXPLORER_REQUEST_TYPE.to_string(),
                block_number: height,
            };

            let response = self
                .explorer_client
                .post(EXPLORER_API_URL)
                .json(&request_body)
                .send()
                .await;

            last_duration = attempt_start.elapsed();
            let metric_label = format!("lookup.explorer_attempt_{}", attempt + 1);
            Self::observe_duration(&metric_label, last_duration);

            let parsed = match response {
                Ok(resp) if resp.status().is_success() => match resp.json::<ExplorerBlockResponse>().await {
                    Ok(explorer_data) => Some(explorer_data),
                    Err(err) => {
                        warn!(height, attempt = attempt + 1, %err, "Failed to parse Explorer API response");
                        None
                    }
                },
                Ok(resp) => {
                    warn!(
                        height,
                        attempt = attempt + 1,
                        status = %resp.status(),
                        "Explorer API returned error status"
                    );
                    None
                }
                Err(err) => {
                    warn!(height, attempt = attempt + 1, %err, "Explorer API request failed");
                    None
                }
            };

            if let Some(explorer_data) = parsed {
                if let (Some(hash), Some(block_time), Some(proposer)) =
                    (explorer_data.hash, explorer_data.block_time, explorer_data.proposer)
                {
                    Self::observe_duration("lookup.explorer_success", last_duration);
                    Self::inc_explorer_fallback("success");

                    return Some(RedisBlockData {
                        block_time,
                        proposer,
                        hash,
                    });
                }

                warn!(
                    height,
                    attempt = attempt + 1,
                    "Explorer API response missing required fields"
                );
            }

            if attempt + 1 < EXPLORER_RETRY_DELAYS_MS.len() {
                sleep(Duration::from_millis(*delay_ms)).await;
            }
        }

        Self::observe_duration("lookup.explorer_failed", last_duration);
        Self::inc_explorer_fallback("failed");

        warn!(
            height,
            attempts = EXPLORER_RETRY_DELAYS_MS.len(),
            "Explorer API fallback failed after all retries"
        );
        None
    }

    /// Process a block from the file parser asynchronously, fetching the hash if available.
    /// Returns None if Redis data exists but validation fails (block_time or proposer mismatch).
    pub async fn process_file_block(&self, data: ReplicaBlockData) -> Option<MergedBlock> {
        self.stats.blocks_processed.fetch_add(1, Ordering::Relaxed);

        let total_start = Instant::now();
        let lookup_start = Instant::now();
        let lookup = self.hash_store.get_block_data(data.height).await;
        let lookup_elapsed = lookup_start.elapsed();

        Self::observe_duration(lookup.metric_label(), lookup_elapsed);

        match lookup {
            BlockLookupResult::CacheHit(redis_data) | BlockLookupResult::RedisHit(redis_data) => {
                self.handle_lookup_hit(data, redis_data, total_start)
            }
            BlockLookupResult::CacheOnlyMiss | BlockLookupResult::RedisMiss => {
                self.handle_lookup_miss(data, total_start).await
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

    fn handle_lookup_hit(
        &self,
        data: ReplicaBlockData,
        redis_data: RedisBlockData,
        total_start: Instant,
    ) -> Option<MergedBlock> {
        if let Some(reason) =
            Self::validation_reason(&data, redis_data.block_time, &redis_data.proposer)
        {
            self.stats.hash_misses.fetch_add(1, Ordering::Relaxed);
            self.stats
                .validation_failures
                .fetch_add(1, Ordering::Relaxed);
            Self::inc_validation_failure(reason);
            Self::inc_redis_cache("miss");
            Self::observe_duration("total.dropped", total_start.elapsed());

            warn!(
                height = data.height,
                expected_block_time = data.block_time,
                actual_block_time = redis_data.block_time,
                expected_proposer = %data.proposer,
                actual_proposer = %redis_data.proposer,
                "Redis block data validation failed; dropping block"
            );
            return None;
        }

        self.stats.hash_hits.fetch_add(1, Ordering::Relaxed);
        Self::inc_redis_cache("hit");
        Self::observe_duration("total.emitted", total_start.elapsed());
        Some(MergedBlock::from_replica_data(data, Some(redis_data.hash)))
    }

    async fn handle_lookup_miss(
        &self,
        data: ReplicaBlockData,
        total_start: Instant,
    ) -> Option<MergedBlock> {
        self.stats.hash_misses.fetch_add(1, Ordering::Relaxed);
        Self::inc_redis_cache("miss");

        if let Some(explorer_data) = self.fetch_from_explorer(data.height).await {
            if let Some(reason) =
                Self::validation_reason(&data, explorer_data.block_time, &explorer_data.proposer)
            {
                warn!(
                    height = data.height,
                    expected_block_time = data.block_time,
                    actual_block_time = explorer_data.block_time,
                    expected_proposer = %data.proposer,
                    actual_proposer = %explorer_data.proposer,
                    "Explorer API data validation failed; emitting without hash"
                );

                Self::inc_validation_failure(reason);
            } else {
                self.store_explorer_data(data.height, &explorer_data).await;
                Self::observe_duration("total.emitted", total_start.elapsed());
                return Some(MergedBlock::from_replica_data(
                    data,
                    Some(explorer_data.hash),
                ));
            }
        }

        crate::metrics::BLOCKS_WITHOUT_HASH_TOTAL
            .with_label_values(&["explorer_failed"])
            .inc();
        Self::observe_duration("total.emitted", total_start.elapsed());
        Some(MergedBlock::from_replica_data(data, None))
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
    async fn merges_blocks_across_cache_paths() {
        let store = test_store().await;
        let merger = BlockMerger::with_store(store.clone());

        let block = sample_block(100);
        store.cache_insert_for_test(100, redis_data(block.block_time, "PROPOSER", "0xabc123"));

        let hit = merger
            .process_file_block(block)
            .await
            .expect("block should be emitted");
        assert_eq!(hit.hash, "0xabc123");

        let miss = merger
            .process_file_block(sample_block(200))
            .await
            .expect("block should be emitted");
        assert_eq!(miss.hash, "");

        store.cache_insert_for_test(
            400,
            redis_data(1_800_000_000_000, "proposer", "0xfeedface"),
        );

        assert!(
            merger.process_file_block(sample_block(400)).await.is_none(),
            "block should be dropped on validation failure"
        );
        assert_eq!(merger.stats.validation_failures.load(Ordering::Relaxed), 1);
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

}
