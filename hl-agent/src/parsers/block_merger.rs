use super::hash_store::{BlockLookupResult, HashStore, RedisBlockData};
use super::schemas::Block;
use serde::Serialize;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Instant;
use tokio::runtime::{Builder, Handle};
use tokio::task;
use tracing::warn;

pub type MergedBlock = Block;

impl MergedBlock {
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

pub struct BlockMerger {
    hash_store: Arc<HashStore>,
}

static GLOBAL_BLOCK_MERGER: OnceLock<Arc<BlockMerger>> = OnceLock::new();

impl Default for BlockMerger {
    fn default() -> Self {
        Self {
            hash_store: HashStore::global(),
        }
    }
}

impl BlockMerger {
    pub fn new() -> Self {
        Self::default()
    }

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

    fn observe_duration(label: &str, duration: std::time::Duration) {
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

    pub async fn process_file_block(&self, data: ReplicaBlockData) -> Option<MergedBlock> {
        let total_start = Instant::now();
        let lookup_start = Instant::now();
        let lookup = self.hash_store.get_block_data(data.height).await;
        let lookup_elapsed = lookup_start.elapsed();

        Self::observe_duration(lookup.metric_label(), lookup_elapsed);

        match lookup {
            BlockLookupResult::RedisHit(redis_data) | BlockLookupResult::ExplorerHit(redis_data) => {
                self.handle_lookup_hit(data, redis_data, total_start)
            }
            BlockLookupResult::Miss => self.handle_lookup_miss(data, total_start),
        }
    }

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

    fn handle_lookup_hit(
        &self,
        data: ReplicaBlockData,
        redis_data: RedisBlockData,
        total_start: Instant,
    ) -> Option<MergedBlock> {
        Self::inc_redis_cache("hit");

        // Validate data matches
        if let Some(reason) =
            Self::validation_reason(&data, redis_data.block_time, &redis_data.proposer)
        {
            warn!(
                height = data.height,
                expected_block_time = data.block_time,
                actual_block_time = redis_data.block_time,
                expected_proposer = %data.proposer,
                actual_proposer = %redis_data.proposer,
                "Data validation failed; emitting without hash"
            );

            Self::inc_validation_failure(reason);

            crate::metrics::BLOCKS_WITHOUT_HASH_TOTAL
                .with_label_values(&["validation_failed"])
                .inc();
            Self::observe_duration("total.emitted", total_start.elapsed());

            return Some(MergedBlock::from_replica_data(data, None));
        }

        // Validation passed
        Self::observe_duration("total.emitted", total_start.elapsed());
        Some(MergedBlock::from_replica_data(data, Some(redis_data.hash)))
    }

    fn handle_lookup_miss(&self, data: ReplicaBlockData, total_start: Instant) -> Option<MergedBlock> {
        Self::inc_redis_cache("miss");

        crate::metrics::BLOCKS_WITHOUT_HASH_TOTAL
            .with_label_values(&["lookup_miss"])
            .inc();
        Self::observe_duration("total.emitted", total_start.elapsed());
        Some(MergedBlock::from_replica_data(data, None))
    }
}
