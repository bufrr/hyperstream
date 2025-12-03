use anyhow::{anyhow, Context, Result};
use lru::LruCache;
use parking_lot::RwLock;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, info, warn};

/// Default in-memory cache size (~20k hashes)
pub const DEFAULT_HASH_STORE_CACHE_SIZE: usize = 20_000;

static GLOBAL_HASH_STORE: OnceLock<Arc<HashStore>> = OnceLock::new();

#[derive(Default)]
pub struct HashStoreStats {
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    redis_hits: AtomicU64,
    redis_misses: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct RedisBlockData {
    pub block_time: u64,
    pub proposer: String,
    pub hash: String,
}

/// Outcome of a block data lookup.
pub enum BlockLookupResult {
    /// Served from the in-memory cache.
    CacheHit(RedisBlockData),
    /// Served from Redis and cached.
    RedisHit(RedisBlockData),
    /// Cache miss with no Redis access (cache-only mode).
    CacheOnlyMiss,
    /// Cache miss and Redis did not have usable data.
    RedisMiss,
}

impl BlockLookupResult {
    pub fn metric_label(&self) -> &'static str {
        match self {
            BlockLookupResult::CacheHit(_) => "lookup.cache_hit",
            BlockLookupResult::RedisHit(_) => "lookup.redis_hit",
            BlockLookupResult::CacheOnlyMiss => "lookup.cache_only_miss",
            BlockLookupResult::RedisMiss => "lookup.redis_miss",
        }
    }
}

pub struct HashStore {
    /// LRU cache for hot hashes
    cache: RwLock<LruCache<u64, RedisBlockData>>,
    /// Redis connection used for lookups
    redis_conn: Option<AsyncMutex<ConnectionManager>>,
    /// Whether to skip Redis lookups entirely (used when no data source exists)
    cache_only_mode: bool,
    /// Metrics
    stats: HashStoreStats,
}

impl HashStore {
    pub async fn new(redis_url: &str, cache_size: usize, cache_only_mode: bool) -> Result<Self> {
        let cache_size = cache_size.max(1);
        let cache = LruCache::new(
            NonZeroUsize::new(cache_size).expect("nonzero cache size required for LruCache"),
        );

        let redis_conn = if cache_only_mode {
            None
        } else {
            Some(AsyncMutex::new(
                Self::create_connection_manager(redis_url).await?,
            ))
        };

        Ok(Self {
            cache: RwLock::new(cache),
            redis_conn,
            cache_only_mode,
            stats: HashStoreStats::default(),
        })
    }

    async fn create_connection_manager(redis_url: &str) -> Result<ConnectionManager> {
        let client = redis::Client::open(redis_url)
            .with_context(|| format!("failed to create Redis client for {redis_url}"))?;

        client
            .get_connection_manager()
            .await
            .context("failed to establish Redis connection")
    }

    pub async fn init(
        redis_url: &str,
        cache_size: usize,
        cache_only_mode: bool,
    ) -> Result<Arc<HashStore>> {
        let store = Arc::new(Self::new(redis_url, cache_size, cache_only_mode).await?);
        GLOBAL_HASH_STORE
            .set(store.clone())
            .map_err(|_| anyhow!("hash store already initialized"))?;
        Ok(store)
    }

    /// Retrieve block data from cache or Redis.
    ///
    /// When cache_only_mode is true, only the LRU cache is checked.
    pub async fn get_block_data(&self, height: u64) -> BlockLookupResult {
        if let Some(data) = self.get_from_cache(height) {
            self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            return BlockLookupResult::CacheHit(data);
        }
        self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);

        if self.cache_only_mode {
            return BlockLookupResult::CacheOnlyMiss;
        }

        match self.fetch_from_redis(height).await {
            Some(data) => {
                self.stats.redis_hits.fetch_add(1, Ordering::Relaxed);
                let mut cache = self.cache.write();
                cache.put(height, data.clone());
                BlockLookupResult::RedisHit(data)
            }
            None => {
                self.stats.redis_misses.fetch_add(1, Ordering::Relaxed);
                BlockLookupResult::RedisMiss
            }
        }
    }

    pub fn global() -> Arc<HashStore> {
        GLOBAL_HASH_STORE
            .get()
            .expect("hash store not initialized")
            .clone()
    }

    /// Log current counters for observability.
    pub fn log_stats(&self) {
        info!(
            cache_hits = self.stats.cache_hits.load(Ordering::Relaxed),
            cache_misses = self.stats.cache_misses.load(Ordering::Relaxed),
            redis_hits = self.stats.redis_hits.load(Ordering::Relaxed),
            redis_misses = self.stats.redis_misses.load(Ordering::Relaxed),
            cache_entries = self.cache_len(),
            "HashStore stats"
        );
    }

    /// Public helper for monitoring the in-memory cache.
    pub fn cache_len(&self) -> usize {
        self.cache.read().len()
    }

    fn get_from_cache(&self, height: u64) -> Option<RedisBlockData> {
        let mut cache = self.cache.write();
        cache.get(&height).cloned()
    }

    async fn fetch_from_redis(&self, height: u64) -> Option<RedisBlockData> {
        let conn_mutex = match self.redis_conn.as_ref() {
            Some(conn) => conn,
            None => return None,
        };

        let key = format!("block:{height}");
        let mut conn = conn_mutex.lock().await;
        let result: redis::RedisResult<Option<String>> = conn.get(&key).await;

        match result {
            Ok(Some(payload)) => match sonic_rs::from_str::<Value>(&payload) {
                Ok(value) => match parse_redis_block_data(&value) {
                    Ok(data) => Some(data),
                    Err(RedisBlockParseError::MissingHash) => {
                        warn!(height, key = %key, "Redis block payload missing hash field");
                        None
                    }
                    Err(RedisBlockParseError::MissingBlockTime) => {
                        warn!(
                            height,
                            key = %key,
                            "Redis block payload missing blockTime field required for validation"
                        );
                        None
                    }
                    Err(RedisBlockParseError::MissingProposer) => {
                        warn!(
                            height,
                            key = %key,
                            "Redis block payload missing proposer field required for validation"
                        );
                        None
                    }
                    Err(RedisBlockParseError::NotJsonObject) => {
                        warn!(
                            height,
                            key = %key,
                            "Redis block payload must be a JSON object to validate data"
                        );
                        None
                    }
                },
                Err(err) => {
                    warn!(height, %err, "failed to parse Redis block payload");
                    None
                }
            },
            Ok(None) => {
                debug!(height, key = %key, "block hash missing in Redis");
                None
            }
            Err(err) => {
                warn!(height, key = %key, %err, "Redis block lookup failed");
                None
            }
        }
    }

    #[cfg(test)]
    pub fn cache_insert_for_test(&self, height: u64, data: RedisBlockData) {
        let mut cache = self.cache.write();
        cache.put(height, data);
    }
}

fn extract_hash(value: &Value) -> Option<String> {
    if let Some(s) = value.as_str() {
        return Some(s.to_string());
    }

    if let Some(map) = value.as_object() {
        if let Some(hash) = map
            .get(&"hash".to_string())
            .and_then(|h| h.as_str())
            .map(|s| s.to_string())
        {
            return Some(hash);
        }
        for (_, nested) in map.iter() {
            if let Some(hash) = extract_hash(nested) {
                return Some(hash);
            }
        }
    }

    if let Some(items) = value.as_array() {
        for item in items.iter() {
            if let Some(hash) = extract_hash(item) {
                return Some(hash);
            }
        }
    }

    None
}

enum RedisBlockParseError {
    MissingHash,
    MissingBlockTime,
    MissingProposer,
    NotJsonObject,
}

fn parse_redis_block_data(value: &Value) -> Result<RedisBlockData, RedisBlockParseError> {
    let hash = extract_hash(value).ok_or(RedisBlockParseError::MissingHash)?;
    let map = value
        .as_object()
        .ok_or(RedisBlockParseError::NotJsonObject)?;

    let block_time = map
        .get(&"blockTime".to_string())
        .and_then(|v| v.as_u64())
        .or_else(|| map.get(&"block_time".to_string()).and_then(|v| v.as_u64()))
        .ok_or(RedisBlockParseError::MissingBlockTime)?;

    let proposer = map
        .get(&"proposer".to_string())
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or(RedisBlockParseError::MissingProposer)?;

    Ok(RedisBlockData {
        block_time,
        proposer,
        hash,
    })
}
