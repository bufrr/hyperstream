use anyhow::{anyhow, Context, Result};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::Deserialize;
use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value};
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::{sleep, Duration};
use tracing::{debug, warn};

static GLOBAL_HASH_STORE: OnceLock<Arc<HashStore>> = OnceLock::new();

#[derive(Clone, Debug)]
pub struct RedisBlockData {
    pub block_time: u64,
    pub proposer: String,
    pub hash: String,
}

pub enum BlockLookupResult {
    /// Served from Redis.
    RedisHit(RedisBlockData),
    /// Served from Explorer API fallback.
    ExplorerHit(RedisBlockData),
    /// Both Redis and Explorer failed.
    Miss,
}

impl BlockLookupResult {
    pub fn metric_label(&self) -> &'static str {
        match self {
            BlockLookupResult::RedisHit(_) => "lookup.redis_hit",
            BlockLookupResult::ExplorerHit(_) => "lookup.explorer_hit",
            BlockLookupResult::Miss => "lookup.miss",
        }
    }
}

#[derive(serde::Serialize)]
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

pub struct HashStore {
    /// Redis connection used for lookups
    redis_conn: AsyncMutex<ConnectionManager>,
    /// HTTP client for Explorer API fallback
    explorer_client: reqwest::Client,
}

impl HashStore {
    pub async fn new(redis_url: &str) -> Result<Self> {
        let redis_conn = Self::create_connection_manager(redis_url).await?;
        let explorer_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .context("failed to create Explorer API client")?;

        Ok(Self {
            redis_conn: AsyncMutex::new(redis_conn),
            explorer_client,
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

    pub async fn init(redis_url: &str) -> Result<Arc<HashStore>> {
        let store = Arc::new(Self::new(redis_url).await?);
        GLOBAL_HASH_STORE
            .set(store.clone())
            .map_err(|_| anyhow!("hash store already initialized"))?;
        Ok(store)
    }

    /// Retrieve block data from Redis, falling back to Explorer API if needed.
    pub async fn get_block_data(&self, height: u64) -> BlockLookupResult {
        // Try Redis first
        if let Some(data) = self.fetch_from_redis(height).await {
            return BlockLookupResult::RedisHit(data);
        }

        // Fallback to Explorer API
        if let Some(data) = self.fetch_from_explorer(height).await {
            // Store in Redis for future lookups
            if let Err(err) = self.set_block_data(height, &data).await {
                warn!(height, %err, "Failed to cache Explorer data in Redis");
            }
            return BlockLookupResult::ExplorerHit(data);
        }

        BlockLookupResult::Miss
    }

    /// Store block data in Redis (called when Explorer API provides new data).
    pub async fn set_block_data(&self, height: u64, data: &RedisBlockData) -> Result<()> {
        let key = format!("block:{height}");
        let payload = sonic_rs::to_string(&serde_json::json!({
            "hash": data.hash,
            "blockTime": data.block_time,
            "proposer": data.proposer,
        }))
        .context("failed to serialize block data")?;

        let mut conn = self.redis_conn.lock().await;
        conn.set::<_, _, ()>(&key, payload)
            .await
            .with_context(|| format!("failed to write block data to Redis for height {height}"))?;

        Ok(())
    }

    pub fn global() -> Arc<HashStore> {
        GLOBAL_HASH_STORE
            .get()
            .expect("hash store not initialized")
            .clone()
    }

    async fn fetch_from_redis(&self, height: u64) -> Option<RedisBlockData> {
        let key = format!("block:{height}");
        let mut conn = self.redis_conn.lock().await;
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

    /// Fetch block details from Explorer API with exponential backoff retry.
    async fn fetch_from_explorer(&self, height: u64) -> Option<RedisBlockData> {
        for (attempt, delay_ms) in EXPLORER_RETRY_DELAYS_MS.iter().enumerate() {
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

            let parsed = match response {
                Ok(resp) if resp.status().is_success() => {
                    match resp.json::<ExplorerBlockResponse>().await {
                        Ok(explorer_data) => Some(explorer_data),
                        Err(err) => {
                            warn!(height, attempt = attempt + 1, %err, "Failed to parse Explorer API response");
                            None
                        }
                    }
                }
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
                if let (Some(hash), Some(block_time), Some(proposer)) = (
                    explorer_data.hash,
                    explorer_data.block_time,
                    explorer_data.proposer,
                ) {
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

        warn!(
            height,
            attempts = EXPLORER_RETRY_DELAYS_MS.len(),
            "Explorer API fallback failed after all retries"
        );
        None
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
