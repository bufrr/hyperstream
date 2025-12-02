//! Configuration management for the hl-agent.
//!
//! Loads and validates configuration from TOML files.

use anyhow::Result;
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

// Default values as constants
const DEFAULT_POLL_INTERVAL_MS: u64 = 100;
const DEFAULT_BATCH_SIZE: usize = 100;
const DEFAULT_SKIP_HISTORICAL: bool = true;
const DEFAULT_TAIL_BYTES: u64 = 0;
const DEFAULT_INACTIVE_TIMEOUT_SEC: u64 = 300; // 5 minutes
const DEFAULT_MAX_CONCURRENT_TAILERS: usize = 64;
const DEFAULT_BULK_LOAD_WARN_BYTES: u64 = 500 * 1024 * 1024; // 500 MiB
const DEFAULT_BULK_LOAD_ABORT_BYTES: u64 = 1024 * 1024 * 1024; // 1 GiB
const DEFAULT_REDIS_URL: &str = "redis://127.0.0.1:6379";
const DEFAULT_SINK_RETRY_MAX_ATTEMPTS: usize = 5;
const DEFAULT_SINK_RETRY_BASE_DELAY_MS: u64 = 500;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    #[serde(default)]
    pub node: NodeConfig,
    #[serde(default)]
    pub watcher: WatcherConfig,
    pub sorter: SorterConfig,
    #[serde(default)]
    pub checkpoint: CheckpointConfig,
    #[serde(default)]
    pub performance: PerformanceConfig,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct NodeConfig {
    #[serde(default)]
    pub node_id: String,
    #[serde(default)]
    pub data_dir: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct WatcherConfig {
    #[serde(default)]
    pub watch_paths: Vec<String>,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// Skip historical data when starting (DEFAULT: true)
    #[serde(default = "default_skip_historical")]
    pub skip_historical: bool,
    /// When skip_historical=true, read the last N bytes of each existing file (DEFAULT: 0)
    #[serde(default = "default_tail_bytes")]
    pub tail_bytes: u64,
    /// Timeout in seconds after which inactive files (no growth) will have their tailers stopped (DEFAULT: 300)
    #[serde(default = "default_inactive_timeout_sec")]
    pub inactive_timeout_sec: u64,
}

impl Default for WatcherConfig {
    fn default() -> Self {
        Self {
            watch_paths: Vec::new(),
            poll_interval_ms: DEFAULT_POLL_INTERVAL_MS,
            skip_historical: DEFAULT_SKIP_HISTORICAL,
            tail_bytes: DEFAULT_TAIL_BYTES,
            inactive_timeout_sec: default_inactive_timeout_sec(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct SorterConfig {
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub output_dir: Option<String>,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CheckpointConfig {
    #[serde(default)]
    pub db_path: String,
    #[serde(default = "default_redis_url")]
    pub redis_url: String,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            db_path: String::new(),
            redis_url: default_redis_url(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct PerformanceConfig {
    #[serde(default = "default_max_concurrent_tailers")]
    pub max_concurrent_tailers: usize,
    #[serde(default = "default_bulk_load_warn_bytes")]
    pub bulk_load_warn_bytes: u64,
    #[serde(default = "default_bulk_load_abort_bytes")]
    pub bulk_load_abort_bytes: u64,
    #[serde(default = "default_sink_retry_max_attempts")]
    pub sink_retry_max_attempts: usize,
    #[serde(default = "default_sink_retry_base_delay_ms")]
    pub sink_retry_base_delay_ms: u64,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            max_concurrent_tailers: DEFAULT_MAX_CONCURRENT_TAILERS,
            bulk_load_warn_bytes: DEFAULT_BULK_LOAD_WARN_BYTES,
            bulk_load_abort_bytes: DEFAULT_BULK_LOAD_ABORT_BYTES,
            sink_retry_max_attempts: DEFAULT_SINK_RETRY_MAX_ATTEMPTS,
            sink_retry_base_delay_ms: DEFAULT_SINK_RETRY_BASE_DELAY_MS,
        }
    }
}

// Serde default functions (must be regular fn, not const fn)
fn default_poll_interval_ms() -> u64 {
    DEFAULT_POLL_INTERVAL_MS
}
fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}
fn default_skip_historical() -> bool {
    DEFAULT_SKIP_HISTORICAL
}
fn default_tail_bytes() -> u64 {
    DEFAULT_TAIL_BYTES
}
fn default_inactive_timeout_sec() -> u64 {
    DEFAULT_INACTIVE_TIMEOUT_SEC
}
fn default_max_concurrent_tailers() -> usize {
    DEFAULT_MAX_CONCURRENT_TAILERS
}
fn default_bulk_load_warn_bytes() -> u64 {
    DEFAULT_BULK_LOAD_WARN_BYTES
}
fn default_bulk_load_abort_bytes() -> u64 {
    DEFAULT_BULK_LOAD_ABORT_BYTES
}
fn default_sink_retry_max_attempts() -> usize {
    DEFAULT_SINK_RETRY_MAX_ATTEMPTS
}
fn default_sink_retry_base_delay_ms() -> u64 {
    DEFAULT_SINK_RETRY_BASE_DELAY_MS
}
fn default_redis_url() -> String {
    DEFAULT_REDIS_URL.to_string()
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let raw = fs::read_to_string(path.as_ref())?;
        let config: Config = toml::from_str(&raw)?;

        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        let config = self;

        let has_endpoint = config
            .sorter
            .endpoint
            .as_ref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false);
        let has_output_dir = config
            .sorter
            .output_dir
            .as_ref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false);

        match (has_endpoint, has_output_dir) {
            (true, false) | (false, true) => {}
            (true, true) => {
                anyhow::bail!("sorter configuration must set only one of endpoint or output_dir")
            }
            (false, false) => {
                anyhow::bail!("sorter configuration requires either endpoint or output_dir")
            }
        }

        if config.node.node_id.trim().is_empty() {
            anyhow::bail!("node.node_id cannot be empty");
        }

        if config.node.data_dir.trim().is_empty() {
            anyhow::bail!("node.data_dir cannot be empty");
        }
        if config.watcher.watch_paths.is_empty() {
            anyhow::bail!("watch_paths cannot be empty");
        }
        if config.checkpoint.db_path.trim().is_empty() {
            anyhow::bail!("checkpoint.db_path must be set");
        }
        if config.performance.bulk_load_warn_bytes >= config.performance.bulk_load_abort_bytes {
            anyhow::bail!(
                "performance.bulk_load_warn_bytes ({}) must be less than performance.bulk_load_abort_bytes ({})",
                config.performance.bulk_load_warn_bytes,
                config.performance.bulk_load_abort_bytes
            );
        }

        Ok(())
    }

    pub fn expand_data_dir(&self) -> PathBuf {
        shellexpand::tilde(&self.node.data_dir).into_owned().into()
    }

    pub fn watch_paths(&self) -> Vec<PathBuf> {
        let data_dir = self.expand_data_dir();
        self.watcher
            .watch_paths
            .iter()
            .map(|p| {
                let candidate = PathBuf::from(p);
                if candidate.is_absolute() {
                    candidate
                } else {
                    data_dir.join(candidate)
                }
            })
            .collect()
    }

    pub fn checkpoint_db_path(&self) -> PathBuf {
        let expanded = shellexpand::tilde(&self.checkpoint.db_path);
        PathBuf::from(expanded.as_ref())
    }

    pub fn redis_url(&self) -> String {
        self.checkpoint.redis_url.clone()
    }

    pub fn skip_historical(&self) -> bool {
        self.watcher.skip_historical
    }

    pub fn tail_bytes(&self) -> u64 {
        self.watcher.tail_bytes
    }
}

impl SorterConfig {
    pub fn output_dir_path(&self) -> Option<PathBuf> {
        self.output_dir.as_ref().map(|dir| {
            let expanded = shellexpand::tilde(dir);
            PathBuf::from(expanded.as_ref())
        })
    }
}
