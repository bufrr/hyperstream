use anyhow::Result;
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub node: NodeConfig,
    pub watcher: WatcherConfig,
    pub sorter: SorterConfig,
    pub checkpoint: CheckpointConfig,
    #[serde(default)]
    pub performance: PerformanceConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct NodeConfig {
    pub node_id: String,
    pub data_dir: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct WatcherConfig {
    pub watch_paths: Vec<String>,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// Skip historical data and only process new data after startup (DEFAULT: true)
    /// - true: Start from end of existing files (prevents OOM when many historical files exist)
    /// - false: Process all data from the beginning (use for backfilling)
    #[serde(default = "default_skip_historical")]
    pub skip_historical: bool,
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
    pub db_path: String,
    #[allow(dead_code)]
    #[serde(default = "default_update_interval_records")]
    pub update_interval_records: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PerformanceConfig {
    /// Maximum number of concurrent file tailer tasks (default: 64)
    #[serde(default = "default_max_concurrent_tailers")]
    pub max_concurrent_tailers: usize,
    /// Warn when bulk-loading files exceed this size (default: 500 MiB)
    #[serde(default = "default_bulk_load_warn_bytes")]
    pub bulk_load_warn_bytes: u64,
    /// Abort bulk-loading when files exceed this size (default: 1 GiB)
    #[serde(default = "default_bulk_load_abort_bytes")]
    pub bulk_load_abort_bytes: u64,
}

const DEFAULT_POLL_INTERVAL_MS: u64 = 100;
const DEFAULT_BATCH_SIZE: usize = 100;
const DEFAULT_UPDATE_INTERVAL_RECORDS: u64 = 1_000;
const DEFAULT_SKIP_HISTORICAL: bool = true;
const DEFAULT_MAX_CONCURRENT_TAILERS: usize = 64;
const DEFAULT_BULK_LOAD_WARN_BYTES: u64 = 500 * 1024 * 1024; // 500 MiB
const DEFAULT_BULK_LOAD_ABORT_BYTES: u64 = 1024 * 1024 * 1024; // 1 GiB

fn default_poll_interval_ms() -> u64 {
    DEFAULT_POLL_INTERVAL_MS
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_update_interval_records() -> u64 {
    DEFAULT_UPDATE_INTERVAL_RECORDS
}

fn default_skip_historical() -> bool {
    DEFAULT_SKIP_HISTORICAL
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

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            max_concurrent_tailers: DEFAULT_MAX_CONCURRENT_TAILERS,
            bulk_load_warn_bytes: DEFAULT_BULK_LOAD_WARN_BYTES,
            bulk_load_abort_bytes: DEFAULT_BULK_LOAD_ABORT_BYTES,
        }
    }
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let raw = fs::read_to_string(path.as_ref())?;
        let config: Config = toml::from_str(&raw)?;
        if config.watcher.watch_paths.is_empty() {
            anyhow::bail!("watch_paths cannot be empty");
        }
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
        Ok(config)
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

    pub fn skip_historical(&self) -> bool {
        self.watcher.skip_historical
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
