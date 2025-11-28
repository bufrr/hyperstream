//! Hyperliquid Agent - Data streaming agent for Hyperliquid blockchain.
//!
//! This agent reads blockchain data from local files,
//! parses it into standardized formats, and forwards to downstream systems.

mod checkpoint;
mod config;
mod output_writer;
mod parsers;
mod runner;
mod sorter_client;
mod tailer;
mod watcher;

use anyhow::{Context, Result};
use config::Config;
use parsers::hash_store::{HashStore, DEFAULT_HASH_STORE_CACHE_SIZE};

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let config_path =
        std::env::var("HL_AGENT_CONFIG").unwrap_or_else(|_| "config.toml".to_string());
    let config = Config::load(&config_path)
        .with_context(|| format!("failed to load config from {config_path}"))?;

    init_hash_store(&config).await?;

    runner::file_mode::run(&config).await
}

async fn init_hash_store(config: &Config) -> Result<()> {
    let redis_url = config.redis_url();

    HashStore::init(&redis_url, DEFAULT_HASH_STORE_CACHE_SIZE, false)
        .await
        .context("failed to initialize hash store")?;

    Ok(())
}

fn init_tracing() {
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_target(false)
        .finish();

    let _ = tracing::subscriber::set_global_default(subscriber);
}
