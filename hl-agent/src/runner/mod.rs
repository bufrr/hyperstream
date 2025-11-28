//! Application runner modules for the hl-agent.
//!
//! This module contains the main execution logic for the hl-agent
//! when operating in file mode.

pub mod file_mode;

use crate::config::Config;
use crate::output_writer::{FileWriter, RecordSink};
use crate::sorter_client::SorterClient;
use anyhow::{Context, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// Environment variable to force immediate batch flushing (testing mode).
pub const FORCE_FLUSH_ENV: &str = "HL_AGENT_FORCE_FLUSH";

/// Build the appropriate record sink based on configuration.
///
/// Returns either a SorterClient (for gRPC streaming) or FileWriter (for local output).
pub async fn build_record_sink(
    config: &Config,
    cancel_token: CancellationToken,
) -> Result<Arc<dyn RecordSink>> {
    if let Some(endpoint) = config
        .sorter
        .endpoint
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        let mut sorter_client =
            SorterClient::connect(endpoint.clone(), config.node.node_id.clone())
                .await
                .context("failed to connect to sorter endpoint")?;

        let agent_id = sorter_client.agent_id().to_string();
        let batch_sender = sorter_client
            .start_stream(
                config.performance.sink_retry_max_attempts,
                Duration::from_millis(config.performance.sink_retry_base_delay_ms),
                cancel_token,
            )
            .await
            .context("failed to establish sorter stream")?;

        info!(
            endpoint = %endpoint,
            agent_id = %agent_id,
            "connected to sorter"
        );

        Ok(Arc::new(batch_sender))
    } else {
        let output_dir = config
            .sorter
            .output_dir_path()
            .expect("configuration validation ensures output_dir is set");

        info!(
            output_dir = %output_dir.display(),
            "configured local file output sink"
        );

        Ok(Arc::new(FileWriter::new(output_dir)))
    }
}

/// Resolve the effective batch size based on config and environment overrides.
pub fn resolve_batch_size(configured: usize) -> usize {
    use tracing::warn;

    match std::env::var(FORCE_FLUSH_ENV) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.eq_ignore_ascii_case("true") || trimmed == "1" {
                warn!(
                    env = FORCE_FLUSH_ENV,
                    configured, "forcing batch size to 1 for immediate flush (testing mode)"
                );
                1
            } else if trimmed.eq_ignore_ascii_case("false") || trimmed == "0" {
                configured.max(1)
            } else {
                warn!(
                    env = FORCE_FLUSH_ENV,
                    value = trimmed,
                    configured,
                    "unrecognized value for {}; using configured batch size",
                    FORCE_FLUSH_ENV
                );
                configured.max(1)
            }
        }
        Err(_) => configured.max(1),
    }
}
