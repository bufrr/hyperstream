mod checkpoint;
mod config;
mod output_writer;
mod parsers;
mod sorter_client;
mod tailer;
mod watcher;

use anyhow::{Context, Result};
use checkpoint::CheckpointDB;
use config::Config;
use output_writer::{FileWriter, RecordSink};
use parsers::route_parser;
use sorter_client::SorterClient;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tailer::tail_file;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};
use watcher::{watch_directories, FileEvent};

const FORCE_FLUSH_ENV: &str = "HL_AGENT_FORCE_FLUSH";

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let config_path =
        std::env::var("HL_AGENT_CONFIG").unwrap_or_else(|_| "config.toml".to_string());
    let config = Config::load(&config_path)
        .with_context(|| format!("failed to load config from {config_path}"))?;
    let batch_size = resolve_batch_size(config.sorter.batch_size);

    let watch_paths = config.watch_paths();
    let poll_interval = Duration::from_millis(config.watcher.poll_interval_ms);
    let checkpoint_db = Arc::new(CheckpointDB::new(config.checkpoint_db_path())?);
    let record_sink: Arc<dyn RecordSink> = if let Some(endpoint) = config.sorter.endpoint.clone() {
        let mut sorter_client =
            SorterClient::connect(endpoint.clone(), config.node.node_id.clone())
                .await
                .context("failed to connect to sorter endpoint")?;

        let agent_id = sorter_client.agent_id().to_string();
        let batch_sender = sorter_client
            .start_stream()
            .await
            .context("failed to establish sorter stream")?;

        info!(
            endpoint = %endpoint,
            agent_id = %agent_id,
            "connected to sorter"
        );

        Arc::new(batch_sender)
    } else {
        let output_dir = config
            .sorter
            .output_dir_path()
            .expect("configuration validation ensures output_dir is set");

        info!(
            output_dir = %output_dir.display(),
            "configured local file output sink"
        );

        Arc::new(FileWriter::new(output_dir))
    };

    let (event_tx, mut event_rx) = mpsc::unbounded_channel();

    let watcher_handle = tokio::spawn({
        let paths = watch_paths.clone();
        let event_tx = event_tx.clone();
        async move {
            if let Err(err) = watch_directories(paths, poll_interval, event_tx).await {
                error!(error = %err, "file watcher exited unexpectedly");
            }
        }
    });

    let mut active_tailers: HashMap<PathBuf, JoinHandle<()>> = HashMap::new();

    if let Err(err) = seed_existing_files(
        &watch_paths,
        &mut active_tailers,
        checkpoint_db.clone(),
        record_sink.clone(),
        poll_interval,
        batch_size,
    ) {
        warn!(error = %err, "failed to discover existing files on startup");
    }

    info!("hl-agent started; awaiting file events");

    loop {
        tokio::select! {
            maybe_event = event_rx.recv() => {
                match maybe_event {
                    Some(event) => {
                        let path = event_path(&event);
                        spawn_tailer_if_needed(
                            &mut active_tailers,
                            path,
                            checkpoint_db.clone(),
                            record_sink.clone(),
                            poll_interval,
                            batch_size,
                        );
                    }
                    None => {
                        warn!("event channel closed; shutting down");
                        break;
                    }
                }
            }
            result = tokio::signal::ctrl_c() => {
                if let Err(err) = result {
                    error!(error = %err, "failed while waiting for shutdown signal");
                }
                info!("shutdown signal received");
                break;
            }
        }
    }

    info!("stopping tailers");
    for (path, handle) in active_tailers.drain() {
        if !handle.is_finished() {
            handle.abort();
        }
        info!(path = %path.display(), "tailer stopped");
    }

    watcher_handle.abort();

    Ok(())
}

fn resolve_batch_size(configured: usize) -> usize {
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

fn init_tracing() {
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_target(false)
        .finish();

    let _ = tracing::subscriber::set_global_default(subscriber);
}

fn event_path(event: &FileEvent) -> PathBuf {
    match event {
        FileEvent::Created(path) | FileEvent::Modified(path) => path.clone(),
    }
}

fn seed_existing_files(
    watch_paths: &[PathBuf],
    active_tailers: &mut HashMap<PathBuf, JoinHandle<()>>,
    checkpoint_db: Arc<CheckpointDB>,
    record_sink: Arc<dyn RecordSink>,
    poll_interval: Duration,
    batch_size: usize,
) -> Result<()> {
    let existing_files = discover_existing_files(watch_paths)?;
    for path in existing_files {
        spawn_tailer_if_needed(
            active_tailers,
            path,
            checkpoint_db.clone(),
            record_sink.clone(),
            poll_interval,
            batch_size,
        );
    }
    Ok(())
}

fn spawn_tailer_if_needed(
    active_tailers: &mut HashMap<PathBuf, JoinHandle<()>>,
    path: PathBuf,
    checkpoint_db: Arc<CheckpointDB>,
    record_sink: Arc<dyn RecordSink>,
    poll_interval: Duration,
    batch_size: usize,
) {
    if let Some(handle) = active_tailers.get(&path) {
        if handle.is_finished() {
            active_tailers.remove(&path);
        } else {
            return;
        }
    }

    if let Err(err) = ensure_parsable(&path) {
        warn!(error = %err, path = %path.display(), "skipping unrecognized file");
        return;
    }

    let tail_path = path.clone();
    let handle = tokio::spawn(async move {
        if let Err(err) = tail_file(
            tail_path.clone(),
            checkpoint_db,
            record_sink,
            batch_size,
            poll_interval,
        )
        .await
        {
            error!(error = %err, path = %tail_path.display(), "tailer terminated with error");
        }
    });

    active_tailers.insert(path, handle);
}

fn ensure_parsable(path: &Path) -> Result<()> {
    route_parser(path)?;
    Ok(())
}

fn discover_existing_files(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for path in paths {
        if !path.exists() {
            continue;
        }
        collect_files(path, &mut files)?;
    }
    Ok(files)
}

fn collect_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("failed to read metadata for {}", path.display()))?;
    if metadata.is_file() {
        if ensure_parsable(path).is_ok() {
            files.push(path.to_path_buf());
        }
        return Ok(());
    }

    if metadata.is_dir() {
        for entry in std::fs::read_dir(path)
            .with_context(|| format!("failed to read directory {}", path.display()))?
        {
            let entry = entry?;
            collect_files(&entry.path(), files)?;
        }
    }
    Ok(())
}
