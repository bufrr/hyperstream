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
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tailer::tail_file;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
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
    let startup_time = SystemTime::now();
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
    let tailer_semaphore = Arc::new(Semaphore::new(config.performance.max_concurrent_tailers));

    let skip_historical = config.skip_historical();

    if let Err(err) = seed_existing_files(
        &watch_paths,
        &mut active_tailers,
        checkpoint_db.clone(),
        record_sink.clone(),
        poll_interval,
        batch_size,
        skip_historical,
        startup_time,
        tailer_semaphore.clone(),
        config.performance.bulk_load_warn_bytes,
        config.performance.bulk_load_abort_bytes,
    )
    .await
    {
        warn!(error = %err, "failed to discover existing files on startup");
    }

    info!("hl-agent started; awaiting file events");

    loop {
        tokio::select! {
            maybe_event = event_rx.recv() => {
                match maybe_event {
                    Some(event) => {
                        let path = event_path(&event);
                        if let Err(err) = maybe_skip_historical_for_path(
                            &path,
                            &checkpoint_db,
                            skip_historical,
                            startup_time,
                        )
                        .await
                        {
                            warn!(
                                error = %err,
                                path = %path.display(),
                                "failed to honor skip_historical for new file"
                            );
                        }
                        spawn_tailer_if_needed(
                            &mut active_tailers,
                            path,
                            checkpoint_db.clone(),
                            record_sink.clone(),
                            poll_interval,
                            batch_size,
                            tailer_semaphore.clone(),
                            config.performance.bulk_load_warn_bytes,
                            config.performance.bulk_load_abort_bytes,
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

#[allow(clippy::too_many_arguments)] // Tailer start-up depends on many contextual knobs; grouping keeps call site clearer.
async fn seed_existing_files(
    watch_paths: &[PathBuf],
    active_tailers: &mut HashMap<PathBuf, JoinHandle<()>>,
    checkpoint_db: Arc<CheckpointDB>,
    record_sink: Arc<dyn RecordSink>,
    poll_interval: Duration,
    batch_size: usize,
    skip_historical: bool,
    startup_time: SystemTime,
    tailer_semaphore: Arc<Semaphore>,
    bulk_load_warn_bytes: u64,
    bulk_load_abort_bytes: u64,
) -> Result<()> {
    let mut existing_files = discover_existing_files(watch_paths)?;

    existing_files.sort_by(|a, b| {
        let a_priority = file_priority(a.as_path());
        let b_priority = file_priority(b.as_path());
        a_priority.cmp(&b_priority).then_with(|| a.cmp(b))
    });

    info!(
        skip_historical,
        file_count = existing_files.len(),
        "seeding existing files"
    );

    for path in existing_files {
        if let Err(err) =
            maybe_skip_historical_for_path(&path, &checkpoint_db, skip_historical, startup_time)
                .await
        {
            warn!(
                error = %err,
                path = %path.display(),
                "failed to initialize checkpoint; will use default behavior"
            );
        }

        spawn_tailer_if_needed(
            active_tailers,
            path,
            checkpoint_db.clone(),
            record_sink.clone(),
            poll_interval,
            batch_size,
            tailer_semaphore.clone(),
            bulk_load_warn_bytes,
            bulk_load_abort_bytes,
        );
    }
    Ok(())
}

async fn maybe_skip_historical_for_path(
    path: &PathBuf,
    checkpoint_db: &Arc<CheckpointDB>,
    skip_historical: bool,
    startup_time: SystemTime,
) -> Result<()> {
    if !skip_historical {
        return Ok(());
    }

    let offset = checkpoint_db.get_offset(path).await?;
    if offset != 0 {
        return Ok(());
    }

    let metadata = tokio::fs::metadata(path).await?;
    let last_modified = metadata.modified().unwrap_or(UNIX_EPOCH);
    if last_modified >= startup_time {
        return Ok(());
    }

    let last_modified_ts = last_modified
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs() as i64;
    let file_size = metadata.len();

    checkpoint_db
        .set_offset(path, file_size, file_size, last_modified_ts)
        .await?;

    info!(
        path = %path.display(),
        offset = file_size,
        "skip_historical enabled - initialized checkpoint to end of file"
    );

    Ok(())
}

fn spawn_tailer_if_needed(
    active_tailers: &mut HashMap<PathBuf, JoinHandle<()>>,
    path: PathBuf,
    checkpoint_db: Arc<CheckpointDB>,
    record_sink: Arc<dyn RecordSink>,
    poll_interval: Duration,
    batch_size: usize,
    tailer_semaphore: Arc<Semaphore>,
    bulk_load_warn_bytes: u64,
    bulk_load_abort_bytes: u64,
) {
    if let Some(handle) = active_tailers.get(&path) {
        if handle.is_finished() {
            active_tailers.remove(&path);
        } else {
            return;
        }
    }

    let parsers = match route_parser(&path) {
        Ok(parsers) => parsers,
        Err(err) => {
            warn!(error = %err, path = %path.display(), "skipping unrecognized file");
            return;
        }
    };

    let is_periodic_abci = path
        .iter()
        .any(|component| component.to_str() == Some("periodic_abci_states"));
    let is_replica_cmds = path
        .iter()
        .any(|component| component.to_str() == Some("replica_cmds"));

    if is_periodic_abci {
        info!(
            path = %path.display(),
            "spawning tailer for periodic_abci_states file"
        );
    } else if is_replica_cmds {
        info!(
            path = %path.display(),
            "spawning tailer for replica_cmds file"
        );
    } else {
        debug!(path = %path.display(), "spawning tailer for file");
    }

    let tail_path = path.clone();
    let semaphore = tailer_semaphore.clone();
    let handle = tokio::spawn(async move {
        let permit = semaphore.acquire_owned().await;
        if permit.is_err() {
            warn!(path = %tail_path.display(), "tailer semaphore closed; skipping file");
            return;
        }
        let _permit = permit.unwrap();
        if let Err(err) = tail_file(
            tail_path.clone(),
            parsers,
            checkpoint_db,
            record_sink,
            batch_size,
            poll_interval,
            bulk_load_warn_bytes,
            bulk_load_abort_bytes,
        )
        .await
        {
            error!(error = %err, path = %tail_path.display(), "tailer terminated with error");
        }
    });

    active_tailers.insert(path, handle);
}

fn file_priority(path: &Path) -> u8 {
    if path
        .iter()
        .any(|component| component.to_str() == Some("node_fills_by_block"))
    {
        0
    } else {
        1
    }
}

fn discover_existing_files(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for path in paths {
        debug!(path = %path.display(), "scanning watch path for existing files");
        if !path.exists() {
            debug!(
                path = %path.display(),
                "watch path does not exist on disk; skipping"
            );
            continue;
        }
        collect_files(path, &mut files)?;
    }
    debug!(file_count = files.len(), "discover_existing_files complete");
    Ok(files)
}

fn collect_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("failed to read metadata for {}", path.display()))?;
    if metadata.is_file() {
        debug!(path = %path.display(), "found file candidate while seeding");
        match route_parser(path) {
            Ok(_) => {
                debug!(
                    path = %path.display(),
                    "route_parser matched file; scheduling for tailing"
                );
                files.push(path.to_path_buf());
            }
            Err(err) => {
                debug!(
                    path = %path.display(),
                    error = %err,
                    "route_parser rejected file; skipping"
                );
            }
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
