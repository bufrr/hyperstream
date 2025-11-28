//! File-based mode runner.
//!
//! Handles execution when the agent is configured to read from local files.

use crate::checkpoint::CheckpointDB;
use crate::config::Config;
use crate::output_writer::RecordSink;
use crate::parsers::block_merger::BlockMerger;
use crate::parsers::hash_store::HashStore;
use crate::parsers::route_parser;
use crate::runner::{build_record_sink, resolve_batch_size};
use crate::tailer::tail_file;
use crate::watcher::{watch_directories, FileEvent, WATCHER_CHANNEL_CAPACITY};
use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Run the agent in file mode.
pub async fn run(config: &Config) -> Result<()> {
    info!("hl-agent starting in file mode");

    let cancel_token = CancellationToken::new();
    let batch_size = resolve_batch_size(config.sorter.batch_size);
    let watch_paths = config.watch_paths();
    let poll_interval = Duration::from_millis(config.watcher.poll_interval_ms);
    let checkpoint_db = Arc::new(CheckpointDB::new(config.checkpoint_db_path())?);
    let record_sink = build_record_sink(config, cancel_token.clone()).await?;

    let (event_tx, mut event_rx) = mpsc::channel(WATCHER_CHANNEL_CAPACITY);

    let mut active_tailers: HashMap<PathBuf, JoinHandle<()>> = HashMap::new();
    let tailer_semaphore = Arc::new(Semaphore::new(config.performance.max_concurrent_tailers));

    let skip_historical = config.skip_historical();
    let tail_bytes = config.tail_bytes();

    if let Err(err) = seed_existing_files(
        &watch_paths,
        &mut active_tailers,
        checkpoint_db.clone(),
        record_sink.clone(),
        poll_interval,
        batch_size,
        skip_historical,
        tail_bytes,
        tailer_semaphore.clone(),
        config.performance.bulk_load_warn_bytes,
        config.performance.bulk_load_abort_bytes,
        cancel_token.clone(),
    )
    .await
    {
        warn!(error = %err, "failed to discover existing files on startup");
    }

    let mut watcher_handle = tokio::spawn({
        let paths = watch_paths.clone();
        let event_tx = event_tx.clone();
        let cancel_token = cancel_token.clone();
        async move {
            if let Err(err) = watch_directories(paths, poll_interval, event_tx, cancel_token).await
            {
                error!(error = %err, "file watcher exited unexpectedly");
            }
        }
    });

    info!("hl-agent started; awaiting file events");

    let mut shutdown_reason = "event channel closed";
    loop {
        tokio::select! {
            maybe_event = event_rx.recv() => {
                match maybe_event {
                    Some(event) => {
                        let path = event_path(&event);
                        if let Err(err) =
                            maybe_skip_historical_for_path(&path, &checkpoint_db, skip_historical, tail_bytes)
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
                            cancel_token.clone(),
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
                shutdown_reason = "signal";
                break;
            }
        }
    }

    info!(reason = shutdown_reason, "initiating shutdown");
    cancel_token.cancel();

    let shutdown_timeout = Duration::from_secs(5);

    info!("stopping tailers");
    for (path, mut handle) in active_tailers.drain() {
        let timeout = sleep(shutdown_timeout);
        tokio::pin!(timeout);

        let result = tokio::select! {
            res = &mut handle => Some(res),
            _ = &mut timeout => None,
        };

        match result {
            Some(Ok(())) => {
                info!(path = %path.display(), "tailer stopped");
            }
            Some(Err(err)) => {
                warn!(path = %path.display(), error = %err, "tailer exited with error during shutdown");
            }
            None => {
                warn!(path = %path.display(), "tailer did not stop within timeout; aborting");
                handle.abort();
            }
        }
    }

    let watcher_timeout = sleep(shutdown_timeout);
    tokio::pin!(watcher_timeout);
    let watcher_result = tokio::select! {
        res = &mut watcher_handle => Some(res),
        _ = &mut watcher_timeout => None,
    };

    match watcher_result {
        Some(Ok(())) => info!("file watcher stopped"),
        Some(Err(err)) => warn!(error = %err, "file watcher exited with error during shutdown"),
        None => {
            warn!("file watcher did not stop within timeout; aborting");
            watcher_handle.abort();
        }
    }

    // Log final merger statistics
    BlockMerger::global().log_stats();
    HashStore::global().log_stats();

    Ok(())
}

fn event_path(event: &FileEvent) -> PathBuf {
    match event {
        FileEvent::Created(path) | FileEvent::Modified(path) => path.clone(),
    }
}

#[allow(clippy::too_many_arguments)]
async fn seed_existing_files(
    watch_paths: &[PathBuf],
    active_tailers: &mut HashMap<PathBuf, JoinHandle<()>>,
    checkpoint_db: Arc<CheckpointDB>,
    record_sink: Arc<dyn RecordSink>,
    poll_interval: Duration,
    batch_size: usize,
    skip_historical: bool,
    tail_bytes: u64,
    tailer_semaphore: Arc<Semaphore>,
    bulk_load_warn_bytes: u64,
    bulk_load_abort_bytes: u64,
    cancel_token: CancellationToken,
) -> Result<()> {
    let mut existing_files = discover_existing_files(watch_paths)?;

    // Sort by priority first, then by modification time (NEWEST first)
    existing_files.sort_by(|a, b| {
        let a_priority = file_priority(a.as_path());
        let b_priority = file_priority(b.as_path());

        a_priority.cmp(&b_priority).then_with(|| {
            let a_mtime = std::fs::metadata(a).ok().and_then(|m| m.modified().ok());
            let b_mtime = std::fs::metadata(b).ok().and_then(|m| m.modified().ok());

            match (a_mtime, b_mtime) {
                (Some(a_time), Some(b_time)) => b_time.cmp(&a_time),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => a.cmp(b),
            }
        })
    });

    info!(
        skip_historical,
        file_count = existing_files.len(),
        "seeding existing files"
    );

    for path in existing_files {
        if let Err(err) =
            maybe_skip_historical_for_path(&path, &checkpoint_db, skip_historical, tail_bytes).await
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
            cancel_token.clone(),
        );
    }
    Ok(())
}

async fn maybe_skip_historical_for_path(
    path: &PathBuf,
    checkpoint_db: &Arc<CheckpointDB>,
    skip_historical: bool,
    tail_bytes: u64,
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
    let last_modified_ts = last_modified
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs() as i64;
    let file_size = metadata.len();

    let start_offset = if tail_bytes == 0 {
        file_size
    } else {
        file_size.saturating_sub(tail_bytes)
    };

    let line_count = if start_offset == 0 || !is_replica_cmds_path(path) {
        0
    } else {
        let file = File::open(path).await?;
        let mut reader = file.take(start_offset);
        let mut buf = vec![0u8; 8192];
        let mut newline_count: u64 = 0;

        loop {
            let bytes_read = reader.read(&mut buf).await?;
            if bytes_read == 0 {
                break;
            }
            newline_count += buf[..bytes_read].iter().filter(|&&b| b == b'\n').count() as u64;
        }

        newline_count
    };

    checkpoint_db
        .set_offset(path, start_offset, file_size, last_modified_ts, line_count)
        .await?;

    if tail_bytes == 0 {
        info!(
            path = %path.display(),
            offset = start_offset,
            "skip_historical enabled - skipping to end of file"
        );
    } else {
        let bytes_to_read = file_size.saturating_sub(start_offset);
        info!(
            path = %path.display(),
            offset = start_offset,
            file_size,
            bytes_to_read,
            "skip_historical enabled - reading last {} bytes",
            bytes_to_read
        );
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
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
    cancel_token: CancellationToken,
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
    let tailer_cancel = cancel_token;
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
            tailer_cancel,
        )
        .await
        {
            error!(error = %err, path = %tail_path.display(), "tailer terminated with error");
        }
    });

    active_tailers.insert(path, handle);
}

fn is_replica_cmds_path(path: &Path) -> bool {
    path.iter()
        .filter_map(|c| c.to_str())
        .any(|component| component == "replica_cmds")
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
    let mut collected_files = Vec::new();

    for watch_path in paths {
        debug!(path = %watch_path.display(), "scanning watch path for existing files");
        if !watch_path.exists() {
            debug!(
                path = %watch_path.display(),
                "watch path does not exist on disk; skipping"
            );
            continue;
        }

        collect_files(watch_path, &mut collected_files)?;
    }

    collected_files.sort_by(|a, b| {
        let a_priority = file_priority(a.as_path());
        let b_priority = file_priority(b.as_path());

        a_priority.cmp(&b_priority).then_with(|| {
            let a_mtime = std::fs::metadata(a).ok().and_then(|m| m.modified().ok());
            let b_mtime = std::fs::metadata(b).ok().and_then(|m| m.modified().ok());

            match (a_mtime, b_mtime) {
                (Some(a_time), Some(b_time)) => a_time.cmp(&b_time),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => a.cmp(b),
            }
        })
    });
    collected_files.dedup();

    debug!(
        file_count = collected_files.len(),
        "discover_existing_files complete (all files per watch path)"
    );
    Ok(collected_files)
}

fn collect_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    use anyhow::Context;

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
