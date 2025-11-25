//! Pipeline initialization and tailer management.
//!
//! This module handles:
//! - Discovering existing files at startup
//! - Seeding tailers for discovered files
//! - Spawning and managing tailer tasks
//! - File prioritization and historical data skipping

use crate::checkpoint::CheckpointDB;
use crate::output_writer::RecordSink;
use crate::parsers::route_parser;
use crate::tailer::tail_file;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// Seeds tailers for all existing files in the watch paths.
///
/// Files are sorted by priority (e.g., fills data first) and modification time (newest first)
/// to ensure actively-written files are monitored when total files exceed semaphore limit.
#[allow(clippy::too_many_arguments)]
pub async fn seed_existing_files(
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

    // Sort by priority first, then by modification time (NEWEST first) to ensure
    // latest actively-written files are monitored when total files exceed semaphore limit
    existing_files.sort_by(|a, b| {
        let a_priority = file_priority(a.as_path());
        let b_priority = file_priority(b.as_path());

        a_priority.cmp(&b_priority).then_with(|| {
            // Get modification times for both files
            let a_mtime = std::fs::metadata(a).ok().and_then(|m| m.modified().ok());
            let b_mtime = std::fs::metadata(b).ok().and_then(|m| m.modified().ok());

            match (a_mtime, b_mtime) {
                (Some(a_time), Some(b_time)) => b_time.cmp(&a_time), // NEWEST first (reversed)
                (Some(_), None) => std::cmp::Ordering::Less,         // a has mtime, prefer it
                (None, Some(_)) => std::cmp::Ordering::Greater,      // b has mtime, prefer it
                (None, None) => a.cmp(b),                            // fallback to alphabetical
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

/// Skips historical data for a file by setting checkpoint to end of file if conditions are met.
///
/// Conditions:
/// - `skip_historical` is enabled
/// - File has no existing checkpoint (offset == 0)
/// - File was last modified before agent startup
pub async fn maybe_skip_historical_for_path(
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

/// Spawns a new tailer task for the file if one isn't already running.
///
/// Files are routed to appropriate parsers based on path components. If the file
/// doesn't match any known parser, it's skipped with a warning.
#[allow(clippy::too_many_arguments)]
pub fn spawn_tailer_if_needed(
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

/// Returns priority value for file (lower = higher priority).
///
/// Files in `node_fills_by_block` get priority 0, all others get priority 1.
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

/// Discovers all existing files in the watch paths that match known parsers.
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

/// Recursively collects all files in a directory that match known parsers.
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
