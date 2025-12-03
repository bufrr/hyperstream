use anyhow::{Context, Result};
use notify::{
    Config as NotifyConfig, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone)]
pub enum FileEvent {
    Created(PathBuf),
    Modified(PathBuf),
}

pub const WATCHER_CHANNEL_CAPACITY: usize = 1000;

pub async fn watch_directories(
    watch_paths: Vec<PathBuf>,
    poll_interval: Duration,
    event_tx: mpsc::Sender<FileEvent>,
    cancel_token: CancellationToken,
) -> Result<()> {
    let watcher_tx = event_tx.clone();

    let mut watcher = RecommendedWatcher::new(
        move |res| match res {
            Ok(event) => handle_event(&watcher_tx, event),
            Err(err) => error!(error = %err, "file watcher error"),
        },
        NotifyConfig::default().with_poll_interval(poll_interval),
    )
    .context("failed to create notify watcher")?;

    for path in &watch_paths {
        watcher
            .watch(path, RecursiveMode::Recursive)
            .with_context(|| format!("failed to watch path {}", path.display()))?;
    }

    info!(
        count = watch_paths.len(),
        "file watcher started for configured paths"
    );

    // Track seen files to detect newly created ones during scans
    let mut seen_files: HashSet<PathBuf> = HashSet::new();

    // Create periodic scanner that runs every poll_interval
    let mut scan_interval = interval(poll_interval);
    scan_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Keep the watcher alive and periodically scan for new directories/files
    loop {
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => {
                info!("file watcher received shutdown signal");
                break;
            }
            _ = scan_interval.tick() => {
                // Remove files that disappeared to avoid unbounded growth
                seen_files.retain(|path| path.exists());

                // Proactively scan watch paths for new files
                for watch_path in &watch_paths {
                    scan_directory_recursive(&watch_path, &event_tx, &mut seen_files);
                }
            }
        }
    }

    Ok(())
}

/// Recursively scan a directory for new files and emit Created events
fn scan_directory_recursive(
    dir: &PathBuf,
    event_tx: &mpsc::Sender<FileEvent>,
    seen_files: &mut HashSet<PathBuf>,
) {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) => {
            debug!(path = %dir.display(), error = %err, "failed to read directory during scan");
            return;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();

        if path.is_dir() {
            // Recursively scan subdirectories
            scan_directory_recursive(&path, event_tx, seen_files);
        } else if path.is_file() {
            // Check if this is a new file we haven't seen before
            if seen_files.insert(path.clone()) {
                debug!(path = %path.display(), "proactive scan detected new file");
                send_event(event_tx, FileEvent::Created(path), "scan_created");
            }
        }
    }
}

fn handle_event(event_tx: &mpsc::Sender<FileEvent>, event: Event) {
    match event.kind {
        EventKind::Create(_) => {
            for path in event.paths {
                if path.is_dir() {
                    continue;
                }
                debug!(path = %path.display(), "detected file creation");
                send_event(event_tx, FileEvent::Created(path), "created");
            }
        }
        EventKind::Modify(_) => {
            for path in event.paths {
                if path.is_dir() {
                    continue;
                }
                debug!(path = %path.display(), "detected file modification");
                send_event(event_tx, FileEvent::Modified(path), "modified");
            }
        }
        _ => {}
    }
}

fn send_event(event_tx: &mpsc::Sender<FileEvent>, event: FileEvent, kind: &str) {
    match event_tx.try_send(event) {
        Ok(_) => {}
        Err(TrySendError::Full(_)) => {
            warn!(kind, "watcher channel full; dropping file event");
        }
        Err(TrySendError::Closed(_)) => {
            warn!(kind, "watcher channel closed; dropping file event");
        }
    }
}
