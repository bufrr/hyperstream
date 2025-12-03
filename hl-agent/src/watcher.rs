use anyhow::{Context, Result};
use notify::{
    Config as NotifyConfig, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone)]
pub enum FileEvent {
    Created(PathBuf),
    Modified(PathBuf),
    Closed(PathBuf),
}

pub const WATCHER_CHANNEL_CAPACITY: usize = 1000;
const IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const IDLE_CHECK_INTERVAL: Duration = Duration::from_secs(30);

pub async fn watch_directories(
    watch_paths: Vec<PathBuf>,
    poll_interval: Duration,
    event_tx: mpsc::Sender<FileEvent>,
    cancel_token: CancellationToken,
) -> Result<()> {
    let (watcher_tx, mut watcher_rx) = mpsc::channel(WATCHER_CHANNEL_CAPACITY);
    let notify_tx = watcher_tx.clone();

    let mut watcher = RecommendedWatcher::new(
        move |res| match res {
            Ok(event) => handle_event(&notify_tx, event),
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

    let mut seen_files: HashSet<PathBuf> = HashSet::new();
    let mut file_activity: HashMap<PathBuf, Instant> = HashMap::new();
    let mut scan_interval = interval(poll_interval);
    let mut idle_check_interval = interval(IDLE_CHECK_INTERVAL);
    scan_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    idle_check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => {
                info!("file watcher received shutdown signal");
                break;
            }
            Some(event) = watcher_rx.recv() => {
                record_activity(&event, &mut file_activity);
                send_event(&event_tx, event, "forward");
            }
            _ = scan_interval.tick() => {
                seen_files.retain(|path| path.exists());
                for watch_path in &watch_paths {
                    scan_directory_recursive(watch_path, &watcher_tx, &mut seen_files);
                }
            }
            _ = idle_check_interval.tick() => {
                close_idle_files(&mut file_activity, &event_tx);
            }
        }
    }

    Ok(())
}

fn scan_directory_recursive(
    dir: &Path,
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

    for path in entries.flatten().map(|entry| entry.path()) {
        if path.is_dir() {
            scan_directory_recursive(&path, event_tx, seen_files);
        } else if path.is_file() && seen_files.insert(path.clone()) {
            debug!(path = %path.display(), "proactive scan detected new file");
            send_event(event_tx, FileEvent::Created(path), "scan_created");
        }
    }
}

fn record_activity(event: &FileEvent, file_activity: &mut HashMap<PathBuf, Instant>) {
    match event {
        FileEvent::Created(path) | FileEvent::Modified(path) => {
            file_activity.insert(path.clone(), Instant::now());
        }
        FileEvent::Closed(path) => {
            file_activity.remove(path);
        }
    }
}

fn close_idle_files(file_activity: &mut HashMap<PathBuf, Instant>, event_tx: &mpsc::Sender<FileEvent>) {
    let now = Instant::now();
    let mut stale_paths = Vec::new();

    for (path, last_activity) in file_activity.iter() {
        if now.duration_since(*last_activity) > IDLE_TIMEOUT {
            stale_paths.push(path.clone());
        }
    }

    for path in stale_paths {
        file_activity.remove(&path);
        debug!(path = %path.display(), "closing idle file after inactivity");
        send_event(event_tx, FileEvent::Closed(path), "idle_close");
    }
}

fn handle_event(event_tx: &mpsc::Sender<FileEvent>, event: Event) {
    let (label, builder): (&str, fn(PathBuf) -> FileEvent) = match event.kind {
        EventKind::Create(_) => ("creation", FileEvent::Created),
        EventKind::Modify(_) => ("modification", FileEvent::Modified),
        _ => return,
    };

    for path in event.paths.into_iter().filter(|path| path.is_file()) {
        debug!(path = %path.display(), "detected file {label}");
        send_event(event_tx, builder(path), label);
    }
}

fn send_event(event_tx: &mpsc::Sender<FileEvent>, event: FileEvent, kind: &str) {
    if let Err(err) = event_tx.try_send(event) {
        match err {
            TrySendError::Full(_) => warn!(kind, "watcher channel full; dropping file event"),
            TrySendError::Closed(_) => warn!(kind, "watcher channel closed; dropping file event"),
        }
    }
}
