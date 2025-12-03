use anyhow::Result;
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};
use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub enum FileEvent {
    Created(PathBuf),
    Modified(PathBuf),
    Closed(PathBuf),
}

pub const FILE_RECENCY_WINDOW: Duration = Duration::from_secs(60 * 60);
pub const WATCHER_CHANNEL_CAPACITY: usize = 1000;
const IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const IDLE_CHECK_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Default)]
struct ScanStats {
    dirs_scanned: usize,
    files_found: usize,
    directories_discovered: usize,
    old_files_skipped: usize,
}

pub async fn watch_directories(
    watch_paths: Vec<PathBuf>,
    poll_interval: Duration,
    event_tx: mpsc::Sender<FileEvent>,
    cancel_token: CancellationToken,
) -> Result<()> {
    let mut seen_files: HashSet<PathBuf> = HashSet::new();
    let mut seen_dirs: HashSet<PathBuf> = HashSet::new();
    for watch_path in &watch_paths {
        seen_dirs.insert(watch_path.clone());
    }
    let mut file_activity: HashMap<PathBuf, Instant> = HashMap::new();
    let mut file_mod_times: HashMap<PathBuf, SystemTime> = HashMap::new();
    let mut scan_interval = interval(poll_interval);
    let mut idle_check_interval = interval(IDLE_CHECK_INTERVAL);
    scan_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    idle_check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    info!(
        count = watch_paths.len(),
        "file watcher started for configured paths"
    );

    loop {
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => {
                info!("file watcher received shutdown signal");
                break;
            }
            _ = scan_interval.tick() => {
                seen_files.retain(|path| path.exists());
                seen_dirs.retain(|path| path.exists());
                file_mod_times.retain(|path, _| path.exists());
                let mut scan_stats = ScanStats::default();
                for watch_path in &watch_paths {
                    scan_directory_recursive(
                        watch_path,
                        &event_tx,
                        &mut seen_files,
                        &mut seen_dirs,
                        &mut file_mod_times,
                        &mut file_activity,
                        &mut scan_stats,
                    );
                }
                info!(
                    dirs_scanned = scan_stats.dirs_scanned,
                    files_found = scan_stats.files_found,
                    directories_discovered = scan_stats.directories_discovered,
                    old_files_skipped = scan_stats.old_files_skipped,
                    "proactive scan completed"
                );
                if scan_stats.files_found == 0 {
                    warn!(
                        dirs_scanned = scan_stats.dirs_scanned,
                        "proactive scan found zero files"
                    );
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
    seen_dirs: &mut HashSet<PathBuf>,
    file_mod_times: &mut HashMap<PathBuf, SystemTime>,
    file_activity: &mut HashMap<PathBuf, Instant>,
    scan_stats: &mut ScanStats,
) {
    trace!(path = %dir.display(), "scanning directory");

    for entry in WalkDir::new(dir).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if entry.file_type().is_dir() {
            scan_stats.dirs_scanned += 1;
            if seen_dirs.insert(path.to_path_buf()) {
                scan_stats.directories_discovered += 1;
                info!(path = %path.display(), "discovered new directory during scan");
            }
            continue;
        }

        if entry.file_type().is_file() {
            scan_stats.files_found += 1;

            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(err) => {
                    debug!(path = %path.display(), error = %err, "failed to read metadata during scan");
                    continue;
                }
            };

            let modified_time = metadata.modified().ok();
            let path_buf = path.to_path_buf();
            let mut event = None;

            if seen_files.insert(path_buf.clone()) {
                if let Some(mod_time) = modified_time {
                    if let Ok(age) = SystemTime::now().duration_since(mod_time) {
                        if age > FILE_RECENCY_WINDOW {
                            debug!(
                                path = %path.display(),
                                age_seconds = age.as_secs(),
                                window_seconds = FILE_RECENCY_WINDOW.as_secs(),
                                "skipping old file outside recency window"
                            );
                            scan_stats.old_files_skipped += 1;
                            file_mod_times.insert(path_buf.clone(), mod_time);
                            continue;
                        }
                    }
                    file_mod_times.insert(path_buf.clone(), mod_time);
                }
                log_file_detection(path, "scan_created");
                event = Some(FileEvent::Created(path_buf));
            } else if let (Some(mod_time), Some(previous)) = (modified_time, file_mod_times.get(&path_buf)) {
                if mod_time > *previous {
                    file_mod_times.insert(path_buf.clone(), mod_time);
                    log_file_detection(path, "scan_modified");
                    event = Some(FileEvent::Modified(path_buf));
                }
            } else if let Some(mod_time) = modified_time {
                file_mod_times.entry(path_buf.clone()).or_insert(mod_time);
            }

            if let Some(event) = event {
                record_activity(&event, file_activity);
                send_event(event_tx, event, "scan");
            }
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

fn send_event(event_tx: &mpsc::Sender<FileEvent>, event: FileEvent, kind: &str) {
    if let Err(err) = event_tx.try_send(event) {
        match err {
            TrySendError::Full(_) => warn!(kind, "watcher channel full; dropping file event"),
            TrySendError::Closed(_) => warn!(kind, "watcher channel closed; dropping file event"),
        }
    }
}

fn log_file_detection(path: &Path, source: &str) {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(err) => {
            debug!(path = %path.display(), error = %err, source, "failed to read metadata for file detection");
            info!(path = %path.display(), source, "detected file without metadata");
            return;
        }
    };

    let size_bytes = metadata.len();
    let modified = metadata.modified().ok().map(|time| {
        let datetime: DateTime<Utc> = time.into();
        datetime.to_rfc3339()
    });

    info!(
        path = %path.display(),
        size_bytes,
        modified = modified.as_deref().unwrap_or("unknown"),
        source,
        "detected file"
    );
}
