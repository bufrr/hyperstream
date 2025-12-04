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
pub const WATCHER_CHANNEL_CAPACITY: usize = 10000;
const IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const IDLE_CHECK_INTERVAL: Duration = Duration::from_secs(30);
const MODIFIED_EVENT_THROTTLE: Duration = Duration::from_secs(5);

#[derive(Default)]
struct ScanStats {
    dirs_scanned: usize,
    files_found: usize,
    directories_discovered: usize,
    old_files_skipped: usize,
}

struct ScanContext<'a> {
    event_tx: &'a mpsc::Sender<FileEvent>,
    seen_files: &'a mut HashSet<PathBuf>,
    seen_dirs: &'a mut HashSet<PathBuf>,
    file_mod_times: &'a mut HashMap<PathBuf, SystemTime>,
    file_activity: &'a mut HashMap<PathBuf, Instant>,
    last_event_time: &'a mut HashMap<PathBuf, Instant>,
    scan_stats: &'a mut ScanStats,
}

pub async fn watch_directories(
    watch_paths: Vec<PathBuf>,
    poll_interval: Duration,
    event_tx: mpsc::Sender<FileEvent>,
    cancel_token: CancellationToken,
    skip_historical: bool,
) -> Result<()> {
    let mut seen_files: HashSet<PathBuf> = HashSet::new();
    let mut seen_dirs: HashSet<PathBuf> = HashSet::new();
    for watch_path in &watch_paths {
        seen_dirs.insert(watch_path.clone());
    }
    let mut file_activity: HashMap<PathBuf, Instant> = HashMap::new();
    let mut last_event_time: HashMap<PathBuf, Instant> = HashMap::new();
    let mut file_mod_times: HashMap<PathBuf, SystemTime> = HashMap::new();
    let mut scan_interval = interval(poll_interval);
    let mut idle_check_interval = interval(IDLE_CHECK_INTERVAL);
    scan_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    idle_check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut first_scan = true;

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
                last_event_time.retain(|path, _| path.exists());
                let mut scan_stats = ScanStats::default();
                let mut ctx = ScanContext {
                    event_tx: &event_tx,
                    seen_files: &mut seen_files,
                    seen_dirs: &mut seen_dirs,
                    file_mod_times: &mut file_mod_times,
                    file_activity: &mut file_activity,
                    last_event_time: &mut last_event_time,
                    scan_stats: &mut scan_stats,
                };

                if first_scan && skip_historical {
                    initial_skip_historical_scan(&watch_paths, &mut ctx);
                } else if skip_historical {
                    ongoing_skip_historical_scan(&watch_paths, &mut ctx);
                } else {
                    for watch_path in &watch_paths {
                        scan_directory_recursive(watch_path, &mut ctx);
                    }
                }
                first_scan = false;
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
                close_idle_files(&mut file_activity, &mut last_event_time, &event_tx);
            }
        }
    }

    Ok(())
}

fn initial_skip_historical_scan(
    watch_paths: &[PathBuf],
    ctx: &mut ScanContext,
) {
    for watch_path in watch_paths {
        trace!(path = %watch_path.display(), "scanning directory (skip_historical)");
        let mut latest_file: Option<(PathBuf, SystemTime)> = None;
        let mut fallback_latest: Option<PathBuf> = None;

        for entry in WalkDir::new(watch_path).into_iter().filter_map(|e| e.ok()) {
            let path = entry.path();
            if entry.file_type().is_dir() {
                ctx.scan_stats.dirs_scanned += 1;
                if ctx.seen_dirs.insert(path.to_path_buf()) {
                    ctx.scan_stats.directories_discovered += 1;
                    info!(path = %path.display(), "discovered new directory during scan");
                }
                continue;
            }

            if entry.file_type().is_file() {
                ctx.scan_stats.files_found += 1;
                let metadata = match entry.metadata() {
                    Ok(metadata) => metadata,
                    Err(err) => {
                        debug!(path = %path.display(), error = %err, "failed to read metadata during scan");
                        continue;
                    }
                };

                let modified_time = metadata.modified().ok();
                let path_buf = path.to_path_buf();
                ctx.seen_files.insert(path_buf.clone());

                if let Some(mod_time) = modified_time {
                    if latest_file
                        .as_ref()
                        .map(|(_, t)| mod_time > *t)
                        .unwrap_or(true)
                    {
                        latest_file = Some((path_buf.clone(), mod_time));
                    }
                    ctx.file_mod_times.insert(path_buf, mod_time);
                } else {
                    fallback_latest.get_or_insert(path_buf);
                }
            }
        }

        if let Some(path) = latest_file
            .map(|(path, _)| path)
            .or_else(|| fallback_latest.clone())
        {
            info!(path = %path.display(), "skip_historical: starting with latest file");
            log_file_detection(&path, "scan_modified");
            // Emit Modified (not Created) so runner knows this is an existing file
            // and should start from end rather than creating checkpoint at offset 0
            let event = FileEvent::Modified(path.clone());
            record_activity(&event, ctx.file_activity, ctx.last_event_time);
            send_event(ctx.event_tx, event, "scan");
        }
    }
}

fn ongoing_skip_historical_scan(
    watch_paths: &[PathBuf],
    ctx: &mut ScanContext,
) {
    for watch_path in watch_paths {
        trace!(path = %watch_path.display(), "ongoing scan (skip_historical)");

        for entry in WalkDir::new(watch_path).into_iter().filter_map(|e| e.ok()) {
            let path = entry.path();

            if entry.file_type().is_dir() {
                ctx.scan_stats.dirs_scanned += 1;
                if ctx.seen_dirs.insert(path.to_path_buf()) {
                    ctx.scan_stats.directories_discovered += 1;
                    info!(path = %path.display(), "discovered new directory during ongoing scan");
                }
                continue;
            }

            if entry.file_type().is_file() {
                ctx.scan_stats.files_found += 1;

                let metadata = match entry.metadata() {
                    Ok(metadata) => metadata,
                    Err(err) => {
                        debug!(path = %path.display(), error = %err, "failed to read metadata");
                        continue;
                    }
                };

                let modified_time = metadata.modified().ok();
                let path_buf = path.to_path_buf();
                let mut event = None;

                if ctx.seen_files.insert(path_buf.clone()) {
                    if let Some(mod_time) = modified_time {
                        ctx.file_mod_times.insert(path_buf.clone(), mod_time);
                    }
                    log_file_detection(path, "ongoing_scan_new_file");
                    event = Some(FileEvent::Created(path_buf));
                } else if let (Some(mod_time), Some(previous)) =
                    (modified_time, ctx.file_mod_times.get(&path_buf))
                {
                    if mod_time > *previous {
                        ctx.file_mod_times.insert(path_buf.clone(), mod_time);
                        let now = Instant::now();
                        let should_emit = ctx.last_event_time
                            .get(&path_buf)
                            .map(|last| now.duration_since(*last) > MODIFIED_EVENT_THROTTLE)
                            .unwrap_or(true);

                        if should_emit {
                            log_file_detection(path, "ongoing_scan_modified");
                            event = Some(FileEvent::Modified(path_buf));
                        } else {
                            trace!(path = %path.display(), "throttling modified event");
                        }
                    }
                } else if let Some(mod_time) = modified_time {
                    ctx.file_mod_times.entry(path_buf.clone()).or_insert(mod_time);
                }

                if let Some(event) = event {
                    record_activity(&event, ctx.file_activity, ctx.last_event_time);
                    send_event(ctx.event_tx, event, "ongoing_scan");
                }
            }
        }
    }
}

fn scan_directory_recursive(
    dir: &Path,
    ctx: &mut ScanContext,
) {
    trace!(path = %dir.display(), "scanning directory");

    for entry in WalkDir::new(dir).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if entry.file_type().is_dir() {
            ctx.scan_stats.dirs_scanned += 1;
            if ctx.seen_dirs.insert(path.to_path_buf()) {
                ctx.scan_stats.directories_discovered += 1;
                info!(path = %path.display(), "discovered new directory during scan");
            }
            continue;
        }

        if entry.file_type().is_file() {
            ctx.scan_stats.files_found += 1;

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

            if ctx.seen_files.insert(path_buf.clone()) {
                if let Some(mod_time) = modified_time {
                    if let Ok(age) = SystemTime::now().duration_since(mod_time) {
                        if age > FILE_RECENCY_WINDOW {
                            debug!(
                                path = %path.display(),
                                age_seconds = age.as_secs(),
                                window_seconds = FILE_RECENCY_WINDOW.as_secs(),
                                "skipping old file outside recency window"
                            );
                            ctx.scan_stats.old_files_skipped += 1;
                            ctx.file_mod_times.insert(path_buf.clone(), mod_time);
                            continue;
                        }
                    }
                    ctx.file_mod_times.insert(path_buf.clone(), mod_time);
                }
                log_file_detection(path, "scan_created");
                event = Some(FileEvent::Created(path_buf));
            } else if let (Some(mod_time), Some(previous)) =
                (modified_time, ctx.file_mod_times.get(&path_buf))
            {
                if mod_time > *previous {
                    ctx.file_mod_times.insert(path_buf.clone(), mod_time);
                    let now = Instant::now();
                    let should_emit_modified = ctx.last_event_time
                        .get(&path_buf)
                        .map(|last| now.duration_since(*last) > MODIFIED_EVENT_THROTTLE)
                        .unwrap_or(true);

                    if should_emit_modified {
                        log_file_detection(path, "scan_modified");
                        event = Some(FileEvent::Modified(path_buf));
                    } else {
                        trace!(path = %path.display(), "throttling modified event");
                    }
                }
            } else if let Some(mod_time) = modified_time {
                ctx.file_mod_times.entry(path_buf.clone()).or_insert(mod_time);
            }

            if let Some(event) = event {
                record_activity(&event, ctx.file_activity, ctx.last_event_time);
                send_event(ctx.event_tx, event, "scan");
            }
        }
    }
}

fn record_activity(
    event: &FileEvent,
    file_activity: &mut HashMap<PathBuf, Instant>,
    last_event_time: &mut HashMap<PathBuf, Instant>,
) {
    match event {
        FileEvent::Created(path) | FileEvent::Modified(path) => {
            let now = Instant::now();
            file_activity.insert(path.clone(), now);
            last_event_time.insert(path.clone(), now);
        }
        FileEvent::Closed(path) => {
            file_activity.remove(path);
            last_event_time.remove(path);
        }
    }
}

fn close_idle_files(
    file_activity: &mut HashMap<PathBuf, Instant>,
    last_event_time: &mut HashMap<PathBuf, Instant>,
    event_tx: &mpsc::Sender<FileEvent>,
) {
    let now = Instant::now();
    let mut stale_paths = Vec::new();

    for (path, last_activity) in file_activity.iter() {
        if now.duration_since(*last_activity) > IDLE_TIMEOUT {
            stale_paths.push(path.clone());
        }
    }

    for path in stale_paths {
        file_activity.remove(&path);
        last_event_time.remove(&path);
        debug!(path = %path.display(), "closing idle file after inactivity");
        send_event(event_tx, FileEvent::Closed(path), "idle_close");
    }
}

fn send_event(event_tx: &mpsc::Sender<FileEvent>, event: FileEvent, kind: &str) {
    let event_path = match &event {
        FileEvent::Created(p) | FileEvent::Modified(p) | FileEvent::Closed(p) => {
            p.display().to_string()
        }
    };

    if let Err(err) = event_tx.try_send(event) {
        match err {
            TrySendError::Full(_) => {
                warn!(kind, path = %event_path, "watcher channel full; dropping file event");
            }
            TrySendError::Closed(_) => {
                warn!(kind, path = %event_path, "watcher channel closed; dropping file event");
            }
        }
    } else {
        trace!(kind, path = %event_path, "file event sent successfully");
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
