//! File-based mode runner.
//!
//! Handles execution when the agent is configured to read from local files.

use crate::checkpoint::CheckpointDB;
use crate::config::Config;
use crate::parsers::block_merger::BlockMerger;
use crate::parsers::hash_store::HashStore;
use crate::parsers::route_parser;
use crate::runner::{build_record_sink, resolve_batch_size};
use crate::tailer::{tail_file, TailerConfig};
use crate::watcher::{watch_directories, FileEvent, WATCHER_CHANNEL_CAPACITY};
use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, mpsc::error::TrySendError, Mutex, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

struct TailerHandle {
    handle: JoinHandle<()>,
    shutdown_tx: mpsc::Sender<()>,
}

struct FileActivity {
    last_size: u64,
    last_checked: SystemTime,
}

struct FileRunner {
    config: Config,
    checkpoint_db: Arc<CheckpointDB>,
    tailer_config: TailerConfig,
    watch_paths: Vec<PathBuf>,
    skip_historical: bool,
    tail_bytes: u64,
    event_tx: mpsc::Sender<FileEvent>,
    event_rx: mpsc::Receiver<FileEvent>,
    active_tailers: Arc<Mutex<HashMap<PathBuf, TailerHandle>>>,
    file_activity: Arc<Mutex<HashMap<PathBuf, FileActivity>>>,
    tailer_semaphore: Arc<Semaphore>,
    cancel_token: CancellationToken,
    watcher_handle: Option<JoinHandle<()>>,
    activity_monitor_handle: Option<JoinHandle<()>>,
}

impl FileRunner {
    async fn new(config: &Config) -> Result<Self> {
        let cancel_token = CancellationToken::new();
        let batch_size = resolve_batch_size(config.sorter.batch_size);
        let watch_paths = config.watch_paths();
        let poll_interval = Duration::from_millis(config.watcher.poll_interval_ms);
        let checkpoint_db = Arc::new(CheckpointDB::new(config.checkpoint_db_path())?);
        let record_sink = build_record_sink(config, cancel_token.clone()).await?;
        let tailer_config = TailerConfig {
            checkpoint_db: checkpoint_db.clone(),
            record_sink: record_sink.clone(),
            batch_size,
            poll_interval,
            bulk_load_warn_bytes: config.performance.bulk_load_warn_bytes,
            bulk_load_abort_bytes: config.performance.bulk_load_abort_bytes,
            cancel_token: cancel_token.clone(),
            skip_checkpoints: config.skip_historical(),
        };

        let (event_tx, event_rx) = mpsc::channel(WATCHER_CHANNEL_CAPACITY);

        // Clear all checkpoints when skip_historical is enabled to ensure fresh starts
        if config.skip_historical() {
            info!("skip_historical enabled - clearing all checkpoints for fresh start");
            checkpoint_db.clear_all().await?;
        }

        Ok(Self {
            config: config.clone(),
            checkpoint_db,
            tailer_config,
            watch_paths,
            skip_historical: config.skip_historical(),
            tail_bytes: config.tail_bytes(),
            event_tx,
            event_rx,
            active_tailers: Arc::new(Mutex::new(HashMap::new())),
            file_activity: Arc::new(Mutex::new(HashMap::new())),
            tailer_semaphore: Arc::new(Semaphore::new(config.performance.max_concurrent_tailers)),
            cancel_token,
            watcher_handle: None,
            activity_monitor_handle: None,
        })
    }

    fn start_background_tasks(&mut self) {
        let poll_interval = Duration::from_millis(self.config.watcher.poll_interval_ms);
        let paths = self.watch_paths.clone();
        let event_tx = self.event_tx.clone();
        let cancel_token = self.cancel_token.clone();
        let skip_historical = self.skip_historical;

        self.watcher_handle = Some(tokio::spawn(async move {
            if let Err(err) = watch_directories(
                paths,
                poll_interval,
                event_tx,
                cancel_token,
                skip_historical,
            )
            .await
            {
                error!(error = %err, "file watcher exited unexpectedly");
            }
        }));

        let inactive_timeout = Duration::from_secs(self.config.watcher.inactive_timeout_sec);
        self.activity_monitor_handle = Some(tokio::spawn(activity_monitor(
            self.active_tailers.clone(),
            self.file_activity.clone(),
            inactive_timeout,
            self.cancel_token.clone(),
        )));
    }

    async fn seed_existing_files(&mut self) -> Result<()> {
        if self.skip_historical {
            info!("skip_historical enabled; deferring file discovery to watcher");
            return Ok(());
        }

        let mut existing_files = discover_existing_files(&self.watch_paths)?;
        let checkpoint_db = self.checkpoint_db.clone();

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
            skip_historical = self.skip_historical,
            file_count = existing_files.len(),
            "seeding existing files"
        );

        for path in existing_files {
            if let Err(err) = maybe_skip_historical_for_path(
                &path,
                &checkpoint_db,
                self.skip_historical,
                self.tail_bytes,
                false,
            )
            .await
            {
                warn!(
                    error = %err,
                    path = %path.display(),
                    "failed to initialize checkpoint; will use default behavior"
                );
            }

            spawn_tailer_if_needed(
                &self.active_tailers,
                &self.file_activity,
                path,
                self.tailer_semaphore.clone(),
                self.tailer_config.clone(),
            )
            .await;
        }
        Ok(())
    }

    async fn handle_file_event(&mut self, event: FileEvent) {
        let event_type = match &event {
            FileEvent::Created(_) => "Created",
            FileEvent::Modified(_) => "Modified",
            FileEvent::Closed(_) => "Closed",
        };
        let path_display = match &event {
            FileEvent::Created(p) | FileEvent::Modified(p) | FileEvent::Closed(p) => {
                p.display().to_string()
            }
        };
        debug!(event_type, path = %path_display, "received file event from watcher");

        if let FileEvent::Closed(path) = event {
            shutdown_tailer_for_path(&self.active_tailers, &self.file_activity, &path).await;
            return;
        }

        let (path, is_new_file) = event_details(&event);
        if let Err(err) = maybe_skip_historical_for_path(
            &path,
            &self.checkpoint_db,
            self.skip_historical,
            self.tail_bytes,
            is_new_file,
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
            &self.active_tailers,
            &self.file_activity,
            path,
            self.tailer_semaphore.clone(),
            self.tailer_config.clone(),
        )
        .await;
    }

    async fn drain_tailers(&mut self, shutdown_timeout: Duration) {
        info!("stopping tailers");
        let mut draining_tailers: Vec<(PathBuf, TailerHandle)> = {
            let mut guard = self.active_tailers.lock().await;
            guard.drain().collect()
        };
        self.file_activity.lock().await.clear();

        for (path, mut handle) in draining_tailers.drain(..) {
            let _ = handle.shutdown_tx.try_send(());
            let timeout = sleep(shutdown_timeout);
            tokio::pin!(timeout);

            let result = tokio::select! {
                res = &mut handle.handle => Some(res),
                _ = &mut timeout => None,
            };

            match result {
                Some(Ok(())) => info!(path = %path.display(), "tailer stopped"),
                Some(Err(err)) => {
                    warn!(path = %path.display(), error = %err, "tailer exited with error during shutdown");
                }
                None => {
                    warn!(path = %path.display(), "tailer did not stop within timeout; aborting");
                    handle.handle.abort();
                }
            }
        }
    }

    async fn stop_activity_monitor(&mut self, shutdown_timeout: Duration) {
        if let Some(mut handle) = self.activity_monitor_handle.take() {
            let monitor_timeout = sleep(shutdown_timeout);
            tokio::pin!(monitor_timeout);
            let monitor_result = tokio::select! {
                res = &mut handle => Some(res),
                _ = &mut monitor_timeout => None,
            };

            match monitor_result {
                Some(Ok(())) => info!("file activity monitor stopped"),
                Some(Err(err)) => {
                    warn!(error = %err, "file activity monitor exited with error during shutdown")
                }
                None => {
                    warn!("file activity monitor did not stop within timeout; aborting");
                    handle.abort();
                }
            }
        }
    }

    async fn stop_watcher(&mut self, shutdown_timeout: Duration) {
        if let Some(mut handle) = self.watcher_handle.take() {
            let watcher_timeout = sleep(shutdown_timeout);
            tokio::pin!(watcher_timeout);
            let watcher_result = tokio::select! {
                res = &mut handle => Some(res),
                _ = &mut watcher_timeout => None,
            };

            match watcher_result {
                Some(Ok(())) => info!("file watcher stopped"),
                Some(Err(err)) => {
                    warn!(error = %err, "file watcher exited with error during shutdown")
                }
                None => {
                    warn!("file watcher did not stop within timeout; aborting");
                    handle.abort();
                }
            }
        }
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.cancel_token.cancel();

        let shutdown_timeout = Duration::from_secs(5);
        self.drain_tailers(shutdown_timeout).await;
        self.stop_activity_monitor(shutdown_timeout).await;
        self.stop_watcher(shutdown_timeout).await;

        // Log final merger statistics
        BlockMerger::global().log_stats();
        HashStore::global().log_stats();

        Ok(())
    }

    async fn run(mut self) -> Result<()> {
        if let Err(err) = self.seed_existing_files().await {
            warn!(error = %err, "failed to discover existing files on startup");
        }

        self.start_background_tasks();

        info!("hl-agent started; awaiting file events");

        let mut shutdown_reason = "event channel closed".to_string();
        loop {
            tokio::select! {
                maybe_event = self.event_rx.recv() => {
                    match maybe_event {
                        Some(event) => self.handle_file_event(event).await,
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
                    shutdown_reason = "signal".to_string();
                    break;
                }
            }
        }

        info!(reason = shutdown_reason, "initiating shutdown");
        self.shutdown().await
    }
}

/// Run the agent in file mode.
pub async fn run(config: &Config) -> Result<()> {
    info!("hl-agent starting in file mode");

    let runner = FileRunner::new(config).await?;
    runner.run().await
}

fn event_details(event: &FileEvent) -> (PathBuf, bool) {
    match event {
        FileEvent::Created(path) => (path.clone(), true),
        FileEvent::Modified(path) => (path.clone(), false),
        FileEvent::Closed(path) => (path.clone(), false),
    }
}

async fn maybe_skip_historical_for_path(
    path: &PathBuf,
    checkpoint_db: &Arc<CheckpointDB>,
    skip_historical: bool,
    _tail_bytes: u64,
    is_new_file: bool,
) -> Result<()> {
    // When skip_historical=true, DON'T create any checkpoints during file discovery.
    // The tailer will handle starting from the current end of file naturally.
    // This applies to BOTH existing files AND newly created files.
    // Checkpoints will be created as tailers process data.
    if skip_historical {
        info!(
            path = %path.display(),
            is_new_file,
            "skip_historical enabled - letting tailer start from current end of file"
        );
        return Ok(());
    }

    // skip_historical=false: Process all data from the beginning

    // If this is a newly created file detected by watcher, start from 0
    if is_new_file {
        let metadata = tokio::fs::metadata(path).await?;
        let last_modified = metadata.modified().unwrap_or(UNIX_EPOCH);
        let last_modified_ts = last_modified
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs() as i64;
        let file_size = metadata.len();

        checkpoint_db
            .set_offset(path, 0, file_size, last_modified_ts, 0)
            .await?;

        info!(
            path = %path.display(),
            "Newly created file detected - starting from beginning"
        );
        return Ok(());
    }

    // Respect existing checkpoints (resume where we left off)
    let existing_checkpoint = checkpoint_db.get(path).await?;
    if existing_checkpoint.is_some() {
        // Checkpoint exists and we're not skipping history - respect it
        return Ok(());
    }
    // No checkpoint exists and skip_historical=false - start from beginning (offset 0)
    Ok(())
}

async fn spawn_tailer_if_needed(
    active_tailers: &Arc<Mutex<HashMap<PathBuf, TailerHandle>>>,
    file_activity: &Arc<Mutex<HashMap<PathBuf, FileActivity>>>,
    path: PathBuf,
    tailer_semaphore: Arc<Semaphore>,
    config: TailerConfig,
) {
    {
        let mut tailers = active_tailers.lock().await;
        if let Some(handle) = tailers.get(&path) {
            if handle.handle.is_finished() {
                tailers.remove(&path);
            } else {
                return;
            }
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
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
    let tailer_config = config;
    let handle = tokio::spawn(async move {
        let permit = semaphore.acquire_owned().await;
        if permit.is_err() {
            warn!(path = %tail_path.display(), "tailer semaphore closed; skipping file");
            return;
        }
        let _permit = permit.unwrap();
        if let Err(err) =
            tail_file(tail_path.clone(), parsers, tailer_config, Some(shutdown_rx)).await
        {
            error!(error = %err, path = %tail_path.display(), "tailer terminated with error");
        }
    });

    let now = SystemTime::now();
    let initial_size = match tokio::fs::metadata(&path).await {
        Ok(metadata) => metadata.len(),
        Err(err) => {
            debug!(error = %err, path = %path.display(), "failed to read metadata for activity tracking");
            0
        }
    };

    {
        let mut activity = file_activity.lock().await;
        activity
            .entry(path.clone())
            .and_modify(|state| {
                state.last_size = initial_size;
                state.last_checked = now;
            })
            .or_insert(FileActivity {
                last_size: initial_size,
                last_checked: now,
            });
    }

    let mut tailers = active_tailers.lock().await;
    tailers.insert(
        path,
        TailerHandle {
            handle,
            shutdown_tx,
        },
    );
}

async fn shutdown_tailer_for_path(
    active_tailers: &Arc<Mutex<HashMap<PathBuf, TailerHandle>>>,
    file_activity: &Arc<Mutex<HashMap<PathBuf, FileActivity>>>,
    path: &PathBuf,
) {
    let handle = {
        let mut tailers = active_tailers.lock().await;
        tailers.remove(path)
    };

    let handle = match handle {
        Some(handle) => handle,
        None => {
            debug!(path = %path.display(), "close event received but no active tailer found");
            return;
        }
    };

    file_activity.lock().await.remove(path);
    match handle.shutdown_tx.try_send(()) {
        Ok(_) => info!(path = %path.display(), "stopping tailer due to idle close event"),
        Err(TrySendError::Full(_)) => {
            debug!(path = %path.display(), "tailer shutdown already signaled");
        }
        Err(TrySendError::Closed(_)) => {
            debug!(path = %path.display(), "tailer shutdown channel closed; tailer likely exiting");
        }
    }

    if let Err(err) = handle.handle.await {
        warn!(path = %path.display(), error = %err, "tailer task finished with error during idle shutdown");
    }
}

async fn activity_monitor(
    active_tailers: Arc<Mutex<HashMap<PathBuf, TailerHandle>>>,
    file_activity: Arc<Mutex<HashMap<PathBuf, FileActivity>>>,
    inactive_timeout: Duration,
    cancel_token: CancellationToken,
) {
    let poll_interval = Duration::from_secs(10);

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                debug!("file activity monitor received shutdown signal");
                break;
            }
            _ = sleep(poll_interval) => {}
        }

        let now = SystemTime::now();

        let finished_tailers: Vec<(PathBuf, TailerHandle)> = {
            let mut tailers = active_tailers.lock().await;
            let finished_paths: Vec<PathBuf> = tailers
                .iter()
                .filter(|(_, handle)| handle.handle.is_finished())
                .map(|(path, _)| path.clone())
                .collect();

            finished_paths
                .into_iter()
                .filter_map(|path| tailers.remove(&path).map(|handle| (path, handle)))
                .collect()
        };

        if !finished_tailers.is_empty() {
            let mut activity = file_activity.lock().await;
            for (path, tailer) in finished_tailers {
                activity.remove(&path);
                match tailer.handle.await {
                    Ok(()) => {
                        debug!(path = %path.display(), "tailer completed; removed from active list")
                    }
                    Err(err) => {
                        warn!(path = %path.display(), error = %err, "tailer task finished with error")
                    }
                }
            }
        }

        let paths_to_check: Vec<PathBuf> = {
            let tailers = active_tailers.lock().await;
            tailers.keys().cloned().collect()
        };

        for path in paths_to_check {
            let metadata = match tokio::fs::metadata(&path).await {
                Ok(meta) => meta,
                Err(err) => {
                    debug!(
                        error = %err,
                        path = %path.display(),
                        "metadata unavailable during activity check; skipping"
                    );
                    continue;
                }
            };

            let current_size = metadata.len();
            let mut should_shutdown = false;

            {
                let mut activity = file_activity.lock().await;
                let entry = activity.entry(path.clone()).or_insert(FileActivity {
                    last_size: current_size,
                    last_checked: now,
                });

                if current_size != entry.last_size {
                    entry.last_size = current_size;
                    entry.last_checked = now;
                } else {
                    let inactive_duration = now
                        .duration_since(entry.last_checked)
                        .unwrap_or_else(|_| Duration::from_secs(0));

                    if inactive_duration >= inactive_timeout {
                        entry.last_checked = now;
                        should_shutdown = true;
                    }
                }
            }

            if should_shutdown {
                let shutdown_tx = {
                    let tailers = active_tailers.lock().await;
                    tailers.get(&path).map(|handle| handle.shutdown_tx.clone())
                };

                if let Some(tx) = shutdown_tx {
                    match tx.try_send(()) {
                        Ok(_) => info!(path = %path.display(), "stopping tailer due to inactivity"),
                        Err(TrySendError::Full(_)) => {
                            debug!(path = %path.display(), "tailer inactivity shutdown already signaled");
                        }
                        Err(TrySendError::Closed(_)) => {
                            debug!(path = %path.display(), "tailer shutdown channel closed; assuming tailer is exiting");
                        }
                    }
                }
            }
        }
    }
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
