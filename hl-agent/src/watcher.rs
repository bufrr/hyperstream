use anyhow::{Context, Result};
use notify::{
    Config as NotifyConfig, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::time::sleep;
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

    // Keep the watcher alive for the lifetime of the process.
    loop {
        sleep(Duration::from_secs(3600)).await;
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
