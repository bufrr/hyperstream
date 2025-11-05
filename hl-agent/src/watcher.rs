use anyhow::{Context, Result};
use notify::{
    Config as NotifyConfig, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone)]
pub enum FileEvent {
    Created(PathBuf),
    Modified(PathBuf),
}

pub async fn watch_directories(
    watch_paths: Vec<PathBuf>,
    poll_interval: Duration,
    event_tx: mpsc::UnboundedSender<FileEvent>,
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

fn handle_event(event_tx: &mpsc::UnboundedSender<FileEvent>, event: Event) {
    match event.kind {
        EventKind::Create(_) => {
            for path in event.paths {
                if path.is_dir() {
                    continue;
                }
                debug!(path = %path.display(), "detected file creation");
                if let Err(err) = event_tx.send(FileEvent::Created(path)) {
                    warn!(error = %err, "failed to enqueue created file event");
                }
            }
        }
        EventKind::Modify(_) => {
            for path in event.paths {
                if path.is_dir() {
                    continue;
                }
                debug!(path = %path.display(), "detected file modification");
                if let Err(err) = event_tx.send(FileEvent::Modified(path)) {
                    warn!(error = %err, "failed to enqueue modified file event");
                }
            }
        }
        _ => {}
    }
}
