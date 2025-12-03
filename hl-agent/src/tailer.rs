use crate::checkpoint::CheckpointDB;
use crate::output_writer::RecordSink;
use crate::parsers;
use crate::sorter_client::proto::DataRecord;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const DEFAULT_POLL_INTERVAL_MS: u64 = 100;
const MAX_READ_CHUNK_BYTES: usize = 8 * 1024 * 1024; // 8 MiB per iteration
const FILE_SIZE_STABLE_POLLS: u32 = 2; // Number of polls with unchanged size to consider file stable

#[derive(Clone)]
pub struct TailerConfig {
    pub checkpoint_db: Arc<CheckpointDB>,
    pub record_sink: Arc<dyn RecordSink>,
    pub batch_size: usize,
    pub poll_interval: Duration,
    pub bulk_load_warn_bytes: u64,
    pub bulk_load_abort_bytes: u64,
    pub cancel_token: CancellationToken,
}

async fn sleep_or_cancel(duration: Duration, cancel_token: &CancellationToken) -> bool {
    tokio::select! {
        biased;
        _ = cancel_token.cancelled() => true,
        _ = sleep(duration) => false,
    }
}

async fn sleep_or_shutdown(
    duration: Duration,
    cancel_token: &CancellationToken,
    shutdown_rx: &mut Option<mpsc::Receiver<()>>,
) -> bool {
    if let Some(rx) = shutdown_rx {
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => true,
            _ = rx.recv() => true,
            _ = sleep(duration) => false,
        }
    } else {
        sleep_or_cancel(duration, cancel_token).await
    }
}

struct ParsedChunk {
    chunk_start: u64,
    read_latency_ms: u128,
    parse_latency_ms: u128,
    parser_types: Vec<&'static str>,
    checkpoint_offset: u64,
    file_size: u64,
    last_modified_ts: i64,
    line_count: u64,
    records: Vec<DataRecord>,
}

enum ChunkOutcome {
    Parsed(ParsedChunk),
    ParseFailed,
    NoData,
}

enum ParseStatus {
    Success(Vec<DataRecord>),
    Failed,
}

struct TailerState {
    file_path: PathBuf,
    active_parsers: Vec<Box<dyn parsers::Parser>>,
    read_offset: u64,
    sleep_interval: Duration,
}

impl TailerState {
    fn new(
        file_path: PathBuf,
        active_parsers: Vec<Box<dyn parsers::Parser>>,
        read_offset: u64,
        sleep_interval: Duration,
    ) -> Self {
        Self {
            file_path,
            active_parsers,
            read_offset,
            sleep_interval,
        }
    }

    fn offset(&self) -> u64 {
        self.read_offset
    }

    async fn handle_file_truncation(
        &mut self,
        file_size: u64,
        last_modified_ts: i64,
        checkpoint_db: &CheckpointDB,
    ) -> Result<bool> {
        if file_size >= self.read_offset {
            return Ok(false);
        }

        warn!(
            path = %self.file_path.display(),
            previous_offset = self.read_offset,
            current_size = file_size,
            "file truncated or rotated; resetting parser state"
        );
        self.read_offset = 0;
        self.active_parsers = parsers::route_parser(&self.file_path)?;
        for parser in self.active_parsers.iter_mut() {
            parser.set_initial_line_count(0);
        }
        checkpoint_db
            .set_offset(&self.file_path, 0, file_size, last_modified_ts, 0)
            .await?;
        Ok(true)
    }

    fn parse_buffer(&mut self, buffer: &[u8], chunk_start: u64) -> ParseStatus {
        let mut records: Vec<DataRecord> = Vec::new();
        for (parser_index, parser) in self.active_parsers.iter_mut().enumerate() {
            let parser_type = parser.parser_type();
            let parse_start = std::time::Instant::now();
            match parser.parse(&self.file_path, buffer) {
                Ok(mut parsed_records) => {
                    let parse_duration = parse_start.elapsed();
                    let parse_duration_ms = parse_duration.as_millis();

                    crate::metrics::PARSE_DURATION
                        .with_label_values(&[parser_type])
                        .observe(parse_duration.as_secs_f64());

                    debug!(
                        path = %self.file_path.display(),
                        parser_index,
                        returned_count = parsed_records.len(),
                        parse_duration_ms,
                        "parser produced records"
                    );
                    records.append(&mut parsed_records);
                }
                Err(err) => {
                    let parse_duration = parse_start.elapsed();
                    crate::metrics::PARSE_DURATION
                        .with_label_values(&[parser_type])
                        .observe(parse_duration.as_secs_f64());
                    crate::metrics::PARSE_ERRORS_TOTAL
                        .with_label_values(&[parser_type, "parse_failed"])
                        .inc();
                    warn!(
                        error = %err,
                        path = %self.file_path.display(),
                        parser_index,
                        chunk_start,
                        bytes_read = buffer.len(),
                        "parse failed; continuing without retry"
                    );
                    return ParseStatus::Failed;
                }
            }
        }

        ParseStatus::Success(records)
    }

    async fn read_and_parse_chunk(
        &mut self,
        file_size: u64,
        last_modified_ts: i64,
        loop_start: std::time::Instant,
        checkpoint_db: &CheckpointDB,
    ) -> Result<ChunkOutcome> {
        let chunk_start = self.read_offset;
        let bytes_available = file_size.saturating_sub(chunk_start);
        let bytes_to_read = bytes_available.min(MAX_READ_CHUNK_BYTES as u64) as usize;
        if bytes_to_read == 0 {
            return Ok(ChunkOutcome::NoData);
        }

        let buffer = match read_new_bytes(&self.file_path, chunk_start, bytes_to_read).await {
            Ok(buffer) => buffer,
            Err(err) => {
                warn!(
                    error = %err,
                    path = %self.file_path.display(),
                    "failed to read newly appended bytes"
                );
                return Ok(ChunkOutcome::NoData);
            }
        };

        if buffer.is_empty() {
            return Ok(ChunkOutcome::NoData);
        }

        let read_complete = std::time::Instant::now();
        let read_latency_ms = read_complete.duration_since(loop_start).as_millis();
        crate::metrics::FILE_READ_DURATION
            .observe(read_complete.duration_since(loop_start).as_secs_f64());

        let next_read_offset = chunk_start
            .checked_add(buffer.len() as u64)
            .unwrap_or(chunk_start);

        debug!(
            path = %self.file_path.display(),
            parser_count = self.active_parsers.len(),
            chunk_len = buffer.len(),
            "processing file chunk"
        );

        let parse_result = self.parse_buffer(&buffer, chunk_start);
        let parse_complete = std::time::Instant::now();
        let parse_latency_ms = parse_complete.duration_since(read_complete).as_millis();

        let records = match parse_result {
            ParseStatus::Success(records) => records,
            ParseStatus::Failed => {
                self.read_offset = next_read_offset;
                let line_count = max_line_count(&self.active_parsers);
                checkpoint_parse_failure(
                    &self.file_path,
                    self.read_offset,
                    file_size,
                    last_modified_ts,
                    line_count,
                    checkpoint_db,
                )
                .await?;

                return Ok(ChunkOutcome::ParseFailed);
            }
        };

        self.read_offset = next_read_offset;

        let mut updated_file_size = file_size;
        let mut updated_last_modified_ts = last_modified_ts;

        if let Ok(meta) = fs::metadata(&self.file_path).await {
            updated_file_size = meta.len();
            updated_last_modified_ts = meta
                .modified()
                .ok()
                .and_then(system_time_seconds)
                .unwrap_or(updated_last_modified_ts);
        }

        let backlog = self
            .active_parsers
            .iter()
            .map(|parser| parser.backlog_len())
            .max()
            .unwrap_or(0);
        let checkpoint_offset = self.read_offset.saturating_sub(backlog as u64);
        let line_count = max_line_count(&self.active_parsers);
        let parser_types = self
            .active_parsers
            .iter()
            .map(|parser| parser.parser_type())
            .collect();

        Ok(ChunkOutcome::Parsed(ParsedChunk {
            chunk_start,
            read_latency_ms,
            parse_latency_ms,
            parser_types,
            checkpoint_offset,
            file_size: updated_file_size,
            last_modified_ts: updated_last_modified_ts,
            line_count,
            records,
        }))
    }
}

pub async fn tail_file(
    file_path: PathBuf,
    active_parsers: Vec<Box<dyn parsers::Parser>>,
    config: TailerConfig,
    shutdown_rx: Option<mpsc::Receiver<()>>,
) -> Result<()> {
    let TailerConfig {
        checkpoint_db,
        record_sink,
        batch_size,
        poll_interval,
        bulk_load_warn_bytes,
        bulk_load_abort_bytes,
        cancel_token,
    } = config;

    let mut active_parsers = active_parsers;
    let checkpoint = checkpoint_db.get(&file_path).await?;
    if let Some(rec) = &checkpoint {
        debug!(path = %rec.file_path.display(), file_size = rec.file_size, last_modified_ts = rec.last_modified_ts, updated_at = rec.updated_at, "resuming from checkpoint");
    }
    let read_offset = checkpoint.as_ref().map(|rec| rec.byte_offset).unwrap_or(0);
    let initial_line_count = checkpoint.as_ref().map(|rec| rec.line_count).unwrap_or(0);
    for parser in active_parsers.iter_mut() {
        parser.set_initial_line_count(initial_line_count);
    }
    let sleep_interval = if poll_interval.is_zero() {
        Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)
    } else {
        poll_interval
    };

    let is_blocks_file = file_path
        .iter()
        .any(|component| component.to_str() == Some("periodic_abci_states"));

    if is_blocks_file {
        info!(path = %file_path.display(), "detected periodic_abci_states file - using bulk load strategy");
        return tail_blocks_file_bulk(
            file_path,
            active_parsers,
            TailerConfig {
                checkpoint_db,
                record_sink,
                batch_size,
                poll_interval,
                bulk_load_warn_bytes,
                bulk_load_abort_bytes,
                cancel_token: cancel_token.clone(),
            },
            sleep_interval,
            shutdown_rx,
        )
        .await;
    }

    info!(path = %file_path.display(), offset = read_offset, "starting tailer");

    let mut state = TailerState::new(file_path, active_parsers, read_offset, sleep_interval);

    run_tail_loop(
        &mut state,
        checkpoint_db,
        record_sink,
        batch_size,
        cancel_token,
        shutdown_rx,
    )
    .await
}

async fn run_tail_loop(
    state: &mut TailerState,
    checkpoint_db: Arc<CheckpointDB>,
    record_sink: Arc<dyn RecordSink>,
    batch_size: usize,
    cancel_token: CancellationToken,
    mut shutdown_rx: Option<mpsc::Receiver<()>>,
) -> Result<()> {
    loop {
        let loop_start = std::time::Instant::now();

        if let Some(rx) = shutdown_rx.as_mut() {
            match rx.try_recv() {
                Ok(_) => {
                    info!(
                        path = %state.file_path.display(),
                        "tailer received inactivity shutdown signal"
                    );
                    return Ok(());
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    info!(
                        path = %state.file_path.display(),
                        "shutdown channel closed; exiting tailer"
                    );
                    return Ok(());
                }
            }
        }

        let metadata = match fs::metadata(&state.file_path).await {
            Ok(meta) => meta,
            Err(err) => {
                debug!(
                    error = %err,
                    path = %state.file_path.display(),
                    "metadata unavailable; retrying"
                );
                if sleep_or_shutdown(state.sleep_interval, &cancel_token, &mut shutdown_rx).await {
                    info!(path = %state.file_path.display(), "tailer received shutdown signal");
                    return Ok(());
                }
                continue;
            }
        };

        let file_size = metadata.len();
        let last_modified_ts = metadata
            .modified()
            .ok()
            .and_then(system_time_seconds)
            .unwrap_or_default();

        if state
            .handle_file_truncation(file_size, last_modified_ts, &checkpoint_db)
            .await?
        {
            if sleep_or_shutdown(state.sleep_interval, &cancel_token, &mut shutdown_rx).await {
                info!(path = %state.file_path.display(), "tailer received shutdown signal");
                return Ok(());
            }
            continue;
        }

        if file_size > state.offset() {
            match state
                .read_and_parse_chunk(file_size, last_modified_ts, loop_start, &checkpoint_db)
                .await?
            {
                ChunkOutcome::Parsed(mut chunk) => {
                    let had_records = !chunk.records.is_empty();
                    dispatch_batches(
                        &state.file_path,
                        &mut chunk,
                        &record_sink,
                        batch_size,
                        loop_start,
                    )
                    .await?;
                    update_checkpoint(&state.file_path, &chunk, &checkpoint_db).await?;

                    if !had_records
                        && sleep_or_shutdown(state.sleep_interval, &cancel_token, &mut shutdown_rx)
                            .await
                    {
                        info!(path = %state.file_path.display(), "tailer received shutdown signal");
                        return Ok(());
                    }
                }
                ChunkOutcome::ParseFailed => {
                    if sleep_or_shutdown(state.sleep_interval, &cancel_token, &mut shutdown_rx)
                        .await
                    {
                        info!(path = %state.file_path.display(), "tailer received shutdown signal");
                        return Ok(());
                    }
                    continue;
                }
                ChunkOutcome::NoData => {}
            }
        }

        if sleep_or_shutdown(state.sleep_interval, &cancel_token, &mut shutdown_rx).await {
            info!(path = %state.file_path.display(), "tailer received shutdown signal");
            return Ok(());
        }
    }
}

async fn dispatch_batches(
    file_path: &PathBuf,
    chunk: &mut ParsedChunk,
    record_sink: &Arc<dyn RecordSink>,
    batch_size: usize,
    loop_start: Instant,
) -> Result<()> {
    if chunk.records.is_empty() {
        return Ok(());
    }

    let total_records = chunk.records.len();
    let planned_batches = if batch_size == 0 {
        1
    } else {
        total_records.div_ceil(batch_size)
    };
    let batches = chunk_records(std::mem::take(&mut chunk.records), batch_size);
    debug!(
        path = %file_path.display(),
        batch_count = batches.len(),
        batch_size,
        total_records,
        planned_batches,
        "dispatching parsed batches"
    );

    send_batches_with_metrics(
        record_sink,
        file_path,
        chunk.chunk_start,
        batches,
        |batch_idx, record_count| {
            let total_latency_ms = Instant::now().duration_since(loop_start).as_millis();
            for parser_type in &chunk.parser_types {
                crate::metrics::END_TO_END_LATENCY
                    .with_label_values(&[parser_type])
                    .observe(total_latency_ms as f64 / 1000.0);
            }
            info!(
                path = %file_path.display(),
                batch_idx,
                record_count,
                read_latency_ms = chunk.read_latency_ms,
                parse_latency_ms = chunk.parse_latency_ms,
                total_latency_ms,
                "batch sent"
            );
        },
    )
    .await
}

async fn update_checkpoint(
    file_path: &PathBuf,
    chunk: &ParsedChunk,
    checkpoint_db: &CheckpointDB,
) -> Result<()> {
    let checkpoint_start = std::time::Instant::now();
    checkpoint_db
        .set_offset(
            file_path,
            chunk.checkpoint_offset,
            chunk.file_size,
            chunk.last_modified_ts,
            chunk.line_count,
        )
        .await?;
    crate::metrics::CHECKPOINT_DURATION
        .with_label_values(&["set_offset"])
        .observe(checkpoint_start.elapsed().as_secs_f64());

    Ok(())
}

async fn checkpoint_parse_failure(
    file_path: &PathBuf,
    read_offset: u64,
    file_size: u64,
    last_modified_ts: i64,
    line_count: u64,
    checkpoint_db: &CheckpointDB,
) -> Result<()> {
    if let Err(err) = checkpoint_db
        .set_offset(
            file_path,
            read_offset,
            file_size,
            last_modified_ts,
            line_count,
        )
        .await
    {
        error!(
            error = %err,
            file_path = %file_path.display(),
            read_offset,
            "Failed to checkpoint after parse failure - aborting tailer to prevent data loss"
        );
        crate::metrics::CHECKPOINT_ERRORS_TOTAL
            .with_label_values(&["set_offset_after_parse_failure"])
            .inc();
        return Err(err).context("Checkpoint write failed after parse error");
    }

    Ok(())
}

async fn read_new_bytes(path: &PathBuf, offset: u64, max_bytes: usize) -> Result<Vec<u8>> {
    let mut file = fs::File::open(path)
        .await
        .with_context(|| format!("failed to open {}", path.display()))?;
    file.seek(tokio::io::SeekFrom::Start(offset))
        .await
        .with_context(|| format!("failed to seek {} to offset {}", path.display(), offset))?;
    let mut buffer = Vec::with_capacity(max_bytes);
    file.take(max_bytes as u64)
        .read_to_end(&mut buffer)
        .await
        .with_context(|| format!("failed to read from {}", path.display()))?;
    Ok(buffer)
}

fn system_time_seconds(time: SystemTime) -> Option<i64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs() as i64)
}

fn max_line_count(parsers: &[Box<dyn parsers::Parser>]) -> u64 {
    parsers
        .iter()
        .map(|parser| parser.get_line_count())
        .max()
        .unwrap_or(0)
}

fn chunk_records(records: Vec<DataRecord>, batch_size: usize) -> Vec<Vec<DataRecord>> {
    if batch_size == 0 {
        vec![records]
    } else {
        records
            .chunks(batch_size)
            .map(|chunk| chunk.to_vec())
            .collect()
    }
}

async fn send_batches_with_metrics<F>(
    sink: &Arc<dyn RecordSink>,
    file_path: &Path,
    base_offset: u64,
    batches: Vec<Vec<DataRecord>>,
    mut on_batch_sent: F,
) -> Result<()>
where
    F: FnMut(usize, usize),
{
    let file_path_str = file_path.to_string_lossy().to_string();

    for (batch_idx, chunk) in batches.into_iter().enumerate() {
        let record_count = chunk.len();

        let mut topic_counts: HashMap<String, u64> = HashMap::new();
        for record in &chunk {
            *topic_counts.entry(record.topic.clone()).or_insert(0) += 1;
        }
        for (topic, count) in topic_counts {
            crate::metrics::RECORDS_EMITTED_TOTAL
                .with_label_values(&[&topic])
                .inc_by(count);
        }

        let send_start = Instant::now();
        let send_result = sink
            .send_batch(file_path_str.clone(), base_offset, chunk)
            .await;
        let send_duration = send_start.elapsed();

        let status = if send_result.is_ok() {
            "success"
        } else {
            "error"
        };
        crate::metrics::SORTER_SEND_DURATION
            .with_label_values(&[status])
            .observe(send_duration.as_secs_f64());
        crate::metrics::BATCHES_SENT_TOTAL
            .with_label_values(&[status])
            .inc();

        send_result?;
        on_batch_sent(batch_idx, record_count);
    }

    Ok(())
}

async fn tail_blocks_file_bulk(
    file_path: PathBuf,
    active_parsers: Vec<Box<dyn parsers::Parser>>,
    config: TailerConfig,
    sleep_interval: Duration,
    mut shutdown_rx: Option<mpsc::Receiver<()>>,
) -> Result<()> {
    let TailerConfig {
        checkpoint_db,
        record_sink,
        batch_size,
        poll_interval: _,
        bulk_load_warn_bytes,
        bulk_load_abort_bytes,
        cancel_token,
    } = config;

    let mut active_parsers = active_parsers;
    let checkpoint = checkpoint_db.get(&file_path).await?;
    let initial_offset = checkpoint.as_ref().map(|rec| rec.byte_offset).unwrap_or(0);
    let initial_line_count = checkpoint.as_ref().map(|rec| rec.line_count).unwrap_or(0);
    for parser in active_parsers.iter_mut() {
        parser.set_initial_line_count(initial_line_count);
    }

    info!(path = %file_path.display(), initial_offset, "starting bulk-load tailer for periodic_abci_states file");

    let mut last_seen_size: Option<u64> = None;
    let mut stable_count: u32 = 0;

    loop {
        let loop_start = std::time::Instant::now();
        let checkpoint_offset = checkpoint_db.get_offset(&file_path).await?;

        let metadata = match fs::metadata(&file_path).await {
            Ok(meta) => meta,
            Err(err) => {
                debug!(error = %err, path = %file_path.display(), "metadata unavailable; retrying");
                if sleep_or_shutdown(sleep_interval, &cancel_token, &mut shutdown_rx).await {
                    info!(path = %file_path.display(), "tailer received shutdown signal");
                    return Ok(());
                }
                continue;
            }
        };

        let current_size = metadata.len();
        let last_modified_ts = metadata
            .modified()
            .ok()
            .and_then(system_time_seconds)
            .unwrap_or_default();

        if checkpoint_offset >= current_size {
            debug!(path = %file_path.display(), file_size = current_size, checkpoint_offset = checkpoint_offset, "file already fully processed; monitoring for changes");
            if sleep_or_shutdown(sleep_interval, &cancel_token, &mut shutdown_rx).await {
                info!(path = %file_path.display(), "tailer received shutdown signal");
                return Ok(());
            }
            last_seen_size = Some(current_size);
            continue;
        }

        match last_seen_size {
            Some(prev_size) if prev_size == current_size => {
                stable_count += 1;
                debug!(path = %file_path.display(), file_size = current_size, stable_count, required = FILE_SIZE_STABLE_POLLS, "file size unchanged");
            }
            _ => {
                stable_count = 1;
                last_seen_size = Some(current_size);
                debug!(path = %file_path.display(), file_size = current_size, "file size changed or first observation; resetting stability counter");
            }
        }

        // Wait for file to stabilize before bulk loading
        if stable_count < FILE_SIZE_STABLE_POLLS {
            if sleep_or_shutdown(sleep_interval, &cancel_token, &mut shutdown_rx).await {
                info!(path = %file_path.display(), "tailer received shutdown signal");
                return Ok(());
            }
            continue;
        }

        let remaining_bytes = current_size.saturating_sub(checkpoint_offset);

        if remaining_bytes >= bulk_load_abort_bytes {
            error!(path = %file_path.display(), file_size = current_size, remaining_bytes, abort_threshold = bulk_load_abort_bytes, "bulk load aborted; refusing to read multi-GB file into memory");
            return Err(anyhow::anyhow!(
                "bulk load aborted for {}; remaining bytes {} exceed threshold {}",
                file_path.display(),
                remaining_bytes,
                bulk_load_abort_bytes
            ));
        }

        if remaining_bytes >= bulk_load_warn_bytes {
            warn!(path = %file_path.display(), file_size = current_size, remaining_bytes, warn_threshold = bulk_load_warn_bytes, "bulk loading large file; entire buffer will be held in memory");
        }

        info!(path = %file_path.display(), file_size = current_size, "file size stabilized; starting bulk load");

        if remaining_bytes == 0 {
            debug!(path = %file_path.display(), checkpoint_offset, "no new data beyond checkpoint after stabilization");
            stable_count = 0;
            last_seen_size = Some(current_size);
            if sleep_or_shutdown(sleep_interval, &cancel_token, &mut shutdown_rx).await {
                info!(path = %file_path.display(), "tailer received shutdown signal");
                return Ok(());
            }
            continue;
        }

        match read_new_bytes(&file_path, checkpoint_offset, remaining_bytes as usize).await {
            Ok(buffer) => {
                if buffer.is_empty() {
                    warn!(path = %file_path.display(), "bulk load returned empty buffer");
                    if sleep_or_shutdown(sleep_interval, &cancel_token, &mut shutdown_rx).await {
                        info!(path = %file_path.display(), "tailer received shutdown signal");
                        return Ok(());
                    }
                    continue;
                }

                let read_complete = std::time::Instant::now();
                let read_latency_ms = read_complete.duration_since(loop_start).as_millis();
                crate::metrics::FILE_READ_DURATION
                    .observe(read_complete.duration_since(loop_start).as_secs_f64());

                info!(
                    path = %file_path.display(),
                    bytes_read = buffer.len(),
                    read_latency_ms,
                    "bulk load complete; parsing MessagePack data"
                );

                let mut records: Vec<DataRecord> = Vec::new();
                let mut parse_failed = false;

                for (parser_index, parser) in active_parsers.iter_mut().enumerate() {
                    let parser_type = parser.parser_type();
                    let parse_start = std::time::Instant::now();
                    match parser.parse(&file_path, &buffer) {
                        Ok(mut parsed_records) => {
                            let parse_duration = parse_start.elapsed();

                            // Record parse duration once per parser invocation (not per record)
                            crate::metrics::PARSE_DURATION
                                .with_label_values(&[parser_type])
                                .observe(parse_duration.as_secs_f64());

                            debug!(
                                path = %file_path.display(),
                                parser_index,
                                record_count = parsed_records.len(),
                                "parser produced records"
                            );
                            records.append(&mut parsed_records);
                        }
                        Err(err) => {
                            let parse_duration = parse_start.elapsed();
                            // Record parse duration and error even on failure
                            crate::metrics::PARSE_DURATION
                                .with_label_values(&[parser_type])
                                .observe(parse_duration.as_secs_f64());
                            crate::metrics::PARSE_ERRORS_TOTAL
                                .with_label_values(&[parser_type, "parse_failed"])
                                .inc();
                            warn!(
                                error = %err,
                                path = %file_path.display(),
                                parser_index,
                                bytes_read = buffer.len(),
                                "parse failed; file may be corrupted or incomplete"
                            );
                            parse_failed = true;
                            break;
                        }
                    }
                }

                if parse_failed {
                    // Reset for next attempt
                    stable_count = 0;
                    last_seen_size = None;
                    if sleep_or_shutdown(sleep_interval, &cancel_token, &mut shutdown_rx).await {
                        info!(path = %file_path.display(), "tailer received shutdown signal");
                        return Ok(());
                    }
                    continue;
                }

                let parse_complete = std::time::Instant::now();
                let parse_latency_ms = parse_complete.duration_since(read_complete).as_millis();

                info!(path = %file_path.display(), record_count = records.len(), parse_latency_ms, "parsing complete");

                let next_offset = checkpoint_offset.saturating_add(buffer.len() as u64);
                let line_count = max_line_count(&active_parsers);

                // Send records in batches
                if !records.is_empty() {
                    let total_records = records.len();
                    let batches = chunk_records(records, batch_size);
                    info!(path = %file_path.display(), total_records, batch_count = batches.len(), "sending bulk records");

                    // Bulk loads process the entire file at once; report the full byte offset so
                    // downstream diagnostics know we've consumed the whole file.
                    send_batches_with_metrics(
                        &record_sink,
                        &file_path,
                        next_offset,
                        batches,
                        |_, _| {},
                    )
                    .await?;

                    let total_latency = std::time::Instant::now().duration_since(loop_start);
                    for parser_type in active_parsers.iter().map(|parser| parser.parser_type()) {
                        crate::metrics::END_TO_END_LATENCY
                            .with_label_values(&[parser_type])
                            .observe(total_latency.as_secs_f64());
                    }
                    let total_latency_ms = total_latency.as_millis();
                    info!(path = %file_path.display(), total_records, read_latency_ms, parse_latency_ms, total_latency_ms, "bulk load complete - all batches sent");
                }

                // Mark file as fully processed
                // Record checkpoint duration to track bulk load progress
                let checkpoint_start = std::time::Instant::now();
                checkpoint_db
                    .set_offset(
                        &file_path,
                        next_offset.min(current_size),
                        current_size,
                        last_modified_ts,
                        line_count,
                    )
                    .await?;
                crate::metrics::CHECKPOINT_DURATION
                    .with_label_values(&["set_offset"])
                    .observe(checkpoint_start.elapsed().as_secs_f64());

                info!(path = %file_path.display(), checkpoint_offset = next_offset, "file fully processed; monitoring for new files");

                // Reset counters for potential file changes
                stable_count = 0;
                last_seen_size = Some(current_size);
            }
            Err(err) => {
                warn!(error = %err, path = %file_path.display(), "failed to bulk load file; will retry");
                stable_count = 0;
                last_seen_size = None;
            }
        }

        if sleep_or_shutdown(sleep_interval, &cancel_token, &mut shutdown_rx).await {
            info!(path = %file_path.display(), "tailer received shutdown signal");
            return Ok(());
        }
    }
}
