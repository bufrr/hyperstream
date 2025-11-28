use crate::checkpoint::CheckpointDB;
use crate::output_writer::RecordSink;
use crate::parsers;
use crate::sorter_client::proto::DataRecord;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const DEFAULT_POLL_INTERVAL_MS: u64 = 100;
const MAX_READ_CHUNK_BYTES: usize = 8 * 1024 * 1024; // 8 MiB per iteration
const FILE_SIZE_STABLE_POLLS: u32 = 2; // Number of polls with unchanged size to consider file stable

/// Configuration for file tailers.
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

pub async fn tail_file(
    file_path: PathBuf,
    active_parsers: Vec<Box<dyn parsers::Parser>>,
    config: TailerConfig,
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
    let mut read_offset = checkpoint.as_ref().map(|rec| rec.byte_offset).unwrap_or(0);
    let initial_line_count = checkpoint.as_ref().map(|rec| rec.line_count).unwrap_or(0);
    for parser in active_parsers.iter_mut() {
        parser.set_initial_line_count(initial_line_count);
    }
    let sleep_interval = if poll_interval.is_zero() {
        Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)
    } else {
        poll_interval
    };

    // Detect if this is a blocks file requiring bulk loading
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
                cancel_token,
            },
            sleep_interval,
        )
        .await;
    }

    info!(path = %file_path.display(), offset = read_offset, "starting tailer");

    loop {
        let loop_start = std::time::Instant::now();

        let metadata = match fs::metadata(&file_path).await {
            Ok(meta) => meta,
            Err(err) => {
                debug!(
                    error = %err,
                    path = %file_path.display(),
                    "metadata unavailable; retrying"
                );
                if sleep_or_cancel(sleep_interval, &cancel_token).await {
                    return Ok(());
                }
                continue;
            }
        };

        let mut file_size = metadata.len();
        let mut last_modified_ts = metadata
            .modified()
            .ok()
            .and_then(system_time_seconds)
            .unwrap_or_default();

        if file_size < read_offset {
            warn!(
                path = %file_path.display(),
                previous_offset = read_offset,
                current_size = file_size,
                "file truncated or rotated; resetting parser state"
            );
            read_offset = 0;
            active_parsers = parsers::route_parser(&file_path)?;
            for parser in active_parsers.iter_mut() {
                parser.set_initial_line_count(0);
            }
            checkpoint_db
                .set_offset(&file_path, 0, file_size, last_modified_ts, 0)
                .await?;
            if sleep_or_cancel(sleep_interval, &cancel_token).await {
                return Ok(());
            }
            continue;
        }

        if file_size > read_offset {
            let chunk_start = read_offset;
            let bytes_available = file_size.saturating_sub(chunk_start);
            let bytes_to_read = bytes_available.min(MAX_READ_CHUNK_BYTES as u64) as usize;
            if bytes_to_read == 0 {
                if sleep_or_cancel(sleep_interval, &cancel_token).await {
                    return Ok(());
                }
                continue;
            }

            match read_new_bytes(&file_path, chunk_start, bytes_to_read).await {
                Ok(buffer) => {
                    if buffer.is_empty() {
                        if sleep_or_cancel(sleep_interval, &cancel_token).await {
                            return Ok(());
                        }
                        continue;
                    }

                    let read_complete = std::time::Instant::now();
                    let read_latency_ms = read_complete.duration_since(loop_start).as_millis();

                    let next_read_offset = chunk_start
                        .checked_add(buffer.len() as u64)
                        .unwrap_or(chunk_start);

                    let mut records: Vec<DataRecord> = Vec::new();
                    let mut parse_failed = false;
                    debug!(
                        path = %file_path.display(),
                        parser_count = active_parsers.len(),
                        chunk_len = buffer.len(),
                        "processing file chunk"
                    );
                    for (parser_index, parser) in active_parsers.iter_mut().enumerate() {
                        let parse_start = std::time::Instant::now();
                        match parser.parse(&file_path, &buffer) {
                            Ok(mut parsed_records) => {
                                let parse_duration_ms = parse_start.elapsed().as_millis();
                                debug!(
                                    path = %file_path.display(),
                                    parser_index,
                                    returned_count = parsed_records.len(),
                                    parse_duration_ms,
                                    "parser produced records"
                                );
                                records.append(&mut parsed_records);
                            }
                            Err(err) => {
                                warn!(
                                    error = %err,
                                    path = %file_path.display(),
                                    parser_index,
                                    chunk_start,
                                    bytes_read = buffer.len(),
                                    "parse failed; continuing without retry (file data may be corrupted)"
                                );
                                parse_failed = true;
                                break;
                            }
                        }
                    }

                    if parse_failed {
                        read_offset = next_read_offset;
                        let line_count = max_line_count(&active_parsers);
                        // Persist checkpoint even when parsing fails to avoid re-reading corrupt data
                        if let Ok(meta) = fs::metadata(&file_path).await {
                            let file_size = meta.len();
                            let last_modified_ts = meta
                                .modified()
                                .ok()
                                .and_then(system_time_seconds)
                                .unwrap_or_default();
                            let _ = checkpoint_db
                                .set_offset(
                                    &file_path,
                                    read_offset,
                                    file_size,
                                    last_modified_ts,
                                    line_count,
                                )
                                .await;
                        }
                        if sleep_or_cancel(sleep_interval, &cancel_token).await {
                            return Ok(());
                        }
                        continue;
                    }

                    read_offset = next_read_offset;

                    let parse_complete = std::time::Instant::now();
                    let parse_latency_ms = parse_complete.duration_since(read_complete).as_millis();

                    if let Ok(meta) = fs::metadata(&file_path).await {
                        file_size = meta.len();
                        last_modified_ts = meta
                            .modified()
                            .ok()
                            .and_then(system_time_seconds)
                            .unwrap_or(last_modified_ts);
                    }

                    let backlog = active_parsers
                        .iter()
                        .map(|parser| parser.backlog_len())
                        .max()
                        .unwrap_or(0);
                    let checkpoint_offset = read_offset.saturating_sub(backlog as u64);
                    let line_count = max_line_count(&active_parsers);

                    let had_records = !records.is_empty();
                    if had_records {
                        let total_records = records.len();
                        let mut topic_counts: HashMap<String, usize> = HashMap::new();
                        for record in &records {
                            *topic_counts.entry(record.topic.clone()).or_default() += 1;
                        }

                        // Log all topics at INFO level for visibility
                        for (topic, count) in &topic_counts {
                            info!(
                                path = %file_path.display(),
                                %topic,
                                record_count = count,
                                "{} records emitted from parser chunk", topic
                            );
                        }

                        let planned_batches = if batch_size == 0 {
                            1
                        } else {
                            total_records.div_ceil(batch_size)
                        };

                        for (topic, count) in topic_counts {
                            debug!(
                                path = %file_path.display(),
                                %topic,
                                record_count = count,
                                total_records,
                                batch_size,
                                planned_batches,
                                "parser produced records; handing off to sink"
                            );
                        }

                        let batches: Vec<Vec<DataRecord>> = if batch_size == 0 {
                            vec![records]
                        } else {
                            records
                                .chunks(batch_size)
                                .map(|chunk| chunk.to_vec())
                                .collect()
                        };

                        debug!(
                            path = %file_path.display(),
                            batch_count = batches.len(),
                            batch_size,
                            total_records,
                            "dispatching parsed batches"
                        );

                        let file_path_str = file_path.to_string_lossy().to_string();
                        for (batch_idx, chunk) in batches.into_iter().enumerate() {
                            let record_count = chunk.len();
                            record_sink
                                .send_batch(file_path_str.clone(), chunk_start, chunk)
                                .await?;

                            let send_complete = std::time::Instant::now();
                            let total_latency_ms =
                                send_complete.duration_since(loop_start).as_millis();

                            info!(
                                path = %file_path.display(),
                                batch_idx,
                                record_count,
                                read_latency_ms,
                                parse_latency_ms,
                                total_latency_ms,
                                "batch sent - latency breakdown"
                            );
                        }
                    }

                    checkpoint_db
                        .set_offset(
                            &file_path,
                            checkpoint_offset,
                            file_size,
                            last_modified_ts,
                            line_count,
                        )
                        .await?;

                    if !had_records {
                        if sleep_or_cancel(sleep_interval, &cancel_token).await {
                            return Ok(());
                        }
                        continue;
                    }
                }
                Err(err) => {
                    warn!(
                        error = %err,
                        path = %file_path.display(),
                        "failed to read newly appended bytes"
                    );
                }
            }
        }

        if sleep_or_cancel(sleep_interval, &cancel_token).await {
            return Ok(());
        }
    }
}

async fn read_new_bytes(path: &PathBuf, offset: u64, max_bytes: usize) -> Result<Vec<u8>> {
    let mut file = fs::File::open(path)
        .await
        .with_context(|| format!("failed to open {}", path.display()))?;
    file.seek(tokio::io::SeekFrom::Start(offset))
        .await
        .with_context(|| format!("failed to seek {} to offset {}", path.display(), offset))?;
    if max_bytes == 0 {
        return Ok(Vec::new());
    }

    let mut buffer = vec![0u8; max_bytes];
    let mut total_read = 0usize;
    while total_read < max_bytes {
        let bytes_read = file
            .read(&mut buffer[total_read..])
            .await
            .with_context(|| format!("failed to read from {}", path.display()))?;
        if bytes_read == 0 {
            break;
        }
        total_read += bytes_read;
    }
    buffer.truncate(total_read);
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

/// Specialized tailer for periodic_abci_states files that bulk loads complete MessagePack objects.
///
/// Strategy:
/// 1. Monitor file size for stabilization (unchanged for FILE_SIZE_STABLE_POLLS cycles)
/// 2. Once stable, bulk load entire file in one operation
/// 3. Pass complete data to parser for MessagePack decoding
/// 4. Mark file as fully processed in checkpoint
///
/// This eliminates the 60+ second chunked reading for 846MB files, reducing to ~2-3 seconds.
async fn tail_blocks_file_bulk(
    file_path: PathBuf,
    active_parsers: Vec<Box<dyn parsers::Parser>>,
    config: TailerConfig,
    sleep_interval: Duration,
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

    info!(
        path = %file_path.display(),
        initial_offset,
        "starting bulk-load tailer for periodic_abci_states file"
    );

    let mut last_seen_size: Option<u64> = None;
    let mut stable_count: u32 = 0;

    loop {
        let loop_start = std::time::Instant::now();
        let checkpoint_offset = checkpoint_db.get_offset(&file_path).await?;

        let metadata = match fs::metadata(&file_path).await {
            Ok(meta) => meta,
            Err(err) => {
                debug!(
                    error = %err,
                    path = %file_path.display(),
                    "metadata unavailable; retrying"
                );
                if sleep_or_cancel(sleep_interval, &cancel_token).await {
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

        // Check if we've already processed this file completely
        if checkpoint_offset >= current_size {
            debug!(
                path = %file_path.display(),
                file_size = current_size,
                checkpoint_offset = checkpoint_offset,
                "file already fully processed; monitoring for changes"
            );
            if sleep_or_cancel(sleep_interval, &cancel_token).await {
                return Ok(());
            }
            last_seen_size = Some(current_size);
            continue;
        }

        // Check if file size has stabilized
        match last_seen_size {
            Some(prev_size) if prev_size == current_size => {
                stable_count += 1;
                debug!(
                    path = %file_path.display(),
                    file_size = current_size,
                    stable_count,
                    required = FILE_SIZE_STABLE_POLLS,
                    "file size unchanged"
                );
            }
            _ => {
                // Size changed or first observation
                stable_count = 1;
                last_seen_size = Some(current_size);
                debug!(
                    path = %file_path.display(),
                    file_size = current_size,
                    "file size changed or first observation; resetting stability counter"
                );
            }
        }

        // Wait for file to stabilize before bulk loading
        if stable_count < FILE_SIZE_STABLE_POLLS {
            if sleep_or_cancel(sleep_interval, &cancel_token).await {
                return Ok(());
            }
            continue;
        }

        // File is stable - bulk load unprocessed portion of the file
        let remaining_bytes = current_size.saturating_sub(checkpoint_offset);

        // Bulk loads hold the remaining portion in memory, so refuse to process oversized inputs.
        if remaining_bytes >= bulk_load_abort_bytes {
            error!(
                path = %file_path.display(),
                file_size = current_size,
                remaining_bytes,
                abort_threshold = bulk_load_abort_bytes,
                "bulk load aborted; refusing to read multi-GB file into memory"
            );
            return Err(anyhow::anyhow!(
                "bulk load aborted for {}; remaining bytes {} exceed threshold {}",
                file_path.display(),
                remaining_bytes,
                bulk_load_abort_bytes
            ));
        }

        if remaining_bytes >= bulk_load_warn_bytes {
            warn!(
                path = %file_path.display(),
                file_size = current_size,
                remaining_bytes,
                warn_threshold = bulk_load_warn_bytes,
                "bulk loading large file; entire buffer will be held in memory"
            );
        }

        info!(
            path = %file_path.display(),
            file_size = current_size,
            "file size stabilized; starting bulk load"
        );

        if remaining_bytes == 0 {
            debug!(
                path = %file_path.display(),
                checkpoint_offset,
                "no new data beyond checkpoint after stabilization"
            );
            stable_count = 0;
            last_seen_size = Some(current_size);
            if sleep_or_cancel(sleep_interval, &cancel_token).await {
                return Ok(());
            }
            continue;
        }

        match read_new_bytes(&file_path, checkpoint_offset, remaining_bytes as usize).await {
            Ok(buffer) => {
                if buffer.is_empty() {
                    warn!(path = %file_path.display(), "bulk load returned empty buffer");
                    if sleep_or_cancel(sleep_interval, &cancel_token).await {
                        return Ok(());
                    }
                    continue;
                }

                let read_complete = std::time::Instant::now();
                let read_latency_ms = read_complete.duration_since(loop_start).as_millis();

                info!(
                    path = %file_path.display(),
                    bytes_read = buffer.len(),
                    read_latency_ms,
                    "bulk load complete; parsing MessagePack data"
                );

                let mut records: Vec<DataRecord> = Vec::new();
                let mut parse_failed = false;

                for (parser_index, parser) in active_parsers.iter_mut().enumerate() {
                    match parser.parse(&file_path, &buffer) {
                        Ok(mut parsed_records) => {
                            debug!(
                                path = %file_path.display(),
                                parser_index,
                                record_count = parsed_records.len(),
                                "parser produced records"
                            );
                            records.append(&mut parsed_records);
                        }
                        Err(err) => {
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
                    if sleep_or_cancel(sleep_interval, &cancel_token).await {
                        return Ok(());
                    }
                    continue;
                }

                let parse_complete = std::time::Instant::now();
                let parse_latency_ms = parse_complete.duration_since(read_complete).as_millis();

                info!(
                    path = %file_path.display(),
                    record_count = records.len(),
                    parse_latency_ms,
                    "parsing complete"
                );

                let next_offset = checkpoint_offset.saturating_add(buffer.len() as u64);
                let line_count = max_line_count(&active_parsers);

                // Send records in batches
                if !records.is_empty() {
                    let total_records = records.len();
                    let mut topic_counts: HashMap<String, usize> = HashMap::new();
                    let mut blocks_count = 0usize;
                    let mut transactions_count = 0usize;
                    for record in &records {
                        match record.topic.as_str() {
                            "hl.blocks" => blocks_count += 1,
                            "hl.transactions" => transactions_count += 1,
                            _ => {}
                        }
                        *topic_counts.entry(record.topic.clone()).or_default() += 1;
                    }

                    if blocks_count > 0 {
                        info!(
                            path = %file_path.display(),
                            record_count = blocks_count,
                            "hl.blocks records emitted from bulk parser"
                        );
                    }
                    if transactions_count > 0 {
                        info!(
                            path = %file_path.display(),
                            record_count = transactions_count,
                            "hl.transactions records emitted from bulk parser"
                        );
                    }

                    for (topic, count) in topic_counts {
                        info!(
                            path = %file_path.display(),
                            %topic,
                            record_count = count,
                            total_records,
                            "sending records to sink"
                        );
                    }

                    let batches: Vec<Vec<DataRecord>> = if batch_size == 0 {
                        vec![records]
                    } else {
                        records
                            .chunks(batch_size)
                            .map(|chunk| chunk.to_vec())
                            .collect()
                    };

                    let file_path_str = file_path.to_string_lossy().to_string();
                    for chunk in batches {
                        // Bulk loads process the entire file at once; report the full byte offset so
                        // downstream diagnostics know we've consumed the whole file.
                        record_sink
                            .send_batch(file_path_str.clone(), next_offset, chunk)
                            .await?;
                    }

                    let send_complete = std::time::Instant::now();
                    let total_latency_ms = send_complete.duration_since(loop_start).as_millis();

                    info!(
                        path = %file_path.display(),
                        total_records,
                        read_latency_ms,
                        parse_latency_ms,
                        total_latency_ms,
                        "bulk load complete - all batches sent"
                    );
                }

                // Mark file as fully processed
                checkpoint_db
                    .set_offset(
                        &file_path,
                        next_offset.min(current_size),
                        current_size,
                        last_modified_ts,
                        line_count,
                    )
                    .await?;

                info!(
                    path = %file_path.display(),
                    checkpoint_offset = next_offset,
                    "file fully processed; monitoring for new files"
                );

                // Reset counters for potential file changes
                stable_count = 0;
                last_seen_size = Some(current_size);
            }
            Err(err) => {
                warn!(
                    error = %err,
                    path = %file_path.display(),
                    "failed to bulk load file; will retry"
                );
                stable_count = 0;
                last_seen_size = None;
            }
        }

        if sleep_or_cancel(sleep_interval, &cancel_token).await {
            return Ok(());
        }
    }
}
