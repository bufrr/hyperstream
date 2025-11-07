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
use tracing::{debug, info, warn};

const DEFAULT_POLL_INTERVAL_MS: u64 = 100;
const MAX_PARSE_RETRIES: u32 = 3;
const MAX_READ_CHUNK_BYTES: usize = 8 * 1024 * 1024; // 8 MiB per iteration

pub async fn tail_file(
    file_path: PathBuf,
    checkpoint_db: Arc<CheckpointDB>,
    record_sink: Arc<dyn RecordSink>,
    batch_size: usize,
    poll_interval: Duration,
) -> Result<()> {
    let mut parser = parsers::route_parser(&file_path)?;
    let mut read_offset = checkpoint_db.get_offset(&file_path).await?;
    let sleep_interval = if poll_interval.is_zero() {
        Duration::from_millis(DEFAULT_POLL_INTERVAL_MS)
    } else {
        poll_interval
    };

    info!(path = %file_path.display(), offset = read_offset, "starting tailer");

    let mut parse_failures: u32 = 0;

    loop {
        let metadata = match fs::metadata(&file_path).await {
            Ok(meta) => meta,
            Err(err) => {
                debug!(
                    error = %err,
                    path = %file_path.display(),
                    "metadata unavailable; retrying"
                );
                sleep(sleep_interval).await;
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
            parser = parsers::route_parser(&file_path)?;
            parse_failures = 0;
            checkpoint_db
                .set_offset(&file_path, 0, file_size, last_modified_ts)
                .await?;
            sleep(sleep_interval).await;
            continue;
        }

        if file_size > read_offset {
            let chunk_start = read_offset;
            let bytes_available = file_size.saturating_sub(chunk_start);
            let bytes_to_read = bytes_available.min(MAX_READ_CHUNK_BYTES as u64) as usize;
            if bytes_to_read == 0 {
                sleep(sleep_interval).await;
                continue;
            }

            match read_new_bytes(&file_path, chunk_start, bytes_to_read).await {
                Ok(buffer) => {
                    if buffer.is_empty() {
                        sleep(sleep_interval).await;
                        continue;
                    }

                    let next_read_offset = chunk_start
                        .checked_add(buffer.len() as u64)
                        .unwrap_or(chunk_start);

                    let records = match parser.parse(&file_path, &buffer) {
                        Ok(records) => {
                            parse_failures = 0;
                            read_offset = next_read_offset;
                            records
                        }
                        Err(err) => {
                            parse_failures = parse_failures.saturating_add(1);
                            warn!(
                                error = %err,
                                path = %file_path.display(),
                                attempt = parse_failures,
                                "failed to parse new bytes"
                            );

                            let multiplier = parse_failures.max(1);
                            let backoff = sleep_interval
                                .checked_mul(multiplier)
                                .unwrap_or(sleep_interval);

                            if parse_failures >= MAX_PARSE_RETRIES {
                                warn!(
                                    path = %file_path.display(),
                                    "max parse retries exceeded; resetting parser and skipping bytes"
                                );
                                parser = parsers::route_parser(&file_path)?;
                                read_offset = next_read_offset;
                                let checkpoint_offset = read_offset;
                                checkpoint_db
                                    .set_offset(
                                        &file_path,
                                        checkpoint_offset,
                                        file_size,
                                        last_modified_ts,
                                    )
                                    .await?;
                                parse_failures = 0;
                            }

                            sleep(backoff).await;
                            continue;
                        }
                    };

                    if let Ok(meta) = fs::metadata(&file_path).await {
                        file_size = meta.len();
                        last_modified_ts = meta
                            .modified()
                            .ok()
                            .and_then(system_time_seconds)
                            .unwrap_or(last_modified_ts);
                    }

                    let backlog = parser.backlog_len();
                    let checkpoint_offset = read_offset.saturating_sub(backlog as u64);

                    let had_records = !records.is_empty();
                    if had_records {
                        let total_records = records.len();
                        let mut topic_counts: HashMap<String, usize> = HashMap::new();
                        for record in &records {
                            *topic_counts.entry(record.topic.clone()).or_default() += 1;
                        }
                        let planned_batches = if batch_size == 0 || total_records == 0 {
                            1
                        } else {
                            (total_records + batch_size - 1) / batch_size
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

                        let file_path_str = file_path.to_string_lossy().to_string();
                        for chunk in batches {
                            let record_count = chunk.len();
                            record_sink
                                .send_batch(file_path_str.clone(), chunk_start, chunk)
                                .await?;

                            debug!(
                                "sent batch with {} records from {}",
                                record_count,
                                file_path.display()
                            );
                        }
                    }

                    checkpoint_db
                        .set_offset(&file_path, checkpoint_offset, file_size, last_modified_ts)
                        .await?;

                    if !had_records {
                        sleep(sleep_interval).await;
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

        sleep(sleep_interval).await;
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
