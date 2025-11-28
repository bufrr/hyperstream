//! Mock sorter gRPC server for local testing.
//!
//! Usage:
//! 1. Run the server: `cargo run --example mock_sorter -- --listen-addr 127.0.0.1:50051`.
//! 2. Point an agent at `http://127.0.0.1:50051` (e.g., update `config.toml` and run `cargo run -- --config config.toml`).
//! 3. Optional flags: `--output-dir ./received`, `--stats-interval-ms 2000`, `--delay-ms 5`.

use std::collections::{HashMap, HashSet};
use std::env;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use prost::Message;
use serde::Serialize;
use tokio::signal;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

pub mod proto {
    tonic::include_proto!("hyperstream");
}

use proto::sorter_service_server::{SorterService, SorterServiceServer};
use proto::{DataBatch, DataRecord};

#[derive(Clone, Debug)]
struct Config {
    listen_addr: SocketAddr,
    stats_interval: Duration,
    output_dir: Option<PathBuf>,
    processing_delay: Duration,
}

impl Config {
    fn from_args() -> Result<Self, Box<dyn std::error::Error>> {
        let mut listen = "0.0.0.0:50051".to_string();
        let mut stats_interval_ms: u64 = 5_000;
        let mut delay_ms: u64 = 0;
        let mut output_dir: Option<PathBuf> = None;

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--listen-addr" => {
                    listen = args
                        .next()
                        .ok_or_else(|| format!("missing value for {arg}"))?;
                }
                "--stats-interval-ms" => {
                    stats_interval_ms = args
                        .next()
                        .ok_or_else(|| format!("missing value for {arg}"))?
                        .parse()?;
                }
                "--output-dir" => {
                    let dir = args
                        .next()
                        .ok_or_else(|| format!("missing value for {arg}"))?;
                    output_dir = Some(PathBuf::from(dir));
                }
                "--delay-ms" => {
                    delay_ms = args
                        .next()
                        .ok_or_else(|| format!("missing value for {arg}"))?
                        .parse()?;
                }
                "--help" | "-h" => {
                    println!("{}", Config::usage());
                    std::process::exit(0);
                }
                other => return Err(format!("unknown flag: {other}").into()),
            }
        }

        if stats_interval_ms == 0 {
            return Err("stats interval must be > 0".into());
        }

        let listen_addr: SocketAddr = listen.parse()?;
        let stats_interval = Duration::from_millis(stats_interval_ms);
        let processing_delay = Duration::from_millis(delay_ms);

        Ok(Self {
            listen_addr,
            stats_interval,
            output_dir,
            processing_delay,
        })
    }

    fn usage() -> &'static str {
        "mock_sorter options:\n  --listen-addr <addr>         Socket address to bind (default 0.0.0.0:50051)\n  --stats-interval-ms <ms>     Interval for stats logs (default 5000)\n  --output-dir <path>          Write each batch to JSON inside <path>\n  --delay-ms <ms>              Simulate per-batch processing delay (default 0)\n  --help                       Show this message"
    }
}

#[derive(Clone)]
struct MockSorter {
    stats: Arc<StatsTracker>,
    record_writer: Option<Arc<RecordWriter>>,
    processing_delay: Duration,
}

#[tonic::async_trait]
impl SorterService for MockSorter {
    async fn stream_data(
        &self,
        request: Request<tonic::Streaming<DataBatch>>,
    ) -> Result<Response<()>, Status> {
        let mut stream = request.into_inner();
        while let Some(batch) = stream.message().await.map_err(|status| {
            warn!(error = %status, "client stream error");
            status
        })? {
            if !self.processing_delay.is_zero() {
                tokio::time::sleep(self.processing_delay).await;
            }

            let report = self.stats.ingest_batch(&batch);
            if report.invalid_records > 0 {
                warn!(
                    invalid = report.invalid_records,
                    samples = %report.invalid_reasons.join(", "),
                    "batch contained invalid records"
                );
            }
            if let Some(writer) = &self.record_writer {
                if let Err(err) = writer.write_batch(&batch).await {
                    error!(error = %err, "failed to persist batch to disk");
                }
            }
        }

        Ok(Response::new(()))
    }
}

struct BatchIngestReport {
    invalid_records: u64,
    invalid_reasons: Vec<String>,
}

struct StatsTracker {
    total_batches: AtomicU64,
    total_records: AtomicU64,
    total_bytes: AtomicU64,
    invalid_records: AtomicU64,
    records_by_topic: Mutex<HashMap<String, u64>>,
    unique_tx_hashes: Mutex<HashSet<String>>,
    unique_block_heights: Mutex<HashSet<u64>>,
}

impl StatsTracker {
    fn new() -> Self {
        Self {
            total_batches: AtomicU64::new(0),
            total_records: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            invalid_records: AtomicU64::new(0),
            records_by_topic: Mutex::new(HashMap::new()),
            unique_tx_hashes: Mutex::new(HashSet::new()),
            unique_block_heights: Mutex::new(HashSet::new()),
        }
    }

    fn ingest_batch(&self, batch: &DataBatch) -> BatchIngestReport {
        self.total_batches.fetch_add(1, Ordering::Relaxed);
        self.total_records
            .fetch_add(batch.records.len() as u64, Ordering::Relaxed);
        self.total_bytes
            .fetch_add(batch.encoded_len() as u64, Ordering::Relaxed);

        let mut invalid_records = 0_u64;
        let mut invalid_reasons = Vec::new();
        let mut per_topic: HashMap<String, u64> = HashMap::new();
        let mut tx_hashes: Vec<String> = Vec::new();
        let mut block_heights: Vec<u64> = Vec::new();

        for record in &batch.records {
            match validate_record(record) {
                Ok(()) => {
                    *per_topic.entry(record.topic.clone()).or_insert(0) += 1;
                    if let Some(tx) = record.tx_hash.clone() {
                        tx_hashes.push(tx);
                    }
                    if let Some(height) = record.block_height {
                        block_heights.push(height);
                    }
                }
                Err(err) => {
                    invalid_records += 1;
                    if invalid_reasons.len() < 5 {
                        invalid_reasons.push(err.to_string());
                    }
                }
            }
        }

        {
            let mut topics = self.records_by_topic.lock().unwrap();
            for (topic, count) in per_topic {
                *topics.entry(topic).or_insert(0) += count;
            }
        }

        {
            let mut seen = self.unique_tx_hashes.lock().unwrap();
            for tx in tx_hashes {
                seen.insert(tx);
            }
        }

        {
            let mut heights = self.unique_block_heights.lock().unwrap();
            for height in block_heights {
                heights.insert(height);
            }
        }

        self.invalid_records
            .fetch_add(invalid_records, Ordering::Relaxed);

        BatchIngestReport {
            invalid_records,
            invalid_reasons,
        }
    }

    fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            total_batches: self.total_batches.load(Ordering::Relaxed),
            total_records: self.total_records.load(Ordering::Relaxed),
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
            invalid_records: self.invalid_records.load(Ordering::Relaxed),
            topic_counts: self.records_by_topic.lock().unwrap().clone(),
            unique_tx: self.unique_tx_hashes.lock().unwrap().len() as u64,
            unique_block_heights: self.unique_block_heights.lock().unwrap().len() as u64,
        }
    }
}

#[derive(Clone, Default)]
struct StatsSnapshot {
    total_batches: u64,
    total_records: u64,
    total_bytes: u64,
    invalid_records: u64,
    topic_counts: HashMap<String, u64>,
    unique_tx: u64,
    unique_block_heights: u64,
}

struct RecordWriter {
    dir: PathBuf,
    counter: AtomicU64,
}

impl RecordWriter {
    async fn new(dir: PathBuf) -> io::Result<Self> {
        tokio::fs::create_dir_all(&dir).await?;
        Ok(Self {
            dir,
            counter: AtomicU64::new(0),
        })
    }

    async fn write_batch(&self, batch: &DataBatch) -> io::Result<()> {
        let idx = self.counter.fetch_add(1, Ordering::Relaxed);
        let filename = format!("batch-{idx}.json");
        let path = self.dir.join(filename);
        let snapshot = BatchSnapshot::from_batch(batch);
        let data = serde_json::to_vec_pretty(&snapshot).map_err(io::Error::other)?;
        tokio::fs::write(path, data).await
    }
}

#[derive(Serialize)]
struct BatchSnapshot {
    source: Option<SourceSnapshot>,
    records: Vec<RecordSnapshot>,
}

#[derive(Serialize)]
struct SourceSnapshot {
    node_id: String,
    agent_id: String,
    file_path: String,
    byte_offset: u64,
}

#[derive(Serialize)]
struct RecordSnapshot {
    block_height: Option<u64>,
    tx_hash: Option<String>,
    timestamp_ms: u64,
    topic: String,
    partition_key: String,
    payload_hex: String,
}

impl BatchSnapshot {
    fn from_batch(batch: &DataBatch) -> Self {
        let source = batch.source.as_ref().map(|meta| SourceSnapshot {
            node_id: meta.node_id.clone(),
            agent_id: meta.agent_id.clone(),
            file_path: meta.file_path.clone(),
            byte_offset: meta.byte_offset,
        });

        let records = batch
            .records
            .iter()
            .map(|record| RecordSnapshot {
                block_height: record.block_height,
                tx_hash: record.tx_hash.clone(),
                timestamp_ms: record.timestamp,
                topic: record.topic.clone(),
                partition_key: record.partition_key.clone(),
                payload_hex: to_hex(&record.payload),
            })
            .collect();

        Self { source, records }
    }
}

#[derive(Debug)]
#[allow(clippy::enum_variant_names)] // Keeping descriptive Missing* variants is clearer at call sites.
enum ValidationError {
    MissingTopic,
    MissingPartitionKey,
    MissingPayload,
    MissingTimestamp,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationError::MissingTopic => write!(f, "missing topic"),
            ValidationError::MissingPartitionKey => write!(f, "missing partition_key"),
            ValidationError::MissingPayload => write!(f, "missing payload"),
            ValidationError::MissingTimestamp => write!(f, "missing timestamp"),
        }
    }
}

fn validate_record(record: &DataRecord) -> Result<(), ValidationError> {
    if record.timestamp == 0 {
        return Err(ValidationError::MissingTimestamp);
    }
    if record.topic.trim().is_empty() {
        return Err(ValidationError::MissingTopic);
    }
    if record.partition_key.trim().is_empty() {
        return Err(ValidationError::MissingPartitionKey);
    }
    if record.payload.is_empty() {
        return Err(ValidationError::MissingPayload);
    }
    Ok(())
}

fn to_hex(bytes: &[u8]) -> String {
    const LUT: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(LUT[(b >> 4) as usize] as char);
        out.push(LUT[(b & 0x0f) as usize] as char);
    }
    out
}

fn spawn_stats_logger(stats: Arc<StatsTracker>, interval: Duration) {
    tokio::spawn(async move {
        let mut previous = StatsSnapshot::default();
        loop {
            tokio::time::sleep(interval).await;
            let snapshot = stats.snapshot();
            log_stats(&previous, &snapshot, interval);
            previous = snapshot;
        }
    });
}

fn log_stats(previous: &StatsSnapshot, current: &StatsSnapshot, interval: Duration) {
    let secs = interval.as_secs_f64().max(0.001);
    let batches_delta = current.total_batches.saturating_sub(previous.total_batches);
    let records_delta = current.total_records.saturating_sub(previous.total_records);
    let bytes_delta = current.total_bytes.saturating_sub(previous.total_bytes);

    let topic_delta = topic_diff(&previous.topic_counts, &current.topic_counts);
    let topics_rate = if topic_delta.is_empty() {
        "n/a".to_string()
    } else {
        topic_delta
            .into_iter()
            .map(|(topic, count)| format!("{topic}:{:.0}/s", (count as f64) / secs))
            .collect::<Vec<_>>()
            .join(", ")
    };

    // Build total counts per topic (all 6 expected topics)
    let expected_topics = [
        "hl.blocks",
        "hl.transactions",
        "hl.trades",
        "hl.fills",
        "hl.orders",
        "hl.misc_events",
    ];
    let topics_total: Vec<String> = expected_topics
        .iter()
        .map(|&topic| {
            let count = current.topic_counts.get(topic).copied().unwrap_or(0);
            format!("{}={}", topic.strip_prefix("hl.").unwrap_or(topic), count)
        })
        .collect();

    // Count how many of the 6 expected topics have data
    let topics_with_data = expected_topics
        .iter()
        .filter(|&&t| current.topic_counts.get(t).copied().unwrap_or(0) > 0)
        .count();

    info!(
        total_batches = current.total_batches,
        total_records = current.total_records,
        total_bytes = current.total_bytes,
        batches_per_sec = (batches_delta as f64) / secs,
        records_per_sec = (records_delta as f64) / secs,
        mb_per_sec = (bytes_delta as f64) / (secs * 1_048_576.0),
        unique_tx = current.unique_tx,
        unique_block_heights = current.unique_block_heights,
        invalid_records = current.invalid_records,
        topics_coverage = format!("{}/6", topics_with_data),
        topic_totals = %topics_total.join(", "),
        topic_rates = %topics_rate,
        "sorter stats"
    );
}

fn topic_diff(
    previous: &HashMap<String, u64>,
    current: &HashMap<String, u64>,
) -> Vec<(String, u64)> {
    let mut diffs: Vec<(String, u64)> = current
        .iter()
        .filter_map(|(topic, &count)| {
            let old = previous.get(topic).copied().unwrap_or(0);
            if count > old {
                Some((topic.clone(), count - old))
            } else {
                None
            }
        })
        .collect();

    diffs.sort_by(|a, b| b.1.cmp(&a.1));
    // Keep all 6 expected topics
    diffs.truncate(6);
    diffs
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let config = Config::from_args()?;
    info!(?config, "starting mock sorter");

    let stats = Arc::new(StatsTracker::new());
    spawn_stats_logger(stats.clone(), config.stats_interval);

    let record_writer = if let Some(dir) = &config.output_dir {
        Some(Arc::new(RecordWriter::new(dir.clone()).await?))
    } else {
        None
    };

    let sorter = MockSorter {
        stats,
        record_writer,
        processing_delay: config.processing_delay,
    };

    let addr = config.listen_addr;

    info!(%addr, "mock sorter listening");

    // Increase gRPC message size limits to handle large batches
    // Default: 4MB, Increased to: 100MB
    const MAX_MESSAGE_SIZE: usize = 100 * 1024 * 1024; // 100 MB

    Server::builder()
        .add_service(
            SorterServiceServer::new(sorter)
                .max_encoding_message_size(MAX_MESSAGE_SIZE)
                .max_decoding_message_size(MAX_MESSAGE_SIZE),
        )
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    info!("mock sorter stopped");
    Ok(())
}

async fn shutdown_signal() {
    let _ = signal::ctrl_c().await;
    info!("shutdown signal received");
}
