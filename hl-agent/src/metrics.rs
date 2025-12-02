//! Prometheus metrics for monitoring hl-agent performance and errors.
//!
//! All metrics use the default Prometheus registry and follow naming conventions:
//! - Histograms: *_seconds (for latency)
//! - Counters: *_total (for counts)

use lazy_static::lazy_static;
use prometheus::{
    register_histogram_vec, register_int_counter_vec, register_int_gauge, Histogram, HistogramVec,
    IntCounterVec, IntGauge,
};

lazy_static! {
    // ==================== LATENCY METRICS ====================

    /// Parse duration by parser type (seconds)
    pub static ref PARSE_DURATION: HistogramVec = register_histogram_vec!(
        "hl_agent_parse_duration_seconds",
        "Time spent parsing data by parser type",
        &["parser_type"],
        vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
    )
    .unwrap();

    /// Checkpoint write duration (seconds)
    pub static ref CHECKPOINT_DURATION: HistogramVec = register_histogram_vec!(
        "hl_agent_checkpoint_duration_seconds",
        "Time spent writing checkpoints",
        &["operation"],
        vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
    )
    .unwrap();

    /// Sorter send duration by status (seconds)
    pub static ref SORTER_SEND_DURATION: HistogramVec = register_histogram_vec!(
        "hl_agent_sorter_send_duration_seconds",
        "Time spent sending batches to sorter",
        &["status"],
        vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]
    )
    .unwrap();

    /// File read duration (seconds)
    pub static ref FILE_READ_DURATION: Histogram = prometheus::register_histogram!(
        "hl_agent_file_read_duration_seconds",
        "Time spent reading file chunks",
        vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
    )
    .unwrap();

    // ==================== ERROR COUNTERS ====================

    /// Parse errors by type
    pub static ref PARSE_ERRORS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "hl_agent_parse_errors_total",
        "Parse errors by parser and error type",
        &["parser_type", "error_type"]
    )
    .unwrap();

    /// Checkpoint errors by operation
    pub static ref CHECKPOINT_ERRORS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "hl_agent_checkpoint_errors_total",
        "Checkpoint write errors",
        &["operation"]
    )
    .unwrap();

    /// Block validation failures by reason
    pub static ref BLOCK_VALIDATION_FAILURES_TOTAL: IntCounterVec = register_int_counter_vec!(
        "hl_agent_block_validation_failures_total",
        "Blocks dropped due to Redis validation failure",
        &["reason"]
    )
    .unwrap();

    // ==================== THROUGHPUT METRICS ====================

    /// Records emitted by topic
    pub static ref RECORDS_EMITTED_TOTAL: IntCounterVec = register_int_counter_vec!(
        "hl_agent_records_emitted_total",
        "Records emitted by topic",
        &["topic"]
    )
    .unwrap();

    /// Batches sent by status
    pub static ref BATCHES_SENT_TOTAL: IntCounterVec = register_int_counter_vec!(
        "hl_agent_batches_sent_total",
        "Batches sent to sorter",
        &["status"]
    )
    .unwrap();

    /// Redis cache hits/misses
    pub static ref REDIS_CACHE_TOTAL: IntCounterVec = register_int_counter_vec!(
        "hl_agent_redis_cache_total",
        "Redis cache hits and misses",
        &["result"]
    )
    .unwrap();

    /// Explorer API fallback outcomes
    pub static ref EXPLORER_API_FALLBACK_TOTAL: IntCounterVec = register_int_counter_vec!(
        "hl_agent_explorer_api_fallback_total",
        "Explorer API fallback results when Redis is missing block data",
        &["result"]
    )
    .unwrap();

    // ==================== GAUGE METRICS ====================

    /// Latest block height being processed
    pub static ref LATEST_BLOCK_HEIGHT: IntGauge = register_int_gauge!(
        "hl_agent_latest_block_height",
        "Latest block height currently being processed"
    )
    .unwrap();
}
