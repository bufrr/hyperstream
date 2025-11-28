use crate::sorter_client::proto::DataRecord;
use anyhow::{anyhow, Result};
use std::path::Path;

pub mod batch;
pub mod block_merger;
pub mod blocks;
pub mod buffered;
mod fill_types;
pub mod fills;
pub mod hash_store;
pub mod misc_events;
pub mod orders;
pub mod schemas;
pub mod trades;
pub mod transactions;
mod utils;

pub use buffered::{BufferedLineParser, LineParser};
pub(crate) use utils::*;
pub use utils::{current_timestamp, LINE_PREVIEW_LIMIT};

pub use blocks::BlocksParser;
pub use fills::new_fills_parser;
pub use misc_events::new_misc_events_parser;
pub use orders::OrdersParser;
pub use trades::TradesParser;
pub use transactions::TransactionsParser;

/// State machine that converts file fragments produced by the tailer into `DataRecord`s.
///
/// Parsers own whatever incremental buffers they need and are invoked every time the tailer reads
/// another chunk from disk. Implementations should avoid blocking work and only keep lightweight
/// state so multiple parsers can be run for the same file.
///
/// # Examples
///
/// ```ignore
/// # use anyhow::Result;
/// use hl_agent::parsers::{BlocksParser, Parser};
/// use std::path::Path;
///
/// # fn demo() -> Result<()> {
/// let mut parser = BlocksParser::default();
/// let payload = br#"{"abci_block":{"round":1,"proposer":"","time":"","signed_action_bundles":[]}}"#;
/// let records = parser.parse(Path::new("replica_cmds/sample.jsonl"), payload)?;
/// assert!(records.is_empty());
/// # Ok(())
/// # }
/// # demo().unwrap();
/// ```
pub trait Parser: Send {
    /// Consume a new chunk of bytes for `file_path` and emit the parsed `DataRecord`s.
    ///
    /// Tailers will call `parse` repeatedly whenever new bytes are available. Implementations can
    /// buffer partial lines between calls, but they should never mutate the `data` slice or block
    /// the async tailer thread from making progress.
    fn parse(&mut self, file_path: &Path, data: &[u8]) -> Result<Vec<DataRecord>>;

    /// Return how many bytes are still buffered internally.
    ///
    /// This value lets the tailer compute a safe checkpoint: it subtracts the backlog size from the
    /// current file offset so that restarted runs re-feed any partial lines still in memory. A
    /// parser that performs its own buffering must reflect that state here.
    fn backlog_len(&self) -> usize;

    /// Initialize the parser's internal line counter when resuming from a checkpoint.
    fn set_initial_line_count(&mut self, _count: u64) {}

    /// Return how many complete lines have been processed so far.
    fn get_line_count(&self) -> u64 {
        0
    }
}

/// Routes a file path to the appropriate parser(s).
///
/// Some files may be processed by multiple parsers simultaneously (e.g., `replica_cmds` generates
/// both blocks and transactions from the same ABCI data).
///
/// # Parser Routing Table
///
/// | Path Component                | Parser(s)                          | Output Topics          |
/// |-------------------------------|------------------------------------|------------------------|
/// | `replica_cmds`                | BlocksParser + TransactionsParser  | hl.blocks + hl.transactions |
/// | `node_trades`                 | TradesParser                       | hl.trades              |
/// | `node_fills_by_block`         | FillsParser + TradesParser         | hl.fills + hl.trades   |
/// | `node_order_statuses`         | OrdersParser                       | hl.orders              |
/// | `misc_events`                 | MiscEventsParser                   | hl.misc_events         |
///
/// Both `_by_block` and non-`_by_block` variants are supported where applicable.
pub fn route_parser(file_path: &Path) -> Result<Vec<Box<dyn Parser>>> {
    if path_contains(file_path, "replica_cmds") {
        // ABCI blocks contain both block metadata and transaction data
        return Ok(vec![
            Box::new(BlocksParser::default()),
            Box::new(TransactionsParser::default()),
        ]);
    }

    if path_contains(file_path, "node_trades") {
        return Ok(vec![Box::new(TradesParser::default())]);
    }

    if path_contains(file_path, "node_fills_by_block") {
        // Fill data generates both hl.fills (position fills) and hl.trades (aggregated trades)
        return Ok(vec![
            Box::new(new_fills_parser()),
            Box::new(TradesParser::default()),
        ]);
    }

    if path_contains(file_path, "node_order_statuses")
        || path_contains(file_path, "node_order_statuses_by_block")
    {
        return Ok(vec![Box::new(OrdersParser::default())]);
    }

    if path_contains(file_path, "misc_events") || path_contains(file_path, "misc_events_by_block") {
        return Ok(vec![Box::new(new_misc_events_parser())]);
    }

    Err(anyhow!(
        "no parser registered for path {}",
        file_path.display()
    ))
}

fn path_contains(path: &Path, needle: &str) -> bool {
    path.iter()
        .filter_map(|c| c.to_str())
        .any(|component| component == needle)
}
