use crate::sorter_client::proto::DataRecord;
use anyhow::{anyhow, Result};
use std::path::Path;

pub mod blocks;
mod fill_types;
pub mod fills;
pub mod misc_events;
pub mod orders;
pub mod trades;
pub mod transactions;
mod utils;

// Explorer WebSocket parsers
pub mod explorer_blocks;
pub mod explorer_txs;

pub(crate) use utils::*;

pub use blocks::BlocksParser;
pub use fills::FillsParser;
pub use misc_events::MiscEventsParser;
pub use orders::OrdersParser;
pub use trades::TradesParser;
pub use transactions::TransactionsParser;

// Export Explorer parsers
pub use explorer_blocks::ExplorerBlocksParser;
pub use explorer_txs::ExplorerTxsParser;

/// State machine that converts file fragments produced by the tailer into `DataRecord`s.
///
/// Parsers own whatever incremental buffers they need and are invoked every time the tailer reads
/// another chunk from disk. Implementations should avoid blocking work and only keep lightweight
/// state so multiple parsers can be run for the same file.
///
/// # Examples
///
/// ```
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
            Box::new(FillsParser::default()),
            Box::new(TradesParser::default()),
        ]);
    }

    if path_contains(file_path, "node_order_statuses")
        || path_contains(file_path, "node_order_statuses_by_block")
    {
        return Ok(vec![Box::new(OrdersParser::default())]);
    }

    if path_contains(file_path, "misc_events") || path_contains(file_path, "misc_events_by_block") {
        return Ok(vec![Box::new(MiscEventsParser::default())]);
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
