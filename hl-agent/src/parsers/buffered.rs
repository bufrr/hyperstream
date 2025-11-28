//! Buffered line parser abstraction.
//!
//! This module provides a `BufferedLineParser` wrapper that handles the common
//! pattern of buffering input bytes and extracting complete lines for parsing.
//! Individual parsers implement the `LineParser` trait to handle line-level logic.

use crate::parsers::{drain_complete_lines, trim_line_bytes, Parser};
use crate::sorter_client::proto::DataRecord;
use anyhow::Result;
use std::path::Path;

/// Trait for parsers that process complete lines.
///
/// Implementors handle the parsing of individual lines after buffering
/// is managed by `BufferedLineParser`.
pub trait LineParser: Send {
    /// Parse a single complete line and return any records produced.
    ///
    /// The line has already been trimmed of trailing whitespace.
    /// Return an empty Vec if the line produces no records (e.g., empty lines).
    fn parse_line(&mut self, file_path: &Path, line: &[u8]) -> Result<Vec<DataRecord>>;
}

/// Wrapper that adds line buffering to any `LineParser`.
///
/// Handles the common pattern of:
/// 1. Accumulating incoming bytes in a buffer
/// 2. Extracting complete lines (ending with `\n`)
/// 3. Trimming whitespace from lines
/// 4. Passing complete lines to the inner parser
///
/// This eliminates ~15 lines of duplicate buffer management code per parser.
pub struct BufferedLineParser<P: LineParser> {
    inner: P,
    buffer: Vec<u8>,
}

impl<P: LineParser> BufferedLineParser<P> {
    /// Create a new buffered parser wrapping the given line parser.
    pub fn new(inner: P) -> Self {
        Self {
            inner,
            buffer: Vec::new(),
        }
    }

    /// Get a reference to the inner line parser.
    pub fn inner(&self) -> &P {
        &self.inner
    }
}

impl<P: LineParser> Parser for BufferedLineParser<P> {
    fn parse(&mut self, file_path: &Path, data: &[u8]) -> Result<Vec<DataRecord>> {
        self.buffer.extend_from_slice(data);
        let lines = drain_complete_lines(&mut self.buffer);
        let mut all_records = Vec::new();

        for raw_line in lines {
            let line = trim_line_bytes(raw_line);
            if line.is_empty() {
                continue;
            }

            let records = self.inner.parse_line(file_path, &line)?;
            all_records.extend(records);
        }

        Ok(all_records)
    }

    fn backlog_len(&self) -> usize {
        self.buffer.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestLineParser {
        prefix: String,
    }

    impl LineParser for TestLineParser {
        fn parse_line(&mut self, _file_path: &Path, line: &[u8]) -> Result<Vec<DataRecord>> {
            let text = String::from_utf8_lossy(line);
            Ok(vec![DataRecord {
                block_height: None,
                tx_hash: None,
                timestamp: 0,
                topic: format!("{}{}", self.prefix, text),
                payload: line.to_vec(),
            }])
        }
    }

    #[test]
    fn buffered_parser_handles_complete_lines() {
        let inner = TestLineParser {
            prefix: "test:".to_string(),
        };
        let mut parser = BufferedLineParser::new(inner);

        let data = b"line1\nline2\n";
        let records = parser
            .parse(Path::new("test.txt"), data)
            .expect("should parse");

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].topic, "test:line1");
        assert_eq!(records[1].topic, "test:line2");
    }

    #[test]
    fn buffered_parser_buffers_incomplete_lines() {
        let inner = TestLineParser {
            prefix: "".to_string(),
        };
        let mut parser = BufferedLineParser::new(inner);

        // First chunk has incomplete line
        let records = parser
            .parse(Path::new("test.txt"), b"partial")
            .expect("should parse");
        assert!(records.is_empty());
        assert_eq!(parser.backlog_len(), 7); // "partial"

        // Second chunk completes the line
        let records = parser
            .parse(Path::new("test.txt"), b" line\n")
            .expect("should parse");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].topic, "partial line");
        assert_eq!(parser.backlog_len(), 0);
    }

    #[test]
    fn buffered_parser_skips_empty_lines() {
        let inner = TestLineParser {
            prefix: "".to_string(),
        };
        let mut parser = BufferedLineParser::new(inner);

        let data = b"line1\n\n\nline2\n";
        let records = parser
            .parse(Path::new("test.txt"), data)
            .expect("should parse");

        assert_eq!(records.len(), 2);
    }
}
