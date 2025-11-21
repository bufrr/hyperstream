use super::fill_types::{parse_fill_line, FillLine, NodeFill};
use crate::parsers::{
    drain_complete_lines, line_preview, normalize_tx_hash, partition_key_or_unknown,
    trim_line_bytes, Parser,
};
use crate::sorter_client::proto::DataRecord;
use anyhow::{Context, Result};
use serde::Serialize;
use std::collections::HashMap;
use std::path::Path;
use tracing::warn;

const LINE_PREVIEW_LIMIT: usize = 256;

/// Aggregates fill events into user-to-user trade records for the `hl.trades` topic.
///
/// Input is identical to the fill parser (`node_trades` / `node_fills_by_block` JSONL files). The
/// parser buffers fills keyed by trade id (tid) and emits one trade per tid, or single-user trades
/// when only one fill exists in a block.
#[derive(Default)]
pub struct TradesParser {
    buffer: Vec<u8>,
    pending: HashMap<u64, Vec<FillForTrade>>,
}

#[derive(Serialize)]
struct Trade {
    coin: String,
    side: String,
    px: String,
    sz: String,
    time: u64,
    hash: String,
    tid: u64,
    users: Vec<String>,
}

#[derive(Clone, Default)]
struct FillForTrade {
    user: String,
    coin: String,
    px: String,
    sz: String,
    side: String,
    time: u64,
    hash: String,
    tid: u64,
    block_height: Option<u64>,
}

impl Parser for TradesParser {
    fn parse(&mut self, _file_path: &Path, data: &[u8]) -> Result<Vec<DataRecord>> {
        self.buffer.extend_from_slice(data);
        let lines = drain_complete_lines(&mut self.buffer);
        let mut records = Vec::new();

        for raw_line in lines {
            let line = trim_line_bytes(raw_line);
            if line.is_empty() {
                continue;
            }

            match parse_fill_line(&line) {
                Ok(FillLine::Batch(batch)) => {
                    for (user, mut fill) in batch.events {
                        fill.user = user;
                        fill.block_height = Some(batch.block_number);
                        let fill = FillForTrade::from(fill);
                        self.try_emit_trade(fill, &mut records)?;
                    }
                    self.flush_block_single_fill_trades(batch.block_number, &mut records)?;
                }
                Ok(FillLine::Single(fill)) => {
                    let fill = FillForTrade::from(*fill);
                    self.try_emit_trade(fill, &mut records)?;
                }
                Err(err) => {
                    warn!(
                        error = %err,
                        preview = %line_preview(&line, LINE_PREVIEW_LIMIT),
                        "failed to parse trade fill line as JSON"
                    );
                }
            }
        }

        // Flush all pending trades at end of file
        // (we're processing historical files, so emit what we have)
        for (_tid, fills) in self.pending.drain() {
            if let Some(record) = build_trade_from_fills(fills)? {
                records.push(record);
            }
        }

        Ok(records)
    }

    fn backlog_len(&self) -> usize {
        self.buffer.len()
    }
}

impl From<NodeFill> for FillForTrade {
    fn from(fill: NodeFill) -> Self {
        Self {
            user: fill.user,
            coin: fill.coin,
            px: fill.px,
            sz: fill.sz,
            side: fill.side,
            time: fill.time,
            hash: fill.hash,
            tid: fill.tid,
            block_height: fill.block_height,
        }
    }
}

impl TradesParser {
    fn try_emit_trade(&mut self, fill: FillForTrade, records: &mut Vec<DataRecord>) -> Result<()> {
        // If tid is 0 (missing), emit immediately as single-user trade
        // Otherwise, try to aggregate by tid for proper 2-user trades
        if fill.tid == 0 {
            if let Some(record) = build_trade_from_fills(vec![fill])? {
                records.push(record);
            }
            return Ok(());
        }

        let tid = fill.tid;
        let bucket_len = {
            let bucket = self.pending.entry(tid).or_default();
            bucket.push(fill);
            bucket.len()
        };

        if bucket_len == 2 {
            if let Some(fills) = self.pending.remove(&tid) {
                if let Some(record) = build_trade_from_fills(fills)? {
                    records.push(record);
                }
            }
        } else if bucket_len > 2 {
            warn!(
                tid = tid,
                count = bucket_len,
                "trade had more than 2 fills; dropping"
            );
            self.pending.remove(&tid);
        }

        Ok(())
    }

    fn flush_block_single_fill_trades(
        &mut self,
        block_number: u64,
        records: &mut Vec<DataRecord>,
    ) -> Result<()> {
        let ready_tids: Vec<u64> = self
            .pending
            .iter()
            .filter_map(|(tid, fills)| {
                if fills.len() == 1 && fills[0].block_height == Some(block_number) {
                    Some(*tid)
                } else {
                    None
                }
            })
            .collect();

        for tid in ready_tids {
            if let Some(fills) = self.pending.remove(&tid) {
                if let Some(record) = build_trade_from_fills(fills)? {
                    records.push(record);
                }
            }
        }

        Ok(())
    }
}

fn build_trade_from_fills(fills: Vec<FillForTrade>) -> Result<Option<DataRecord>> {
    match fills.len() {
        2 => {
            let maker = &fills[0];
            let taker = &fills[1];
            let sz = maker.sz.clone();
            if maker.sz != taker.sz {
                warn!(
                    tid = maker.tid,
                    maker_sz = %maker.sz,
                    taker_sz = %taker.sz,
                    "trade fill sizes differed"
                );
            }
            emit_trade_record(
                maker,
                Some(taker),
                sz,
                vec![maker.user.clone(), taker.user.clone()],
            )
        }
        1 => {
            let fill = &fills[0];
            emit_trade_record(fill, None, fill.sz.clone(), vec![fill.user.clone()])
        }
        len => {
            warn!(len = len, "unexpected fill count for trade");
            Ok(None)
        }
    }
}

fn emit_trade_record(
    primary: &FillForTrade,
    secondary: Option<&FillForTrade>,
    sz: String,
    users: Vec<String>,
) -> Result<Option<DataRecord>> {
    let normalized_hash = normalize_tx_hash(&primary.hash)
        .or_else(|| secondary.and_then(|fill| normalize_tx_hash(&fill.hash)));
    let payload_hash = normalized_hash.clone().unwrap_or_default();

    let trade = Trade {
        coin: primary.coin.clone(),
        side: primary.side.clone(),
        px: primary.px.clone(),
        sz,
        time: primary.time,
        hash: payload_hash,
        tid: primary.tid,
        users,
    };

    let partition_key = partition_key_or_unknown(&trade.coin);

    let payload = serde_json::to_vec(&trade).context("failed to encode trade payload to JSON")?;

    let block_height = primary
        .block_height
        .or_else(|| secondary.and_then(|fill| fill.block_height));

    Ok(Some(DataRecord {
        block_height,
        // Drop zero-hash trades so system matches don't emit fake tx ids.
        tx_hash: normalized_hash,
        timestamp: trade.time,
        topic: "hl.trades".to_string(),
        partition_key,
        payload,
    }))
}
