use crate::parsers::blocks::{proposer_cache, SharedProposerCache};
use crate::parsers::utils::{deserialize_option_string, extract_starting_block};
use crate::parsers::{
    drain_complete_lines, line_preview, parse_iso8601_to_millis, trim_line_bytes, Parser,
    LINE_PREVIEW_LIMIT,
};
use crate::sorter_client::proto::DataRecord;
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value};
use std::path::Path;
use tracing::warn;

/// Maximum buffer size before refusing to accept more data (32 MiB).
const MAX_BUFFER_SIZE: usize = 32 * 1024 * 1024;

/// Decodes `replica_cmds` lines containing blocks and responses into `hl.transactions` records.
///
/// The parser aligns signed actions with execution responses, emits a `DataRecord` per transaction,
/// and keeps track of proposer data shared with the blocks parser.
///
/// Block height is calculated as: `starting_block (from filename) + line_number`
pub struct TransactionsParser {
    buffer: Vec<u8>,
    proposer_cache: SharedProposerCache,
    /// Starting block number extracted from filename (e.g., 808750000 from ".../808750000")
    starting_block: Option<u64>,
    /// Current line count within the file (0-indexed, first line = block at starting_block)
    line_count: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TransactionRecord {
    time: u64,
    user: String,
    hash: String,
    action: Value,
    block: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Deserialize)]
struct ReplicaCmd {
    abci_block: AbciBlock,
    #[serde(default)]
    resps: Option<Resps>,
}

#[derive(Deserialize)]
struct AbciBlock {
    #[serde(default, deserialize_with = "deserialize_option_string")]
    time: Option<String>,
    round: u64,
    #[serde(default, deserialize_with = "deserialize_option_string")]
    proposer: Option<String>,
    #[serde(default)]
    signed_action_bundles: Vec<BundleWithHash>,
}

#[derive(Deserialize)]
struct BundlePayload {
    #[serde(
        default,
        rename = "broadcaster",
        deserialize_with = "deserialize_option_string"
    )]
    _broadcaster: Option<String>,
    #[serde(default)]
    signed_actions: Vec<SignedAction>,
}

#[derive(Deserialize)]
struct BundleWithHash(
    #[allow(dead_code)] // retained for schema compatibility even though hashes are dropped
    #[serde(deserialize_with = "deserialize_option_string")]
    Option<String>,
    BundlePayload,
);

#[derive(Deserialize)]
struct SignedAction {
    #[serde(default)]
    action: Value,
    #[serde(default, rename = "nonce")]
    _nonce: u64,
}

#[derive(Deserialize, Default)]
struct Resps {
    #[serde(rename = "Full", default)]
    full: Vec<ResponseBundle>,
}

#[derive(Deserialize)]
struct ResponseBundle(
    #[serde(deserialize_with = "deserialize_option_string")] Option<String>,
    Vec<ActionResponse>,
);

#[derive(Deserialize)]
struct ActionResponse {
    #[serde(default, deserialize_with = "deserialize_option_string")]
    user: Option<String>,
    #[serde(default)]
    res: ResponseResult,
}

#[derive(Deserialize, Default)]
struct ResponseResult {
    #[serde(default, deserialize_with = "deserialize_option_string")]
    status: Option<String>,
    #[serde(default)]
    response: Value,
}

impl Default for TransactionsParser {
    fn default() -> Self {
        Self {
            buffer: Vec::new(),
            proposer_cache: proposer_cache(),
            starting_block: None,
            line_count: 0,
        }
    }
}

impl Parser for TransactionsParser {
    fn parse(&mut self, file_path: &Path, data: &[u8]) -> Result<Vec<DataRecord>> {
        // Extract starting block from filename on first call
        if self.starting_block.is_none() {
            self.starting_block = extract_starting_block(file_path);
            if self.starting_block.is_none() {
                warn!(
                    file_path = %file_path.display(),
                    "could not extract starting block from filename, falling back to round"
                );
            }
        }

        // Check buffer size limit to prevent runaway buffering
        let new_size = self.buffer.len() + data.len();
        if new_size > MAX_BUFFER_SIZE {
            warn!(
                file_path = %file_path.display(),
                current_buffer_size = self.buffer.len(),
                incoming_data_size = data.len(),
                "TransactionsParser buffer size limit exceeded"
            );
            bail!(
                "TransactionsParser buffer exceeded {MAX_BUFFER_SIZE} bytes"
            );
        }

        self.buffer.extend_from_slice(data);
        let lines = drain_complete_lines(&mut self.buffer);
        let mut records = Vec::new();

        for (line_idx, raw_line) in lines.into_iter().enumerate() {
            let line = trim_line_bytes(raw_line);
            if line.is_empty() {
                self.line_count += 1;
                continue;
            }

            // Calculate block height: starting_block + line_count + 1
            // Files are named with starting block but first line is actually starting_block + 1
            // e.g., file "810800000" contains blocks 810800001 to 810810000
            let calculated_height = self.starting_block.map(|start| start + self.line_count + 1);

            match sonic_rs::from_slice::<ReplicaCmd>(&line) {
                Ok(cmd) => {
                    self.process_replica_cmd(cmd, calculated_height, &mut records)?;
                }
                Err(err) => {
                    warn!(
                        error = %err,
                        line_idx,
                        preview = %line_preview(&line, LINE_PREVIEW_LIMIT),
                        "failed to parse replica_cmds line"
                    );
                }
            }

            self.line_count += 1;
        }

        Ok(records)
    }

    fn backlog_len(&self) -> usize {
        self.buffer.len()
    }

    fn set_initial_line_count(&mut self, count: u64) {
        self.line_count = count;
    }

    fn get_line_count(&self) -> u64 {
        self.line_count
    }

    fn parser_type(&self) -> &'static str {
        "transactions"
    }
}

impl TransactionsParser {
    fn process_replica_cmd(
        &self,
        cmd: ReplicaCmd,
        calculated_height: Option<u64>,
        records: &mut Vec<DataRecord>,
    ) -> Result<()> {
        let ReplicaCmd { abci_block, resps } = cmd;
        let AbciBlock {
            time,
            round,
            proposer,
            signed_action_bundles,
        } = abci_block;

        // Use calculated height, fall back to round if filename parsing failed
        let block_height = calculated_height.unwrap_or(round);

        if let Some(proposer) = proposer.filter(|p| !p.is_empty()) {
            if block_height != 0 {
                self.proposer_cache.insert(block_height, proposer);
            }
        }

        let timestamp = time
            .as_deref()
            .and_then(parse_iso8601_to_millis)
            .unwrap_or(0);
        let actions = flatten_actions(signed_action_bundles);
        let responses = flatten_responses(resps);

        let action_count = actions.len();
        let response_count = responses.len();
        if action_count != response_count {
            warn!(
                round,
                actions = action_count,
                responses = response_count,
                "actions/responses length mismatch"
            );
        }

        let mut responses_iter = responses.into_iter();
        for signed_action in actions {
            let response = responses_iter.next().unwrap_or_else(|| ActionResponse {
                user: None,
                res: ResponseResult::default(),
            });
            // Bundle hashes are discarded because they're non-unique across transactions within
            // the same bundle; emitting them would mislead downstream consumers.
            let ActionResponse { user, res } = response;
            let tx = TransactionRecord {
                time: timestamp,
                user: user.unwrap_or_default(),
                hash: String::new(), // Hash not available for transactions
                action: signed_action.action,
                block: block_height,
                error: parse_error(&res),
            };
            records.push(transaction_to_data_record(tx, block_height)?);
        }

        Ok(())
    }
}

fn flatten_actions(mut bundles: Vec<BundleWithHash>) -> Vec<SignedAction> {
    let capacity = bundles
        .iter()
        .map(|bundle| bundle.1.signed_actions.len())
        .sum();
    let mut actions = Vec::with_capacity(capacity);
    for BundleWithHash(_, bundle) in bundles.drain(..) {
        actions.extend(bundle.signed_actions);
    }
    actions
}

fn flatten_responses(resps: Option<Resps>) -> Vec<ActionResponse> {
    let Some(resps) = resps else {
        return Vec::new();
    };

    let capacity = resps.full.iter().map(|bundle| bundle.1.len()).sum();
    let mut responses = Vec::with_capacity(capacity);
    for ResponseBundle(hash, bundle_responses) in resps.full {
        let _ = hash;
        responses.extend(bundle_responses);
    }
    responses
}

fn transaction_to_data_record(mut tx: TransactionRecord, block_height: u64) -> Result<DataRecord> {
    let payload = sonic_rs::to_vec(&tx).context("failed to encode hl.transactions payload")?;
    let raw_hash = std::mem::take(&mut tx.hash);
    let metadata_hash = if raw_hash.trim().is_empty() {
        None
    } else {
        Some(raw_hash)
    };

    Ok(DataRecord {
        block_height: Some(block_height),
        tx_hash: metadata_hash,
        timestamp: tx.time,
        topic: "hl.transactions".to_string(),
        payload,
    })
}

fn parse_error(res: &ResponseResult) -> Option<String> {
    match res.status.as_deref().unwrap_or("") {
        "err" => res.response.as_str().map(|msg| msg.to_string()),
        "ok" => res
            .response
            .as_object()
            .and_then(|map| map.get(&"data".to_string()))
            .and_then(|data| data.as_object())
            .and_then(|data| data.get(&"statuses".to_string()))
            .and_then(|statuses| statuses.as_array())
            .map(|statuses| {
                statuses
                    .iter()
                    .filter_map(|status| {
                        status.as_object().and_then(|status_obj| {
                            status_obj
                                .get(&"error".to_string())
                                .and_then(|v| v.as_str())
                                .map(|msg| msg.to_string())
                        })
                    })
                    .collect::<Vec<String>>()
            })
            .filter(|errors| !errors.is_empty())
            .map(|errors| errors.join("; ")),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_error_from_err_status() {
        let res = ResponseResult {
            status: Some("err".to_string()),
            response: sonic_rs::json!("Invalid nonce: duplicate nonce 1764221206959"),
        };
        assert_eq!(
            parse_error(&res),
            Some("Invalid nonce: duplicate nonce 1764221206959".to_string())
        );
    }

    #[test]
    fn parse_error_from_ok_status_with_nested_error() {
        let res = ResponseResult {
            status: Some("ok".to_string()),
            response: sonic_rs::json!({
                "type": "order",
                "data": {
                    "statuses": [
                        {"error": "Order could not immediately match against any resting orders. asset=123"}
                    ]
                }
            }),
        };
        assert_eq!(
            parse_error(&res),
            Some(
                "Order could not immediately match against any resting orders. asset=123"
                    .to_string()
            )
        );
    }

    #[test]
    fn parse_error_from_ok_status_success() {
        let res = ResponseResult {
            status: Some("ok".to_string()),
            response: sonic_rs::json!({
                "type": "order",
                "data": {
                    "statuses": [{"resting": {"oid": 123456}}]
                }
            }),
        };
        assert_eq!(parse_error(&res), None);
    }
}
