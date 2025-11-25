use crate::parsers::blocks::{proposer_cache, SharedProposerCache};
use crate::parsers::{
    drain_complete_lines, line_preview, parse_iso8601_to_millis, partition_key_or_unknown,
    trim_line_bytes, Parser,
};
use crate::sorter_client::proto::DataRecord;
use anyhow::{Context, Result};
use serde::{de::Deserializer, Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;
use tracing::warn;

const LINE_PREVIEW_LIMIT: usize = 256;

/// Decodes `replica_cmds` lines containing blocks and responses into `hl.transactions` records.
///
/// The parser aligns signed actions with execution responses, emits a `DataRecord` per transaction,
/// and keeps track of proposer data shared with the blocks parser.
pub struct TransactionsParser {
    buffer: Vec<u8>,
    proposer_cache: SharedProposerCache,
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
    #[serde(default, deserialize_with = "string_or_default")]
    time: String,
    round: u64,
    #[serde(default, deserialize_with = "string_or_default")]
    proposer: String,
    #[serde(default)]
    signed_action_bundles: Vec<BundleWithHash>,
}

#[derive(Deserialize)]
struct BundlePayload {
    #[serde(
        default,
        rename = "broadcaster",
        deserialize_with = "string_or_default"
    )]
    _broadcaster: String,
    #[serde(default)]
    signed_actions: Vec<SignedAction>,
}

#[derive(Deserialize)]
struct BundleWithHash(
    #[serde(deserialize_with = "string_or_default")] String,
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
    #[serde(deserialize_with = "string_or_default")] String,
    Vec<ActionResponse>,
);

#[derive(Deserialize)]
struct ActionResponse {
    #[serde(default, deserialize_with = "string_or_default")]
    user: String,
    #[serde(default)]
    res: ResponseResult,
}

#[derive(Deserialize, Default)]
struct ResponseResult {
    #[serde(default, deserialize_with = "string_or_default")]
    status: String,
    #[serde(default)]
    response: Value,
}

impl Default for TransactionsParser {
    fn default() -> Self {
        Self {
            buffer: Vec::new(),
            proposer_cache: proposer_cache(),
        }
    }
}

impl Parser for TransactionsParser {
    fn parse(&mut self, _file_path: &Path, data: &[u8]) -> Result<Vec<DataRecord>> {
        self.buffer.extend_from_slice(data);
        let lines = drain_complete_lines(&mut self.buffer);
        let mut records = Vec::new();

        for (line_idx, raw_line) in lines.into_iter().enumerate() {
            let line = trim_line_bytes(raw_line);
            if line.is_empty() {
                continue;
            }

            match serde_json::from_slice::<ReplicaCmd>(&line) {
                Ok(cmd) => {
                    self.process_replica_cmd(cmd, &mut records)?;
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
        }

        Ok(records)
    }

    fn backlog_len(&self) -> usize {
        self.buffer.len()
    }
}

impl TransactionsParser {
    fn process_replica_cmd(&self, cmd: ReplicaCmd, records: &mut Vec<DataRecord>) -> Result<()> {
        let ReplicaCmd { abci_block, resps } = cmd;
        let AbciBlock {
            time,
            round,
            proposer,
            signed_action_bundles,
        } = abci_block;

        if !(round == 0 || proposer.is_empty()) {
            self.proposer_cache.insert(round, proposer);
        }

        let block_height = round;

        let timestamp = parse_iso8601_to_millis(&time).unwrap_or(0);
        let actions_with_hashes = flatten_actions_with_hashes(signed_action_bundles);
        let responses = flatten_responses(resps);

        let action_count = actions_with_hashes.len();
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
        for (hash, signed_action) in actions_with_hashes.into_iter() {
            let response = responses_iter.next().unwrap_or_else(|| ActionResponse {
                user: String::new(),
                res: ResponseResult::default(),
            });
            // Hash is extracted from signed_action_bundles[i].0
            // This is the consensus-generated transaction hash that includes block number
            let ActionResponse { user, res } = response;
            let tx = TransactionRecord {
                time: timestamp,
                user,
                hash,
                action: signed_action.action,
                block: block_height,
                error: parse_error(&res),
            };
            records.push(transaction_to_data_record(tx, block_height)?);
        }

        Ok(())
    }
}

fn flatten_actions_with_hashes(bundles: Vec<BundleWithHash>) -> Vec<(String, SignedAction)> {
    let mut actions_with_hashes = Vec::new();
    for BundleWithHash(hash, bundle) in bundles {
        for action in bundle.signed_actions {
            actions_with_hashes.push((hash.clone(), action));
        }
    }
    actions_with_hashes
}

fn flatten_responses(resps: Option<Resps>) -> Vec<ActionResponse> {
    let mut responses = Vec::new();
    if let Some(resps) = resps {
        for ResponseBundle(hash, bundle_responses) in resps.full {
            let _ = hash;
            responses.extend(bundle_responses);
        }
    }
    responses
}

fn transaction_to_data_record(mut tx: TransactionRecord, block_height: u64) -> Result<DataRecord> {
    let payload = serde_json::to_vec(&tx).context("failed to encode hl.transactions payload")?;
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
        partition_key: partition_key_or_unknown(&tx.user),
        payload,
    })
}

fn parse_error(res: &ResponseResult) -> Option<String> {
    match res.status.as_str() {
        "err" => res.response.as_str().map(|msg| msg.to_string()),
        "ok" => match &res.response {
            Value::Object(map) => map
                .get("data")
                .and_then(Value::as_object)
                .and_then(|data| data.get("statuses"))
                .and_then(Value::as_array)
                .map(|statuses| {
                    statuses
                        .iter()
                        .filter_map(|status| {
                            status
                                .get("error")
                                .and_then(Value::as_str)
                                .map(|msg| msg.to_string())
                        })
                        .collect::<Vec<String>>()
                })
                .filter(|errors| !errors.is_empty())
                .map(|errors| errors.join("; ")),
            _ => None,
        },
        _ => None,
    }
}

fn string_or_default<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(|value| value.unwrap_or_default())
}
