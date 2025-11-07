use crate::parsers::{drain_complete_lines, parse_iso8601_to_millis, trim_line_bytes, Parser};
use crate::sorter_client::proto::DataRecord;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{self, Value};
use std::path::Path;
use tracing::{debug, warn};

const LINE_PREVIEW_LIMIT: usize = 256;

#[derive(Default)]
pub struct TransactionsParser {
    buffer: Vec<u8>,
}

#[derive(Serialize)]
struct Transaction {
    time: u64,
    user: String,
    hash: String,
    action: Value,
    block: u64,
    error: Option<String>,
}

#[derive(Deserialize)]
struct ReplicaCmd {
    abci_block: AbciBlock,
    #[serde(rename = "resps", default, deserialize_with = "deserialize_value_vec")]
    _resps: Vec<Value>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct AbciBlock {
    time: String,
    #[serde(default)]
    round: u64,
    #[serde(default, deserialize_with = "deserialize_signed_action_bundles")]
    signed_action_bundles: Vec<SignedActionBundle>,
    #[serde(default)]
    proposer: Option<String>,
    #[serde(default)]
    parent_round: Option<u64>,
    #[serde(default)]
    hardfork: Option<Value>,
}

type SignedActionBundle = (String, SignedActions); // (hash, actions)

#[allow(dead_code)]
#[derive(Deserialize)]
struct SignedActions {
    #[serde(default, deserialize_with = "deserialize_signed_actions_array")]
    signed_actions: Vec<SignedAction>,
    #[serde(default)]
    broadcaster: String,
    #[serde(default)]
    broadcaster_nonce: u64,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct SignedAction {
    #[serde(rename = "vaultAddress", default)]
    vault_address: Option<String>,
    action: Value,
    #[serde(default)]
    nonce: u64,
    #[serde(default)]
    signature: Value,
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

            let line_len = line.len();
            match serde_json::from_slice::<ReplicaCmd>(&line) {
                Ok(replica_cmd) => {
                    let block_number = replica_cmd.abci_block.round;
                    let block_timestamp =
                        parse_iso8601_to_millis(&replica_cmd.abci_block.time).unwrap_or(0);
                    let block_height = if block_number == 0 {
                        None
                    } else {
                        Some(block_number)
                    };
                    let bundle_count = replica_cmd.abci_block.signed_action_bundles.len();

                    debug!(
                        line_idx,
                        line_len,
                        block_round = block_number,
                        bundle_count,
                        "parsed replica_cmd line"
                    );

                    // Extract transactions from signed_action_bundles
                    let mut tx_emitted = 0usize;
                    for (tx_hash, signed_actions) in replica_cmd.abci_block.signed_action_bundles {
                        for signed_action in signed_actions.signed_actions {
                            let transaction = Transaction {
                                time: block_timestamp,
                                hash: tx_hash.clone(),
                                user: signed_action.vault_address.clone().unwrap_or_default(),
                                action: signed_action.action,
                                block: block_number,
                                error: None,
                            };

                            let partition_key = if tx_hash.is_empty() {
                                "unknown".to_string()
                            } else {
                                tx_hash.clone()
                            };

                            let payload = serde_json::to_vec(&transaction)
                                .context("failed to encode transaction payload to JSON")?;

                            // Transactions are timestamped with the enclosing block time.
                            records.push(DataRecord {
                                block_height,
                                tx_hash: if tx_hash.is_empty() {
                                    None
                                } else {
                                    Some(tx_hash.clone())
                                },
                                timestamp: block_timestamp,
                                topic: "hl.transactions".to_string(),
                                partition_key,
                                payload,
                            });
                            tx_emitted += 1;
                            debug!(
                                topic = "hl.transactions",
                                block_round = block_number,
                                tx_hash = %tx_hash,
                                "emitted record"
                            );
                        }
                    }

                    debug!(
                        line_idx,
                        block_round = block_number,
                        tx_emitted,
                        "replica_cmd line emitted records"
                    );
                }
                Err(err) => {
                    let preview = line_preview(&line);
                    warn!(
                        error = %err,
                        line_idx,
                        line_len,
                        preview = %preview,
                        "failed to parse replica_cmds JSON line"
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

fn line_preview(line: &[u8]) -> String {
    let text = String::from_utf8_lossy(line);
    let mut preview = String::new();
    let mut consumed = 0usize;
    for ch in text.chars() {
        if consumed >= LINE_PREVIEW_LIMIT {
            preview.push('…');
            return preview;
        }
        preview.push(ch);
        consumed += 1;
    }
    preview
}

/// Upstream sometimes supplies `resps` as an array or as a map keyed by opaque strings.
/// This deserializer flattens both layouts into a single vector so the rest of the code
/// can treat it uniformly.
fn deserialize_value_vec<'de, D>(deserializer: D) -> Result<Vec<Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    collect_sequence_like(deserializer, "vector field")
}

/// Signed action bundles arrive either as an array of `[tx_hash, bundle]` tuples or as an
/// object keyed by transaction hash. We normalize both shapes into `(hash, bundle)` pairs.
fn deserialize_signed_action_bundles<'de, D>(
    deserializer: D,
) -> Result<Vec<SignedActionBundle>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    match value {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(arr)) => arr
            .into_iter()
            .map(|item| serde_json::from_value(item).map_err(serde::de::Error::custom))
            .collect(),
        Some(Value::Object(map)) => map
            .into_iter()
            .map(|(hash, bundle_value)| {
                serde_json::from_value(bundle_value)
                    .map(|actions| (hash, actions))
                    .map_err(serde::de::Error::custom)
            })
            .collect(),
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected signed action bundles array or map, got {other:?}"
        ))),
    }
}

/// Similar to bundles, `signed_actions` might be either an array or an object keyed by nonce.
/// This keeps deserialization resilient to both versions.
fn deserialize_signed_actions_array<'de, D>(deserializer: D) -> Result<Vec<SignedAction>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    collect_sequence_like(deserializer, "signed actions")?
        .into_iter()
        .map(|item| serde_json::from_value(item).map_err(serde::de::Error::custom))
        .collect()
}

fn collect_sequence_like<'de, D>(
    deserializer: D,
    label: &'static str,
) -> Result<Vec<Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    match value {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) => Ok(items),
        Some(Value::Object(map)) => Ok(map.into_values().collect()),
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected {label} array or object, got {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replica_cmd_accepts_mixed_structures() {
        let json = r#"
        {
            "abci_block": {
                "time": "2024-06-01T00:00:00.000000000",
                "round": 42,
                "signed_action_bundles": {
                    "0xabc": {
                        "broadcaster": "node",
                        "signed_actions": {
                            "first": {
                                "vaultAddress": "vault",
                                "action": {"kind": "test"},
                                "nonce": 7,
                                "signature": {}
                            }
                        }
                    }
                }
            },
            "resps": {
                "foo": {"bar": 1}
            }
        }
        "#;

        let parsed: ReplicaCmd = serde_json::from_str(json).expect("should parse");
        assert_eq!(parsed._resps.len(), 1);
        assert_eq!(parsed.abci_block.signed_action_bundles.len(), 1);
        let (hash, bundle) = &parsed.abci_block.signed_action_bundles[0];
        assert_eq!(hash, "0xabc");
        assert_eq!(bundle.signed_actions.len(), 1);
        assert_eq!(bundle.signed_actions[0].nonce, 7);
    }

    #[test]
    fn signed_actions_accept_array_input() {
        let json = r#"
        {
            "abci_block": {
                "time": "2024-06-01T00:00:00.000000000",
                "round": 1,
                "signed_action_bundles": [
                    [
                        "0xabc",
                        {
                            "signed_actions": [
                                {
                                    "vaultAddress": "vault",
                                    "action": {"kind": "array"},
                                    "nonce": 1,
                                    "signature": {}
                                }
                            ]
                        }
                    ]
                ]
            }
        }
        "#;

        let parsed: ReplicaCmd = serde_json::from_str(json).expect("should parse array");
        assert_eq!(parsed.abci_block.signed_action_bundles.len(), 1);
        let (_, bundle) = &parsed.abci_block.signed_action_bundles[0];
        assert_eq!(bundle.signed_actions.len(), 1);
        assert_eq!(bundle.signed_actions[0].nonce, 1);
    }
}
