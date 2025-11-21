//! Schema verification example - validates all parser outputs against downstream spec
//!
//! Usage: cargo run --example verify_schemas

use hl_agent::parsers::{
    BlocksParser, FillsParser, MiscEventsParser, OrdersParser, Parser, TradesParser,
    TransactionsParser,
};
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Schema Compliance Verification ===\n");

    let summary = vec![
        ("Blocks", verify_blocks_schema()?),
        ("Transactions", verify_transactions_schema()?),
        ("Trades", verify_trades_schema()?),
        ("Fills", verify_fills_schema()?),
        ("Orders", verify_orders_schema()?),
        ("Misc events", verify_misc_events_schema()?),
    ];

    println!("\n=== Verification Summary ===");
    for (name, status) in &summary {
        match status {
            SchemaStatus::Verified => println!(" - {name}: ✅ verified"),
            SchemaStatus::Skipped(reason) => println!(" - {name}: ⚠️ skipped ({reason})"),
        }
    }

    println!("\n=== Verification Complete ===");
    Ok(())
}

fn verify_blocks_schema() -> Result<SchemaStatus, Box<dyn std::error::Error>> {
    println!("📦 Blocks Schema Verification");
    println!(
        "Expected fields: height (number), blockTime (number), hash (string), proposer (string), numTxs (number)"
    );

    let sample_data = match read_sample_data(&[
        "~/hl-data/periodic_abci_states/hourly/test_blocks.jsonl",
        "/home/bytenoob/hl-data/periodic_abci_states/hourly/test_blocks.jsonl",
    ])? {
        Some(data) => data,
        None => {
            println!("⚠️  No blocks sample data found\n");
            return Ok(SchemaStatus::Skipped("sample data missing".into()));
        }
    };

    let mut parser = BlocksParser::default();
    let records = parser.parse(Path::new("blocks_sample"), &sample_data)?;

    if records.is_empty() {
        println!("⚠️  No blocks parsed from sample data\n");
        return Ok(SchemaStatus::Skipped("parser returned 0 records".into()));
    }

    let payload: Value = serde_json::from_slice(&records[0].payload)?;
    let fields = payload
        .as_object()
        .ok_or("❌ Blocks payload is not a JSON object")?;

    let expected_types: HashMap<&str, &[&str]> = HashMap::from([
        ("height", &["Number"][..]),
        ("blockTime", &["Number"][..]),
        ("hash", &["String"][..]),
        ("proposer", &["String"][..]),
        ("numTxs", &["Number"][..]),
    ]);

    for field in ["height", "blockTime", "hash", "proposer", "numTxs"] {
        if let Some(types) = expected_types.get(field) {
            check_field(fields, field, types)?;
        }
    }

    println!("✅ Blocks schema compliant\n");
    Ok(SchemaStatus::Verified)
}

fn verify_transactions_schema() -> Result<SchemaStatus, Box<dyn std::error::Error>> {
    println!("💸 Transactions Schema Verification");
    println!("Expected fields: time (number ms), user (string), action (object), block (number), hash (string), error (string|null)");

    let sample_data = match read_sample_data(&[
        "/home/bytenoob/hl-data/replica_cmds/hourly/20251107",
        "~/hl-data/replica_cmds/hourly/20251107",
    ])? {
        Some(data) => data,
        None => {
            println!("⚠️  No transactions sample data found\n");
            return Ok(SchemaStatus::Skipped("sample data missing".into()));
        }
    };

    let mut parser = TransactionsParser::default();
    let records = parser.parse(Path::new("transactions_sample"), &sample_data)?;

    if records.is_empty() {
        println!("⚠️  No transactions parsed\n");
        return Ok(SchemaStatus::Skipped("parser returned 0 records".into()));
    }

    let payload: Value = serde_json::from_slice(&records[0].payload)?;
    let fields = payload
        .as_object()
        .ok_or("❌ Transactions payload is not a JSON object")?;

    let expected_types: HashMap<&str, &[&str]> = HashMap::from([
        ("time", &["Number"][..]),
        ("user", &["String"][..]),
        ("action", &["Object"][..]),
        ("block", &["Number"][..]),
        ("hash", &["String"][..]),
        ("error", &["Null", "String"]),
    ]);

    for field in ["time", "user", "action", "block", "hash", "error"] {
        if let Some(types) = expected_types.get(field) {
            check_field(fields, field, types)?;
        }
    }

    if fields.contains_key("vaultAddress") {
        return Err("❌ Found deprecated 'vaultAddress' field - should be 'user'".into());
    }

    println!("✅ Transactions schema compliant\n");
    Ok(SchemaStatus::Verified)
}

fn verify_trades_schema() -> Result<SchemaStatus, Box<dyn std::error::Error>> {
    println!("📊 Trades Schema Verification");
    println!(
        "Expected fields: coin, side, px, sz, time (number), hash, tid (number), users (array)"
    );

    let sample_data = match read_sample_data(&[
        "/home/bytenoob/hl-data/node_trades/hourly/test_trades.jsonl",
        "~/hl-data/node_trades/hourly/test_trades.jsonl",
    ])? {
        Some(data) => data,
        None => {
            println!("⚠️  No trades sample data found\n");
            return Ok(SchemaStatus::Skipped("sample data missing".into()));
        }
    };

    let mut parser = TradesParser::default();
    let records = parser.parse(Path::new("trades_sample"), &sample_data)?;

    if records.is_empty() {
        println!("⚠️  No trades parsed\n");
        return Ok(SchemaStatus::Skipped("parser returned 0 records".into()));
    }

    let payload: Value = serde_json::from_slice(&records[0].payload)?;
    let fields = payload
        .as_object()
        .ok_or("❌ Trades payload is not a JSON object")?;

    let expected_types: HashMap<&str, &[&str]> = HashMap::from([
        ("coin", &["String"][..]),
        ("side", &["String"][..]),
        ("px", &["Number", "String"]),
        ("sz", &["Number", "String"]),
        ("time", &["Number"][..]),
        ("hash", &["String"][..]),
        ("tid", &["Number"][..]),
        ("users", &["Array"][..]),
    ]);

    for field in ["coin", "side", "px", "sz", "time", "hash", "tid", "users"] {
        if let Some(types) = expected_types.get(field) {
            check_field(fields, field, types)?;
        }
    }

    println!("✅ Trades schema compliant (tid is number)\n");
    Ok(SchemaStatus::Verified)
}

fn verify_fills_schema() -> Result<SchemaStatus, Box<dyn std::error::Error>> {
    println!("💰 Fills Schema Verification");
    println!(
        "Expected fields: hash (string), crossed (bool), tid (number), liquidation (object|null)"
    );

    let sample_data = match read_sample_data(&[
        "/home/bytenoob/hl-data/node_fills_by_block/hourly/20251107",
        "~/hl-data/node_fills_by_block/hourly/20251107",
    ])? {
        Some(data) => data,
        None => {
            println!("⚠️  No fills sample data found\n");
            return Ok(SchemaStatus::Skipped("sample data missing".into()));
        }
    };

    let mut parser = FillsParser::default();
    let records = parser.parse(Path::new("fills_sample"), &sample_data)?;

    if records.is_empty() {
        println!("⚠️  No fills parsed\n");
        return Ok(SchemaStatus::Skipped("parser returned 0 records".into()));
    }

    let payload: Value = serde_json::from_slice(&records[0].payload)?;
    let fields = payload
        .as_object()
        .ok_or("❌ Fills payload is not a JSON object")?;

    let expected_types: HashMap<&str, &[&str]> = HashMap::from([
        ("hash", &["String"][..]),
        ("crossed", &["Bool"][..]),
        ("tid", &["Number"][..]),
        ("liquidation", &["Null", "Object"]),
    ]);

    for field in ["hash", "crossed", "tid", "liquidation"] {
        if let Some(types) = expected_types.get(field) {
            check_field(fields, field, types)?;
        }
    }

    println!("✅ Fills schema compliant (hash/crossed/tid added)\n");
    Ok(SchemaStatus::Verified)
}

fn verify_orders_schema() -> Result<SchemaStatus, Box<dyn std::error::Error>> {
    println!("📋 Orders Schema Verification");
    println!("(No published Schema - verifying structure exists)");

    let sample_data = match read_sample_data(&[
        "/home/bytenoob/hl-data/node_raw_book_diffs/hourly/test/sample.jsonl",
        "~/hl-data/node_raw_book_diffs/hourly/test/sample.jsonl",
    ])? {
        Some(data) => data,
        None => {
            println!("⚠️  No orders sample data found\n");
            return Ok(SchemaStatus::Skipped("sample data missing".into()));
        }
    };

    let mut parser = OrdersParser::default();
    let records = parser.parse(Path::new("orders_sample"), &sample_data)?;

    if records.is_empty() {
        println!("⚠️  No orders parsed\n");
        return Ok(SchemaStatus::Skipped("parser returned 0 records".into()));
    }

    println!("✅ Orders parser functional (schema TBD by downstream)\n");
    Ok(SchemaStatus::Verified)
}

fn verify_misc_events_schema() -> Result<SchemaStatus, Box<dyn std::error::Error>> {
    println!("🔔 Misc Events Schema Verification");
    println!(
        "Expected fields: time (ISO string), hash (string), inner (object with event type key)"
    );

    let sample_data = match read_sample_data(&[
        "/home/bytenoob/hl-data/misc_events_by_block/hourly",
        "~/hl-data/misc_events_by_block/hourly",
        "/home/bytenoob/hl-data/misc_events/hourly",
        "~/hl-data/misc_events/hourly",
    ])? {
        Some(data) => data,
        None => {
            println!("⚠️  No misc events files found\n");
            return Ok(SchemaStatus::Skipped("sample data missing".into()));
        }
    };

    let mut parser = MiscEventsParser::default();
    let records = parser.parse(Path::new("misc_events_sample"), &sample_data)?;

    if records.is_empty() {
        println!("⚠️  No misc events parsed\n");
        return Ok(SchemaStatus::Skipped("parser returned 0 records".into()));
    }

    let payload: Value = serde_json::from_slice(&records[0].payload)?;
    let fields = payload
        .as_object()
        .ok_or("❌ Misc events payload is not a JSON object")?;

    let expected_types: HashMap<&str, &[&str]> = HashMap::from([
        ("time", &["String"][..]),
        ("hash", &["String"][..]),
        ("inner", &["Object"][..]),
    ]);

    for field in ["time", "hash", "inner"] {
        if let Some(types) = expected_types.get(field) {
            check_field(fields, field, types)?;
        }
    }

    if fields.contains_key("eventType") || fields.contains_key("data") {
        return Err("❌ Found deprecated flat structure - should use 'inner' wrapper".into());
    }

    println!("✅ Misc events schema compliant (inner wrapper added)\n");
    Ok(SchemaStatus::Verified)
}

fn check_field(
    fields: &serde_json::Map<String, Value>,
    field_name: &str,
    expected_types: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let value = fields
        .get(field_name)
        .ok_or_else(|| format!("❌ Missing required field: {field_name}"))?;

    let actual_type = match value {
        Value::Null => "Null",
        Value::Bool(_) => "Bool",
        Value::Number(_) => "Number",
        Value::String(_) => "String",
        Value::Array(_) => "Array",
        Value::Object(_) => "Object",
    };

    if !expected_types.contains(&actual_type) {
        return Err(
            format!(
                "❌ Field '{field_name}' has wrong type: expected {expected_types:?}, got {actual_type}"
            )
            .into(),
        );
    }

    Ok(())
}

#[derive(Debug, Clone)]
enum SchemaStatus {
    Verified,
    Skipped(String),
}

fn read_sample_data(paths: &[&str]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    for raw in paths {
        let candidate = expand_home_path(raw);
        match try_read_path(&candidate) {
            Ok(bytes) => {
                println!("   ↳ Loaded sample data from {}", candidate.display());
                return Ok(Some(bytes));
            }
            Err(err) => {
                if err.kind() != io::ErrorKind::NotFound {
                    println!("   ↳ ⚠️  Failed to read {}: {}", candidate.display(), err);
                }
            }
        }
    }
    Ok(None)
}

fn try_read_path(path: &Path) -> io::Result<Vec<u8>> {
    match fs::metadata(path) {
        Ok(metadata) => {
            if metadata.is_file() {
                fs::read(path)
            } else if metadata.is_dir() {
                let mut entries: Vec<_> =
                    fs::read_dir(path)?.filter_map(|entry| entry.ok()).collect();
                entries.sort_by_key(|entry| entry.file_name());
                for entry in entries {
                    if entry.file_type().map(|ty| ty.is_file()).unwrap_or(false) {
                        if let Ok(bytes) = fs::read(entry.path()) {
                            return Ok(bytes);
                        }
                    }
                }
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("no files found inside {}", path.display()),
                ))
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported path type {}", path.display()),
                ))
            }
        }
        Err(err) => Err(err),
    }
}

fn expand_home_path(path: &str) -> PathBuf {
    if let Some(stripped) = path.strip_prefix("~/") {
        if let Ok(home) = env::var("HOME") {
            return PathBuf::from(home).join(stripped);
        }
    }
    PathBuf::from(path)
}
