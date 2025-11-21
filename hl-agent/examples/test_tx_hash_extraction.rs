use hl_agent::parsers::{transactions::TransactionsParser, Parser};
use std::fs::File;
use std::io::Read;
use std::path::Path;

fn main() {
    println!("{}", "=".repeat(80));
    println!("TRANSACTION HASH EXTRACTION TEST");
    println!("{}", "=".repeat(80));
    println!();

    let test_file = "/home/bytenoob/hl-data/replica_cmds/2025-11-17T03:40:44Z/20251117/799380000";

    println!("📂 Reading: {test_file}");

    let mut file = File::open(test_file).expect("Failed to open file");
    let mut data = Vec::new();
    file.read_to_end(&mut data).expect("Failed to read file");

    println!("   ✓ Read {} bytes", data.len());
    println!();

    println!("🔍 Parsing transactions...");
    let mut parser = TransactionsParser::default();
    let path = Path::new(test_file);

    match parser.parse(path, &data) {
        Ok(records) => {
            println!("   ✓ Parsed {} records", records.len());
            println!();

            if records.is_empty() {
                println!("⚠️  No records found!");
            } else {
                println!("✅ SUCCESS! Showing first 3 transactions:");
                println!();

                for (i, record) in records.iter().take(3).enumerate() {
                    println!("Transaction {}:", i + 1);
                    println!("  Topic:        {}", record.topic);
                    println!("  Partition key: {}", record.partition_key);
                    println!("  Block height: {:?}", record.block_height);
                    println!("  TX hash:      {:?}", record.tx_hash);
                    println!("  Timestamp:    {}", record.timestamp);

                    // Decode payload to show hash inside
                    if let Ok(payload_json) =
                        serde_json::from_slice::<serde_json::Value>(&record.payload)
                    {
                        println!("  Payload:");
                        if let Some(hash) = payload_json.get("hash") {
                            println!("    hash: {hash}");
                        }
                        if let Some(user) = payload_json.get("user") {
                            println!("    user: {user}");
                        }
                        if let Some(block) = payload_json.get("block") {
                            println!("    block: {block}");
                        }
                    }
                    println!();
                }
            }
        }
        Err(e) => {
            println!("❌ Error: {e}");
        }
    }
}
