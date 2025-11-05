use hl_agent::parsers::{FillsParser, Parser};
use std::path::Path;

fn main() {
    tracing_subscriber::fmt::init();

    let sample_file = "/tmp/3_compact.jsonl";

    println!("📁 Reading file: {}", sample_file);
    let sample_data = match std::fs::read(sample_file) {
        Ok(data) => {
            println!("✅ Read {} bytes", data.len());
            data
        }
        Err(e) => {
            eprintln!("❌ Failed to read file: {}", e);
            return;
        }
    };

    println!("\n📝 Parsing with FillsParser...");
    let mut parser = FillsParser::default();

    match parser.parse(
        Path::new("node_order_statuses/hourly/20251105/3"),
        &sample_data,
    ) {
        Ok(records) => {
            println!("✅ Successfully parsed {} records", records.len());
            println!();

            if let Some(first) = records.first() {
                println!("📊 First Record:");
                println!("  ├─ block_height: {:?}", first.block_height);
                println!(
                    "  ├─ tx_hash: {:?}",
                    first.tx_hash.as_ref().map(|h| &h[..20])
                );
                println!("  ├─ timestamp: {}", first.timestamp);
                println!("  ├─ topic: {}", first.topic);
                println!("  ├─ partition_key: {}", first.partition_key);
                println!("  └─ payload size: {} bytes", first.payload.len());
                println!();

                // Try to parse payload as JSON
                if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&first.payload) {
                    println!("📄 Payload (formatted):");
                    let formatted = serde_json::to_string_pretty(&json).unwrap();
                    for (i, line) in formatted.lines().take(15).enumerate() {
                        println!("  {}", line);
                    }
                    if formatted.lines().count() > 15 {
                        println!("  ... ({} more lines)", formatted.lines().count() - 15);
                    }
                } else {
                    println!("⚠️  Payload is not valid JSON");
                }
            }

            println!("\n📊 Summary:");
            println!("  ├─ Total records: {}", records.len());
            println!(
                "  ├─ Has block_height: {}",
                records.iter().filter(|r| r.block_height.is_some()).count()
            );
            println!(
                "  ├─ Has tx_hash: {}",
                records.iter().filter(|r| r.tx_hash.is_some()).count()
            );
            if records.len() > 0 {
                println!(
                    "  └─ Avg payload size: {} bytes",
                    records.iter().map(|r| r.payload.len()).sum::<usize>() / records.len()
                );
            }
        }
        Err(e) => {
            eprintln!("❌ Parse failed: {}", e);
            eprintln!("\n🔍 Error details:");
            eprintln!("{:?}", e);
        }
    }
}
