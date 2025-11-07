use hl_agent::parsers::{BlocksParser, Parser};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = "/home/bytenoob/hl-data/periodic_abci_states/20251107/788420000.rmp";
    let data = std::fs::read(path)?;
    println!("Read {} bytes from {}", data.len(), path);

    let mut parser = BlocksParser::default();
    match parser.parse(Path::new(path), &data) {
        Ok(records) => {
            println!("✓ Parsed {} block records", records.len());
            if let Some(record) = records.first() {
                println!("\n=== First record ===");
                println!("Topic: {}", record.topic);
                println!("Partition key: {}", record.partition_key);
                println!("Block height: {:?}", record.block_height);
                println!("Timestamp: {}", record.timestamp);
                println!("\nPayload:");
                let payload: serde_json::Value = serde_json::from_slice(&record.payload)?;
                println!("{}", serde_json::to_string_pretty(&payload)?);
            }
        }
        Err(e) => {
            eprintln!("✗ Failed to parse: {}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}
