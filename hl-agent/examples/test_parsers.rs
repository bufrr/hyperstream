use hl_agent::parsers::{BlocksParser, OrdersParser, Parser, TransactionsParser};
use std::fs;
use std::path::Path;

fn main() {
    println!("=== Testing TransactionsParser ===");
    test_transactions();

    println!("\n=== Testing OrdersParser ===");
    test_orders();

    println!("\n=== Testing BlocksParser ===");
    test_blocks();
}

fn test_transactions() {
    // Test with production data first
    let prod_path = Path::new("/tmp/test_failing_tx.jsonl");
    if prod_path.exists() {
        println!("  Testing production data first...");
        let data = fs::read(prod_path).expect("Failed to read test_failing_tx.jsonl");
        println!("  Read {} bytes from production data", data.len());
        let mut parser = TransactionsParser::default();
        match parser.parse(prod_path, &data) {
            Ok(records) => println!("  ✓ Production data: {} records", records.len()),
            Err(e) => println!("  ✗ Production data error: {e}"),
        }
    }

    let path = Path::new("/tmp/test_tx.jsonl");
    let data = fs::read(path).expect("Failed to read test_tx.jsonl");
    println!("  Read {} bytes from test data", data.len());
    let mut parser = TransactionsParser::default();

    match parser.parse(path, &data) {
        Ok(records) => {
            if records.is_empty() {
                println!("✗ Parsed 0 transaction records (expected at least 1)");
            } else {
                // Count records by topic
                let mut topic_counts = std::collections::HashMap::new();
                for record in &records {
                    *topic_counts.entry(&record.topic).or_insert(0) += 1;
                }
                println!("✓ Parsed {} records total:", records.len());
                for (topic, count) in &topic_counts {
                    println!("   - {topic}: {count} records");
                }
                if let Some(first) = records.first() {
                    println!("  First record topic: {}", first.topic);
                    println!("  First record partition_key: {}", first.partition_key);
                }
            }
        }
        Err(e) => {
            println!("✗ Parse error: {e}");
        }
    }
    println!("  Backlog: {} bytes", parser.backlog_len());
}

fn test_orders() {
    let path = Path::new("/tmp/test_orders.jsonl");
    let data = fs::read(path).expect("Failed to read test_orders.jsonl");
    let mut parser = OrdersParser::default();

    match parser.parse(path, &data) {
        Ok(records) => {
            println!("✓ Parsed {} order records", records.len());
            if let Some(first) = records.first() {
                println!("  First record topic: {}", first.topic);
                println!("  First record partition_key: {}", first.partition_key);
                println!("  First record payload size: {} bytes", first.payload.len());
            }
        }
        Err(e) => {
            println!("✗ Parse error: {e}");
        }
    }
    println!("  Backlog: {} bytes", parser.backlog_len());
}

fn test_blocks() {
    let path = Path::new("/tmp/test_block.rmp");
    let data = fs::read(path).expect("Failed to read test_block.rmp");
    let mut parser = BlocksParser::default();

    match parser.parse(path, &data) {
        Ok(records) => {
            println!("✓ Parsed {} block records", records.len());
            if let Some(first) = records.first() {
                println!("  First record topic: {}", first.topic);
                println!("  First record partition_key: {}", first.partition_key);
                println!("  First record payload size: {} bytes", first.payload.len());
            }
        }
        Err(e) => {
            println!("✗ Parse error: {e}");
        }
    }
    println!("  Backlog: {} bytes", parser.backlog_len());
}
