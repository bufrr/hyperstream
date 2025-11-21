use rmp_serde::Deserializer;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::io::Cursor;

#[derive(Deserialize)]
struct AnyBlock {
    #[serde(flatten)]
    fields: HashMap<String, Value>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: cargo run --example inspect_blocks <file.rmp>");
        std::process::exit(1);
    });

    let data = std::fs::read(&path)?;
    let mut cursor = Cursor::new(&data[..]);
    let mut deserializer = Deserializer::new(&mut cursor);

    match AnyBlock::deserialize(&mut deserializer) {
        Ok(block) => {
            println!("✓ Successfully parsed MessagePack block");
            println!("\nFields found:");
            for (key, value) in &block.fields {
                println!("  {key}: {value:?}");
            }

            println!("\n=== Full JSON representation ===");
            let pretty = serde_json::to_string_pretty(&block.fields)?;
            println!("{pretty}");
        }
        Err(e) => {
            eprintln!("✗ Failed to parse: {e}");
            eprintln!("\nFirst 200 bytes (hex):");
            for chunk in data.chunks(20).take(10) {
                for byte in chunk {
                    eprint!("{byte:02x} ");
                }
                eprintln!();
            }
        }
    }

    Ok(())
}
