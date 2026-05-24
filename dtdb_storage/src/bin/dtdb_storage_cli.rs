use dtdb_storage::{CompressionType, DbKey, DbValue, EngineOptions, StorageEngine};
use std::env;
use std::io::{self, Write};
use std::path::Path;

fn main() {
    // 1. Parse CLI arguments
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: cargo run --bin dtdb_storage_cli -- <database_directory>");
        std::process::exit(1);
    }
    let db_path = Path::new(&args[1]);

    println!("Opening database at: {:?}", db_path);
    // Open storage engine with default limits and parameters
    let options = EngineOptions {
        compression: CompressionType::Lz4,
        memtable_size_limit: 1024 * 1024,
        block_size_limit: 4096,
        wal_size_limit: 32 * 1024 * 1024,
        l0_compaction_threshold: 4,
        sstable_target_size: 2 * 1024 * 1024,
        base_level_size_limit: 10 * 1024 * 1024,
        level_size_multiplier: 10,
        max_level: 7,
    };
    let engine = match StorageEngine::open(db_path, options) {
        Ok(eng) => eng,
        Err(e) => {
            eprintln!("Failed to open database: {}", e);
            std::process::exit(1);
        }
    };

    println!("\nWelcome to DuctTapeDB Storage CLI!");
    println!("Supported commands:");
    println!("  put <key> <value>   - Insert or update a key");
    println!("  get <key>           - Fetch a key");
    println!("  delete <key>        - Delete a key");
    println!("  scan                - Scan the entire database");
    println!("  scan <k1> <k2>      - Scan a range of keys [k1, k2] (inclusive)");
    println!("  flush               - Flush memtable to disk");
    println!("  exit / quit         - Exit the CLI\n");

    let mut stdout = io::stdout();
    let stdin = io::stdin();

    loop {
        print!("dtdb > ");
        let _ = stdout.flush();

        let mut input = String::new();
        if stdin.read_line(&mut input).is_err() || input.is_empty() {
            // EOF or error, break loop
            break;
        }

        let trimmed = input.trim();
        if trimmed.is_empty() {
            continue;
        }

        let tokens: Vec<&str> = trimmed.split_whitespace().collect();
        let cmd = tokens[0].to_lowercase();

        match cmd.as_str() {
            "exit" | "quit" => {
                println!("Goodbye!");
                break;
            }
            "put" => {
                if tokens.len() < 3 {
                    println!("Usage: put <key> <value>");
                    continue;
                }
                let key = parse_key(tokens[1]);
                let val = parse_value(tokens[2]);
                match engine.put(key, val) {
                    Ok(_) => println!("OK"),
                    Err(e) => println!("Error: {}", e),
                }
            }
            "get" => {
                if tokens.len() < 2 {
                    println!("Usage: get <key>");
                    continue;
                }
                let key = parse_key(tokens[1]);
                match engine.get(&key) {
                    Ok(Some(val)) => println!("{:?}", val),
                    Ok(None) => println!("Key not found"),
                    Err(e) => println!("Error: {}", e),
                }
            }
            "delete" => {
                if tokens.len() < 2 {
                    println!("Usage: delete <key>");
                    continue;
                }
                let key = parse_key(tokens[1]);
                match engine.delete(key) {
                    Ok(_) => println!("OK"),
                    Err(e) => println!("Error: {}", e),
                }
            }
            "scan" => {
                if tokens.len() == 1 {
                    // Full scan: Scan Int keys and String keys separately and combine.
                    // This is because DbKey has two variants (Int and String).
                    // In Rust enum ordering: Int comes before String.
                    let int_start = DbKey::Int(i64::MIN);
                    let int_end = DbKey::Int(i64::MAX);
                    let str_start = DbKey::String("".to_string());
                    let str_end = DbKey::String("\u{10ffff}".to_string());

                    match engine.filtered_scan(&int_start, &int_end, |_, _| true) {
                        Ok(int_res) => {
                            for (k, v) in int_res {
                                println!("{:?} => {:?}", k, v);
                            }
                        }
                        Err(e) => println!("Int Scan Error: {}", e),
                    }
                    match engine.filtered_scan(&str_start, &str_end, |_, _| true) {
                        Ok(str_res) => {
                            for (k, v) in str_res {
                                println!("{:?} => {:?}", k, v);
                            }
                        }
                        Err(e) => println!("String Scan Error: {}", e),
                    }
                } else if tokens.len() == 3 {
                    // Range scan: scan [k1, k2]
                    let k1 = parse_key(tokens[1]);
                    let k2 = parse_key(tokens[2]);
                    match engine.filtered_scan(&k1, &k2, |_, _| true) {
                        Ok(res) => {
                            if res.is_empty() {
                                println!("No keys found in range");
                            } else {
                                for (k, v) in res {
                                    println!("{:?} => {:?}", k, v);
                                }
                            }
                        }
                        Err(e) => println!("Scan Error: {}", e),
                    }
                } else {
                    println!("Usage: scan  OR  scan <k1> <k2>");
                }
            }
            "flush" => {
                println!("Flushing memtable...");
                match engine.flush_memtable() {
                    Ok(_) => println!("Flush complete."),
                    Err(e) => println!("Error: {}", e),
                }
            }
            other => {
                println!("Unknown command: '{}'. Type exit to quit.", other);
            }
        }
    }
}

/// Helper to parse input string into a DbKey (Int if numeric, otherwise String).
fn parse_key(s: &str) -> DbKey {
    if let Ok(val) = s.parse::<i64>() {
        DbKey::Int(val)
    } else {
        DbKey::String(s.to_string())
    }
}

/// Helper to parse input string into a DbValue (Int if numeric, Float if decimal, otherwise String).
fn parse_value(s: &str) -> DbValue {
    if let Ok(val) = s.parse::<i64>() {
        DbValue::Int(val)
    } else if s.contains('.') {
        if let Ok(val) = s.parse::<f64>() {
            DbValue::Float(val)
        } else {
            DbValue::String(s.to_string())
        }
    } else {
        DbValue::String(s.to_string())
    }
}
