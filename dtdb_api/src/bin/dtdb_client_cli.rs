use dtdb_api::client::RemoteClient;
use dtdb_relational::DatabaseOptions;
use dtdb_storage::CompressionType;
use futures_util::StreamExt;
use std::io::{self, Write};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server_addr = "http://127.0.0.1:50051".to_string();
    println!(
        "Connecting to remote DuctTapeDB server at {}...",
        server_addr
    );

    let mut client = match RemoteClient::connect(server_addr).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to connect to server: {}", e);
            std::process::exit(1);
        }
    };

    println!("\nWelcome to DuctTapeDB gRPC SQL Client CLI!");
    println!("Commands:");
    println!("  use <database_name>;                                   - Switch database context");
    println!("  create database <name> [options...];                   - Create a new database");
    println!("       Options: compression=uncompressed|lz4");
    println!("                memtable_size=<bytes>");
    println!("                block_size=<bytes>");
    println!("                wal_size=<bytes>");
    println!("                flush_interval=<ms>");
    println!("  drop database <name>;                                  - Delete a database");
    println!(
        "  flush database [<name>];                               - Flush memory buffers to disk"
    );
    println!(
        "  <SQL Query>;                                           - Run SELECT/INSERT/CREATE TABLE/DROP TABLE"
    );
    println!("  exit | quit;                                           - Exit shell");
    println!();

    let mut stdout = io::stdout();
    let stdin = io::stdin();

    let mut query_buffer = String::new();
    let mut active_db: Option<String> = None;

    loop {
        let prompt = match &active_db {
            Some(db) => format!("dtdb_api ({}) > ", db),
            None => "dtdb_api (none) > ".to_string(),
        };

        if query_buffer.is_empty() {
            print!("{}", prompt);
        } else {
            print!("              -> ");
        }
        let _ = stdout.flush();

        let mut line = String::new();
        if stdin.read_line(&mut line).is_err() || line.is_empty() {
            // EOF
            break;
        }

        let trimmed = line.trim();
        if query_buffer.is_empty() && (trimmed == "exit" || trimmed == "quit") {
            println!("Goodbye!");
            break;
        }

        query_buffer.push_str(&line);

        // Check if query ends with semicolon
        if query_buffer.trim_end().ends_with(';') {
            let sql = query_buffer.trim().to_string();
            query_buffer.clear();

            if sql.is_empty() || sql == ";" {
                continue;
            }

            let cmd_clean = sql.trim_end_matches(';').trim();
            let parts: Vec<&str> = cmd_clean.split_whitespace().collect();

            if parts.is_empty() {
                continue;
            }

            // 1. Intercept "USE <db>"
            if parts.len() == 2 && parts[0].eq_ignore_ascii_case("use") {
                let db_name = parts[1];
                active_db = Some(db_name.to_string());
                println!("Database changed to '{}'.", db_name);
                println!();
                continue;
            }

            // 2. Intercept "CREATE DATABASE <name> [options...]"
            if parts.len() >= 3
                && parts[0].eq_ignore_ascii_case("create")
                && parts[1].eq_ignore_ascii_case("database")
            {
                let db_name = parts[2];
                let mut compression = CompressionType::Lz4;
                let mut memtable_size_limit = 1024 * 1024;
                let mut block_size_limit = 4096;
                let mut wal_size_limit = 32 * 1024 * 1024;
                let mut flush_interval_ms = None;

                for part in &parts[3..] {
                    let part_lower = part.to_lowercase();
                    if part_lower.starts_with("compression=") {
                        let comp_val = part_lower.split('=').nth(1).unwrap_or("");
                        if comp_val == "uncompressed" {
                            compression = CompressionType::Uncompressed;
                        } else if comp_val == "lz4" {
                            compression = CompressionType::Lz4;
                        } else {
                            println!(
                                "Warning: Unknown compression type '{}', defaulting to LZ4.",
                                comp_val
                            );
                        }
                    } else if part_lower.starts_with("memtable_size=") {
                        if let Ok(v) = part_lower.split('=').nth(1).unwrap_or("").parse::<usize>() {
                            memtable_size_limit = v;
                        } else {
                            println!("Warning: Invalid memtable_size value.");
                        }
                    } else if part_lower.starts_with("block_size=") {
                        if let Ok(v) = part_lower.split('=').nth(1).unwrap_or("").parse::<usize>() {
                            block_size_limit = v;
                        } else {
                            println!("Warning: Invalid block_size value.");
                        }
                    } else if part_lower.starts_with("wal_size=") {
                        if let Ok(v) = part_lower.split('=').nth(1).unwrap_or("").parse::<usize>() {
                            wal_size_limit = v;
                        } else {
                            println!("Warning: Invalid wal_size value.");
                        }
                    } else if part_lower.starts_with("flush_interval=") {
                        if let Ok(v) = part_lower.split('=').nth(1).unwrap_or("").parse::<u64>() {
                            flush_interval_ms = Some(v);
                        } else {
                            println!("Warning: Invalid flush_interval value.");
                        }
                    }
                }

                let options = DatabaseOptions {
                    compression,
                    memtable_size_limit,
                    block_size_limit,
                    wal_size_limit,
                    flush_interval_ms,
                    l0_compaction_threshold: None,
                    sstable_target_size: None,
                    base_level_size_limit: None,
                    level_size_multiplier: None,
                    max_level: None,
                    block_cache_capacity: Some(1000),
                    analyze_frequency_ms: None,
                    wal_sync_interval_ms: None,
                    memory_budget: None,
                    fsync_method: dtdb_storage::FsyncMethod::default(),
                };

                match client.create_db_with_options(db_name, options).await {
                    Ok(resp) => {
                        println!("{}", resp.message);
                    }
                    Err(e) => {
                        println!("Error: {}", e.message());
                    }
                }
                println!();
                continue;
            }

            // 3. Intercept "DROP DATABASE <name>"
            if parts.len() == 3
                && parts[0].eq_ignore_ascii_case("drop")
                && parts[1].eq_ignore_ascii_case("database")
            {
                let db_name = parts[2];
                match client.drop_db(db_name).await {
                    Ok(resp) => {
                        if resp.success && Some(db_name.to_string()) == active_db {
                            active_db = None;
                        }
                        println!("{}", resp.message);
                    }
                    Err(e) => {
                        println!("Error: {}", e.message());
                    }
                }
                println!();
                continue;
            }

            // 4. Intercept "FLUSH DATABASE [name]"
            if parts.len() >= 2
                && parts[0].eq_ignore_ascii_case("flush")
                && parts[1].eq_ignore_ascii_case("database")
            {
                let target_db = if parts.len() >= 3 {
                    Some(parts[2].to_string())
                } else {
                    active_db.clone()
                };

                match target_db {
                    Some(db) => match client.flush_db(&db).await {
                        Ok(resp) => {
                            println!("{}", resp.message);
                        }
                        Err(e) => {
                            println!("Error: {}", e.message());
                        }
                    },
                    None => {
                        println!(
                            "Error: No database selected. Run 'USE <database_name>;' or specify database name."
                        );
                    }
                }
                println!();
                continue;
            }

            // 5. Regular SQL execution
            let db_name = match &active_db {
                Some(db) => db,
                None => {
                    println!("Error: No database selected. Run 'USE <database_name>;' first.");
                    println!();
                    continue;
                }
            };

            match client
                .execute_query(db_name, dtdb_api::SqlQuery::new(sql))
                .await
            {
                Ok(mut stream) => {
                    let mut columns = Vec::new();
                    let mut rows = Vec::new();
                    let mut info_msg = None;
                    let mut stream_error = None;

                    while let Some(res) = stream.next().await {
                        match res {
                            Ok(resp) => {
                                if let Some(payload) = resp.payload {
                                    match payload {
                                        dtdb_api::proto::execute_query_response::Payload::Header(header) => {
                                            columns = header.column_names;
                                        }
                                        dtdb_api::proto::execute_query_response::Payload::Row(row) => {
                                            rows.push(row.values);
                                        }
                                        dtdb_api::proto::execute_query_response::Payload::InfoMessage(msg) => {
                                            info_msg = Some(msg);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                stream_error = Some(e);
                                break;
                            }
                        }
                    }

                    if let Some(err) = stream_error {
                        println!("Stream Error: {}", err.message());
                    } else if let Some(msg) = info_msg {
                        println!("{}", msg);
                    } else {
                        display_results(&columns, &rows);
                    }
                }
                Err(e) => {
                    println!("Error: {}", e.message());
                }
            }
            println!();
        }
    }

    Ok(())
}

fn display_results(columns: &[String], rows: &[Vec<String>]) {
    if columns.is_empty() {
        println!("Empty set.");
        return;
    }

    let mut col_widths: Vec<usize> = columns.iter().map(|name| name.len()).collect();

    // Calculate max width for each column
    for row in rows {
        for (i, val_str) in row.iter().enumerate() {
            if i < col_widths.len() && val_str.len() > col_widths[i] {
                col_widths[i] = val_str.len();
            }
        }
    }

    // Construct table borders and header
    let mut divider = String::new();
    for w in &col_widths {
        divider.push('+');
        divider.push_str(&"-".repeat(*w + 2));
    }
    divider.push('+');

    let mut header = String::new();
    for (i, col_name) in columns.iter().enumerate() {
        header.push('|');
        header.push_str(&format!(" {:<width$} ", col_name, width = col_widths[i]));
    }
    header.push('|');

    // Print table
    println!("{}", divider);
    println!("{}", header);
    println!("{}", divider);
    for row_vals in rows {
        let mut r_str = String::new();
        for (i, val_str) in row_vals.iter().enumerate() {
            r_str.push('|');
            r_str.push_str(&format!(" {:<width$} ", val_str, width = col_widths[i]));
        }
        r_str.push('|');
        println!("{}", r_str);
    }
    println!("{}", divider);
    println!("{} row(s) in set", rows.len());
}
