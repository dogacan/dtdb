use dtdb_relational::{Database, Transaction};
use dtdb_sql::{ExecutionResult, SqlEngine};
use dtdb_storage::DbValue;
use std::env;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;

fn main() {
    // 1. Parse CLI arguments
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: cargo run --bin dtdb_sql_cli -- <database_directory>");
        std::process::exit(1);
    }
    let db_path = Path::new(&args[1]);

    println!("Opening database at: {:?}", db_path);
    let db = match Database::open(db_path) {
        Ok(d) => Arc::new(d),
        Err(e) => {
            eprintln!("Failed to open database: {}", e);
            std::process::exit(1);
        }
    };

    let engine = SqlEngine::new(db.clone());

    println!("\nWelcome to DuctTapeDB SQL CLI!");
    println!("Type SQL statements ending with ';' to execute.");
    println!("Type 'exit' or 'quit' to quit.\n");

    let mut stdout = io::stdout();
    let stdin = io::stdin();

    let mut query_buffer = String::new();
    let mut next_tx_id = 1;

    loop {
        if query_buffer.is_empty() {
            print!("dtdb_sql > ");
        } else {
            print!("         -> ");
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

        // Check if the query ends with semicolon
        if query_buffer.trim_end().ends_with(';') {
            let sql = query_buffer.trim().to_string();
            query_buffer.clear();

            if sql.is_empty() || sql == ";" {
                continue;
            }

            // Start an auto-committing transaction
            let tx_id = next_tx_id;
            next_tx_id += 1;
            let tx = Transaction::new(tx_id, db.clone());

            match engine.execute(&sql, &tx) {
                Ok(result) => {
                    if let Err(e) = tx.commit() {
                        println!("Failed to commit transaction: {}", e);
                    } else {
                        display_result(result);
                    }
                }
                Err(e) => {
                    let _ = tx.rollback();
                    println!("Error: {}", e);
                }
            }
            println!();
        }
    }
}

fn display_result(res: ExecutionResult) {
    match res {
        ExecutionResult::CreateTable => {
            println!("Table created successfully.");
        }
        ExecutionResult::DropTable => {
            println!("Table dropped successfully.");
        }
        ExecutionResult::CreateIndex => {
            println!("Index created successfully.");
        }
        ExecutionResult::DropIndex => {
            println!("Index dropped successfully.");
        }
        ExecutionResult::AlterTable => {
            println!("Table altered successfully.");
        }
        ExecutionResult::Insert { count } => {
            println!("Inserted {} row(s).", count);
        }
        ExecutionResult::Delete { count } => {
            println!("Deleted {} row(s).", count);
        }
        ExecutionResult::Update { count } => {
            println!("Updated {} row(s).", count);
        }
        ExecutionResult::Select { schema, rows } => {
            if rows.is_empty() {
                println!("Empty set.");
                return;
            }

            // Format SELECT query results nicely as a text table
            let mut col_widths: Vec<usize> =
                schema.columns.iter().map(|col| col.name.len()).collect();

            // Calculate max width for each column
            let mut row_strings = Vec::new();
            for row in &rows {
                let mut formatted_vals = Vec::new();
                for (i, val) in row.values.iter().enumerate() {
                    let val_str = match val {
                        DbValue::Int(v) => v.to_string(),
                        DbValue::Float(v) => v.to_string(),
                        DbValue::String(s) => s.clone(),
                        DbValue::Bytes(b) => format!("{:?}", b),
                        DbValue::Bool(b) => b.to_string(),
                        DbValue::Null => "NULL".to_string(),
                    };
                    if val_str.len() > col_widths[i] {
                        col_widths[i] = val_str.len();
                    }
                    formatted_vals.push(val_str);
                }
                row_strings.push(formatted_vals);
            }

            // Construct table borders and header
            let mut divider = String::new();
            for w in &col_widths {
                divider.push('+');
                divider.push_str(&"-".repeat(*w + 2));
            }
            divider.push('+');

            let mut header = String::new();
            for (i, col) in schema.columns.iter().enumerate() {
                header.push('|');
                header.push_str(&format!(" {:<width$} ", col.name, width = col_widths[i]));
            }
            header.push('|');

            // Print table
            println!("{}", divider);
            println!("{}", header);
            println!("{}", divider);
            for row_vals in row_strings {
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
        ExecutionResult::Analyze => {
            println!("Table analyzed successfully.");
        }
    }
}
