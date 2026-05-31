//! Phase-timing harness for client-side API point lookup.
//!
//! Run:
//!   cargo run -p dtdb_api --release --example profile_client_point_lookup

use dtdb_api::client::DuctTapeDbClient;
use dtdb_api::sql_query;
use dtdb_relational::{Database, Transaction};
use dtdb_sql::SqlEngine;
use dtdb_storage::{CompressionType, DbValue};
use futures_util::StreamExt;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

const ROWS: usize = 5_000;
const ITERS: usize = 20_000;
const WARMUP: usize = 5_000;

fn bench(iters: usize, mut f: impl FnMut(usize)) -> f64 {
    for i in 0..WARMUP {
        f(i);
    }
    let start = Instant::now();
    for i in 0..iters {
        f(i);
    }
    start.elapsed().as_nanos() as f64 / iters as f64
}

fn main() {
    let dir = std::env::temp_dir().join(format!("dtdb_profile_cpl_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut client = DuctTapeDbClient::in_process(&dir).unwrap();

    // Set up schema and population
    rt.block_on(async {
        client
            .create_db("bench", CompressionType::Lz4)
            .await
            .unwrap();

        let mut stream = client
            .execute_query(
                "bench",
                sql_query!("CREATE TABLE bench (id INT PRIMARY KEY, k INT, v STRING)"),
            )
            .await
            .unwrap();
        while let Some(r) = stream.next().await {
            r.unwrap();
        }

        // Populate in one transaction
        client
            .run_in_transaction("bench", |tx| async move {
                for i in 0..ROWS {
                    tx.execute_query(
                        sql_query!("INSERT INTO bench (id, k, v) VALUES (@id, @k, @v)")
                            .bind("id", i as i64)
                            .bind("k", ((i * 31) % 100_000) as i64)
                            .bind("v", format!("val_{i:08}")),
                    )
                    .await?;
                }
                Ok(())
            })
            .await
            .unwrap();
    });

    // We can extract the underlying service/engine to measure raw execution
    // Let's create a raw SqlEngine on the same DB directory to compare
    let raw_db = Arc::new(Database::open(dir.join("bench")).unwrap());
    let raw_engine = SqlEngine::new(raw_db.clone());
    let (db, engine) = (raw_db, raw_engine);

    // Helper queries
    let sql_for = |i: usize| format!("SELECT k, v FROM bench WHERE id = {}", (i * 97) % ROWS);
    let template = "SELECT k, v FROM bench WHERE id = @id";

    // --- 1. Raw Engine (Non-cached literal SQL) ---
    let mut txid = 1_000u64;
    let t_raw_engine = bench(ITERS, |i| {
        let tx = Transaction::new(txid, db.clone());
        txid += 1;
        let res = engine.execute(&sql_for(i), &tx).unwrap();
        tx.commit().unwrap();
        black_box(res);
    });

    // --- 2. Client API (Non-cached literal SQL, which the benchmark runs) ---
    let mut client2 = client.clone();
    let t_client_literal = bench(ITERS, |i| {
        rt.block_on(async {
            let mut stream = client2
                .execute_query("bench", dtdb_api::SqlQuery::new(sql_for(i)))
                .await
                .unwrap();
            let mut rows = 0;
            while let Some(r) = stream.next().await {
                let _resp = r.unwrap();
                rows += 1;
            }
            black_box(rows);
        });
    });

    // --- 3. Client API (Cached template query with parameters) ---
    let mut client3 = client.clone();
    let t_client_parameterized = bench(ITERS, |i| {
        rt.block_on(async {
            let mut stream = client3
                .execute_query(
                    "bench",
                    sql_query!("SELECT k, v FROM bench WHERE id = @id")
                        .bind("id", ((i * 97) % ROWS) as i64),
                )
                .await
                .unwrap();
            let mut rows = 0;
            while let Some(r) = stream.next().await {
                let _resp = r.unwrap();
                rows += 1;
            }
            black_box(rows);
        });
    });

    // --- 4. Breakdown of Client Wrapper Steps ---
    // 4a. is_ddl on dynamic statement (cache miss)
    let t_is_ddl_miss = bench(ITERS, |i| {
        black_box(engine.is_ddl(&sql_for(i)));
    });

    // 4b. is_ddl on fixed template (cache hit)
    let t_is_ddl_hit = bench(ITERS, |_| {
        black_box(engine.is_ddl(template));
    });

    // 4c. building SqlQuery + bind
    let t_build_query = bench(ITERS, |i| {
        let q = sql_query!("SELECT k, v FROM bench WHERE id = @id")
            .bind("id", ((i * 97) % ROWS) as i64);
        black_box(q);
    });

    // 4d. rebuilding params HashMap
    let sample = sql_query!("SELECT k, v FROM bench WHERE id = @id").bind("id", 1i64);
    let t_build_params = bench(ITERS, |_| {
        let mut params = std::collections::HashMap::with_capacity(sample.bindings().len());
        for (name, val) in sample.bindings() {
            params.insert(name.clone(), val.clone());
        }
        black_box(params);
    });

    // 4e. Engine work (execute_with_params, cached)
    let engine_tx = Transaction::new(50_000, db.clone());
    // WARMUP cache
    let mut warmup_params = std::collections::HashMap::new();
    warmup_params.insert("id".to_string(), DbValue::Int(0));
    engine
        .execute_with_params(template, &engine_tx, &warmup_params)
        .unwrap();

    let t_engine_cached = bench(ITERS, |i| {
        let mut params = std::collections::HashMap::new();
        params.insert("id".to_string(), DbValue::Int(((i * 97) % ROWS) as i64));
        let res = engine
            .execute_with_params(template, &engine_tx, &params)
            .unwrap();
        black_box(res);
    });
    engine_tx.commit().unwrap();

    println!("\nPoint Lookup Profiling (SELECT k, v FROM bench WHERE id = ?)\n");
    println!("  {:^55}", "--- Overall Comparison ---");
    println!(
        "  {:<38} {:>9.2} us/query",
        "Raw Engine (literal SQL)",
        t_raw_engine / 1000.0
    );
    println!(
        "  {:<38} {:>9.2} us/query",
        "Client API (literal SQL, benchmarked)",
        t_client_literal / 1000.0
    );
    println!(
        "  {:<38} {:>9.2} us/query",
        "Client API (parameterized, cached)",
        t_client_parameterized / 1000.0
    );
    println!("  {:-<55}", "");

    println!("\n  {:^55}", "--- Client-Side Wrapper Breakdown ---");
    println!(
        "  {:<38} {:>9.2} us/call",
        "is_ddl (DDL guard, cache miss)",
        t_is_ddl_miss / 1000.0
    );
    println!(
        "  {:<38} {:>9.2} us/call",
        "is_ddl (DDL guard, cache hit)",
        t_is_ddl_hit / 1000.0
    );
    println!(
        "  {:<38} {:>9.2} us/call",
        "build SqlQuery (sql_query! + bind)",
        t_build_query / 1000.0
    );
    println!(
        "  {:<38} {:>9.2} us/call",
        "rebuild params HashMap",
        t_build_params / 1000.0
    );
    println!(
        "  {:<38} {:>9.2} us/call",
        "execute_with_params (engine, cached)",
        t_engine_cached / 1000.0
    );

    let unaccounted_literal = (t_client_literal - t_raw_engine - t_is_ddl_miss).max(0.0);
    let unaccounted_cached =
        (t_client_parameterized - t_engine_cached - t_is_ddl_hit - t_build_query - t_build_params)
            .max(0.0);
    println!("\n  {:^55}", "--- Async & Communication Overhead ---");
    println!(
        "  {:<38} {:>9.2} us/call",
        "API overhead for Literal SQL",
        unaccounted_literal / 1000.0
    );
    println!(
        "  {:<38} {:>9.2} us/call",
        "API overhead for Parameterized SQL",
        unaccounted_cached / 1000.0
    );
    println!("\n  * API overhead includes: tokio block_on runtime entry/exit, spawn_blocking,");
    println!(
        "    mpsc channel stream creation, and consuming response payloads over the stream.\n"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
