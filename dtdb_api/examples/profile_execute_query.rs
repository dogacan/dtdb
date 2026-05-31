//! Phase-timing harness for the per-statement *client wrapper* overhead that
//! `TransactionClient::execute_query` adds on top of the SQL engine.
//!
//! The benchmark found that routing parameterized INSERTs through the client
//! interface showed no gain from the parse cache. This sizes why, by measuring
//! each thing the in-process wrapper does per call:
//!   - `SqlEngine::is_ddl(text)` -- which RE-PARSES the SQL (uncached!) just to
//!     reject DDL inside a transaction;
//!   - building the `SqlQuery` via `sql_query!` + `.bind()`;
//!   - rebuilding the `params` HashMap from the bindings;
//!   - the actual engine work, `execute_with_params` (parse-cached).
//!
//! Run:
//!   cargo run -p dtdb_api --release --example profile_execute_query

use dtdb_api::sql_query;
use dtdb_relational::{Database, Transaction};
use dtdb_sql::SqlEngine;
use dtdb_storage::DbValue;
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

const N: usize = 20_000;
const WARMUP: usize = 5_000;
const TMPL: &str = "INSERT INTO bench (id, k, v) VALUES (@id, @k, @v)";

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

fn build_params(i: usize) -> HashMap<String, DbValue> {
    let mut m = HashMap::new();
    m.insert("id".to_string(), DbValue::Int(i as i64));
    m.insert("k".to_string(), DbValue::Int(((i * 31) % 100_000) as i64));
    m.insert("v".to_string(), DbValue::String(format!("val_{i:08}")));
    m
}

fn main() {
    let dir = std::env::temp_dir().join(format!("dtdb_profile_eq_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let db = Arc::new(Database::open(&dir).unwrap());
    let engine = SqlEngine::new(db.clone());
    {
        let tx = Transaction::new(1, db.clone());
        engine
            .execute(
                "CREATE TABLE bench (id INT PRIMARY KEY, k INT, v STRING)",
                &tx,
            )
            .unwrap();
        tx.commit().unwrap();
    }

    // 1. is_ddl: the wrapper calls this per statement to reject DDL inside a
    //    transaction. It re-parses the SQL with sqlparser (NOT via the cache).
    let t_is_ddl = bench(20_000, |_| {
        black_box(engine.is_ddl(TMPL));
    });

    // 2. SqlQuery construction: sql_query! + three .bind() calls.
    let t_build_query = bench(20_000, |i| {
        let q = sql_query!("INSERT INTO bench (id, k, v) VALUES (@id, @k, @v)")
            .bind("id", i as i64)
            .bind("k", ((i * 31) % 100_000) as i64)
            .bind("v", format!("val_{i:08}"));
        black_box(q);
    });

    // 3. params HashMap rebuilt from the SqlQuery bindings (what the wrapper does
    //    before calling the engine).
    let sample = sql_query!("INSERT INTO bench (id, k, v) VALUES (@id, @k, @v)")
        .bind("id", 1i64)
        .bind("k", 2i64)
        .bind("v", "val_00000001");
    let t_build_params = bench(20_000, |_| {
        let mut params = HashMap::with_capacity(sample.bindings().len());
        for (name, val) in sample.bindings() {
            params.insert(name.clone(), val.clone());
        }
        black_box(params);
    });

    // 4. The actual engine work: execute_with_params on the fixed template
    //    (parse cache hits after the first call). Unique ids satisfy the
    //    duplicate-PK check; params pre-built so only engine cost is timed.
    let params: Vec<HashMap<String, DbValue>> = (0..WARMUP + N).map(build_params).collect();
    {
        let wtx = Transaction::new(2, db.clone());
        for p in &params[..WARMUP] {
            engine.execute_with_params(TMPL, &wtx, p).unwrap();
        }
        wtx.commit().unwrap();
    }
    let mtx = Transaction::new(3, db.clone());
    let start = Instant::now();
    for p in &params[WARMUP..] {
        engine.execute_with_params(TMPL, &mtx, p).unwrap();
    }
    let t_execute_cached = start.elapsed().as_nanos() as f64 / N as f64;
    mtx.commit().unwrap();

    // The wrapper overhead is everything the client does on top of the engine.
    // (Response construction -- a Vec + one InfoMessage String for an INSERT --
    // is small and lives behind a pub(crate) fn, so it is not measured here.)
    let wrapper = t_is_ddl + t_build_query + t_build_params;
    let total = wrapper + t_execute_cached;

    let pct = |ns: f64| 100.0 * ns / total;
    let row = |label: &str, ns: f64| {
        println!("  {label:<34} {ns:>9.0} ns  ({:>5.1}%)", pct(ns));
    };

    println!("\nPer-statement client `execute_query` cost (in-process, INSERT)\n");
    row("is_ddl (RE-PARSE, uncached)", t_is_ddl);
    row("build SqlQuery (sql_query! + bind)", t_build_query);
    row("rebuild params HashMap", t_build_params);
    row("execute_with_params (engine, cached)", t_execute_cached);
    println!("  {:-<52}", "");
    println!("  {:<34} {:>9.0} ns  (100.0%)", "approx total", total);
    println!(
        "\n  wrapper overhead = {:.2} us/call  ({:.0}% of the total), of which is_ddl\n  \
         alone is {:.2} us -- a full redundant parse that negates the parse cache.",
        wrapper / 1000.0,
        pct(wrapper),
        t_is_ddl / 1000.0,
    );
    println!(
        "  the engine itself (cached) is only {:.2} us/call.\n",
        t_execute_cached / 1000.0
    );

    let _ = std::fs::remove_dir_all(&dir);
}
