//! Phase-timing harness for a primary-key point lookup (`SELECT ... WHERE id = ?`).
//!
//! It times each stage of the query pipeline independently so the per-query
//! cost can be attributed to: SQL parsing, logical planning, optimization, and
//! everything left over (physical compilation + storage execution + the
//! transaction begin/commit). This is "profiling by manual instrumentation":
//! we already know the stage boundaries, so we measure each one in a tight,
//! warmed-up loop rather than asking a sampling profiler to find them.
//!
//! Run (release build with the workspace's optimization settings):
//!   cargo run -p dtdb_sql --release --example profile_point_lookup

use dtdb_relational::{Database, Transaction};
use dtdb_sql::{LogicalPlanner, Optimizer, SqlEngine, SqlStatement};
use dtdb_storage::DbValue;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

const ROWS: usize = 5_000;
const ITERS: usize = 200_000;
const WARMUP: usize = 20_000;

/// Runs `f` `WARMUP` times (untimed) to settle caches/branch predictors, then
/// `ITERS` times under a single clock read at each end. Returns nanoseconds per
/// call. One timer around the whole loop (not per call) keeps the ~20ns
/// `Instant::now()` overhead from polluting sub-microsecond measurements.
fn bench(mut f: impl FnMut(usize)) -> f64 {
    for i in 0..WARMUP {
        f(i);
    }
    let start = Instant::now();
    for i in 0..ITERS {
        f(i);
    }
    start.elapsed().as_nanos() as f64 / ITERS as f64
}

fn main() {
    // A throwaway directory (examples can't use the `tempfile` dev-dependency).
    let dir = std::env::temp_dir().join(format!("dtdb_profile_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let db = Arc::new(Database::open(&dir).unwrap());
    let engine = SqlEngine::new(db.clone());

    // Schema + data, matching the dtdb-vs-SQLite read benchmark.
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
    {
        let tx = Transaction::new(2, db.clone());
        for i in 0..ROWS {
            engine
                .execute(
                    &format!(
                        "INSERT INTO bench (id, k, v) VALUES ({i}, {}, 'val_{i:08}')",
                        (i * 31) % 100_000
                    ),
                    &tx,
                )
                .unwrap();
        }
        tx.commit().unwrap();
    }

    let dialect = GenericDialect {};
    // Vary the key each call so we measure real lookups, not a single hot key.
    let sql_for = |i: usize| format!("SELECT k, v FROM bench WHERE id = {}", (i * 97) % ROWS);

    // --- Stage 1: parse only (sqlparser) ---
    let t_parse = bench(|i| {
        let stmts = Parser::parse_sql(&dialect, &sql_for(i)).unwrap();
        black_box(stmts);
    });

    // --- Stage 1+2: parse + logical planning ---
    let t_parse_plan = bench(|i| {
        let mut stmts = Parser::parse_sql(&dialect, &sql_for(i)).unwrap();
        let plan = LogicalPlanner::new(db.clone())
            .plan(&stmts.remove(0))
            .unwrap();
        black_box(plan);
    });

    // --- Stage 1+2+3: parse + plan + optimize ---
    let t_parse_plan_opt = bench(|i| {
        let mut stmts = Parser::parse_sql(&dialect, &sql_for(i)).unwrap();
        let plan = LogicalPlanner::new(db.clone())
            .plan(&stmts.remove(0))
            .unwrap();
        if let SqlStatement::Query(p) = plan {
            let opt = Optimizer::new(db.clone()).optimize(p);
            black_box(opt);
        }
    });

    // --- Transaction begin+commit overhead (empty read-only txn) ---
    let mut txid = 1_000u64;
    let t_txn = bench(|_i| {
        let tx = Transaction::new(txid, db.clone());
        txid += 1;
        tx.commit().unwrap();
    });

    // --- Full realistic cycle: begin + execute + commit (fresh txn each time) ---
    // This is exactly what the benchmark's `query_rows` does per point lookup.
    let mut txid2 = 1_000_000u64;
    let t_full = bench(|i| {
        let tx = Transaction::new(txid2, db.clone());
        txid2 += 1;
        let res = engine.execute(&sql_for(i), &tx).unwrap();
        tx.commit().unwrap();
        black_box(res);
    });

    // --- Prepared statement: parse once, then bind + execute + commit ---
    // Skips re-parsing on every call; everything else is unchanged.
    let prepared = engine
        .prepare("SELECT k, v FROM bench WHERE id = :id")
        .unwrap();
    let mut txid3 = 2_000_000u64;
    let t_prepared = bench(|i| {
        let tx = Transaction::new(txid3, db.clone());
        txid3 += 1;
        let mut params = std::collections::HashMap::new();
        params.insert("id".to_string(), DbValue::Int(((i * 97) % ROWS) as i64));
        let res = engine.execute_prepared(&prepared, &tx, &params).unwrap();
        tx.commit().unwrap();
        black_box(res);
    });

    // Derived per-stage costs.
    let plan = t_parse_plan - t_parse;
    let optimize = t_parse_plan_opt - t_parse_plan;
    // `execute()` internally redoes parse+plan+optimize, so subtracting the
    // standalone pipeline plus the txn begin/commit leaves physical compilation
    // + storage execution (the actual point get + projection).
    let physical = (t_full - t_txn - t_parse_plan_opt).max(0.0);

    let row = |label: &str, ns: f64| {
        println!(
            "  {label:<28} {ns:>9.0} ns  ({:>5.1}%)",
            100.0 * ns / t_full
        );
    };

    println!(
        "\nPoint lookup: SELECT k, v FROM bench WHERE id = ?   ({ROWS} rows, {ITERS} iters)\n"
    );
    row("parse (sqlparser)", t_parse);
    row("logical planning", plan);
    row("optimize (CBO)", optimize);
    row("physical compile + execute", physical);
    row("txn begin + commit", t_txn);
    println!("  {:-<46}", "");
    println!(
        "  {:<28} {:>9.0} ns  (100.0%)",
        "full cycle (measured)", t_full
    );
    println!(
        "\n  full cycle = {:.2} us/query  (~{:.0} k queries/s single-threaded)\n",
        t_full / 1000.0,
        1_000_000.0 / t_full
    );

    println!(
        "  prepared (parse + plan cached) = {:.2} us/query  ({:.0}% of full, {:.2}x faster)\n",
        t_prepared / 1000.0,
        100.0 * t_prepared / t_full,
        t_full / t_prepared
    );

    let _ = std::fs::remove_dir_all(&dir);
}
