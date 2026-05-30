//! Phase-timing harness for a full-scan aggregate
//! (`SELECT COUNT(*), SUM(k) FROM bench WHERE k BETWEEN ? AND ?`).
//!
//! This is the dtdb-vs-SQLite benchmark's worst case (~26x slower than SQLite),
//! and unlike the point lookup it is purely CPU-bound: no fsync, all 5000 rows
//! already resident. We decompose the per-query cost into SQL parse, logical
//! planning, optimization, the raw storage scan (row production), and the
//! leftover operator work (Volcano filter + aggregate dispatch). The raw scan
//! is timed directly via `scan_iter` so the irreducible storage cost is
//! separated from the operator overhead a tighter scan/aggregate could reclaim.
//!
//! Run (release build with the workspace's optimization settings):
//!   cargo run -p dtdb_sql --release --example profile_scan_aggregate

use dtdb_relational::{Database, Transaction};
use dtdb_sql::{LogicalPlanner, Optimizer, SqlEngine, SqlStatement};
use dtdb_storage::DbKey;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

const ROWS: usize = 5_000;
const ITERS: usize = 5_000;
const WARMUP: usize = 500;

/// Runs `f` `WARMUP` times (untimed) to settle caches/branch predictors, then
/// `ITERS` times under a single clock read at each end. Returns nanoseconds per
/// call.
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
    let dir = std::env::temp_dir().join(format!("dtdb_profile_scan_{}", std::process::id()));
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
    let sql = "SELECT COUNT(*), SUM(k) FROM bench WHERE k BETWEEN 0 AND 50000";

    // --- Stage 1: parse only (sqlparser) ---
    let t_parse = bench(|_i| {
        let stmts = Parser::parse_sql(&dialect, sql).unwrap();
        black_box(stmts);
    });

    // --- Stage 1+2: parse + logical planning ---
    let t_parse_plan = bench(|_i| {
        let mut stmts = Parser::parse_sql(&dialect, sql).unwrap();
        let plan = LogicalPlanner::new(db.clone())
            .plan(&stmts.remove(0))
            .unwrap();
        black_box(plan);
    });

    // --- Stage 1+2+3: parse + plan + optimize ---
    let t_parse_plan_opt = bench(|_i| {
        let mut stmts = Parser::parse_sql(&dialect, sql).unwrap();
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

    // --- Full realistic cycle: begin + execute + commit ---
    let mut txid2 = 1_000_000u64;
    let t_full = bench(|_i| {
        let tx = Transaction::new(txid2, db.clone());
        txid2 += 1;
        let res = engine.execute(sql, &tx).unwrap();
        tx.commit().unwrap();
        black_box(res);
    });

    // --- Raw storage scan: produce every row, touch `k`, count. ---
    // This is exactly what the operator tree consumes: `scan_iter` with the
    // referenced-column hint (`k`), driven to exhaustion. It isolates the
    // per-row storage cost (Row::from_bytes + merge_rows + the per-row HashMap
    // churn in TableScanIterator::advance) from the filter/aggregate operators.
    let k_cols = vec!["k".to_string()];
    let (lo, hi) = (DbKey::Int(i64::MIN), DbKey::Int(i64::MAX));
    let mut txid3 = 2_000_000u64;
    let t_scan_raw = bench(|_i| {
        let tx = Transaction::new(txid3, db.clone());
        txid3 += 1;
        let mut it = tx.scan_iter("bench", &lo, &hi, Some(&k_cols)).unwrap();
        let mut n = 0u64;
        while let Some(row) = it.next().unwrap() {
            black_box(&row);
            n += 1;
        }
        tx.commit().unwrap();
        black_box(n);
    });

    // --- Raw storage scan, NO column hint (all locality groups). ---
    // Shows how much the column hint actually saves on this single-group table.
    let mut txid4 = 3_000_000u64;
    let t_scan_full = bench(|_i| {
        let tx = Transaction::new(txid4, db.clone());
        txid4 += 1;
        let mut it = tx.scan_iter("bench", &lo, &hi, None).unwrap();
        let mut n = 0u64;
        while let Some(row) = it.next().unwrap() {
            black_box(&row);
            n += 1;
        }
        tx.commit().unwrap();
        black_box(n);
    });

    // Derived per-stage costs.
    let plan = t_parse_plan - t_parse;
    let optimize = t_parse_plan_opt - t_parse_plan;
    // execute() redoes parse+plan+optimize internally; subtracting the
    // standalone pipeline + txn + raw scan leaves the operator work: the
    // PhysicalFilter predicate eval and the aggregate accumulation, plus the
    // Volcano next()/dyn-dispatch/adapter glue.
    let operators = (t_full - t_txn - t_parse_plan_opt - t_scan_raw).max(0.0);

    let pct = |ns: f64| 100.0 * ns / t_full;
    let row = |label: &str, ns: f64| {
        println!("  {label:<30} {ns:>10.0} ns  ({:>5.1}%)", pct(ns));
    };

    println!("\n{sql}");
    println!("  ({ROWS} rows scanned, {ITERS} iters)\n");
    row("parse (sqlparser)", t_parse);
    row("logical planning", plan);
    row("optimize (CBO)", optimize);
    row("raw scan (storage rows)", t_scan_raw);
    row("operators (filter + aggregate)", operators);
    row("txn begin + commit", t_txn);
    println!("  {:-<50}", "");
    println!(
        "  {:<30} {:>10.0} ns  (100.0%)",
        "full cycle (measured)", t_full
    );

    println!(
        "\n  full cycle = {:.2} us/query   ({:.1} ns per row, amortized)",
        t_full / 1000.0,
        t_full / ROWS as f64
    );
    println!(
        "  raw scan   = {:.2} us         ({:.1} ns per row)",
        t_scan_raw / 1000.0,
        t_scan_raw / ROWS as f64
    );
    println!(
        "  raw scan (no column hint) = {:.2} us   ({:.1} ns per row)",
        t_scan_full / 1000.0,
        t_scan_full / ROWS as f64
    );
    println!(
        "\n  storage scan is {:.0}% of the full cycle; operators are {:.0}%.",
        pct(t_scan_raw),
        pct(operators)
    );

    let _ = std::fs::remove_dir_all(&dir);
}
