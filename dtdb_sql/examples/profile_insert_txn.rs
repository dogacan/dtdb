//! Phase-timing harness for batched INSERTs (`insert_txn` benchmark shape:
//! many INSERTs in one transaction, a single commit/fsync at the end).
//!
//! dtdb is ~10x slower than SQLite here, and with the fsync amortized over the
//! whole batch this is per-INSERT *CPU* cost, not durability. We decompose one
//! `engine.execute("INSERT ...")` into: SQL preprocess+parse, logical planning,
//! the `get_table` schema clone (paid more than once per insert), the
//! duplicate-primary-key `get` (a point lookup before every write), and the
//! write-buffer `put`. The single commit is measured separately and amortized.
//!
//! Run:
//!   cargo run -p dtdb_sql --release --example profile_insert_txn

use dtdb_relational::{Database, Transaction};
use dtdb_sql::{LogicalPlanner, SqlEngine};
use dtdb_storage::{DbKey, DbValue};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

const N: usize = 20_000;
const WARMUP: usize = 5_000;

fn insert_sql(i: usize) -> String {
    format!(
        "INSERT INTO bench (id, k, v) VALUES ({i}, {}, 'val_{i:08}')",
        (i * 31) % 100_000
    )
}

/// Stateless micro-bench: run `f` WARMUP times untimed, then `iters` timed.
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
    let dir = std::env::temp_dir().join(format!("dtdb_profile_ins_{}", std::process::id()));
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

    let dialect = GenericDialect {};

    // Pre-build the SQL strings (the benchmark builds them outside the timed
    // loop), so `format!` is not counted in parse/execute timings. Ids 0..N are
    // unique, satisfying the duplicate-PK check during the full-insert run.
    let sqls: Vec<String> = (0..N).map(insert_sql).collect();
    let sql_at = |i: usize| sqls[i % N].as_str();

    // --- Stateless pipeline stages (don't mutate the table) ---
    let t_parse = bench(50_000, |i| {
        let stmts = Parser::parse_sql(&dialect, sql_at(i)).unwrap();
        black_box(stmts);
    });
    let t_parse_plan = bench(50_000, |i| {
        let mut stmts = Parser::parse_sql(&dialect, sql_at(i)).unwrap();
        let plan = LogicalPlanner::new(db.clone())
            .plan(&stmts.remove(0))
            .unwrap();
        black_box(plan);
    });

    // --- get_table: the per-call Table/Schema clone ---
    let t_get_table = bench(50_000, |_i| {
        black_box(db.get_table("bench").unwrap());
    });

    // --- Duplicate-PK check shape: a point `get` that misses (ids are unique
    // in the batch, so the check always misses) ---
    let probe_tx = Transaction::new(2, db.clone());
    let t_get_miss = bench(50_000, |i| {
        let r = probe_tx
            .get("bench", &DbKey::Int((10_000_000 + i) as i64))
            .unwrap();
        black_box(r);
    });

    // --- Ground truth: N INSERTs in one transaction, then one commit. ---
    // Warm up with a disjoint id range in a throwaway txn (also makes the
    // measured ids unique vs. warmup).
    {
        let tx = Transaction::new(3, db.clone());
        for i in 0..WARMUP {
            engine.execute(&insert_sql(100_000_000 + i), &tx).unwrap();
        }
        tx.commit().unwrap();
    }
    let tx = Transaction::new(4, db.clone());
    let start = Instant::now();
    for s in &sqls {
        engine.execute(s, &tx).unwrap();
    }
    let t_full_insert = start.elapsed().as_nanos() as f64 / N as f64;
    let commit_start = Instant::now();
    tx.commit().unwrap();
    let t_commit_total = commit_start.elapsed().as_nanos() as f64;
    let t_commit_amortized = t_commit_total / N as f64;

    // --- Prepared-statement path: parse + plan ONCE, then execute N times with
    // bound params. Quantifies the main lever (the literal path above re-parses
    // every statement). Params are pre-built, mirroring the pre-built SQL
    // strings, so we compare engine cost to engine cost. Disjoint id range so
    // the duplicate-PK check still misses. ---
    let prepared = engine
        .prepare("INSERT INTO bench (id, k, v) VALUES (:id, :k, :v)")
        .unwrap();
    let build_params = |i: usize| {
        let mut m = HashMap::new();
        m.insert("id".to_string(), DbValue::Int(i as i64));
        m.insert("k".to_string(), DbValue::Int(((i * 31) % 100_000) as i64));
        m.insert("v".to_string(), DbValue::String(format!("val_{i:08}")));
        m
    };
    let prep_params: Vec<HashMap<String, DbValue>> =
        (200_000_000..200_000_000 + N).map(build_params).collect();
    {
        // Warmup in a throwaway txn (disjoint ids).
        let wtx = Transaction::new(5, db.clone());
        for i in 300_000_000..300_000_000 + WARMUP {
            engine
                .execute_prepared(&prepared, &wtx, &build_params(i))
                .unwrap();
        }
        wtx.commit().unwrap();
    }
    let ptx = Transaction::new(6, db.clone());
    let prep_start = Instant::now();
    for p in &prep_params {
        engine.execute_prepared(&prepared, &ptx, p).unwrap();
    }
    let t_prepared = prep_start.elapsed().as_nanos() as f64 / N as f64;
    ptx.commit().unwrap();

    // Derived.
    let plan = t_parse_plan - t_parse;
    let pipeline = t_parse_plan; // preprocess+parse+plan
    let machinery = (t_full_insert - pipeline).max(0.0); // get_table x N + get + put + row build + validate

    let pct = |ns: f64| 100.0 * ns / t_full_insert;
    let row = |label: &str, ns: f64| {
        println!("  {label:<30} {ns:>9.0} ns  ({:>5.1}%)", pct(ns));
    };

    println!("\nBatched INSERT: 1 txn, {N} rows, 1 commit   (per-insert ns)\n");
    row("preprocess + parse", t_parse);
    row("logical planning", plan);
    row("write machinery (rest)", machinery);
    println!("  {:-<48}", "");
    println!(
        "  {:<30} {:>9.0} ns  (100.0%)",
        "full insert (measured)", t_full_insert
    );
    println!(
        "  {:<30} {:>9.0} ns  (amortized over {N})",
        "commit/fsync", t_commit_amortized
    );

    println!("\n  Components within 'write machinery' (measured separately):");
    println!(
        "    get_table (Table/Schema clone)  {:>7.0} ns   (called ~3x per insert)",
        t_get_table
    );
    println!("    dup-PK get (point lookup miss)  {:>7.0} ns", t_get_miss);

    println!(
        "\n  full insert = {:.2} us/row   ({:.0} k rows/s single-threaded)",
        t_full_insert / 1000.0,
        1_000_000.0 / t_full_insert
    );
    println!(
        "  of which pipeline {:.0}% ({:.2} us), machinery {:.0}% ({:.2} us)\n",
        pct(pipeline),
        pipeline / 1000.0,
        pct(machinery),
        machinery / 1000.0
    );

    println!(
        "  prepared (parse+plan once) = {:.2} us/row   ({:.2}x faster than literal)",
        t_prepared / 1000.0,
        t_full_insert / t_prepared
    );
    println!(
        "  -> reusing the parse/plan removes ~{:.2} us/row; the residual {:.2} us is\n     \
         the write machinery + per-call param binding the prepared path still pays.\n",
        (t_full_insert - t_prepared) / 1000.0,
        t_prepared / 1000.0,
    );

    let _ = std::fs::remove_dir_all(&dir);
}
