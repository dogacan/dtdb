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

use dtdb_relational::{Column, DataType, Database, Row, Schema, Transaction};
use dtdb_sql::{LogicalPlanner, Optimizer, SqlEngine, SqlStatement};
use dtdb_storage::{DbKey, DbValue};
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
    // per-row storage cost (Row::from_bytes + TableScanIterator::advance) from
    // the filter/aggregate operators.
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

    // --- Layered decomposition of the raw scan: peel back one layer at a time
    // so we can split the 400 ns/row into INHERENT work (storage read +
    // bincode deserialize) versus CHURN (the per-row HashMap/merge dance and
    // the transaction wrapper). Each probe drops one layer relative to
    // `t_scan_raw` (= tx.scan_iter, the full thing).

    // Grab the default-locality-group engine directly. On the default schema
    // (no LOCALITY GROUP clause) every column lives in group "".
    let table = db.get_table("bench").unwrap();
    let storage_engine = table.engines.get("").unwrap().clone();

    // Layer 0: raw storage read only. Iterate the engine's ScanIterator and
    // touch each (key, Bytes) pair WITHOUT deserializing. This is the
    // irreducible cost of pulling rows out of the LSM.
    let t_engine_read = bench(|_i| {
        let mut it = storage_engine.scan_iter(&lo, &hi).unwrap();
        let mut n = 0u64;
        while let Some((k, v)) = it.next().unwrap() {
            black_box(&k);
            black_box(&v);
            n += 1;
        }
        black_box(n);
    });

    // Layer 1: storage read + bincode deserialize. Same scan, but decode each
    // row's bytes into a Row (the unavoidable `Row::from_bytes` in advance()).
    // The delta over layer 0 is the pure deserialize cost.
    let t_engine_read_deser = bench(|_i| {
        let mut it = storage_engine.scan_iter(&lo, &hi).unwrap();
        let mut n = 0u64;
        while let Some((_k, v)) = it.next().unwrap() {
            if let DbValue::Bytes(bytes) = &v {
                let row = Row::from_bytes(bytes).unwrap();
                black_box(&row);
            }
            n += 1;
        }
        black_box(n);
    });

    // Layer 2: the table-level merge (TableScanIterator), driven by peek/advance
    // but WITHOUT the transaction wrapper. On this single-group table the
    // single-group fast path applies, so the delta over layer 1 is just the
    // fast path's take/refill bookkeeping (~0 ns after the slot-indexed
    // rewrite). The multi-group merge cost is measured separately below.
    let t_table_scan = bench(|_i| {
        let mut it = table.scan_iter(&lo, &hi, Some(&k_cols)).unwrap();
        let mut n = 0u64;
        // peek() only returns the already-materialized row ref (no work);
        // advance() does the per-row merge. The borrow from peek() must end
        // before advance(), hence the is_some()/unwrap shape.
        while it.peek().is_some() {
            let (k, row) = it.peek().unwrap();
            black_box(k);
            black_box(row);
            n += 1;
            it.advance().unwrap();
        }
        black_box(n);
    });

    // (Layer 3 = `t_scan_raw` above: adds the transaction wrapper's write-buffer
    // merge + read-set tracking on top of the table merge.)

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

    // --- The key question: of the raw scan, how much is INHERENT (storage read
    // + deserialize) vs CHURN (the table merge + txn wrapper bookkeeping)? After
    // levers (1)-(3) the churn is essentially gone for single-group scans. ---
    let per = |ns: f64| ns / ROWS as f64;
    let deserialize = (t_engine_read_deser - t_engine_read).max(0.0);
    let merge_churn = (t_table_scan - t_engine_read_deser).max(0.0);
    let txn_layer = (t_scan_raw - t_table_scan).max(0.0);
    let inherent = t_engine_read + deserialize;
    let churn = merge_churn + txn_layer;

    println!("\n  Raw-scan decomposition (ns per row):");
    println!(
        "    storage read (LSM)          {:>7.1}   <- inherent",
        per(t_engine_read)
    );
    println!(
        "    bincode deserialize         {:>7.1}   <- inherent",
        per(deserialize)
    );
    println!(
        "    table merge (fast path)     {:>7.1}   <- ~0 after single-group + slot-index",
        per(merge_churn)
    );
    println!(
        "    txn wrapper (wbuf/read-set) {:>7.1}   <- churn-ish",
        per(txn_layer)
    );
    println!("    {:-<40}", "");
    println!(
        "    raw scan total              {:>7.1}   (inherent {:.0}% / churn {:.0}%)",
        per(t_scan_raw),
        100.0 * inherent / t_scan_raw,
        100.0 * churn / t_scan_raw
    );

    // --- Multi-group merge path ---------------------------------------------
    // The single-group fast path does NOT apply when a scan spans more than one
    // locality group, so this isolates the cost of the index-keyed merge that
    // stitches per-group sub-rows back together. We build a 2-group copy of the
    // dataset (id, k in the default group; v in "lg_v"), scan all columns
    // (forcing both groups), and decompose: 2 raw storage reads + 2 deserializes
    // (inherent) vs the merge glue on top.
    let mg_schema = Schema::new(vec![
        Column {
            id: 0,
            name: "id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_nullable: false,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            id: 0,
            name: "k".to_string(),
            data_type: DataType::Int,
            is_primary_key: false,
            is_nullable: true,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            id: 0,
            name: "v".to_string(),
            data_type: DataType::String,
            is_primary_key: false,
            is_nullable: true,
            locality_group: Some("lg_v".to_string()),
            default_value: None,
            is_auto_increment: false,
        },
    ]);
    db.create_table("bench2", mg_schema).unwrap();
    {
        let tx = Transaction::new(9_000_000, db.clone());
        for i in 0..ROWS {
            engine
                .execute(
                    &format!(
                        "INSERT INTO bench2 (id, k, v) VALUES ({i}, {}, 'val_{i:08}')",
                        (i * 31) % 100_000
                    ),
                    &tx,
                )
                .unwrap();
        }
        tx.commit().unwrap();
    }

    let table2 = db.get_table("bench2").unwrap();
    let eng_default = table2.engines.get("").unwrap().clone();
    let eng_v = table2.engines.get("lg_v").unwrap().clone();

    // Inherent 2-group cost: read both engines in lockstep + deserialize both
    // sub-rows, with no merge.
    let mut txid7 = 6_000_000u64;
    let t_mg_raw = bench(|_i| {
        let _tx = Transaction::new(txid7, db.clone());
        txid7 += 1;
        let mut it1 = eng_default.scan_iter(&lo, &hi).unwrap();
        let mut it2 = eng_v.scan_iter(&lo, &hi).unwrap();
        let mut n = 0u64;
        while let (Some((_, a)), Some((_, b))) = (it1.next().unwrap(), it2.next().unwrap()) {
            if let DbValue::Bytes(ba) = &a {
                black_box(Row::from_bytes(ba).unwrap());
            }
            if let DbValue::Bytes(bb) = &b {
                black_box(Row::from_bytes(bb).unwrap());
            }
            n += 1;
        }
        black_box(n);
    });

    // Table-level merge path (no txn wrapper): exercises the index-keyed merge.
    let t_mg_table = bench(|_i| {
        let mut it = table2.scan_iter(&lo, &hi, None).unwrap();
        let mut n = 0u64;
        // Borrow-only (no per-row clone), matching the single-group probe.
        while it.peek().is_some() {
            let (k, row) = it.peek().unwrap();
            black_box(k);
            black_box(row);
            n += 1;
            it.advance().unwrap();
        }
        black_box(n);
    });

    let mg_merge = (t_mg_table - t_mg_raw).max(0.0);
    println!("\n  Multi-group (2 locality groups) merge path (ns per row):");
    println!(
        "    2x raw read + deserialize   {:>7.1}   <- inherent",
        per(t_mg_raw)
    );
    println!(
        "    index-keyed merge glue      {:>7.1}   <- the (3) path",
        per(mg_merge)
    );
    println!("    {:-<40}", "");
    println!("    table scan total            {:>7.1}", per(t_mg_table));

    let _ = std::fs::remove_dir_all(&dir);
}
