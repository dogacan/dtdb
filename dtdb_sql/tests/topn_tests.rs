//! Correctness coverage for the `LIMIT` + `ORDER BY` top-k fusion
//! (`PhysicalTopN`). Each case sorts on a non-indexed column so the optimizer
//! cannot serve the order from an index scan and the fused heap path is taken,
//! then asserts the fused result matches a plain full sort sliced by hand.

use dtdb_relational::{Database, Transaction};
use dtdb_sql::{ExecutionResult, SqlEngine};
use dtdb_storage::DbValue;
use std::sync::Arc;
use tempfile::TempDir;

fn setup_engine() -> (TempDir, Arc<Database>, SqlEngine) {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    let engine = SqlEngine::new(db.clone());
    (temp_dir, db, engine)
}

/// Runs `sql` and returns the first column of every row as an i64.
fn ids(engine: &SqlEngine, tx: &Transaction, sql: &str) -> Vec<i64> {
    match engine.execute(sql, tx).unwrap() {
        ExecutionResult::Select { rows, .. } => rows
            .iter()
            .map(|r| match &r.values[0] {
                DbValue::Int(v) => *v,
                other => panic!("expected Int id, got {other:?}"),
            })
            .collect(),
        other => panic!("expected Select, got {other:?}"),
    }
}

/// Confirms the `PhysicalTopN` path actually fires for `sql` (so a passing
/// correctness assertion isn't silently exercising the plain-sort fallback).
fn assert_uses_topn(engine: &SqlEngine, tx: &Transaction, sql: &str) {
    let plan = match engine.execute(&format!("EXPLAIN {sql}"), tx).unwrap() {
        ExecutionResult::Select { rows, .. } => match &rows[0].values[0] {
            DbValue::String(s) => s.to_string(),
            other => panic!("expected string plan, got {other:?}"),
        },
        other => panic!("expected Select, got {other:?}"),
    };
    assert!(
        plan.contains("PhysicalTopN"),
        "expected PhysicalTopN in plan for `{sql}`, got:\n{plan}"
    );
}

/// Builds a table whose ordering column `v` is unindexed and contains ties, so
/// `ORDER BY v` requires a real sort. `id` is the primary key used as a stable
/// tiebreaker. Rows are inserted in an order unrelated to `v`.
fn seed(engine: &SqlEngine, db: &Arc<Database>) {
    let tx = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)", &tx)
        .unwrap();
    // v values chosen with duplicates and a non-monotonic insert order.
    let vs = [5, 2, 8, 2, 9, 1, 5, 7, 3, 2, 6, 4];
    for (i, v) in vs.iter().enumerate() {
        engine
            .execute(
                &format!("INSERT INTO t (id, v) VALUES ({}, {v})", i as i64 + 1),
                &tx,
            )
            .unwrap();
    }
    tx.commit().unwrap();
}

#[test]
fn topn_matches_full_sort_across_variants() {
    let (_tmp, db, engine) = setup_engine();
    seed(&engine, &db);
    let tx = Transaction::new(2, db.clone());

    // Full orderings (no limit -> plain PhysicalSort) used as the oracle.
    let asc = ids(&engine, &tx, "SELECT id FROM t ORDER BY v ASC, id ASC");
    let desc = ids(&engine, &tx, "SELECT id FROM t ORDER BY v DESC, id ASC");

    for k in [1usize, 3, 5, 12] {
        let q = format!("SELECT id FROM t ORDER BY v ASC, id ASC LIMIT {k}");
        assert_uses_topn(&engine, &tx, &q);
        assert_eq!(
            ids(&engine, &tx, &q),
            asc[..k.min(asc.len())].to_vec(),
            "asc k={k}"
        );

        let q = format!("SELECT id FROM t ORDER BY v DESC, id ASC LIMIT {k}");
        assert_eq!(
            ids(&engine, &tx, &q),
            desc[..k.min(desc.len())].to_vec(),
            "desc k={k}"
        );
    }

    // LIMIT with OFFSET: the heap retains offset+limit rows, the outer limit slices.
    for (offset, limit) in [(0usize, 4usize), (2, 3), (5, 4), (10, 5)] {
        let q = format!("SELECT id FROM t ORDER BY v ASC, id ASC LIMIT {limit} OFFSET {offset}");
        assert_uses_topn(&engine, &tx, &q);
        let expected: Vec<i64> = asc.iter().skip(offset).take(limit).copied().collect();
        assert_eq!(
            ids(&engine, &tx, &q),
            expected,
            "offset={offset} limit={limit}"
        );
    }
}

#[test]
fn topn_handles_limit_zero_and_overshoot() {
    let (_tmp, db, engine) = setup_engine();
    seed(&engine, &db);
    let tx = Transaction::new(2, db.clone());

    // LIMIT 0 retains nothing.
    assert_eq!(
        ids(&engine, &tx, "SELECT id FROM t ORDER BY v ASC LIMIT 0").len(),
        0
    );

    // OFFSET past the end yields nothing even though k = offset+limit is large.
    assert_eq!(
        ids(
            &engine,
            &tx,
            "SELECT id FROM t ORDER BY v ASC, id ASC LIMIT 5 OFFSET 100"
        )
        .len(),
        0
    );

    // LIMIT larger than the table returns the whole sorted table.
    let asc = ids(&engine, &tx, "SELECT id FROM t ORDER BY v ASC, id ASC");
    assert_eq!(
        ids(
            &engine,
            &tx,
            "SELECT id FROM t ORDER BY v ASC, id ASC LIMIT 1000"
        ),
        asc
    );
}
