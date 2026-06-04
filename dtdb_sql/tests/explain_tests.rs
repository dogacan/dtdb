//! Table-driven coverage for `PhysicalOperator::explain`.
//!
//! `EXPLAIN` compiles the physical operator tree and calls `explain(0, ..)` on
//! it (see `SqlEngine::execute_statement`'s `Explain` arm), so running `EXPLAIN`
//! over a set of queries chosen to instantiate every operator type exercises
//! each operator's `explain` implementation exactly once — without one
//! hand-written test per operator.
//!
//! The fixture deliberately does **not** run `ANALYZE`: with no table
//! statistics the cost model's defaults make each query resolve to the intended
//! operator (e.g. a `MATCH` predicate picks the cheap full-text scan, and an
//! `ORDER BY` on the indexed column is served by an ordered index scan). Adding
//! statistics would let the optimizer re-choose plans and is what these EXPLAIN
//! shapes are written to avoid.

use dtdb_relational::{Database, Transaction};
use dtdb_sql::{ExecutionResult, SqlEngine};
use std::collections::HashSet;
use std::sync::Arc;
use tempfile::TempDir;

fn setup_engine() -> (TempDir, Arc<Database>, SqlEngine) {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    let engine = SqlEngine::new(db.clone());
    (temp_dir, db, engine)
}

/// Returns the `--- Physical Plan ---` section of an `EXPLAIN` result.
fn physical_plan(engine: &SqlEngine, db: &Arc<Database>, tx_id: u64, sql: &str) -> String {
    let tx = Transaction::new(tx_id, db.clone());
    let res = engine
        .execute(&format!("EXPLAIN {sql}"), &tx)
        .unwrap_or_else(|e| panic!("EXPLAIN failed for `{sql}`: {e}"));
    let plan = match res {
        ExecutionResult::Select { rows, .. } => match &rows[0].values[0] {
            dtdb_storage::DbValue::String(s) => s.to_string(),
            other => panic!("expected string plan, got {other:?}"),
        },
        other => panic!("expected Select, got {other:?}"),
    };
    plan.split("--- Physical Plan ---")
        .nth(1)
        .unwrap_or_else(|| panic!("no physical plan section in:\n{plan}"))
        .to_string()
}

#[test]
fn explain_exercises_every_physical_operator() {
    let (_tmp, db, engine) = setup_engine();
    let tx = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT, cat STRING, body STRING)",
            &tx,
        )
        .unwrap();
    engine.execute("CREATE INDEX t_v ON t (v)", &tx).unwrap();
    engine
        .execute("CREATE FULLTEXT INDEX t_body ON t (body) USING simple", &tx)
        .unwrap();
    engine
        .execute("CREATE TABLE u (id INT PRIMARY KEY, w INT)", &tx)
        .unwrap();
    for i in 1..=30i64 {
        let body = if i == 7 { "rare zebra" } else { "common text" };
        engine
            .execute(
                &format!(
                    "INSERT INTO t (id, v, cat, body) VALUES ({i}, {}, 'c{}', '{body}')",
                    i % 5,
                    i % 3
                ),
                &tx,
            )
            .unwrap();
    }
    engine
        .execute(
            "INSERT INTO u (id, w) VALUES (1, 10), (2, 20), (3, 30)",
            &tx,
        )
        .unwrap();
    tx.commit().unwrap();

    // (query, operator labels that must appear in its physical plan).
    let cases: &[(&str, &[&str])] = &[
        (
            "SELECT id, v FROM t WHERE v > 1",
            &["PhysicalProjection", "PhysicalFilter"],
        ),
        ("SELECT id FROM t WHERE v > 0 LIMIT 3", &["PhysicalLimit"]),
        (
            "SELECT id FROM t ORDER BY cat",
            &["PhysicalSort", "PhysicalSeqScan"],
        ),
        ("SELECT id FROM t ORDER BY v", &["PhysicalIndexScan"]),
        (
            "SELECT id FROM t WHERE MATCH(body) AGAINST('zebra')",
            &["PhysicalFullTextScan"],
        ),
        (
            "SELECT t.id FROM t JOIN u ON t.id = u.id",
            &["PhysicalSortMergeJoin"],
        ),
        ("SELECT t.id FROM t CROSS JOIN u", &["PhysicalCrossJoin"]),
        (
            "SELECT cat, COUNT(*) FROM t GROUP BY cat",
            &["PhysicalHashAggregate"],
        ),
        (
            "SELECT v, COUNT(*) FROM t GROUP BY v",
            &["PhysicalSortedAggregate"],
        ),
        ("SELECT v FROM t UNION SELECT w FROM u", &["PhysicalSetOp"]),
        ("SELECT DISTINCT cat FROM t", &["PhysicalDistinct"]),
    ];

    // Every operator type the EXPLAIN surface can print.
    let all_operators = [
        "PhysicalSeqScan",
        "PhysicalIndexScan",
        "PhysicalFullTextScan",
        "PhysicalFilter",
        "PhysicalProjection",
        "PhysicalLimit",
        "PhysicalSort",
        "PhysicalSortMergeJoin",
        "PhysicalCrossJoin",
        "PhysicalHashAggregate",
        "PhysicalSortedAggregate",
        "PhysicalSetOp",
        "PhysicalDistinct",
    ];

    let mut seen: HashSet<&str> = HashSet::new();
    for (tx_id, (sql, expected)) in cases.iter().enumerate() {
        let plan = physical_plan(&engine, &db, 100 + tx_id as u64, sql);
        for op in *expected {
            assert!(
                plan.contains(op),
                "expected `{op}` in physical plan for `{sql}`, got:\n{plan}"
            );
        }
        for op in &all_operators {
            if plan.contains(op) {
                seen.insert(op);
            }
        }
    }

    let missing: Vec<&str> = all_operators
        .iter()
        .copied()
        .filter(|op| !seen.contains(op))
        .collect();
    assert!(
        missing.is_empty(),
        "these physical operators' explain() were never exercised: {missing:?}"
    );
}
