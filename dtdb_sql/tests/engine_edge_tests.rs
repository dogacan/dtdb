//! Edge-case coverage for the `SqlEngine` surface: input validation (empty and
//! multi-statement inputs), the `plan_cache_is_empty` accessor, and the
//! `INSERT ... SELECT` paths through both the streaming and prepared entry
//! points (which optimize the embedded query separately).

use dtdb_relational::{Database, Transaction};
use dtdb_sql::{ExecutionResult, ExecutionStreamingResult, SqlEngine};
use dtdb_storage::DbValue;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

fn setup_engine() -> (TempDir, Arc<Database>, SqlEngine) {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    let engine = SqlEngine::new(db.clone());
    (temp_dir, db, engine)
}

#[test]
fn empty_input_is_rejected() {
    let (_tmp, db, engine) = setup_engine();
    let tx = Transaction::new(1, db.clone());
    let err = engine.execute("   ", &tx).unwrap_err();
    assert!(err.contains("No SQL statements found"), "got: {err}");

    // The prepare path validates the same way.
    let err = engine.prepare(";").unwrap_err();
    assert!(err.contains("No SQL statements found"), "got: {err}");
}

#[test]
fn multi_statement_input_is_rejected() {
    let (_tmp, db, engine) = setup_engine();
    let tx = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY)", &tx)
        .unwrap();
    tx.commit().unwrap();

    let tx = Transaction::new(2, db.clone());
    let err = engine
        .execute("INSERT INTO t VALUES (1); INSERT INTO t VALUES (2);", &tx)
        .unwrap_err();
    assert!(
        err.contains("Multiple SQL statements"),
        "execute error was: {err}"
    );

    let err = engine
        .prepare("SELECT * FROM t; SELECT * FROM t;")
        .unwrap_err();
    assert!(
        err.contains("Multiple SQL statements"),
        "prepare error was: {err}"
    );
}

#[test]
fn plan_cache_is_empty_reflects_state() {
    let (_tmp, db, engine) = setup_engine();
    let tx = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)", &tx)
        .unwrap();
    tx.commit().unwrap();

    assert!(engine.plan_cache_is_empty());
    let _ = engine.prepare("SELECT v FROM t WHERE id = :id").unwrap();
    assert!(!engine.plan_cache_is_empty());
}

fn seed_src_dest(db: &Arc<Database>, engine: &SqlEngine) {
    let tx = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE src (id INT PRIMARY KEY, v INT)", &tx)
        .unwrap();
    engine
        .execute("CREATE TABLE dest (id INT PRIMARY KEY, v INT)", &tx)
        .unwrap();
    engine
        .execute(
            "INSERT INTO src (id, v) VALUES (1, 10), (2, 20), (3, 30)",
            &tx,
        )
        .unwrap();
    tx.commit().unwrap();
}

#[test]
fn insert_select_via_streaming_entry_point() {
    let (_tmp, db, engine) = setup_engine();
    seed_src_dest(&db, &engine);

    // INSERT ... SELECT optimizes the embedded query and completes eagerly
    // (it is not a streaming SELECT), so the streaming entry point returns
    // a Complete(Insert).
    let tx = Transaction::new(2, db.clone());
    let res = engine
        .execute_streaming(
            "INSERT INTO dest (id, v) SELECT id, v FROM src WHERE v >= 20",
            &tx,
            &HashMap::new(),
        )
        .unwrap();
    assert!(matches!(
        res,
        ExecutionStreamingResult::Complete(ExecutionResult::Insert { count: 2 })
    ));
    tx.commit().unwrap();

    let tx = Transaction::new(3, db.clone());
    let res = engine
        .execute("SELECT id FROM dest ORDER BY id ASC", &tx)
        .unwrap();
    match res {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], DbValue::Int(2));
            assert_eq!(rows[1].values[0], DbValue::Int(3));
        }
        other => panic!("expected Select, got {other:?}"),
    }
}

#[test]
fn insert_select_via_prepared_statement() {
    let (_tmp, db, engine) = setup_engine();
    seed_src_dest(&db, &engine);

    // Preparing an INSERT ... SELECT plans and optimizes the embedded query,
    // caching the InsertSelect plan; executing it copies the rows.
    let stmt = engine
        .prepare("INSERT INTO dest (id, v) SELECT id, v FROM src WHERE v >= :min")
        .unwrap();
    let tx = Transaction::new(2, db.clone());
    let mut params = HashMap::new();
    params.insert("min".to_string(), DbValue::Int(10));
    let res = engine.execute_prepared(&stmt, &tx, &params).unwrap();
    assert_eq!(res, ExecutionResult::Insert { count: 3 });
    tx.commit().unwrap();
}
