//! Targeted coverage for `SqlEngine` paths not exercised elsewhere:
//!   * the `is_ddl` classifier (every DDL variant plus negatives),
//!   * prepared-plan refresh after a schema change (`refreshed_plan`),
//!   * the AST-fallback prepared/streaming entry points,
//!   * subquery constant-folding nested inside compound expressions
//!     (`fold_expr` recursion through CASE / functions / CAST / NOT / AND),
//!   * scalar/IN subquery arity errors and join/INSERT..SELECT validation,
//!   * parameter substitution across functions, CASE, BETWEEN, IN-lists, LIMIT,
//!     SUBSTRING, joins, and INSERT/UPDATE/DELETE.

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

/// Creates `t (id INT PRIMARY KEY, v INT)` seeded with (1,10),(2,20),(3,30).
fn seed_t(db: &Arc<Database>, engine: &SqlEngine) {
    let tx = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)", &tx)
        .unwrap();
    engine
        .execute(
            "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)",
            &tx,
        )
        .unwrap();
    tx.commit().unwrap();
}

fn select_rows(res: ExecutionResult) -> Vec<dtdb_relational::Row> {
    match res {
        ExecutionResult::Select { rows, .. } => rows,
        other => panic!("expected Select, got {other:?}"),
    }
}

// ----- is_ddl classifier -----

#[test]
fn is_ddl_recognizes_every_ddl_variant() {
    let (_tmp, db, engine) = setup_engine();
    // A table must exist for ALTER/CREATE INDEX/DROP to parse + plan-classify.
    let tx = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)", &tx)
        .unwrap();
    tx.commit().unwrap();

    assert!(engine.is_ddl("CREATE TABLE u (id INT PRIMARY KEY)"));
    assert!(engine.is_ddl("DROP TABLE t"));
    assert!(engine.is_ddl("CREATE INDEX t_v ON t (v)"));
    assert!(engine.is_ddl("DROP INDEX t_v"));
    assert!(engine.is_ddl("ALTER TABLE t ADD COLUMN w INT"));
}

#[test]
fn is_ddl_rejects_non_ddl_and_unparseable_input() {
    let (_tmp, _db, engine) = setup_engine();
    // DML / queries are not DDL.
    assert!(!engine.is_ddl("SELECT 1"));
    assert!(!engine.is_ddl("INSERT INTO t (id) VALUES (1)"));
    assert!(!engine.is_ddl("UPDATE t SET v = 1"));
    // Unparseable / multi-statement / empty inputs fail cached_parse -> false.
    assert!(!engine.is_ddl("NOT VALID SQL"));
    assert!(!engine.is_ddl("SELECT 1; SELECT 2;"));
    assert!(!engine.is_ddl("   "));
}

// ----- prepared-plan refresh after schema change -----

#[test]
fn prepared_query_replans_after_schema_change() {
    let (_tmp, db, engine) = setup_engine();
    seed_t(&db, &engine);

    // Prepare a SELECT: caches a Planned plan stamped with the current catalog
    // version.
    let stmt = engine.prepare("SELECT v FROM t WHERE id = :id").unwrap();

    // A DDL bumps the schema version, making the cached plan stale.
    let tx = Transaction::new(2, db.clone());
    engine
        .execute("CREATE TABLE other (a INT PRIMARY KEY)", &tx)
        .unwrap();
    tx.commit().unwrap();

    // Executing the (now stale) prepared statement triggers refreshed_plan,
    // which re-plans from the original SQL and still returns correct rows.
    let tx = Transaction::new(3, db.clone());
    let mut params = HashMap::new();
    params.insert("id".to_string(), DbValue::Int(2));
    let rows = select_rows(engine.execute_prepared(&stmt, &tx, &params).unwrap());
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], DbValue::Int(20));
}

#[test]
fn prepared_insert_select_replans_after_schema_change() {
    let (_tmp, db, engine) = setup_engine();
    let tx = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE src (id INT PRIMARY KEY, v INT)", &tx)
        .unwrap();
    engine
        .execute("CREATE TABLE dest (id INT PRIMARY KEY, v INT)", &tx)
        .unwrap();
    engine
        .execute("INSERT INTO src (id, v) VALUES (1, 10), (2, 20)", &tx)
        .unwrap();
    tx.commit().unwrap();

    // Prepare an INSERT ... SELECT (the InsertSelect refresh arm differs from a
    // plain Query).
    let stmt = engine
        .prepare("INSERT INTO dest (id, v) SELECT id, v FROM src WHERE v >= :min")
        .unwrap();

    let tx = Transaction::new(2, db.clone());
    engine
        .execute("CREATE TABLE other (a INT PRIMARY KEY)", &tx)
        .unwrap();
    tx.commit().unwrap();

    let tx = Transaction::new(3, db.clone());
    let mut params = HashMap::new();
    params.insert("min".to_string(), DbValue::Int(10));
    let res = engine.execute_prepared(&stmt, &tx, &params).unwrap();
    assert_eq!(res, ExecutionResult::Insert { count: 2 });
    tx.commit().unwrap();
}

// ----- AST-fallback prepared statement (placeholders the planner can't keep
// symbolic fall back to caching the AST and binding per execution) -----

#[test]
fn prepared_insert_values_with_placeholder_uses_ast_path() {
    let (_tmp, db, engine) = setup_engine();
    seed_t(&db, &engine);

    // INSERT ... VALUES (:x) can't keep the placeholder symbolic, so prepare
    // caches the AST; execution binds into it.
    let stmt = engine
        .prepare("INSERT INTO t (id, v) VALUES (:id, :v)")
        .unwrap();

    let tx = Transaction::new(2, db.clone());
    let mut params = HashMap::new();
    params.insert("id".to_string(), DbValue::Int(4));
    params.insert("v".to_string(), DbValue::Int(40));
    let res = engine.execute_prepared(&stmt, &tx, &params).unwrap();
    assert_eq!(res, ExecutionResult::Insert { count: 1 });
    tx.commit().unwrap();

    // The streaming entry point goes through the same AST branch and completes
    // eagerly for a non-query statement.
    let tx = Transaction::new(3, db.clone());
    let mut params = HashMap::new();
    params.insert("id".to_string(), DbValue::Int(5));
    params.insert("v".to_string(), DbValue::Int(50));
    let res = engine
        .execute_prepared_streaming(&stmt, &tx, &params)
        .unwrap();
    assert!(matches!(
        res,
        ExecutionStreamingResult::Complete(ExecutionResult::Insert { count: 1 })
    ));
    tx.commit().unwrap();

    let tx = Transaction::new(4, db.clone());
    let rows = select_rows(
        engine
            .execute("SELECT id FROM t ORDER BY id ASC", &tx)
            .unwrap(),
    );
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[4].values[0], DbValue::Int(5));
}

// ----- subquery folding nested inside compound expressions -----

#[test]
fn scalar_subquery_folds_inside_case_function_and_cast() {
    let (_tmp, db, engine) = setup_engine();
    seed_t(&db, &engine);
    let tx = Transaction::new(2, db.clone());

    // CASE arm references a scalar subquery (fold_expr recurses through Case).
    let rows = select_rows(
        engine
            .execute(
                "SELECT CASE WHEN v > (SELECT AVG(v) FROM t) THEN 1 ELSE 0 END FROM t ORDER BY id ASC",
                &tx,
            )
            .unwrap(),
    );
    assert_eq!(
        rows.iter().map(|r| r.values[0].clone()).collect::<Vec<_>>(),
        vec![DbValue::Int(0), DbValue::Int(0), DbValue::Int(1)]
    );

    // Function argument is a scalar subquery (fold_expr recurses through
    // Function), and the surrounding CAST (recurses through Cast).
    let rows = select_rows(
        engine
            .execute(
                "SELECT id FROM t WHERE v = CAST(ABS((SELECT MIN(v) FROM t)) AS INT)",
                &tx,
            )
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], DbValue::Int(1));
}

#[test]
fn scalar_subquery_folds_inside_not_and_conjunction() {
    let (_tmp, db, engine) = setup_engine();
    seed_t(&db, &engine);
    let tx = Transaction::new(2, db.clone());

    // NOT ( id = (subquery) ) AND v > 0: fold_expr recurses through Not and the
    // AND BinaryOp into the nested ScalarSubquery.
    let rows = select_rows(
        engine
            .execute(
                "SELECT id FROM t WHERE NOT (id = (SELECT MIN(id) FROM t)) AND v > 0 ORDER BY id ASC",
                &tx,
            )
            .unwrap(),
    );
    assert_eq!(
        rows.iter().map(|r| r.values[0].clone()).collect::<Vec<_>>(),
        vec![DbValue::Int(2), DbValue::Int(3)]
    );
}

// ----- arity / shape validation errors -----

#[test]
fn scalar_subquery_with_multiple_columns_is_rejected() {
    let (_tmp, db, engine) = setup_engine();
    seed_t(&db, &engine);
    let tx = Transaction::new(2, db.clone());
    let err = engine
        .execute(
            "SELECT id FROM t WHERE v = (SELECT id, v FROM t WHERE id = 1)",
            &tx,
        )
        .unwrap_err();
    assert!(
        err.contains("scalar subquery must return exactly one column"),
        "got: {err}"
    );
}

#[test]
fn in_subquery_with_multiple_columns_is_rejected() {
    let (_tmp, db, engine) = setup_engine();
    seed_t(&db, &engine);
    let tx = Transaction::new(2, db.clone());
    let err = engine
        .execute("SELECT id FROM t WHERE v IN (SELECT id, v FROM t)", &tx)
        .unwrap_err();
    assert!(
        err.contains("IN subquery must return exactly one column"),
        "got: {err}"
    );
}

#[test]
fn non_equality_join_condition_is_rejected() {
    let (_tmp, db, engine) = setup_engine();
    let tx = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, x INT)", &tx)
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, y INT)", &tx)
        .unwrap();
    engine
        .execute("INSERT INTO a (id, x) VALUES (1, 1)", &tx)
        .unwrap();
    engine
        .execute("INSERT INTO b (id, y) VALUES (1, 1)", &tx)
        .unwrap();
    tx.commit().unwrap();

    let tx = Transaction::new(2, db.clone());
    let err = engine
        .execute("SELECT a.id FROM a JOIN b ON a.id > b.id", &tx)
        .unwrap_err();
    assert!(
        err.contains("Only equality join conditions supported"),
        "got: {err}"
    );
}

#[test]
fn insert_select_column_count_mismatch_is_rejected() {
    let (_tmp, db, engine) = setup_engine();
    seed_t(&db, &engine);
    let tx = Transaction::new(2, db.clone());
    engine
        .execute("CREATE TABLE dest (id INT PRIMARY KEY, v INT)", &tx)
        .unwrap();
    // Two target columns, one SELECT column.
    let err = engine
        .execute("INSERT INTO dest (id, v) SELECT id FROM t", &tx)
        .unwrap_err();
    assert!(err.contains("col count mismatch"), "got: {err}");
}

// ----- parameter binding into the freshly-parsed AST -----

#[test]
fn parameters_bind_inside_function_case_and_between() {
    let (_tmp, db, engine) = setup_engine();
    seed_t(&db, &engine);
    let tx = Transaction::new(2, db.clone());

    // Parameter inside a function argument.
    let mut params = HashMap::new();
    params.insert("x".to_string(), DbValue::Int(10));
    let rows = select_rows(
        engine
            .execute_with_params("SELECT id FROM t WHERE v = ABS(:x)", &tx, &params)
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], DbValue::Int(1));

    // Parameter inside a CASE branch condition.
    let mut params = HashMap::new();
    params.insert("a".to_string(), DbValue::Int(2));
    let rows = select_rows(
        engine
            .execute_with_params(
                "SELECT CASE WHEN id = :a THEN 99 ELSE 0 END FROM t ORDER BY id ASC",
                &tx,
                &params,
            )
            .unwrap(),
    );
    assert_eq!(
        rows.iter().map(|r| r.values[0].clone()).collect::<Vec<_>>(),
        vec![DbValue::Int(0), DbValue::Int(99), DbValue::Int(0)]
    );

    // Parameters as BETWEEN bounds.
    let mut params = HashMap::new();
    params.insert("lo".to_string(), DbValue::Int(15));
    params.insert("hi".to_string(), DbValue::Int(35));
    let rows = select_rows(
        engine
            .execute_with_params(
                "SELECT id FROM t WHERE v BETWEEN :lo AND :hi ORDER BY id ASC",
                &tx,
                &params,
            )
            .unwrap(),
    );
    assert_eq!(
        rows.iter().map(|r| r.values[0].clone()).collect::<Vec<_>>(),
        vec![DbValue::Int(2), DbValue::Int(3)]
    );

    // Parameter inside a CASE else-branch result.
    let mut params = HashMap::new();
    params.insert("a".to_string(), DbValue::Int(1));
    params.insert("d".to_string(), DbValue::Int(-1));
    let rows = select_rows(
        engine
            .execute_with_params(
                "SELECT CASE WHEN id = :a THEN 99 ELSE :d END FROM t ORDER BY id ASC",
                &tx,
                &params,
            )
            .unwrap(),
    );
    assert_eq!(
        rows.iter().map(|r| r.values[0].clone()).collect::<Vec<_>>(),
        vec![DbValue::Int(99), DbValue::Int(-1), DbValue::Int(-1)]
    );
}

#[test]
fn parameters_bind_inside_substring_and_join_on_clause() {
    let (_tmp, db, engine) = setup_engine();
    let tx = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, s STRING)", &tx)
        .unwrap();
    engine
        .execute("CREATE TABLE u (id INT PRIMARY KEY, w INT)", &tx)
        .unwrap();
    engine
        .execute(
            "INSERT INTO t (id, s) VALUES (1, 'hello'), (2, 'world')",
            &tx,
        )
        .unwrap();
    engine
        .execute("INSERT INTO u (id, w) VALUES (1, 100), (2, 200)", &tx)
        .unwrap();
    tx.commit().unwrap();
    let tx = Transaction::new(2, db.clone());

    // Parameters as SUBSTRING ... FROM ... FOR ... bounds. The `FROM`/`FOR`
    // syntax parses to the dedicated Substring AST node (distinct from the
    // SUBSTR(...) function form), exercising its parameter-binding arm.
    let mut params = HashMap::new();
    params.insert("from".to_string(), DbValue::Int(1));
    params.insert("len".to_string(), DbValue::Int(3));
    let rows = select_rows(
        engine
            .execute_with_params(
                "SELECT SUBSTRING(s FROM :from FOR :len) FROM t ORDER BY id ASC",
                &tx,
                &params,
            )
            .unwrap(),
    );
    assert_eq!(
        rows.iter().map(|r| r.values[0].clone()).collect::<Vec<_>>(),
        vec![DbValue::string("hel"), DbValue::string("wor")]
    );

    // A JOIN ... ON exercises the join-constraint binding traversal even with no
    // placeholders, since binding descends every join operator.
    let rows = select_rows(
        engine
            .execute_with_params(
                "SELECT t.id FROM t JOIN u ON t.id = u.id ORDER BY t.id ASC",
                &tx,
                &HashMap::new(),
            )
            .unwrap(),
    );
    assert_eq!(
        rows.iter().map(|r| r.values[0].clone()).collect::<Vec<_>>(),
        vec![DbValue::Int(1), DbValue::Int(2)]
    );
}

#[test]
fn parameters_bind_inside_in_list_and_limit() {
    let (_tmp, db, engine) = setup_engine();
    seed_t(&db, &engine);
    let tx = Transaction::new(2, db.clone());

    // Parameters as IN-list elements.
    let mut params = HashMap::new();
    params.insert("a".to_string(), DbValue::Int(1));
    params.insert("b".to_string(), DbValue::Int(3));
    let rows = select_rows(
        engine
            .execute_with_params(
                "SELECT id FROM t WHERE id IN (:a, :b) ORDER BY id ASC",
                &tx,
                &params,
            )
            .unwrap(),
    );
    assert_eq!(
        rows.iter().map(|r| r.values[0].clone()).collect::<Vec<_>>(),
        vec![DbValue::Int(1), DbValue::Int(3)]
    );

    // Parameter as a LIMIT value.
    let mut params = HashMap::new();
    params.insert("n".to_string(), DbValue::Int(2));
    let rows = select_rows(
        engine
            .execute_with_params("SELECT id FROM t ORDER BY id ASC LIMIT :n", &tx, &params)
            .unwrap(),
    );
    assert_eq!(rows.len(), 2);
}

#[test]
fn parameters_bind_in_insert_update_and_delete() {
    let (_tmp, db, engine) = setup_engine();
    seed_t(&db, &engine);

    // INSERT ... VALUES (:..) through the fresh-parse + bind path.
    let tx = Transaction::new(2, db.clone());
    let mut params = HashMap::new();
    params.insert("id".to_string(), DbValue::Int(4));
    params.insert("v".to_string(), DbValue::Int(40));
    let res = engine
        .execute_with_params("INSERT INTO t (id, v) VALUES (:id, :v)", &tx, &params)
        .unwrap();
    assert_eq!(res, ExecutionResult::Insert { count: 1 });

    // UPDATE ... SET v = :nv WHERE id = :k (assignment + selection binding).
    let mut params = HashMap::new();
    params.insert("nv".to_string(), DbValue::Int(7));
    params.insert("k".to_string(), DbValue::Int(1));
    let res = engine
        .execute_with_params("UPDATE t SET v = :nv WHERE id = :k", &tx, &params)
        .unwrap();
    assert_eq!(res, ExecutionResult::Update { count: 1 });

    // DELETE ... WHERE id = :k (selection binding).
    let mut params = HashMap::new();
    params.insert("k".to_string(), DbValue::Int(2));
    let res = engine
        .execute_with_params("DELETE FROM t WHERE id = :k", &tx, &params)
        .unwrap();
    assert_eq!(res, ExecutionResult::Delete { count: 1 });
    tx.commit().unwrap();

    let tx = Transaction::new(3, db.clone());
    let rows = select_rows(
        engine
            .execute("SELECT id, v FROM t ORDER BY id ASC", &tx)
            .unwrap(),
    );
    // Remaining: id=1 (v updated to 7), id=3 (v=30), id=4 (v=40); id=2 deleted.
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].values, vec![DbValue::Int(1), DbValue::Int(7)]);
}

#[test]
fn unbound_parameter_is_reported() {
    let (_tmp, db, engine) = setup_engine();
    seed_t(&db, &engine);
    let tx = Transaction::new(2, db.clone());
    let err = engine
        .execute_with_params("SELECT id FROM t WHERE id = :missing", &tx, &HashMap::new())
        .unwrap_err();
    assert!(err.contains("Unbound parameter"), "got: {err}");
}
