//! Targeted coverage for the logical planner paths not exercised elsewhere:
//!   * `eval_default_expr` — column DEFAULT clauses beyond plain literals
//!     (negative numbers, unary plus, CAST, typed temporal strings),
//!   * correlation detection (`collect_local_columns`) recursing through
//!     compound expressions (functions / CASE / CAST) inside a subquery.

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

fn select_rows(res: ExecutionResult) -> Vec<dtdb_relational::Row> {
    match res {
        ExecutionResult::Select { rows, .. } => rows,
        other => panic!("expected Select, got {other:?}"),
    }
}

// ----- column DEFAULT expressions -----

#[test]
fn column_defaults_support_negative_plus_cast_and_typed_literals() {
    let (_tmp, db, engine) = setup_engine();
    let tx = Transaction::new(1, db.clone());
    // Each DEFAULT exercises a distinct eval_default_expr branch:
    //   x: unary minus on an integer literal,
    //   y: unary minus on a float literal,
    //   z: unary plus,
    //   c: CAST(... AS INT),
    //   d: a typed temporal string literal (DATE '...').
    engine
        .execute(
            "CREATE TABLE t (\
                id INT PRIMARY KEY, \
                x INT DEFAULT -5, \
                y FLOAT DEFAULT -1.5, \
                z INT DEFAULT +3, \
                c INT DEFAULT CAST('7' AS INT), \
                d DATE DEFAULT DATE '2026-01-01')",
            &tx,
        )
        .unwrap();
    engine
        .execute("INSERT INTO t (id) VALUES (1)", &tx)
        .unwrap();
    tx.commit().unwrap();

    let tx = Transaction::new(2, db.clone());
    let rows = select_rows(
        engine
            .execute("SELECT x, y, z, c, d FROM t WHERE id = 1", &tx)
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], DbValue::Int(-5));
    assert_eq!(rows[0].values[1], DbValue::Float(-1.5));
    assert_eq!(rows[0].values[2], DbValue::Int(3));
    assert_eq!(rows[0].values[3], DbValue::Int(7));
    assert_eq!(
        rows[0].values[4],
        DbValue::Date(chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap())
    );
}

#[test]
fn unsupported_default_expression_is_rejected() {
    let (_tmp, _db, engine) = setup_engine();
    let tx = Transaction::new(1, _db.clone());
    // A non-constant default (a column reference) is not a supported default
    // expression and is rejected at planning time.
    let err = engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, x INT DEFAULT id)", &tx)
        .unwrap_err();
    assert!(
        err.contains("Unsupported expression type for default"),
        "got: {err}"
    );
}

// ----- correlation detection traversing compound expressions -----

fn seed_two_tables(db: &Arc<Database>, engine: &SqlEngine) {
    let tx = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)", &tx)
        .unwrap();
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, x INT)", &tx)
        .unwrap();
    engine
        .execute("INSERT INTO t (id, v) VALUES (1, 10), (2, 20)", &tx)
        .unwrap();
    engine
        .execute("INSERT INTO t2 (id, x) VALUES (1, 5), (2, 25)", &tx)
        .unwrap();
    tx.commit().unwrap();
}

#[test]
fn uncorrelated_subquery_with_function_case_and_cast_is_accepted() {
    let (_tmp, db, engine) = setup_engine();
    seed_two_tables(&db, &engine);
    let tx = Transaction::new(2, db.clone());

    // Each subquery references only its own column (t2.x), buried inside a
    // function / CASE / CAST. Correlation analysis must descend into those
    // expression nodes and conclude the subquery is uncorrelated.
    for sql in [
        "SELECT id FROM t WHERE v IN (SELECT x FROM t2 WHERE ABS(x) > 0)",
        "SELECT id FROM t WHERE v IN (SELECT x FROM t2 WHERE CASE WHEN x > 0 THEN 1 ELSE 0 END = 1)",
        "SELECT id FROM t WHERE v IN (SELECT x FROM t2 WHERE CAST(x AS INT) > 0)",
    ] {
        let res = engine.execute(sql, &tx);
        assert!(res.is_ok(), "expected ok for `{sql}`, got {res:?}");
    }
}

#[test]
fn correlated_column_buried_in_function_is_rejected() {
    let (_tmp, db, engine) = setup_engine();
    seed_two_tables(&db, &engine);
    let tx = Transaction::new(2, db.clone());

    // The outer column `t.id` is referenced inside a function argument in the
    // subquery — correlation detection must still find it and reject the query.
    let err = engine
        .execute(
            "SELECT id FROM t WHERE v IN (SELECT x FROM t2 WHERE ABS(t.id) = x)",
            &tx,
        )
        .unwrap_err();
    assert!(
        err.contains("correlated subqueries are not supported"),
        "got: {err}"
    );
}
