//! Tests for the streaming execution surface (`execute_streaming`,
//! `execute_prepared_streaming`) and for parameter-bound point lookups / range
//! scans on temporal and decimal primary keys. These paths are exercised by the
//! client streaming RPCs but were otherwise untested.

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use dtdb_relational::{Database, Transaction};
use dtdb_sql::{ExecutionResult, ExecutionStreamingResult, SqlEngine};
use dtdb_storage::DbValue;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tempfile::TempDir;

fn setup_engine() -> (TempDir, Arc<Database>, SqlEngine) {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    let engine = SqlEngine::new(db.clone());
    (temp_dir, db, engine)
}

/// Drains a streaming operator into a Vec of rows for assertions.
fn drain(result: ExecutionStreamingResult) -> (Vec<Vec<DbValue>>, bool) {
    match result {
        ExecutionStreamingResult::Streaming { mut operator, .. } => {
            let mut rows = Vec::new();
            while let Some(row) = operator.next().unwrap() {
                rows.push(row.values);
            }
            (rows, true)
        }
        ExecutionStreamingResult::Complete(ExecutionResult::Select { rows, .. }) => {
            (rows.into_iter().map(|r| r.values).collect(), false)
        }
        ExecutionStreamingResult::Complete(other) => {
            panic!("expected Streaming or Complete(Select), got Complete({other:?})")
        }
    }
}

fn seed_users(db: &Arc<Database>, engine: &SqlEngine) {
    let tx = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE users (id INT PRIMARY KEY, name STRING, score INT)",
            &tx,
        )
        .unwrap();
    tx.commit().unwrap();

    let tx = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO users (id, name, score) VALUES \
             (1, 'Alice', 90), (2, 'Bob', 80), (3, 'Charlie', 70)",
            &tx,
        )
        .unwrap();
    tx.commit().unwrap();
}

#[test]
fn streaming_select_returns_streaming_operator() {
    let (_tmp, db, engine) = setup_engine();
    seed_users(&db, &engine);

    // A full scan compiles to a Volcano operator, so the engine returns the
    // `Streaming` variant rather than a materialized result.
    let tx = Transaction::new(3, db.clone());
    let res = engine
        .execute_streaming(
            "SELECT id, name FROM users ORDER BY id ASC",
            &tx,
            &HashMap::new(),
        )
        .unwrap();
    let (rows, was_streaming) = drain(res);
    assert!(was_streaming, "full scan should stream");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], DbValue::Int(1));
    assert_eq!(rows[2][1], DbValue::string("Charlie"));
}

#[test]
fn streaming_point_get_returns_complete() {
    let (_tmp, db, engine) = setup_engine();
    seed_users(&db, &engine);

    // A primary-key point lookup takes the fast path, which materializes a
    // single row and is wrapped in `Complete` instead of a streaming operator.
    let tx = Transaction::new(3, db.clone());
    let res = engine
        .execute_streaming("SELECT name FROM users WHERE id = 2", &tx, &HashMap::new())
        .unwrap();
    let (rows, was_streaming) = drain(res);
    assert!(!was_streaming, "point-get should complete eagerly");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], DbValue::string("Bob"));
}

#[test]
fn streaming_ddl_and_dml_return_complete() {
    let (_tmp, db, engine) = setup_engine();

    // DDL through the streaming entry point returns a Complete non-Select result.
    let tx = Transaction::new(1, db.clone());
    let res = engine
        .execute_streaming(
            "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
            &tx,
            &HashMap::new(),
        )
        .unwrap();
    assert!(matches!(
        res,
        ExecutionStreamingResult::Complete(ExecutionResult::CreateTable)
    ));
    tx.commit().unwrap();

    let tx = Transaction::new(2, db.clone());
    let res = engine
        .execute_streaming(
            "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)",
            &tx,
            &HashMap::new(),
        )
        .unwrap();
    assert!(matches!(
        res,
        ExecutionStreamingResult::Complete(ExecutionResult::Insert { count: 2 })
    ));
    tx.commit().unwrap();
}

#[test]
fn streaming_explain_returns_complete() {
    let (_tmp, db, engine) = setup_engine();
    seed_users(&db, &engine);

    let tx = Transaction::new(3, db.clone());
    let res = engine
        .execute_streaming("EXPLAIN SELECT * FROM users", &tx, &HashMap::new())
        .unwrap();
    match res {
        ExecutionStreamingResult::Complete(ExecutionResult::Select { rows, .. }) => {
            assert!(!rows.is_empty(), "EXPLAIN should produce plan rows");
        }
        ExecutionStreamingResult::Complete(other) => {
            panic!("expected Complete(Select) for EXPLAIN, got Complete({other:?})")
        }
        ExecutionStreamingResult::Streaming { .. } => {
            panic!("expected Complete(Select) for EXPLAIN, got Streaming")
        }
    }
}

#[test]
fn streaming_prepared_statement() {
    let (_tmp, db, engine) = setup_engine();
    seed_users(&db, &engine);

    // Streaming scan via a prepared statement.
    let stmt = engine
        .prepare("SELECT id FROM users WHERE score >= :min")
        .unwrap();
    let tx = Transaction::new(3, db.clone());
    let mut params = HashMap::new();
    params.insert("min".to_string(), DbValue::Int(80));
    let res = engine
        .execute_prepared_streaming(&stmt, &tx, &params)
        .unwrap();
    let (mut rows, _) = drain(res);
    rows.sort_by_key(|r| match r[0] {
        DbValue::Int(i) => i,
        _ => 0,
    });
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], DbValue::Int(1));
    assert_eq!(rows[1][0], DbValue::Int(2));

    // Prepared point-get streams as Complete.
    let stmt = engine
        .prepare("SELECT name FROM users WHERE id = :id")
        .unwrap();
    let tx = Transaction::new(4, db.clone());
    let mut params = HashMap::new();
    params.insert("id".to_string(), DbValue::Int(3));
    let res = engine
        .execute_prepared_streaming(&stmt, &tx, &params)
        .unwrap();
    let (rows, was_streaming) = drain(res);
    assert!(!was_streaming);
    assert_eq!(rows[0][0], DbValue::string("Charlie"));
}

/// Point lookup on a DATE primary key, with the key supplied as a bound
/// parameter — exercises the `DbValue::Date -> DbKey::Date` resolution in the
/// point-get fast path.
#[test]
fn parameter_point_get_on_date_pk() {
    let (_tmp, db, engine) = setup_engine();
    let tx = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE events (d DATE PRIMARY KEY, label STRING)",
            &tx,
        )
        .unwrap();
    tx.commit().unwrap();

    let tx = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO events (d, label) VALUES \
             (DATE '2026-01-01', 'new year'), (DATE '2026-06-04', 'today')",
            &tx,
        )
        .unwrap();
    tx.commit().unwrap();

    let tx = Transaction::new(3, db.clone());
    let mut params = HashMap::new();
    params.insert(
        "d".to_string(),
        DbValue::Date(NaiveDate::from_ymd_opt(2026, 6, 4).unwrap()),
    );
    let res = engine
        .execute_with_params("SELECT label FROM events WHERE d = :d", &tx, &params)
        .unwrap();
    match res {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], DbValue::string("today"));
        }
        other => panic!("expected Select, got {other:?}"),
    }
}

/// Range scan on a TIMESTAMP primary key bound by a parameter — exercises the
/// `DbValue::Timestamp -> DbKey::Timestamp` resolution on the sequential-scan
/// range path.
#[test]
fn parameter_range_scan_on_timestamp_pk() {
    let (_tmp, db, engine) = setup_engine();
    let tx = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE logs (ts TIMESTAMP PRIMARY KEY, msg STRING)",
            &tx,
        )
        .unwrap();
    tx.commit().unwrap();

    let tx = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO logs (ts, msg) VALUES \
             (TIMESTAMP '2026-01-01 00:00:00', 'a'), \
             (TIMESTAMP '2026-06-04 12:00:00', 'b'), \
             (TIMESTAMP '2026-12-31 23:59:59', 'c')",
            &tx,
        )
        .unwrap();
    tx.commit().unwrap();

    let tx = Transaction::new(3, db.clone());
    let mut params = HashMap::new();
    let cutoff: NaiveDateTime = NaiveDate::from_ymd_opt(2026, 6, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    params.insert("cutoff".to_string(), DbValue::Timestamp(cutoff));
    let res = engine
        .execute_with_params(
            "SELECT msg FROM logs WHERE ts >= :cutoff ORDER BY ts ASC",
            &tx,
            &params,
        )
        .unwrap();
    match res {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].values[0], DbValue::string("b"));
            assert_eq!(rows[1].values[0], DbValue::string("c"));
        }
        other => panic!("expected Select, got {other:?}"),
    }
}

/// Point lookup on a DECIMAL primary key bound by a parameter.
#[test]
fn parameter_point_get_on_decimal_pk() {
    let (_tmp, db, engine) = setup_engine();
    let tx = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE prices (amt DECIMAL PRIMARY KEY, sku STRING)",
            &tx,
        )
        .unwrap();
    tx.commit().unwrap();

    let tx = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO prices (amt, sku) VALUES \
             (DECIMAL '9.99', 'cheap'), (DECIMAL '199.50', 'pricey')",
            &tx,
        )
        .unwrap();
    tx.commit().unwrap();

    let tx = Transaction::new(3, db.clone());
    let mut params = HashMap::new();
    params.insert(
        "amt".to_string(),
        DbValue::Decimal(Decimal::from_str("199.50").unwrap()),
    );
    let res = engine
        .execute_with_params("SELECT sku FROM prices WHERE amt = :amt", &tx, &params)
        .unwrap();
    match res {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], DbValue::string("pricey"));
        }
        other => panic!("expected Select, got {other:?}"),
    }
}

/// Prepared point lookups carry the key as a symbolic `PlanKey::Parameter` in
/// the cached plan, so binding it at execution exercises the parameter-key
/// resolution branches in the point-get matcher for each key type (Int is
/// already covered elsewhere; this covers String/Date/Time/Timestamp/Decimal).
#[test]
fn prepared_point_get_resolves_typed_parameter_keys() {
    let (_tmp, db, engine) = setup_engine();

    // STRING primary key.
    let tx = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE s (k STRING PRIMARY KEY, v INT)", &tx)
        .unwrap();
    engine
        .execute("INSERT INTO s (k, v) VALUES ('alpha', 1), ('beta', 2)", &tx)
        .unwrap();
    tx.commit().unwrap();

    let stmt = engine.prepare("SELECT v FROM s WHERE k = :k").unwrap();
    let tx = Transaction::new(2, db.clone());
    let mut params = HashMap::new();
    params.insert("k".to_string(), DbValue::string("beta"));
    let res = engine.execute_prepared(&stmt, &tx, &params).unwrap();
    match res {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], DbValue::Int(2));
        }
        other => panic!("expected Select, got {other:?}"),
    }

    // DATE / TIME / TIMESTAMP / DECIMAL primary keys, each via a prepared
    // point lookup so the cached plan keeps a parameter key.
    let tx = Transaction::new(3, db.clone());
    engine
        .execute("CREATE TABLE k_date (k DATE PRIMARY KEY, v INT)", &tx)
        .unwrap();
    engine
        .execute(
            "INSERT INTO k_date (k, v) VALUES (DATE '2026-06-04', 42)",
            &tx,
        )
        .unwrap();
    engine
        .execute("CREATE TABLE k_time (k TIME PRIMARY KEY, v INT)", &tx)
        .unwrap();
    engine
        .execute(
            "INSERT INTO k_time (k, v) VALUES (TIME '08:15:00', 43)",
            &tx,
        )
        .unwrap();
    engine
        .execute("CREATE TABLE k_ts (k TIMESTAMP PRIMARY KEY, v INT)", &tx)
        .unwrap();
    engine
        .execute(
            "INSERT INTO k_ts (k, v) VALUES (TIMESTAMP '2026-06-04 08:15:00', 44)",
            &tx,
        )
        .unwrap();
    engine
        .execute("CREATE TABLE k_dec (k DECIMAL PRIMARY KEY, v INT)", &tx)
        .unwrap();
    engine
        .execute("INSERT INTO k_dec (k, v) VALUES (DECIMAL '7.25', 45)", &tx)
        .unwrap();
    tx.commit().unwrap();

    let date_key = DbValue::Date(NaiveDate::from_ymd_opt(2026, 6, 4).unwrap());
    let time_key = DbValue::Time(NaiveTime::from_hms_opt(8, 15, 0).unwrap());
    let ts_key = DbValue::Timestamp(
        NaiveDate::from_ymd_opt(2026, 6, 4)
            .unwrap()
            .and_hms_opt(8, 15, 0)
            .unwrap(),
    );
    let dec_key = DbValue::Decimal(Decimal::from_str("7.25").unwrap());

    for (table, key, expected) in [
        ("k_date", date_key, 42i64),
        ("k_time", time_key, 43),
        ("k_ts", ts_key, 44),
        ("k_dec", dec_key, 45),
    ] {
        let stmt = engine
            .prepare(&format!("SELECT v FROM {table} WHERE k = :k"))
            .unwrap();
        let tx = Transaction::new(10, db.clone());
        let mut params = HashMap::new();
        params.insert("k".to_string(), key);
        let res = engine.execute_prepared(&stmt, &tx, &params).unwrap();
        match res {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 1, "table {table}");
                assert_eq!(rows[0].values[0], DbValue::Int(expected), "table {table}");
            }
            other => panic!("expected Select for {table}, got {other:?}"),
        }
    }
}

/// Prepared range scan on a DATE primary key keeps the bounds as parameter
/// keys, exercising the temporal parameter resolution on the sequential-scan
/// range path.
#[test]
fn prepared_range_scan_resolves_date_parameter_bounds() {
    let (_tmp, db, engine) = setup_engine();
    let tx = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE d (k DATE PRIMARY KEY, v INT)", &tx)
        .unwrap();
    engine
        .execute(
            "INSERT INTO d (k, v) VALUES \
             (DATE '2026-01-01', 1), (DATE '2026-06-01', 2), (DATE '2026-12-01', 3)",
            &tx,
        )
        .unwrap();
    tx.commit().unwrap();

    let stmt = engine
        .prepare("SELECT v FROM d WHERE k >= :lo AND k <= :hi ORDER BY k ASC")
        .unwrap();
    let tx = Transaction::new(2, db.clone());
    let mut params = HashMap::new();
    params.insert(
        "lo".to_string(),
        DbValue::Date(NaiveDate::from_ymd_opt(2026, 3, 1).unwrap()),
    );
    params.insert(
        "hi".to_string(),
        DbValue::Date(NaiveDate::from_ymd_opt(2026, 9, 1).unwrap()),
    );
    let res = engine.execute_prepared(&stmt, &tx, &params).unwrap();
    match res {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], DbValue::Int(2));
        }
        other => panic!("expected Select, got {other:?}"),
    }
}

/// Point lookup on a TIME primary key bound by a parameter.
#[test]
fn parameter_point_get_on_time_pk() {
    let (_tmp, db, engine) = setup_engine();
    let tx = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE slots (t TIME PRIMARY KEY, who STRING)", &tx)
        .unwrap();
    tx.commit().unwrap();

    let tx = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO slots (t, who) VALUES \
             (TIME '09:00:00', 'morning'), (TIME '17:30:00', 'evening')",
            &tx,
        )
        .unwrap();
    tx.commit().unwrap();

    let tx = Transaction::new(3, db.clone());
    let mut params = HashMap::new();
    params.insert(
        "t".to_string(),
        DbValue::Time(NaiveTime::from_hms_opt(17, 30, 0).unwrap()),
    );
    let res = engine
        .execute_with_params("SELECT who FROM slots WHERE t = :t", &tx, &params)
        .unwrap();
    match res {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], DbValue::string("evening"));
        }
        other => panic!("expected Select, got {other:?}"),
    }
}
