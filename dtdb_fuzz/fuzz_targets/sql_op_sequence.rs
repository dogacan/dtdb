//! Property target: random SQL operation sequences against the in-process
//! `InProcessClient`, asserting durability across a flush + reopen.
//!
//! Strategy:
//!   - Fixed schema `T (id INT PRIMARY KEY, v INT)`.
//!   - proptest generates `Vec<SqlOp>` from {Insert, Update, Delete}.
//!   - We replay against the DB and an in-memory `HashMap<i64,i64>` model,
//!     ignoring DB errors per op (e.g. duplicate INSERT) but only updating
//!     the model when the DB call succeeds.
//!   - After replay: flush, drop the client, reopen against the same data
//!     directory, then `SELECT id, v FROM T` and assert the returned rows
//!     match the model.

#![allow(clippy::result_large_err)]

use dtdb_api::in_process::InProcessClient;
use dtdb_storage::CompressionType;
use proptest::prelude::*;
use std::collections::HashMap;
use tempfile::TempDir;

const KEY_SPACE: i64 = 6;

#[derive(Debug, Clone)]
enum SqlOp {
    Insert { id: i64, v: i64 },
    Update { id: i64, v: i64 },
    Delete { id: i64 },
}

fn op_strategy() -> impl Strategy<Value = SqlOp> {
    prop_oneof![
        (0i64..KEY_SPACE, 0i64..100).prop_map(|(id, v)| SqlOp::Insert { id, v }),
        (0i64..KEY_SPACE, 0i64..100).prop_map(|(id, v)| SqlOp::Update { id, v }),
        (0i64..KEY_SPACE).prop_map(|id| SqlOp::Delete { id }),
    ]
}

fn drain(client: &InProcessClient, sql: &str) -> Result<Vec<dtdb_relational::Row>, tonic::Status> {
    // We bypass the sql_query! macro since the macro requires a string
    // literal and we need to build SQL from generated data.
    let q = dtdb_api::SqlQuery::new(sql.to_string());
    let results = client.execute_query("fuzz", q)?;
    let mut out = Vec::new();
    for row_res in results {
        out.push(row_res?);
    }
    Ok(out)
}

fn apply_op(client: &InProcessClient, model: &mut HashMap<i64, i64>, op: &SqlOp) {
    let sql = match op {
        SqlOp::Insert { id, v } => format!("INSERT INTO T (id, v) VALUES ({id}, {v});"),
        SqlOp::Update { id, v } => format!("UPDATE T SET v = {v} WHERE id = {id};"),
        SqlOp::Delete { id } => format!("DELETE FROM T WHERE id = {id};"),
    };
    if drain(client, &sql).is_err() {
        // Conflicting writes / constraint failures are expected and must
        // leave the model unchanged.
        return;
    }
    match op {
        SqlOp::Insert { id, v } => {
            // INSERT fails on duplicate keys; only mirror if absent.
            model.entry(*id).or_insert(*v);
        }
        SqlOp::Update { id, v } => {
            if let Some(slot) = model.get_mut(id) {
                *slot = *v;
            }
        }
        SqlOp::Delete { id } => {
            model.remove(id);
        }
    }
}

fn select_all(client: &InProcessClient) -> HashMap<i64, i64> {
    let rows = drain(client, "SELECT id, v FROM T;").expect("select after reopen must succeed");
    let mut out = HashMap::new();
    for row in rows {
        let id = match row.values[0] {
            dtdb_storage::DbValue::Int(v) => v,
            ref other => panic!("Expected Int id, got {:?}", other),
        };
        let v = match row.values[1] {
            dtdb_storage::DbValue::Int(v) => v,
            ref other => panic!("Expected Int v, got {:?}", other),
        };
        out.insert(id, v);
    }
    out
}

fn run_test(ops: Vec<SqlOp>) {
    let dir = TempDir::new().expect("tempdir");
    let mut model: HashMap<i64, i64> = HashMap::new();

    {
        let client = InProcessClient::open(dir.path()).expect("open client");
        client
            .create_db("fuzz", CompressionType::Uncompressed)
            .expect("create_db");
        drain(&client, "CREATE TABLE T (id int PRIMARY KEY, v int);").expect("create table");

        for op in &ops {
            apply_op(&client, &mut model, op);
        }

        let before_reopen = select_all(&client);
        assert_eq!(before_reopen, model, "pre-flush state diverges from model");

        client.flush_db("fuzz").expect("flush");
        drop(client);
    }

    // Reopen against the same directory and re-verify.
    let client = InProcessClient::open(dir.path()).expect("reopen client");
    let after_reopen = select_all(&client);
    assert_eq!(
        after_reopen, model,
        "post-reopen state diverges from model (durability bug)"
    );
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(32),
        // The fuzz targets live in fuzz_targets/, not src/, so proptest's
        // source-file persistence has nowhere to write — disable it.
        failure_persistence: None,
        .. ProptestConfig::default()
    })]

    #[test]
    #[ignore = "fuzz target — run with `cargo test -p dtdb_fuzz -- --ignored` or scripts/run_fuzz.sh"]
    fn sql_random_op_sequence(ops in prop::collection::vec(op_strategy(), 1..30)) {
        run_test(ops);
    }
}
