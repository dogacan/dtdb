use dtdb_relational::{Database, Transaction};
use dtdb_sql::{ExecutionResult, SqlEngine};
use dtdb_storage::DbValue;
use std::cell::Cell;
use std::sync::Arc;
use tempfile::TempDir;

struct Harness {
    _temp: TempDir,
    db: Arc<Database>,
    engine: SqlEngine,
    txid: Cell<u64>,
}

impl Harness {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let db = Arc::new(Database::open(temp.path()).unwrap());
        let engine = SqlEngine::new(db.clone());
        Harness {
            _temp: temp,
            db,
            engine,
            txid: Cell::new(0),
        }
    }

    fn next_tx(&self) -> Transaction {
        self.txid.set(self.txid.get() + 1);
        Transaction::new(self.txid.get(), self.db.clone())
    }

    /// Run a statement in its own auto-committed transaction.
    fn run(&self, sql: &str) -> Result<ExecutionResult, String> {
        let tx = self.next_tx();
        let res = self.engine.execute(sql, &tx);
        if res.is_ok() {
            tx.commit().unwrap();
        }
        res
    }

    /// Run a SELECT and return its rows as raw value vectors.
    fn select(&self, sql: &str) -> Vec<Vec<DbValue>> {
        match self.run(sql).unwrap() {
            ExecutionResult::Select { rows, .. } => rows.into_iter().map(|r| r.values).collect(),
            other => panic!("expected Select, got {other:?}"),
        }
    }
}

fn setup() -> Harness {
    let h = Harness::new();
    h.run("CREATE TABLE t1 (id INT PRIMARY KEY, x INT)")
        .unwrap();
    h.run("CREATE TABLE t2 (id INT PRIMARY KEY, y INT)")
        .unwrap();
    h.run("INSERT INTO t1 (id, x) VALUES (1, 10), (2, 15), (3, 30)")
        .unwrap();
    h.run("INSERT INTO t2 (id, y) VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    h
}

fn int(i: i64) -> DbValue {
    DbValue::Int(i)
}

#[test]
fn scalar_subquery_in_where() {
    let h = setup();
    // MIN(y) = 10, so x > 10 keeps 15 and 30 -> ids 2, 3.
    let rows = h.select("SELECT id FROM t1 WHERE x > (SELECT MIN(y) FROM t2) ORDER BY id");
    assert_eq!(rows, vec![vec![int(2)], vec![int(3)]]);
}

#[test]
fn scalar_subquery_in_projection() {
    let h = setup();
    // MAX(y) = 30 is attached to every row.
    let rows = h.select("SELECT id, (SELECT MAX(y) FROM t2) AS m FROM t1 ORDER BY id");
    assert_eq!(
        rows,
        vec![
            vec![int(1), int(30)],
            vec![int(2), int(30)],
            vec![int(3), int(30)],
        ]
    );
}

#[test]
fn scalar_subquery_zero_rows_is_null() {
    let h = setup();
    let rows = h.select("SELECT id, (SELECT y FROM t2 WHERE id = 99) AS m FROM t1 WHERE id = 1");
    assert_eq!(rows, vec![vec![int(1), DbValue::Null]]);
}

#[test]
fn scalar_subquery_multiple_rows_is_error() {
    let h = setup();
    let err = h
        .run("SELECT id, (SELECT y FROM t2) AS m FROM t1")
        .unwrap_err();
    assert!(err.contains("more than one row"), "unexpected error: {err}");
}

#[test]
fn in_subquery() {
    let h = setup();
    // t2.y = {10, 20, 30}; t1.x = {10, 15, 30} -> matches 10, 30 -> ids 1, 3.
    let rows = h.select("SELECT id FROM t1 WHERE x IN (SELECT y FROM t2) ORDER BY id");
    assert_eq!(rows, vec![vec![int(1)], vec![int(3)]]);
}

#[test]
fn not_in_subquery() {
    let h = setup();
    // Complement of the IN case -> only x = 15 (id 2).
    let rows = h.select("SELECT id FROM t1 WHERE x NOT IN (SELECT y FROM t2) ORDER BY id");
    assert_eq!(rows, vec![vec![int(2)]]);
}

#[test]
fn not_in_subquery_with_null_is_empty() {
    let h = setup();
    // A NULL in the IN-list makes NOT IN evaluate to NULL for non-members,
    // so no row qualifies (SQL three-valued logic).
    h.run("INSERT INTO t2 (id, y) VALUES (4, NULL)").unwrap();
    let rows = h.select("SELECT id FROM t1 WHERE x NOT IN (SELECT y FROM t2) ORDER BY id");
    assert!(rows.is_empty(), "expected empty, got {rows:?}");
}

#[test]
fn exists_and_not_exists() {
    let h = setup();
    // EXISTS true (t2 has y > 25) -> all rows.
    let rows =
        h.select("SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE y > 25) ORDER BY id");
    assert_eq!(rows, vec![vec![int(1)], vec![int(2)], vec![int(3)]]);

    // EXISTS false -> no rows.
    let rows = h.select("SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE y > 100)");
    assert!(rows.is_empty(), "expected empty, got {rows:?}");

    // NOT EXISTS over an empty inner result -> all rows.
    let rows =
        h.select("SELECT id FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE y > 100) ORDER BY id");
    assert_eq!(rows, vec![vec![int(1)], vec![int(2)], vec![int(3)]]);
}

#[test]
fn folded_scalar_enables_point_get() {
    let h = setup();
    // MIN(id) in t2 = 1; folds to `id = 1`, which the point-get fast path can use.
    let rows = h.select("SELECT x FROM t1 WHERE id = (SELECT MIN(id) FROM t2)");
    assert_eq!(rows, vec![vec![int(10)]]);
}

#[test]
fn delete_with_in_subquery() {
    let h = setup();
    let res = h
        .run("DELETE FROM t1 WHERE x IN (SELECT y FROM t2)")
        .unwrap();
    assert_eq!(res, ExecutionResult::Delete { count: 2 });
    let rows = h.select("SELECT id, x FROM t1 ORDER BY id");
    assert_eq!(rows, vec![vec![int(2), int(15)]]);
}

#[test]
fn update_with_scalar_subquery_assignment() {
    let h = setup();
    let res = h
        .run("UPDATE t1 SET x = (SELECT MAX(y) FROM t2) WHERE id = 2")
        .unwrap();
    assert_eq!(res, ExecutionResult::Update { count: 1 });
    let rows = h.select("SELECT x FROM t1 WHERE id = 2");
    assert_eq!(rows, vec![vec![int(30)]]);
}

#[test]
fn insert_select_with_in_subquery() {
    let h = setup();
    h.run("CREATE TABLE t3 (id INT PRIMARY KEY, x INT)")
        .unwrap();
    let res = h
        .run("INSERT INTO t3 (id, x) SELECT id, x FROM t1 WHERE x IN (SELECT y FROM t2)")
        .unwrap();
    assert_eq!(res, ExecutionResult::Insert { count: 2 });
    let rows = h.select("SELECT id, x FROM t3 ORDER BY id");
    assert_eq!(rows, vec![vec![int(1), int(10)], vec![int(3), int(30)]]);
}

// ---- Correlation detection (ADR 0005 step 4) ----

/// Every correlated shape must be rejected at plan time with a clear message,
/// never silently mis-evaluated.
#[test]
fn correlated_subqueries_are_rejected() {
    let h = setup();
    let correlated = [
        // scalar correlated
        "SELECT id FROM t1 WHERE x = (SELECT y FROM t2 WHERE t2.id = t1.id)",
        // IN correlated
        "SELECT id FROM t1 WHERE x IN (SELECT y FROM t2 WHERE t2.y = t1.x)",
        // EXISTS correlated (the classic)
        "SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)",
        // unqualified outer reference: `x` is t1.x, t2 has no x column
        "SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE y = x)",
        // correlated reference in the projection of a scalar subquery
        "SELECT (SELECT t1.x FROM t2 WHERE t2.id = 1) AS m FROM t1",
    ];
    for sql in correlated {
        let err = h.run(sql).unwrap_err();
        assert!(
            err.contains("correlated subqueries are not supported"),
            "expected correlated rejection for `{sql}`, got: {err}"
        );
    }
}

/// The accuracy risk the ADR flagged: an inner column that shares a name with an
/// outer column must bind to the inner scope, not be mistaken for correlation.
#[test]
fn shared_column_name_is_not_correlated() {
    let h = setup();
    // Both t1 and t2 have `id`; the subquery's `id` is t2.id and must resolve
    // inner. Subquery -> {2, 3}; outer t1.id IN {2, 3} -> x = 15, 30.
    let rows = h.select("SELECT x FROM t1 WHERE id IN (SELECT id FROM t2 WHERE id > 1) ORDER BY x");
    assert_eq!(rows, vec![vec![int(15)], vec![int(30)]]);
}

/// A subquery that GROUP BYs and references an aggregate output in HAVING is
/// self-contained and must be accepted.
#[test]
fn grouped_subquery_is_not_correlated() {
    let h = setup();
    // Inner: distinct y with count >= 1 -> {10, 20, 30}; t1.x IN that -> 10, 30.
    let rows = h.select(
        "SELECT x FROM t1 WHERE x IN (SELECT y FROM t2 GROUP BY y HAVING COUNT(*) >= 1) ORDER BY x",
    );
    assert_eq!(rows, vec![vec![int(10)], vec![int(30)]]);
}

/// A subquery containing its own JOIN is self-contained.
#[test]
fn joined_subquery_is_not_correlated() {
    let h = setup();
    // Self-join pairs each t2 row with itself -> a.y = {10, 20, 30}.
    let rows = h.select(
        "SELECT x FROM t1 WHERE x IN (SELECT a.y FROM t2 a JOIN t2 b ON a.id = b.id) ORDER BY x",
    );
    assert_eq!(rows, vec![vec![int(10)], vec![int(30)]]);
}

/// A nested (subquery-within-subquery) chain where every level references only
/// its own table is fully uncorrelated.
#[test]
fn nested_uncorrelated_subquery_is_accepted() {
    let h = setup();
    let rows = h.select(
        "SELECT x FROM t1 WHERE x IN (SELECT y FROM t2 WHERE y IN (SELECT y FROM t2 WHERE y > 5)) ORDER BY x",
    );
    assert_eq!(rows, vec![vec![int(10)], vec![int(30)]]);
}
