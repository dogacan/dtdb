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
fn cte_basic() {
    let h = setup();
    let rows = h.select("WITH cte AS (SELECT id, x FROM t1) SELECT id, x FROM cte ORDER BY id");
    assert_eq!(
        rows,
        vec![
            vec![int(1), int(10)],
            vec![int(2), int(15)],
            vec![int(3), int(30)],
        ]
    );
}

#[test]
fn cte_derived_column_aliases() {
    let h = setup();
    let rows = h.select("WITH cte(a, b) AS (SELECT id, x FROM t1) SELECT a, b FROM cte ORDER BY a");
    assert_eq!(
        rows,
        vec![
            vec![int(1), int(10)],
            vec![int(2), int(15)],
            vec![int(3), int(30)],
        ]
    );
}

#[test]
fn cte_derived_column_alias_count_mismatch() {
    let h = setup();
    let err = h
        .run("WITH cte(a) AS (SELECT id, x FROM t1) SELECT * FROM cte")
        .unwrap_err();
    assert!(
        err.contains("column(s) but"),
        "unexpected error message: {err}"
    );
}

#[test]
fn cte_scan_alias_count_mismatch() {
    let h = setup();
    let err = h
        .run("WITH cte AS (SELECT id, x FROM t1) SELECT * FROM cte AS c(only_one)")
        .unwrap_err();
    assert!(
        err.contains("column(s) but"),
        "unexpected error message: {err}"
    );
}

#[test]
fn cte_scoping_and_shadowing() {
    let h = setup();
    // Inner query shadows outer `cte` with id = 1 by redefining `cte` with id = 2.
    let rows = h.select(
        "WITH cte AS (SELECT id, x FROM t1 WHERE id = 1) \
         SELECT id FROM (WITH cte AS (SELECT id, x FROM t1 WHERE id = 2) SELECT * FROM cte) AS sub",
    );
    assert_eq!(rows, vec![vec![int(2)]]);

    // Outside the subquery, the outer scope `cte` remains visible.
    let rows = h.select(
        "WITH cte AS (SELECT id, x FROM t1 WHERE id = 1) \
         SELECT c.id, sub.id FROM cte c JOIN (WITH cte AS (SELECT id, x FROM t1 WHERE id = 2) SELECT id, x - 5 AS x FROM cte) sub ON c.x = sub.x",
    );
    assert_eq!(rows, vec![vec![int(1), int(2)]]);
}

#[test]
fn cte_multiple_definitions() {
    let h = setup();
    let rows = h.select(
        "WITH cte1 AS (SELECT id, x FROM t1 WHERE id = 1), \
              cte2 AS (SELECT id, x FROM t1 WHERE id = 2) \
         SELECT id FROM cte1 UNION ALL SELECT id FROM cte2 ORDER BY id",
    );
    assert_eq!(rows, vec![vec![int(1)], vec![int(2)]]);
}

#[test]
fn cte_referenced_multiple_times() {
    let h = setup();
    let rows = h.select(
        "WITH cte AS (SELECT id, x FROM t1 WHERE id = 1) \
         SELECT c1.id, c2.x FROM cte c1 JOIN cte c2 ON c1.id = c2.id",
    );
    assert_eq!(rows, vec![vec![int(1), int(10)]]);
}

#[test]
fn cte_nested() {
    let h = setup();
    // cte1 references cte2 defined within its own query context
    let rows = h.select(
        "WITH cte1 AS (WITH cte2 AS (SELECT id, x FROM t1) SELECT id FROM cte2) \
         SELECT id FROM cte1 ORDER BY id",
    );
    assert_eq!(rows, vec![vec![int(1)], vec![int(2)], vec![int(3)]]);
}

#[test]
fn cte_recursive_rejections() {
    let h = setup();

    // 1. Explicit RECURSIVE keyword rejection
    let err1 = h
        .run("WITH RECURSIVE cte AS (SELECT id FROM t1) SELECT * FROM cte")
        .unwrap_err();
    assert!(
        err1.contains("recursive CTEs are not supported"),
        "got: {err1}"
    );

    // 2. Implicit self-reference recursion rejection
    let err2 = h
        .run("WITH cte AS (SELECT * FROM cte) SELECT * FROM cte")
        .unwrap_err();
    assert!(
        err2.contains("recursive CTEs are not supported"),
        "got: {err2}"
    );

    // 3. Mutual recursion cannot be expressed as a non-recursive WITH: `cte1`
    //    would have to forward-reference `cte2`, which is defined later in the
    //    same clause and so is not yet in scope. It is therefore rejected as an
    //    unknown table (by name resolution) rather than by the self-reference
    //    detector — either way, a mutually-recursive attempt does not plan.
    let err3 = h
        .run("WITH cte1 AS (SELECT * FROM cte2), cte2 AS (SELECT * FROM cte1) SELECT * FROM cte1")
        .unwrap_err();
    assert!(err3.contains("Table not found: cte2"), "got: {err3}");
}

#[test]
fn cte_duplicate_name_error() {
    let h = setup();
    let err = h
        .run("WITH cte AS (SELECT id FROM t1), cte AS (SELECT id FROM t1) SELECT * FROM cte")
        .unwrap_err();
    assert!(err.contains("duplicate CTE name"), "got: {err}");
}

#[test]
fn cte_in_subquery() {
    let h = setup();
    // CTE is defined within a subquery in expression position
    let rows = h.select("SELECT id FROM t1 WHERE id = (WITH cte AS (SELECT id FROM t2 WHERE y = 20) SELECT id FROM cte)");
    assert_eq!(rows, vec![vec![int(2)]]);
}

#[test]
fn cte_shadows_base_table() {
    let h = setup();
    // A non-recursive CTE name is not visible within its own body, so `FROM t1`
    // inside the definition refers to the base table `t1`, not the CTE. This is a
    // common idiom (e.g. `WITH t AS (SELECT ... FROM t WHERE ...)`) and must not be
    // mistaken for recursion.
    let rows = h.select("WITH t1 AS (SELECT id FROM t1 WHERE id = 2) SELECT id FROM t1");
    assert_eq!(rows, vec![vec![int(2)]]);
}

#[test]
fn cte_nested_redefines_outer_name() {
    let h = setup();
    // An inner CTE independently reuses an enclosing CTE's name. The inner
    // definition is a fresh, non-recursive CTE and must not be rejected as
    // recursive just because an ancestor of the same name is mid-planning.
    let rows = h.select(
        "WITH cte AS (WITH cte AS (SELECT id FROM t1 WHERE id = 3) SELECT id FROM cte) \
         SELECT id FROM cte",
    );
    assert_eq!(rows, vec![vec![int(3)]]);
}

#[test]
fn cte_inner_shadow_references_outer_scope() {
    let h = setup();
    // Inner CTE `cte` references `cte` in its own body. The inner name is not yet
    // visible there (non-recursive), so the reference resolves to the OUTER `cte`
    // (id = 1) rather than being flagged as a self-reference.
    let rows = h.select(
        "WITH cte AS (SELECT id FROM t1 WHERE id = 1) \
         SELECT id FROM (WITH cte AS (SELECT id FROM cte) SELECT id FROM cte) sub",
    );
    assert_eq!(rows, vec![vec![int(1)]]);
}
