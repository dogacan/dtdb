use dtdb_relational::{Database, Table, Transaction};
use dtdb_sql::{ExecutionResult, SqlEngine};
use dtdb_storage::DbValue;
use std::sync::Arc;
use tempfile::TempDir;

// Helper function to setup database and SQL engine.
fn setup_engine() -> (TempDir, Arc<Database>, SqlEngine) {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    let engine = SqlEngine::new(db.clone());
    (temp_dir, db, engine)
}

#[test]
fn test_sql_ddl_and_crud() {
    let (_temp, db, engine) = setup_engine();

    // 1. CREATE TABLE
    let tx1 = Transaction::new(1, db.clone());
    let res = engine
        .execute(
            "CREATE TABLE users (id INT PRIMARY KEY, name STRING, score FLOAT)",
            &tx1,
        )
        .unwrap();
    assert!(matches!(res, ExecutionResult::CreateTable));
    tx1.commit().unwrap();

    // Verify metadata
    {
        let tables = db.list_tables();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0], "users");
    }

    // 2. INSERT INTO
    let tx2 = Transaction::new(2, db.clone());
    let res = engine
        .execute(
            "INSERT INTO users (id, name, score) VALUES (1, \"Alice\", 95.5), (2, 'Bob', 80.0), (3, 'Charlie', 85.0)",
            &tx2,
        )
        .unwrap();
    assert_eq!(res, ExecutionResult::Insert { count: 3 });
    tx2.commit().unwrap();

    // 3. SELECT (all columns, filter, sorting, limit)
    let tx3 = Transaction::new(3, db.clone());
    let res = engine
        .execute("SELECT id, name, score FROM users ORDER BY id ASC", &tx3)
        .unwrap();
    if let ExecutionResult::Select { schema, rows } = res {
        assert_eq!(rows.len(), 3);
        assert_eq!(schema.columns[0].name, "id");
        assert_eq!(schema.columns[1].name, "name");
        assert_eq!(schema.columns[2].name, "score");
        assert_eq!(
            rows[0].values,
            vec![
                DbValue::Int(1),
                DbValue::string("Alice"),
                DbValue::Float(95.5)
            ]
        );
        assert_eq!(
            rows[1].values,
            vec![
                DbValue::Int(2),
                DbValue::string("Bob"),
                DbValue::Float(80.0)
            ]
        );
        assert_eq!(
            rows[2].values,
            vec![
                DbValue::Int(3),
                DbValue::string("Charlie"),
                DbValue::Float(85.0)
            ]
        );
    } else {
        panic!("Expected ExecutionResult::Select");
    }

    // 4. SELECT with WHERE clause filter, DESC ordering, and LIMIT
    let res = engine
        .execute(
            "SELECT name FROM users WHERE score > 82.0 ORDER BY score DESC LIMIT 1",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { schema, rows } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(schema.columns[0].name, "name");
        assert_eq!(rows[0].values, vec![DbValue::string("Alice")]);
    } else {
        panic!("Expected ExecutionResult::Select");
    }
}

#[test]
fn test_prefix_like_returns_exact_matches() {
    let (_temp, db, engine) = setup_engine();

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE words (name STRING PRIMARY KEY)", &tx1)
        .unwrap();
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO words (name) VALUES \
             ('aardvark'), ('ab'), ('abc'), ('abz'), ('ac'), ('b'), ('ABC')",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();

    // `LIKE 'ab%'` is served by a [ab, ac] prefix range scan plus the residual
    // LIKE filter. The result must be exactly the prefix matches: the filter
    // drops the inclusive "ac" boundary and the case-mismatched "ABC" (LIKE is
    // case-sensitive), while "aardvark"/"b" fall outside the range entirely.
    let tx3 = Transaction::new(3, db.clone());
    let res = engine
        .execute(
            "SELECT name FROM words WHERE name LIKE 'ab%' ORDER BY name ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        let names: Vec<_> = rows.iter().map(|r| r.values[0].clone()).collect();
        assert_eq!(
            names,
            vec![
                DbValue::string("ab"),
                DbValue::string("abc"),
                DbValue::string("abz"),
            ]
        );
    } else {
        panic!("Expected ExecutionResult::Select");
    }
}

#[test]
fn test_sql_joins() {
    let (_temp, db, engine) = setup_engine();

    // 1. Create tables
    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE users (id INT PRIMARY KEY, name STRING)", &tx1)
        .unwrap();
    engine
        .execute(
            "CREATE TABLE orders (order_id INT PRIMARY KEY, user_id INT, amount FLOAT)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();

    // 2. Insert data
    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
            &tx2,
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO orders (order_id, user_id, amount) VALUES (10, 1, 100.5), (20, 2, 250.0), (30, 1, 15.75)",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();

    // 3. Execute INNER JOIN query
    let tx3 = Transaction::new(3, db.clone());
    let res = engine
        .execute(
            "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id ORDER BY orders.amount ASC",
            &tx3,
        )
        .unwrap();

    if let ExecutionResult::Select { schema, rows } = res {
        assert_eq!(rows.len(), 3);
        assert_eq!(schema.columns[0].name, "users.name");
        assert_eq!(schema.columns[1].name, "orders.amount");

        // Row 0: Alice, 15.75
        assert_eq!(
            rows[0].values,
            vec![DbValue::string("Alice"), DbValue::Float(15.75)]
        );
        // Row 1: Alice, 100.5
        assert_eq!(
            rows[1].values,
            vec![DbValue::string("Alice"), DbValue::Float(100.5)]
        );
        // Row 2: Bob, 250.0
        assert_eq!(
            rows[2].values,
            vec![DbValue::string("Bob"), DbValue::Float(250.0)]
        );
    } else {
        panic!("Expected ExecutionResult::Select");
    }
}

#[test]
fn test_sql_aggregations() {
    let (_temp, db, engine) = setup_engine();

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE employees (id INT PRIMARY KEY, dept STRING, salary FLOAT)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO employees (id, dept, salary) VALUES \
             (1, 'Sales', 50000.0), \
             (2, 'Sales', 60000.0), \
             (3, 'Engineering', 90000.0), \
             (4, 'Engineering', 110000.0), \
             (5, 'HR', 45000.0)",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();

    // 1. Global Aggregation (no GROUP BY)
    let tx3 = Transaction::new(3, db.clone());
    let res = engine
        .execute(
            "SELECT COUNT(salary), SUM(salary), MIN(salary), MAX(salary) FROM employees",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { schema, rows } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(schema.columns.len(), 4);
        assert_eq!(
            rows[0].values,
            vec![
                DbValue::Int(5),          // COUNT
                DbValue::Float(355000.0), // SUM
                DbValue::Float(45000.0),  // MIN
                DbValue::Float(110000.0)  // MAX
            ]
        );
    } else {
        panic!("Expected ExecutionResult::Select");
    }

    // 2. Group By Aggregation
    let res = engine
        .execute(
            "SELECT dept, COUNT(salary), SUM(salary) FROM employees GROUP BY dept ORDER BY dept ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { schema, rows } = res {
        assert_eq!(rows.len(), 3);
        assert_eq!(schema.columns[0].name, "dept");

        // Engineering: count=2, sum=200000.0
        assert_eq!(
            rows[0].values,
            vec![
                DbValue::string("Engineering"),
                DbValue::Int(2),
                DbValue::Float(200000.0)
            ]
        );
        // HR: count=1, sum=45000.0
        assert_eq!(
            rows[1].values,
            vec![
                DbValue::string("HR"),
                DbValue::Int(1),
                DbValue::Float(45000.0)
            ]
        );
        // Sales: count=2, sum=110000.0
        assert_eq!(
            rows[2].values,
            vec![
                DbValue::string("Sales"),
                DbValue::Int(2),
                DbValue::Float(110000.0)
            ]
        );
    } else {
        panic!("Expected ExecutionResult::Select");
    }
}

#[test]
fn test_sql_like_wildcard() {
    let (_temp, db, engine) = setup_engine();

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, code STRING)", &tx1)
        .unwrap();
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO items (id, code) VALUES \
             (1, 'abc-123'), \
             (2, 'def-456'), \
             (3, 'abc-789'), \
             (4, 'xyz-abc-999')",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();

    let tx3 = Transaction::new(3, db.clone());
    // Find items starting with 'abc'
    let res = engine
        .execute(
            "SELECT id, code FROM items WHERE code LIKE 'abc-%' ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values[1], DbValue::string("abc-123"));
        assert_eq!(rows[1].values[1], DbValue::string("abc-789"));
    } else {
        panic!("Expected ExecutionResult::Select");
    }

    // Find items containing 'abc' anywhere
    let res = engine
        .execute(
            "SELECT id, code FROM items WHERE code LIKE '%abc%' ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values[1], DbValue::string("abc-123"));
        assert_eq!(rows[1].values[1], DbValue::string("abc-789"));
        assert_eq!(rows[2].values[1], DbValue::string("xyz-abc-999"));
    } else {
        panic!("Expected ExecutionResult::Select");
    }
}

#[test]
fn test_sql_transactions() {
    let (_temp, db, engine) = setup_engine();

    // 1. Create table
    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE users (id INT PRIMARY KEY, name STRING)", &tx1)
        .unwrap();
    tx1.commit().unwrap();

    // 2. Perform write and rollback
    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO users (id, name) VALUES (10, 'RollbackMe')",
            &tx2,
        )
        .unwrap();
    tx2.rollback().unwrap();

    // 3. Perform write and commit
    let tx3 = Transaction::new(3, db.clone());
    engine
        .execute("INSERT INTO users (id, name) VALUES (20, 'KeepMe')", &tx3)
        .unwrap();
    tx3.commit().unwrap();

    // 4. Verify contents in a new transaction
    let tx4 = Transaction::new(4, db.clone());
    let res = engine.execute("SELECT id, name FROM users", &tx4).unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].values,
            vec![DbValue::Int(20), DbValue::string("KeepMe")]
        );
    } else {
        panic!("Expected ExecutionResult::Select");
    }
}

#[test]
fn test_sql_optimizer_pushdown() {
    let (_temp, db, engine) = setup_engine();

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE test_table (id INT PRIMARY KEY, name STRING)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO test_table (id, name) VALUES \
             (10, 'ten'), \
             (20, 'twenty'), \
             (30, 'thirty'), \
             (40, 'forty')",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();

    let tx3 = Transaction::new(3, db.clone());

    // 1. Equal predicate: id = 20
    let res = engine
        .execute("SELECT name FROM test_table WHERE id = 20", &tx3)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values, vec![DbValue::string("twenty")]);
    } else {
        panic!("Expected Select");
    }

    // 2. Range predicate: id >= 20 AND id <= 30
    let res = engine
        .execute(
            "SELECT name FROM test_table WHERE id >= 20 AND id <= 30 ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values, vec![DbValue::string("twenty")]);
        assert_eq!(rows[1].values, vec![DbValue::string("thirty")]);
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_sql_explain() {
    let (_temp, db, engine) = setup_engine();

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE users (id INT PRIMARY KEY, name STRING)", &tx1)
        .unwrap();
    tx1.commit().unwrap();

    let explain = |sql: &str| -> String {
        let tx = Transaction::new(2, db.clone());
        let res = engine.execute(sql, &tx).unwrap();
        tx.commit().unwrap();
        match res {
            ExecutionResult::Select { schema, rows } => {
                assert_eq!(schema.columns[0].name, "Query Plan");
                assert_eq!(rows.len(), 1);
                match &rows[0].values[0] {
                    DbValue::String(s) => s.to_string(),
                    _ => panic!("Expected string plan text"),
                }
            }
            _ => panic!("Expected EXPLAIN Select output"),
        }
    };

    // A primary-key equality is recognized as a point lookup and takes the
    // `execute_point_get` fast path instead of building a scan operator tree.
    let point_plan = explain("EXPLAIN SELECT name FROM users WHERE id = 10");
    assert!(point_plan.contains("--- Logical Plan ---"));
    assert!(point_plan.contains("--- Optimized Plan ---"));
    assert!(point_plan.contains("--- Physical Plan ---"));
    assert!(
        point_plan.contains("PhysicalPointGet"),
        "expected point-get fast path, got:\n{point_plan}"
    );
    assert!(!point_plan.contains("PhysicalSeqScan"));

    // A non-point query (full table scan) still compiles to the general
    // Volcano operator tree.
    let scan_plan = explain("EXPLAIN SELECT name FROM users");
    assert!(
        scan_plan.contains("PhysicalSeqScan"),
        "expected general scan path, got:\n{scan_plan}"
    );
    assert!(!scan_plan.contains("PhysicalPointGet"));
}

#[test]
fn test_sql_point_get_fast_path() {
    let (_temp, db, engine) = setup_engine();

    let tx = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, k INT, v STRING)", &tx)
        .unwrap();
    for i in 0..50 {
        engine
            .execute(
                &format!("INSERT INTO t (id, k, v) VALUES ({i}, {}, 'v{i}')", i * 10),
                &tx,
            )
            .unwrap();
    }
    tx.commit().unwrap();

    // Run a query in its own transaction and return the result rows.
    let select = |sql: &str| -> Vec<Vec<DbValue>> {
        let tx = Transaction::new(100, db.clone());
        let res = engine.execute(sql, &tx).unwrap();
        tx.commit().unwrap();
        match res {
            ExecutionResult::Select { rows, .. } => rows.into_iter().map(|r| r.values).collect(),
            other => panic!("expected Select, got {other:?}"),
        }
    };
    let plan_for = |sql: &str| -> String {
        let tx = Transaction::new(101, db.clone());
        let res = engine.execute(&format!("EXPLAIN {sql}"), &tx).unwrap();
        tx.commit().unwrap();
        match res {
            ExecutionResult::Select { rows, .. } => match &rows[0].values[0] {
                DbValue::String(s) => s.to_string(),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    };

    // 1. Point hit: correct projected row, and the fast path is chosen.
    assert_eq!(
        select("SELECT k, v FROM t WHERE id = 7"),
        vec![vec![DbValue::Int(70), DbValue::string("v7")]]
    );
    assert!(plan_for("SELECT k, v FROM t WHERE id = 7").contains("PhysicalPointGet"));

    // 2. Point miss: empty result (key absent), still the fast path.
    assert_eq!(
        select("SELECT k FROM t WHERE id = 999"),
        Vec::<Vec<DbValue>>::new()
    );
    assert!(plan_for("SELECT k FROM t WHERE id = 999").contains("PhysicalPointGet"));

    // 3. SELECT * point lookup returns the full row in schema order.
    assert_eq!(
        select("SELECT * FROM t WHERE id = 3"),
        vec![vec![
            DbValue::Int(3),
            DbValue::Int(30),
            DbValue::string("v3")
        ]]
    );

    // 4. Residual filter on top of the point key: must still be applied.
    //    id = 5 exists but k = 50, so `k > 1000` filters it out.
    assert_eq!(
        select("SELECT v FROM t WHERE id = 5 AND k > 1000"),
        Vec::<Vec<DbValue>>::new()
    );
    assert!(plan_for("SELECT v FROM t WHERE id = 5 AND k > 1000").contains("residual filter"));
    //    ...and a residual filter that passes keeps the row.
    assert_eq!(
        select("SELECT v FROM t WHERE id = 5 AND k = 50"),
        vec![vec![DbValue::string("v5")]]
    );

    // 5. Expression projection over a point lookup evaluates per-row.
    assert_eq!(
        select("SELECT id + 1, k * 2 FROM t WHERE id = 4"),
        vec![vec![DbValue::Int(5), DbValue::Int(80)]]
    );

    // 6. Non-point queries do NOT take the fast path but stay correct.
    assert!(!plan_for("SELECT k FROM t WHERE id > 45").contains("PhysicalPointGet"));
    assert_eq!(select("SELECT id FROM t WHERE id > 47").len(), 2); // 48, 49
    assert!(!plan_for("SELECT k FROM t").contains("PhysicalPointGet"));
}

#[test]
fn test_sql_point_get_string_pk_and_read_your_writes() {
    let (_temp, db, engine) = setup_engine();

    let tx = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE u (name STRING PRIMARY KEY, age INT)", &tx)
        .unwrap();
    engine
        .execute(
            "INSERT INTO u (name, age) VALUES ('alice', 30), ('bob', 40)",
            &tx,
        )
        .unwrap();
    tx.commit().unwrap();

    // String primary key point lookup.
    let tx = Transaction::new(2, db.clone());
    let res = engine
        .execute("SELECT age FROM u WHERE name = 'bob'", &tx)
        .unwrap();
    tx.commit().unwrap();
    match res {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values, vec![DbValue::Int(40)]);
        }
        _ => panic!("expected Select"),
    }

    // Read-your-writes: a point lookup must see an uncommitted insert from the
    // same transaction (the fast path consults the transaction write buffer).
    let tx = Transaction::new(3, db.clone());
    engine
        .execute("INSERT INTO u (name, age) VALUES ('carol', 25)", &tx)
        .unwrap();
    let res = engine
        .execute("SELECT age FROM u WHERE name = 'carol'", &tx)
        .unwrap();
    match res {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values, vec![DbValue::Int(25)]);
        }
        _ => panic!("expected Select"),
    }
    tx.commit().unwrap();
}

#[test]
fn test_sql_composite_pk_not_point_get() {
    let (_temp, db, engine) = setup_engine();

    let tx = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE ck (a INT, b INT, v STRING, PRIMARY KEY (a, b))",
            &tx,
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO ck (a, b, v) VALUES (1, 1, 'x'), (1, 2, 'y'), (2, 1, 'z')",
            &tx,
        )
        .unwrap();
    tx.commit().unwrap();

    // A partial composite-key predicate must NOT use the single-key fast path
    // (it can match multiple rows), and must return all matching rows.
    let tx = Transaction::new(2, db.clone());
    let explain = engine
        .execute("EXPLAIN SELECT v FROM ck WHERE a = 1", &tx)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = explain
        && let DbValue::String(s) = &rows[0].values[0]
    {
        assert!(
            !s.contains("PhysicalPointGet"),
            "composite key must not point-get:\n{s}"
        );
    }
    let res = engine.execute("SELECT v FROM ck WHERE a = 1", &tx).unwrap();
    tx.commit().unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_sql_delete() {
    let (_temp, db, engine) = setup_engine();

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE users (id INT PRIMARY KEY, name STRING)", &tx1)
        .unwrap();
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();

    // Delete single row
    let tx3 = Transaction::new(3, db.clone());
    let res = engine
        .execute("DELETE FROM users WHERE id = 2", &tx3)
        .unwrap();
    assert_eq!(res, ExecutionResult::Delete { count: 1 });
    tx3.commit().unwrap();

    // Verify row 2 is deleted
    let tx4 = Transaction::new(4, db.clone());
    let res = engine
        .execute("SELECT id, name FROM users ORDER BY id ASC", &tx4)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
        assert_eq!(rows[1].values[0], DbValue::Int(3));
    } else {
        panic!("Expected Select");
    }

    // Delete all remaining rows
    let res = engine.execute("DELETE FROM users", &tx4).unwrap();
    assert_eq!(res, ExecutionResult::Delete { count: 2 });
    tx4.commit().unwrap();

    // Verify empty
    let tx5 = Transaction::new(5, db.clone());
    let res = engine.execute("SELECT id FROM users", &tx5).unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert!(rows.is_empty());
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_sql_update() {
    let (_temp, db, engine) = setup_engine();

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE users (id INT PRIMARY KEY, name STRING, score FLOAT)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO users (id, name, score) VALUES (1, 'Alice', 90.0), (2, 'Bob', 80.0)",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();

    // Update non-pk columns
    let tx3 = Transaction::new(3, db.clone());
    let res = engine
        .execute(
            "UPDATE users SET name = 'AliceUpdated', score = 95.5 WHERE id = 1",
            &tx3,
        )
        .unwrap();
    assert_eq!(res, ExecutionResult::Update { count: 1 });
    tx3.commit().unwrap();

    // Verify update
    let tx4 = Transaction::new(4, db.clone());
    let res = engine
        .execute("SELECT name, score FROM users WHERE id = 1", &tx4)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], DbValue::string("AliceUpdated"));
        assert_eq!(rows[0].values[1], DbValue::Float(95.5));
    } else {
        panic!("Expected Select");
    }

    // Update pk column (causes delete + put)
    let tx5 = Transaction::new(5, db.clone());
    let res = engine
        .execute("UPDATE users SET id = id + 10 WHERE id = 2", &tx5)
        .unwrap();
    assert_eq!(res, ExecutionResult::Update { count: 1 });
    tx5.commit().unwrap();

    // Verify old pk is gone and new pk exists
    let tx6 = Transaction::new(6, db.clone());
    let res = engine
        .execute("SELECT id, name FROM users ORDER BY id ASC", &tx6)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values[0], DbValue::Int(1)); // Alice
        assert_eq!(rows[1].values[0], DbValue::Int(12)); // Bob (2 + 10)
        assert_eq!(rows[1].values[1], DbValue::string("Bob"));
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_sql_left_join() {
    let (_temp, db, engine) = setup_engine();

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE users (id INT PRIMARY KEY, name STRING)", &tx1)
        .unwrap();
    engine
        .execute(
            "CREATE TABLE orders (order_id INT PRIMARY KEY, user_id INT, amount FLOAT)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
            &tx2,
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO orders (order_id, user_id, amount) VALUES (10, 1, 99.9), (20, 2, 199.9)",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();

    let tx3 = Transaction::new(3, db.clone());
    let res = engine.execute(
        "SELECT users.name, orders.amount FROM users LEFT JOIN orders ON users.id = orders.user_id ORDER BY users.id ASC",
        &tx3
    ).unwrap();

    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 3);
        // Alice: matched
        assert_eq!(rows[0].values[0], DbValue::string("Alice"));
        assert_eq!(rows[0].values[1], DbValue::Float(99.9));
        // Bob: matched
        assert_eq!(rows[1].values[0], DbValue::string("Bob"));
        assert_eq!(rows[1].values[1], DbValue::Float(199.9));
        // Charlie: unmatched (padded with NULL)
        assert_eq!(rows[2].values[0], DbValue::string("Charlie"));
        assert_eq!(rows[2].values[1], DbValue::Null);
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_sql_limit_offset() {
    let (_temp, db, engine) = setup_engine();

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE users (id INT PRIMARY KEY)", &tx1)
        .unwrap();
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO users (id) VALUES (10), (20), (30), (40), (50)",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();

    let tx3 = Transaction::new(3, db.clone());
    let res = engine
        .execute(
            "SELECT id FROM users ORDER BY id ASC LIMIT 2 OFFSET 2",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values[0], DbValue::Int(30));
        assert_eq!(rows[1].values[0], DbValue::Int(40));
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_sql_avg_aggregate() {
    let (_temp, db, engine) = setup_engine();

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE employees (id INT PRIMARY KEY, dept STRING, salary FLOAT)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO employees (id, dept, salary) VALUES \
         (1, 'Sales', 50.0), \
         (2, 'Sales', 150.0), \
         (3, 'Eng', 100.0), \
         (4, 'Eng', 300.0)",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();

    let tx3 = Transaction::new(3, db.clone());
    let res = engine
        .execute(
            "SELECT dept, AVG(salary) FROM employees GROUP BY dept ORDER BY dept ASC",
            &tx3,
        )
        .unwrap();

    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
        // Eng
        assert_eq!(rows[0].values[0], DbValue::string("Eng"));
        assert_eq!(rows[0].values[1], DbValue::Float(200.0)); // (100+300)/2
        // Sales
        assert_eq!(rows[1].values[0], DbValue::string("Sales"));
        assert_eq!(rows[1].values[1], DbValue::Float(100.0)); // (50+150)/2
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_sql_arithmetic_expressions() {
    let (_temp, db, engine) = setup_engine();

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE items (id INT PRIMARY KEY, val INT, factor FLOAT)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO items (id, val, factor) VALUES (1, 10, 2.5)",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();

    let tx3 = Transaction::new(3, db.clone());
    let res = engine.execute(
        "SELECT val + 5, val - 3, val * 2, val / 4, val * factor, factor / 0.5 FROM items WHERE id = 1",
        &tx3
    ).unwrap();

    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], DbValue::Int(15));
        assert_eq!(rows[0].values[1], DbValue::Int(7));
        assert_eq!(rows[0].values[2], DbValue::Int(20));
        assert_eq!(rows[0].values[3], DbValue::Int(2)); // integer division: 10 / 4 = 2
        assert_eq!(rows[0].values[4], DbValue::Float(25.0)); // 10 * 2.5 = 25.0
        assert_eq!(rows[0].values[5], DbValue::Float(5.0)); // 2.5 / 0.5 = 5.0
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_sql_case_and_functions() {
    let (_temp, db, engine) = setup_engine();

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE products (id INT PRIMARY KEY, name STRING, price FLOAT, category STRING)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO products (id, name, price, category) VALUES \
         (1, 'Laptop', 1200.0, 'Electronics'), \
         (2, 'Mouse', 25.0, ''), \
         (3, 'Desk', 0.0, 'Furniture'), \
         (4, 'Chair', 150.0, 'Furniture')",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();

    let tx3 = Transaction::new(3, db.clone());

    // 1. Test CASE WHEN (searched)
    let res = engine.execute(
        "SELECT name, CASE WHEN price >= 1000.0 THEN 'expensive' WHEN price >= 100.0 THEN 'moderate' ELSE 'cheap' END FROM products ORDER BY id ASC",
        &tx3
    ).unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].values[1], DbValue::string("expensive"));
        assert_eq!(rows[1].values[1], DbValue::string("cheap"));
        assert_eq!(rows[2].values[1], DbValue::string("cheap"));
        assert_eq!(rows[3].values[1], DbValue::string("moderate"));
    } else {
        panic!("Expected Select");
    }

    // 2. Test CASE WHEN (simple)
    let res = engine.execute(
        "SELECT name, CASE id WHEN 1 THEN 'One' WHEN 2 THEN 'Two' ELSE 'Other' END FROM products ORDER BY id ASC",
        &tx3
    ).unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].values[1], DbValue::string("One"));
        assert_eq!(rows[1].values[1], DbValue::string("Two"));
        assert_eq!(rows[2].values[1], DbValue::string("Other"));
        assert_eq!(rows[3].values[1], DbValue::string("Other"));
    } else {
        panic!("Expected Select");
    }

    // 3. Test LENGTH
    let res = engine
        .execute(
            "SELECT name, LENGTH(name) FROM products ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].values[1], DbValue::Int(6)); // Laptop
        assert_eq!(rows[1].values[1], DbValue::Int(5)); // Mouse
    } else {
        panic!("Expected Select");
    }

    // 4. Test SUBSTR (various bounds and forms)
    let res = engine.execute(
        "SELECT SUBSTR(name, 1, 3), SUBSTR(name, 4), SUBSTR(name, -3, 2), SUBSTR(name, 0, 2) FROM products WHERE id = 1",
        &tx3
    ).unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], DbValue::string("Lap"));
        assert_eq!(rows[0].values[1], DbValue::string("top"));
        assert_eq!(rows[0].values[2], DbValue::string("to"));
        assert_eq!(rows[0].values[3], DbValue::string("L"));
    } else {
        panic!("Expected Select");
    }

    // 5. Test COALESCE (returns first non-empty / non-zero value, but empty/zero are not NULL)
    let res = engine
        .execute(
            "SELECT name, COALESCE(category, 'Uncategorized') FROM products ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].values[1], DbValue::string("Electronics"));
        assert_eq!(rows[1].values[1], DbValue::string("")); // Mouse category is empty "" -> NOT Null -> no fallback
        assert_eq!(rows[2].values[1], DbValue::string("Furniture"));
    } else {
        panic!("Expected Select");
    }

    // 6. Test COALESCE with multiple arguments and numeric fallback
    let res = engine
        .execute(
            "SELECT name, COALESCE(price, 99.0) FROM products ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].values[1], DbValue::Float(1200.0));
        assert_eq!(rows[2].values[1], DbValue::Float(0.0)); // Desk price is 0.0 -> NOT Null -> no fallback
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_sql_explicit_null() {
    let (_temp, db, engine) = setup_engine();

    // 1. Create table with nullable and NOT NULL columns
    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE nullable_test (id INT PRIMARY KEY, name STRING NOT NULL, note STRING)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();

    // 2. Insert explicit NULLs
    let tx2 = Transaction::new(2, db.clone());
    engine.execute("INSERT INTO nullable_test (id, name, note) VALUES (1, 'Alice', NULL), (2, 'Bob', 'First Note')", &tx2).unwrap();
    tx2.commit().unwrap();

    // 3. Try to insert NULL into NOT NULL column (should fail validation)
    let tx3 = Transaction::new(3, db.clone());
    let insert_fail = engine.execute(
        "INSERT INTO nullable_test (id, name, note) VALUES (3, NULL, 'Note')",
        &tx3,
    );
    assert!(
        insert_fail.is_err(),
        "Expected insert of NULL into NOT NULL column to fail"
    );
    let _ = tx3.rollback();

    // 4. Try to insert row omitting nullable column (should default to NULL)
    let tx4 = Transaction::new(4, db.clone());
    engine
        .execute(
            "INSERT INTO nullable_test (id, name) VALUES (3, 'Charlie')",
            &tx4,
        )
        .unwrap();
    tx4.commit().unwrap();

    // 5. Select and verify explicit NULL and defaulted NULL
    let tx5 = Transaction::new(5, db.clone());
    let res = engine
        .execute(
            "SELECT id, name, note FROM nullable_test ORDER BY id ASC",
            &tx5,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 3);
        // Alice note is NULL
        assert_eq!(rows[0].values[0], DbValue::Int(1));
        assert_eq!(rows[0].values[1], DbValue::string("Alice"));
        assert_eq!(rows[0].values[2], DbValue::Null);

        // Bob note is "First Note"
        assert_eq!(rows[1].values[0], DbValue::Int(2));
        assert_eq!(rows[1].values[1], DbValue::string("Bob"));
        assert_eq!(rows[1].values[2], DbValue::string("First Note"));

        // Charlie note defaulted to NULL
        assert_eq!(rows[2].values[0], DbValue::Int(3));
        assert_eq!(rows[2].values[1], DbValue::string("Charlie"));
        assert_eq!(rows[2].values[2], DbValue::Null);
    } else {
        panic!("Expected Select");
    }

    // 6. Test logic with NULL: NULL + 5, NULL AND true, NULL OR true, etc.
    let res2 = engine
        .execute(
            "SELECT note + 5, note AND 1, note OR 1 FROM nullable_test WHERE id = 1",
            &tx5,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res2 {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], DbValue::Null); // NULL + 5 = NULL
        assert_eq!(rows[0].values[1], DbValue::Null); // NULL AND 1 = NULL
        assert_eq!(rows[0].values[2], DbValue::Bool(true)); // NULL OR 1 = true
    } else {
        panic!("Expected Select");
    }

    // 7. Test COALESCE with NULLs
    let res3 = engine
        .execute(
            "SELECT id, COALESCE(note, 'default') FROM nullable_test ORDER BY id ASC",
            &tx5,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res3 {
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values[1], DbValue::string("default"));
        assert_eq!(rows[1].values[1], DbValue::string("First Note"));
        assert_eq!(rows[2].values[1], DbValue::string("default"));
    } else {
        panic!("Expected Select");
    }

    // 8. Test LEFT JOIN padding unmatched columns with NULL instead of 0.0/empty
    engine
        .execute(
            "CREATE TABLE orders (order_id INT PRIMARY KEY, user_id INT, amount FLOAT)",
            &tx5,
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO orders (order_id, user_id, amount) VALUES (100, 2, 9.99)",
            &tx5,
        )
        .unwrap();
    let res4 = engine.execute(
        "SELECT nullable_test.name, orders.amount FROM nullable_test LEFT JOIN orders ON nullable_test.id = orders.user_id ORDER BY nullable_test.id ASC",
        &tx5
    ).unwrap();
    if let ExecutionResult::Select { rows, .. } = res4 {
        assert_eq!(rows.len(), 3);
        // Alice (1): unmatched order -> amount is NULL
        assert_eq!(rows[0].values[0], DbValue::string("Alice"));
        assert_eq!(rows[0].values[1], DbValue::Null);
        // Bob (2): matched order -> amount is 9.99
        assert_eq!(rows[1].values[0], DbValue::string("Bob"));
        assert_eq!(rows[1].values[1], DbValue::Float(9.99));
        // Charlie (3): unmatched order -> amount is NULL
        assert_eq!(rows[2].values[0], DbValue::string("Charlie"));
        assert_eq!(rows[2].values[1], DbValue::Null);
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_locality_groups_end_to_end() {
    let (temp_dir, db, engine) = setup_engine();

    // 1. CREATE TABLE with WITH (locality_groups = '...')
    let tx1 = Transaction::new(1, db.clone());
    let res = engine
        .execute(
            "CREATE TABLE employees (id INT PRIMARY KEY, name STRING, salary INT, department STRING) WITH (locality_groups = 'lg_name:name; lg_finance:salary')",
            &tx1,
        )
        .unwrap();
    assert!(matches!(res, ExecutionResult::CreateTable));
    tx1.commit().unwrap();

    // Verify Schema Columns
    {
        let table = db.get_table("employees").unwrap();
        assert_eq!(table.schema.columns.len(), 4);

        let id_col = &table.schema.columns[0];
        assert_eq!(id_col.name, "id");
        assert_eq!(id_col.locality_group, None);

        let name_col = &table.schema.columns[1];
        assert_eq!(name_col.name, "name");
        assert_eq!(name_col.locality_group.as_deref(), Some("lg_name"));

        let salary_col = &table.schema.columns[2];
        assert_eq!(salary_col.name, "salary");
        assert_eq!(salary_col.locality_group.as_deref(), Some("lg_finance"));

        let dept_col = &table.schema.columns[3];
        assert_eq!(dept_col.name, "department");
        assert_eq!(dept_col.locality_group, None);
    }

    // 2. Insert values
    let tx2 = Transaction::new(2, db.clone());
    let res = engine
        .execute(
            "INSERT INTO employees (id, name, salary, department) VALUES (1, 'Alice', 100000, 'Engineering'), (2, 'Bob', 80000, 'HR')",
            &tx2,
        )
        .unwrap();
    assert_eq!(res, ExecutionResult::Insert { count: 2 });
    tx2.commit().unwrap();

    // 3. SELECT all columns
    let tx3 = Transaction::new(3, db.clone());
    let res = engine
        .execute(
            "SELECT id, name, salary, department FROM employees ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].values,
            vec![
                DbValue::Int(1),
                DbValue::string("Alice"),
                DbValue::Int(100000),
                DbValue::string("Engineering")
            ]
        );
        assert_eq!(
            rows[1].values,
            vec![
                DbValue::Int(2),
                DbValue::string("Bob"),
                DbValue::Int(80000),
                DbValue::string("HR")
            ]
        );
    } else {
        panic!("Expected Select");
    }

    // 4. Update a row
    let tx4 = Transaction::new(4, db.clone());
    let res = engine
        .execute("UPDATE employees SET salary = 110000 WHERE id = 1", &tx4)
        .unwrap();
    assert_eq!(res, ExecutionResult::Update { count: 1 });
    tx4.commit().unwrap();

    // 5. Select and verify update
    let tx5 = Transaction::new(5, db.clone());
    let res = engine
        .execute("SELECT name, salary FROM employees ORDER BY id ASC", &tx5)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].values,
            vec![DbValue::string("Alice"), DbValue::Int(110000)]
        );
        assert_eq!(
            rows[1].values,
            vec![DbValue::string("Bob"), DbValue::Int(80000)]
        );
    } else {
        panic!("Expected Select");
    }

    // 6. Verify directory structure to ensure locality groups were created as subdirectories
    let table_path = temp_dir.path().join("employees");

    // Default group directory should contain manifest and WAL
    let default_dir = table_path.join("default");
    assert!(default_dir.exists());
    assert!(default_dir.join("manifest").join("CURRENT").exists());

    // lg_name directory should contain manifest and WAL
    let lg_name_dir = table_path.join("lg_lg_name");
    assert!(lg_name_dir.exists());
    assert!(lg_name_dir.join("manifest").join("CURRENT").exists());

    // lg_finance directory should contain manifest and WAL
    let lg_finance_dir = table_path.join("lg_lg_finance");
    assert!(lg_finance_dir.exists());
    assert!(lg_finance_dir.join("manifest").join("CURRENT").exists());
}

#[test]
fn test_locality_groups_overrides_end_to_end() {
    let (temp_dir, db, engine) = setup_engine();

    // 1. CREATE TABLE with WITH (locality_groups = '... overrides ...')
    let tx1 = Transaction::new(1, db.clone());
    let res = engine
        .execute(
            "CREATE TABLE employees (id INT PRIMARY KEY, name STRING, salary INT, department STRING) WITH (locality_groups = 'lg_name:name:block_size_limit=8192,compression=uncompressed,block_cache_capacity=500; lg_finance:salary:wal_size_limit=1048576,max_level=5,block_cache_capacity=0')",
            &tx1,
        )
        .unwrap();
    assert!(matches!(res, ExecutionResult::CreateTable));
    tx1.commit().unwrap();

    // 2. Verify Schema columns and their options
    {
        let table = db.get_table("employees").unwrap();
        assert_eq!(table.schema.columns.len(), 4);

        let id_col = &table.schema.columns[0];
        assert_eq!(id_col.name, "id");
        assert_eq!(id_col.locality_group, None);

        let name_col = &table.schema.columns[1];
        assert_eq!(name_col.name, "name");
        assert_eq!(name_col.locality_group.as_deref(), Some("lg_name"));

        let salary_col = &table.schema.columns[2];
        assert_eq!(salary_col.name, "salary");
        assert_eq!(salary_col.locality_group.as_deref(), Some("lg_finance"));

        // Verify the locality group options parsed in Schema
        let lg_name_opts = table.schema.locality_group_options.get("lg_name").unwrap();
        assert_eq!(lg_name_opts.block_size_limit, Some(8192));
        assert_eq!(
            lg_name_opts.compression,
            Some(dtdb_storage::CompressionType::Uncompressed)
        );
        assert_eq!(lg_name_opts.block_cache_capacity, Some(500));

        let lg_finance_opts = table
            .schema
            .locality_group_options
            .get("lg_finance")
            .unwrap();
        assert_eq!(lg_finance_opts.wal_size_limit, Some(1048576));
        assert_eq!(lg_finance_opts.max_level, Some(5));
        assert_eq!(lg_finance_opts.block_cache_capacity, Some(0));

        // 3. Verify on-disk storage engine options.bin for each group
        let table_path = temp_dir.path().join("employees");

        // lg_name options.bin verification
        let lg_name_opts_path = table_path.join("lg_lg_name").join("options.bin");
        assert!(lg_name_opts_path.exists());
        let lg_name_bytes = std::fs::read(&lg_name_opts_path).unwrap();
        let lg_name_engine_opts: dtdb_storage::EngineOptions =
            postcard::from_bytes(&lg_name_bytes).unwrap();
        assert_eq!(lg_name_engine_opts.block_size_limit, 8192);
        assert_eq!(
            lg_name_engine_opts.compression,
            dtdb_storage::CompressionType::Uncompressed
        );
        assert_eq!(lg_name_engine_opts.block_cache_capacity, 500);

        // lg_finance options.bin verification
        let lg_finance_opts_path = table_path.join("lg_lg_finance").join("options.bin");
        assert!(lg_finance_opts_path.exists());
        let lg_finance_bytes = std::fs::read(&lg_finance_opts_path).unwrap();
        let lg_finance_engine_opts: dtdb_storage::EngineOptions =
            postcard::from_bytes(&lg_finance_bytes).unwrap();
        assert_eq!(lg_finance_engine_opts.wal_size_limit, 1048576);
        assert_eq!(lg_finance_engine_opts.max_level, 5);
        assert_eq!(lg_finance_engine_opts.block_cache_capacity, 0);
    }
}

#[test]
fn test_locality_groups_wal_sync_override() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    let engine = SqlEngine::new(db.clone());

    // 1. CREATE TABLE with WITH (locality_groups = '... overrides ...')
    let tx1 = Transaction::new(1, db.clone());
    let res = engine
        .execute(
            "CREATE TABLE test_sync (id INT PRIMARY KEY, name STRING, salary INT) WITH (locality_groups = 'lg_name:name:wal_sync_interval_ms=100; lg_finance:salary:wal_sync_interval_ms=off')",
            &tx1,
        )
        .unwrap();
    assert!(matches!(res, ExecutionResult::CreateTable));
    tx1.commit().unwrap();

    // 2. Verify Schema columns and their options
    let table = db.get_table("test_sync").unwrap();

    let lg_name_opts = table.schema.locality_group_options.get("lg_name").unwrap();
    assert_eq!(lg_name_opts.wal_sync_interval_ms, Some(Some(100)));

    let lg_finance_opts = table
        .schema
        .locality_group_options
        .get("lg_finance")
        .unwrap();
    assert_eq!(lg_finance_opts.wal_sync_interval_ms, Some(None));

    // 3. Verify on-disk storage engine options.bin for each group
    let table_path = temp_dir.path().join("test_sync");

    // lg_name options.bin verification (should be Some(100))
    let lg_name_opts_path = table_path.join("lg_lg_name").join("options.bin");
    assert!(lg_name_opts_path.exists());
    let lg_name_bytes = std::fs::read(&lg_name_opts_path).unwrap();
    let lg_name_engine_opts: dtdb_storage::EngineOptions =
        postcard::from_bytes(&lg_name_bytes).unwrap();
    assert_eq!(lg_name_engine_opts.wal_sync_interval_ms, Some(100));

    // lg_finance options.bin verification (should be None)
    let lg_finance_opts_path = table_path.join("lg_lg_finance").join("options.bin");
    assert!(lg_finance_opts_path.exists());
    let lg_finance_bytes = std::fs::read(&lg_finance_opts_path).unwrap();
    let lg_finance_engine_opts: dtdb_storage::EngineOptions =
        postcard::from_bytes(&lg_finance_bytes).unwrap();
    assert_eq!(lg_finance_engine_opts.wal_sync_interval_ms, None);
}

#[test]
fn test_sql_secondary_indexing() {
    let (temp_dir, db, engine) = setup_engine();

    // 1. Create table and insert initial data
    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE students (id INT PRIMARY KEY, name STRING, score INT)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();
    drop(tx1);

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO students (id, name, score) VALUES (1, 'Alice', 95), (2, 'Bob', 80), (3, 'Charlie', 85)",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();
    drop(tx2);

    // 2. Create index on score (rebuilds and populates from existing table rows)
    let tx3 = Transaction::new(3, db.clone());
    let res = engine
        .execute("CREATE INDEX idx_score ON students (score)", &tx3)
        .unwrap();
    assert!(matches!(res, ExecutionResult::CreateIndex));
    tx3.commit().unwrap();
    drop(tx3);

    // Verify index is registered in schema
    {
        let table = db.get_table("students").unwrap();
        assert_eq!(table.schema.indexes.len(), 1);
        assert_eq!(table.schema.indexes[0].name, "idx_score");
        assert_eq!(table.schema.indexes[0].columns, vec!["score".to_string()]);
        // Directory for index should exist
        let index_dir = Table::index_dir(&temp_dir.path().join("students"), "idx_score");
        assert!(index_dir.exists());
    }

    // 3. Verify EXPLAIN displays PhysicalIndexScan for query filtering on score
    let tx4 = Transaction::new(4, db.clone());
    let explain_res = engine
        .execute("EXPLAIN SELECT name FROM students WHERE score = 85", &tx4)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = explain_res {
        let plan_str = match &rows[0].values[0] {
            DbValue::String(s) => s,
            _ => panic!("Expected string plan representation"),
        };
        assert!(plan_str.contains(
            "- IndexScan: table=students, index=idx_score, range=[Value(Int(85)), Value(Int(85))]"
        ));
        assert!(plan_str.contains("- PhysicalIndexScan"));
    } else {
        panic!("Expected ExecutionResult::Select");
    }

    // 4. Verify index-based SELECT returns correct results
    let select_res = engine
        .execute("SELECT name FROM students WHERE score = 85", &tx4)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = select_res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values, vec![DbValue::string("Charlie")]);
    } else {
        panic!("Expected ExecutionResult::Select");
    }
    drop(tx4);

    // 5. Verify index maintenance on subsequent insertions, updates, and deletions
    let tx5 = Transaction::new(5, db.clone());
    // Insert new row
    engine
        .execute(
            "INSERT INTO students (id, name, score) VALUES (4, 'Dave', 90)",
            &tx5,
        )
        .unwrap();
    // Update Bob's score from 80 to 88
    engine
        .execute("UPDATE students SET score = 88 WHERE id = 2", &tx5)
        .unwrap();
    // Delete Alice (score 95)
    engine
        .execute("DELETE FROM students WHERE id = 1", &tx5)
        .unwrap();
    tx5.commit().unwrap();
    drop(tx5);

    // Verify querying Dave (score 90) uses index scan and is correct
    let tx6 = Transaction::new(6, db.clone());
    let res_dave = engine
        .execute("SELECT name FROM students WHERE score = 90", &tx6)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res_dave {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values, vec![DbValue::string("Dave")]);
    } else {
        panic!("Expected ExecutionResult::Select");
    }

    // Verify Bob's updated score (88) is queryable, and old score (80) returns empty
    let res_bob_new = engine
        .execute("SELECT name FROM students WHERE score = 88", &tx6)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res_bob_new {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values, vec![DbValue::string("Bob")]);
    } else {
        panic!("Expected ExecutionResult::Select");
    }
    let res_bob_old = engine
        .execute("SELECT name FROM students WHERE score = 80", &tx6)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res_bob_old {
        assert!(rows.is_empty());
    } else {
        panic!("Expected ExecutionResult::Select");
    }

    // Verify Alice (score 95) is no longer found
    let res_alice = engine
        .execute("SELECT name FROM students WHERE score = 95", &tx6)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res_alice {
        assert!(rows.is_empty());
    } else {
        panic!("Expected ExecutionResult::Select");
    }

    // 6. Verify index range query (e.g. score >= 85 AND score <= 90) uses IndexScan and returns correct rows
    let range_explain = engine
        .execute(
            "EXPLAIN SELECT name FROM students WHERE score >= 85 AND score <= 90",
            &tx6,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = range_explain {
        let plan_str = match &rows[0].values[0] {
            DbValue::String(s) => s,
            _ => panic!("Expected string plan representation"),
        };
        assert!(plan_str.contains(
            "- IndexScan: table=students, index=idx_score, range=[Value(Int(85)), Value(Int(90))]"
        ));
    } else {
        panic!("Expected ExecutionResult::Select");
    }

    let range_select = engine
        .execute(
            "SELECT name FROM students WHERE score >= 85 AND score <= 90 ORDER BY score ASC",
            &tx6,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = range_select {
        // Charlie (85) and Bob (88) and Dave (90)
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values, vec![DbValue::string("Charlie")]);
        assert_eq!(rows[1].values, vec![DbValue::string("Bob")]);
        assert_eq!(rows[2].values, vec![DbValue::string("Dave")]);
    } else {
        panic!("Expected ExecutionResult::Select");
    }
    drop(tx6);

    // 7. Verify transaction isolation / Read-Your-Own-Writes index scan behavior
    let tx7 = Transaction::new(7, db.clone());
    // Insert new student inside transaction (score 87)
    engine
        .execute(
            "INSERT INTO students (id, name, score) VALUES (5, 'Eve', 87)",
            &tx7,
        )
        .unwrap();
    // Delete Charlie inside transaction (score 85)
    engine
        .execute("DELETE FROM students WHERE id = 3", &tx7)
        .unwrap();

    // Query using index *inside* transaction tx7
    let tx7_select = engine
        .execute(
            "SELECT name FROM students WHERE score >= 85 AND score <= 90 ORDER BY score ASC",
            &tx7,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = tx7_select {
        // Eve (87) and Bob (88) and Dave (90). Charlie (85) must be deleted.
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values, vec![DbValue::string("Eve")]);
        assert_eq!(rows[1].values, vec![DbValue::string("Bob")]);
        assert_eq!(rows[2].values, vec![DbValue::string("Dave")]);
    } else {
        panic!("Expected ExecutionResult::Select");
    }
    tx7.rollback().unwrap();
    drop(tx7);

    // 8. Verify DROP INDEX
    let tx8 = Transaction::new(8, db.clone());
    let drop_res = engine.execute("DROP INDEX idx_score", &tx8).unwrap();
    assert!(matches!(drop_res, ExecutionResult::DropIndex));
    tx8.commit().unwrap();
    drop(tx8);

    // Verify index is removed from schema and disk directory deleted
    {
        let table = db.get_table("students").unwrap();
        assert_eq!(table.schema.indexes.len(), 0);
        let index_dir = Table::index_dir(&temp_dir.path().join("students"), "idx_score");
        assert!(!index_dir.exists());
    }

    // Verify EXPLAIN reverts back to PhysicalSeqScan
    let tx9 = Transaction::new(9, db.clone());
    let explain_after = engine
        .execute("EXPLAIN SELECT name FROM students WHERE score = 85", &tx9)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = explain_after {
        let plan_str = match &rows[0].values[0] {
            DbValue::String(s) => s,
            _ => panic!("Expected string plan representation"),
        };
        assert!(plan_str.contains("- Scan: table=students, range=all"));
        assert!(plan_str.contains("- PhysicalSeqScan"));
    } else {
        panic!("Expected ExecutionResult::Select");
    }
}

#[test]
fn test_sql_pass1_scalar_features() {
    let (_temp, db, engine) = setup_engine();

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE pass1_test (id INT PRIMARY KEY, name STRING, val INT, factor FLOAT, category STRING)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO pass1_test (id, name, val, factor, category) VALUES \
             (1, 'Alice', 10, 1.5, 'Electronics'), \
             (2, 'Bob', NULL, 2.5, NULL), \
             (3, 'Charlie', 30, -3.2, 'Furniture')",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();

    let tx3 = Transaction::new(3, db.clone());

    // 1. Unary NOT
    let res = engine
        .execute(
            "SELECT id FROM pass1_test WHERE NOT (id = 1) ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values[0], DbValue::Int(2));
        assert_eq!(rows[1].values[0], DbValue::Int(3));
    } else {
        panic!("Expected Select");
    }

    // 2. NOT LIKE
    let res = engine
        .execute(
            "SELECT id FROM pass1_test WHERE name NOT LIKE 'A%' ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values[0], DbValue::Int(2));
        assert_eq!(rows[1].values[0], DbValue::Int(3));
    } else {
        panic!("Expected Select");
    }

    // 3. IS NULL & IS NOT NULL
    let res = engine
        .execute("SELECT id FROM pass1_test WHERE val IS NULL", &tx3)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], DbValue::Int(2));
    } else {
        panic!("Expected Select");
    }

    let res = engine
        .execute(
            "SELECT id FROM pass1_test WHERE val IS NOT NULL ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
        assert_eq!(rows[1].values[0], DbValue::Int(3));
    } else {
        panic!("Expected Select");
    }

    // 4. BETWEEN
    let res = engine
        .execute(
            "SELECT id FROM pass1_test WHERE id BETWEEN 2 AND 3 ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values[0], DbValue::Int(2));
        assert_eq!(rows[1].values[0], DbValue::Int(3));
    } else {
        panic!("Expected Select");
    }

    // 5. IN list
    let res = engine
        .execute(
            "SELECT id FROM pass1_test WHERE name IN ('Alice', 'Charlie') ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
        assert_eq!(rows[1].values[0], DbValue::Int(3));
    } else {
        panic!("Expected Select");
    }

    // 6. Functions: UPPER, LOWER, CONCAT, ABS, ROUND
    let res = engine
        .execute("SELECT UPPER(name), LOWER(category), CONCAT(name, '-', category), ABS(factor), ROUND(factor) FROM pass1_test WHERE id = 3", &tx3)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], DbValue::string("CHARLIE"));
        assert_eq!(rows[0].values[1], DbValue::string("furniture"));
        assert_eq!(rows[0].values[2], DbValue::string("Charlie-Furniture"));
        assert_eq!(rows[0].values[3], DbValue::Float(3.2));
        assert_eq!(rows[0].values[4], DbValue::Float(-3.0));
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_sql_pass2_table_and_join_features() {
    let (_temp, db, engine) = setup_engine();

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE pass2_test (id INT PRIMARY KEY, cat STRING, price INT)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO pass2_test (id, cat, price) VALUES \
             (1, 'A', 10), \
             (2, 'A', 20), \
             (3, 'B', 30)",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();

    let tx3 = Transaction::new(3, db.clone());

    // 1. Table Aliasing and join qualification
    let res = engine
        .execute(
            "SELECT t1.cat, t2.price FROM pass2_test t1 JOIN pass2_test t2 ON t1.id = t2.id ORDER BY t1.id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values, vec![DbValue::string("A"), DbValue::Int(10)]);
        assert_eq!(rows[1].values, vec![DbValue::string("A"), DbValue::Int(20)]);
        assert_eq!(rows[2].values, vec![DbValue::string("B"), DbValue::Int(30)]);
    } else {
        panic!("Expected Select");
    }

    // 2. CROSS JOIN
    let res = engine
        .execute(
            "SELECT t1.id, t2.id FROM pass2_test t1 CROSS JOIN pass2_test t2 ORDER BY t1.id ASC, t2.id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 9); // 3 * 3 = 9
        assert_eq!(rows[0].values, vec![DbValue::Int(1), DbValue::Int(1)]);
        assert_eq!(rows[1].values, vec![DbValue::Int(1), DbValue::Int(2)]);
        assert_eq!(rows[2].values, vec![DbValue::Int(1), DbValue::Int(3)]);
        assert_eq!(rows[3].values, vec![DbValue::Int(2), DbValue::Int(1)]);
    } else {
        panic!("Expected Select");
    }

    // 3. HAVING clause with aggregates not in select and in select
    let res = engine
        .execute(
            "SELECT cat, SUM(price) FROM pass2_test GROUP BY cat HAVING COUNT(*) > 1",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        // SUM over an INT column now finalizes as Int (matching the schema
        // declared by the logical planner). It previously returned Float
        // unconditionally.
        assert_eq!(rows[0].values, vec![DbValue::string("A"), DbValue::Int(30)]);
    } else {
        panic!("Expected Select");
    }

    let res = engine
        .execute(
            "SELECT cat, COUNT(*) FROM pass2_test GROUP BY cat HAVING SUM(price) > 15 ORDER BY cat ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values, vec![DbValue::string("A"), DbValue::Int(2)]);
        assert_eq!(rows[1].values, vec![DbValue::string("B"), DbValue::Int(1)]);
    } else {
        panic!("Expected Select");
    }
}

#[test]
fn test_sql_pass3_schema_and_constraints() {
    let (_temp, db, engine) = setup_engine();

    // 1. Boolean data type and predicates
    {
        let tx1 = Transaction::new(1, db.clone());
        engine
            .execute(
                "CREATE TABLE bool_test (id INT PRIMARY KEY, active BOOLEAN)",
                &tx1,
            )
            .unwrap();
        tx1.commit().unwrap();

        let tx2 = Transaction::new(2, db.clone());
        engine
            .execute(
                "INSERT INTO bool_test (id, active) VALUES (1, true), (2, false), (3, NULL)",
                &tx2,
            )
            .unwrap();
        tx2.commit().unwrap();

        let tx3 = Transaction::new(3, db.clone());
        let res = engine
            .execute("SELECT id, active FROM bool_test ORDER BY id ASC", &tx3)
            .unwrap();
        if let ExecutionResult::Select { rows, .. } = res {
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0].values, vec![DbValue::Int(1), DbValue::Bool(true)]);
            assert_eq!(rows[1].values, vec![DbValue::Int(2), DbValue::Bool(false)]);
            assert_eq!(rows[2].values, vec![DbValue::Int(3), DbValue::Null]);
        } else {
            panic!("Expected Select");
        }

        // Test filtering on boolean predicate active = true
        let res = engine
            .execute("SELECT id FROM bool_test WHERE active = true", &tx3)
            .unwrap();
        if let ExecutionResult::Select { rows, .. } = res {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values, vec![DbValue::Int(1)]);
        } else {
            panic!("Expected Select");
        }

        // Test filtering on boolean predicate NOT active
        let res = engine
            .execute("SELECT id FROM bool_test WHERE NOT active", &tx3)
            .unwrap();
        if let ExecutionResult::Select { rows, .. } = res {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values, vec![DbValue::Int(2)]);
        } else {
            panic!("Expected Select");
        }
    }

    // 2. Composite Primary Keys
    {
        let tx4 = Transaction::new(4, db.clone());
        engine
            .execute(
                "CREATE TABLE composite_test (tenant_id INT, user_id INT, role STRING, PRIMARY KEY (tenant_id, user_id))",
                &tx4,
            )
            .unwrap();
        tx4.commit().unwrap();

        let tx5 = Transaction::new(5, db.clone());
        engine
            .execute(
                "INSERT INTO composite_test (tenant_id, user_id, role) VALUES (1, 10, 'admin'), (1, 20, 'user'), (2, 10, 'admin')",
                &tx5,
            )
            .unwrap();
        tx5.commit().unwrap();

        let tx6 = Transaction::new(6, db.clone());
        let res = engine
            .execute(
                "SELECT tenant_id, user_id, role FROM composite_test ORDER BY tenant_id ASC, user_id ASC",
                &tx6,
            )
            .unwrap();
        if let ExecutionResult::Select { rows, .. } = res {
            assert_eq!(rows.len(), 3);
            assert_eq!(
                rows[0].values,
                vec![DbValue::Int(1), DbValue::Int(10), DbValue::string("admin")]
            );
            assert_eq!(
                rows[1].values,
                vec![DbValue::Int(1), DbValue::Int(20), DbValue::string("user")]
            );
            assert_eq!(
                rows[2].values,
                vec![DbValue::Int(2), DbValue::Int(10), DbValue::string("admin")]
            );
        } else {
            panic!("Expected Select");
        }

        // Test duplicate primary key error
        let tx7 = Transaction::new(7, db.clone());
        let insert_fail = engine.execute(
            "INSERT INTO composite_test (tenant_id, user_id, role) VALUES (1, 10, 'super')",
            &tx7,
        );
        assert!(
            insert_fail.is_err(),
            "Expected composite key violation to error out"
        );
        let _ = tx7.rollback();
    }

    // 3. Column DEFAULT values
    {
        let tx8 = Transaction::new(8, db.clone());
        engine
            .execute(
                "CREATE TABLE default_test (id INT PRIMARY KEY, name STRING DEFAULT 'Unnamed', count INT DEFAULT 42)",
                &tx8,
            )
            .unwrap();
        tx8.commit().unwrap();

        let tx9 = Transaction::new(9, db.clone());
        engine
            .execute("INSERT INTO default_test (id) VALUES (1)", &tx9)
            .unwrap();
        engine
            .execute(
                "INSERT INTO default_test (id, name) VALUES (2, 'Bob')",
                &tx9,
            )
            .unwrap();
        tx9.commit().unwrap();

        let tx10 = Transaction::new(10, db.clone());
        let res = engine
            .execute(
                "SELECT id, name, count FROM default_test ORDER BY id ASC",
                &tx10,
            )
            .unwrap();
        if let ExecutionResult::Select { rows, .. } = res {
            assert_eq!(rows.len(), 2);
            assert_eq!(
                rows[0].values,
                vec![
                    DbValue::Int(1),
                    DbValue::string("Unnamed"),
                    DbValue::Int(42)
                ]
            );
            assert_eq!(
                rows[1].values,
                vec![DbValue::Int(2), DbValue::string("Bob"), DbValue::Int(42)]
            );
        } else {
            panic!("Expected Select");
        }
    }

    // 4. Auto-increment / Identity columns
    {
        let tx11 = Transaction::new(11, db.clone());
        engine
            .execute(
                "CREATE TABLE auto_test (id SERIAL PRIMARY KEY, note STRING)",
                &tx11,
            )
            .unwrap();
        tx11.commit().unwrap();

        let tx12 = Transaction::new(12, db.clone());
        engine
            .execute(
                "INSERT INTO auto_test (note) VALUES ('First'), ('Second')",
                &tx12,
            )
            .unwrap();
        tx12.commit().unwrap();

        let tx13 = Transaction::new(13, db.clone());
        let res = engine
            .execute("SELECT id, note FROM auto_test ORDER BY id ASC", &tx13)
            .unwrap();
        if let ExecutionResult::Select { rows, .. } = res {
            assert_eq!(rows.len(), 2);
            assert_eq!(
                rows[0].values,
                vec![DbValue::Int(1), DbValue::string("First")]
            );
            assert_eq!(
                rows[1].values,
                vec![DbValue::Int(2), DbValue::string("Second")]
            );
        } else {
            panic!("Expected Select");
        }

        // Test inserting explicit ID (e.g. 10) manually resets sequence or works
        let tx14 = Transaction::new(14, db.clone());
        engine
            .execute("INSERT INTO auto_test (id, note) VALUES (10, 'Ten')", &tx14)
            .unwrap();
        // Insert without ID should increment from the max value now
        engine
            .execute("INSERT INTO auto_test (note) VALUES ('Eleven')", &tx14)
            .unwrap();
        tx14.commit().unwrap();

        let tx15 = Transaction::new(15, db.clone());
        let res = engine
            .execute("SELECT id, note FROM auto_test ORDER BY id ASC", &tx15)
            .unwrap();
        if let ExecutionResult::Select { rows, .. } = res {
            assert_eq!(rows.len(), 4);
            assert_eq!(
                rows[2].values,
                vec![DbValue::Int(10), DbValue::string("Ten")]
            );
            assert_eq!(
                rows[3].values,
                vec![DbValue::Int(11), DbValue::string("Eleven")]
            );
        } else {
            panic!("Expected Select");
        }
    }
}

#[test]
fn test_sql_pass4_dml_and_set_operations() {
    let (_temp, db, engine) = setup_engine();

    // 1. INSERT INTO SELECT
    {
        let tx1 = Transaction::new(1, db.clone());
        engine
            .execute(
                "CREATE TABLE src_table (id SERIAL PRIMARY KEY, note STRING)",
                &tx1,
            )
            .unwrap();
        engine
            .execute(
                "CREATE TABLE dest_table (id SERIAL PRIMARY KEY, note STRING, extra STRING DEFAULT 'Cool')",
                &tx1,
            )
            .unwrap();
        tx1.commit().unwrap();

        let tx2 = Transaction::new(2, db.clone());
        engine
            .execute(
                "INSERT INTO src_table (note) VALUES ('Apple'), ('Banana'), ('Cherry')",
                &tx2,
            )
            .unwrap();
        tx2.commit().unwrap();

        // Perform INSERT INTO SELECT with explicit columns mapping
        let tx3 = Transaction::new(3, db.clone());
        let res = engine
            .execute(
                "INSERT INTO dest_table (id, note) SELECT id, note FROM src_table",
                &tx3,
            )
            .unwrap();
        assert_eq!(res, ExecutionResult::Insert { count: 3 });
        tx3.commit().unwrap();

        // Verify content in dest_table
        let tx4 = Transaction::new(4, db.clone());
        let res = engine
            .execute(
                "SELECT id, note, extra FROM dest_table ORDER BY id ASC",
                &tx4,
            )
            .unwrap();
        if let ExecutionResult::Select { rows, .. } = res {
            assert_eq!(rows.len(), 3);
            assert_eq!(
                rows[0].values,
                vec![
                    DbValue::Int(1),
                    DbValue::string("Apple"),
                    DbValue::string("Cool")
                ]
            );
            assert_eq!(
                rows[1].values,
                vec![
                    DbValue::Int(2),
                    DbValue::string("Banana"),
                    DbValue::string("Cool")
                ]
            );
            assert_eq!(
                rows[2].values,
                vec![
                    DbValue::Int(3),
                    DbValue::string("Cherry"),
                    DbValue::string("Cool")
                ]
            );
        } else {
            panic!("Expected Select");
        }

        // Test duplicate PK error on INSERT INTO SELECT
        let tx5 = Transaction::new(5, db.clone());
        let err_res = engine.execute(
            "INSERT INTO dest_table (id, note) SELECT id, note FROM src_table",
            &tx5,
        );
        assert!(err_res.is_err());
        assert_eq!(err_res.unwrap_err(), "Duplicate primary key".to_string());
        let _ = tx5.rollback();

        // Test INSERT INTO SELECT auto-increment logic
        let tx6 = Transaction::new(6, db.clone());
        // Since we insert note only, id should generate from sequence starting from next of max(3) = 4
        let res = engine
            .execute(
                "INSERT INTO dest_table (note) SELECT note FROM src_table",
                &tx6,
            )
            .unwrap();
        assert_eq!(res, ExecutionResult::Insert { count: 3 });
        tx6.commit().unwrap();

        let tx7 = Transaction::new(7, db.clone());
        let res = engine
            .execute(
                "SELECT id, note, extra FROM dest_table ORDER BY id ASC",
                &tx7,
            )
            .unwrap();
        if let ExecutionResult::Select { rows, .. } = res {
            assert_eq!(rows.len(), 6);
            assert_eq!(rows[3].values[0], DbValue::Int(4));
            assert_eq!(rows[4].values[0], DbValue::Int(5));
            assert_eq!(rows[5].values[0], DbValue::Int(6));
        } else {
            panic!("Expected Select");
        }
    }

    // 2. Set Operations (UNION, UNION ALL, INTERSECT, EXCEPT)
    {
        let tx8 = Transaction::new(8, db.clone());
        engine
            .execute(
                "CREATE TABLE set_a (row_id SERIAL PRIMARY KEY, id INT, val STRING)",
                &tx8,
            )
            .unwrap();
        engine
            .execute(
                "CREATE TABLE set_b (row_id SERIAL PRIMARY KEY, id INT, val STRING)",
                &tx8,
            )
            .unwrap();
        tx8.commit().unwrap();

        let tx9 = Transaction::new(9, db.clone());
        engine
            .execute(
                "INSERT INTO set_a (id, val) VALUES (1, 'apple'), (2, 'banana'), (2, 'banana'), (3, 'cherry')",
                &tx9,
            )
            .unwrap();
        engine
            .execute(
                "INSERT INTO set_b (id, val) VALUES (2, 'banana'), (3, 'cherry'), (3, 'cherry'), (4, 'date')",
                &tx9,
            )
            .unwrap();
        tx9.commit().unwrap();

        let tx10 = Transaction::new(10, db.clone());

        // UNION DISTINCT (default UNION)
        let res = engine
            .execute(
                "SELECT id, val FROM set_a UNION SELECT id, val FROM set_b ORDER BY id ASC, val ASC",
                &tx10,
            )
            .unwrap();
        if let ExecutionResult::Select { rows, .. } = res {
            assert_eq!(rows.len(), 4);
            assert_eq!(
                rows[0].values,
                vec![DbValue::Int(1), DbValue::string("apple")]
            );
            assert_eq!(
                rows[1].values,
                vec![DbValue::Int(2), DbValue::string("banana")]
            );
            assert_eq!(
                rows[2].values,
                vec![DbValue::Int(3), DbValue::string("cherry")]
            );
            assert_eq!(
                rows[3].values,
                vec![DbValue::Int(4), DbValue::string("date")]
            );
        } else {
            panic!("Expected Select");
        }

        // UNION ALL
        let res = engine
            .execute(
                "SELECT id, val FROM set_a UNION ALL SELECT id, val FROM set_b ORDER BY id ASC, val ASC",
                &tx10,
            )
            .unwrap();
        if let ExecutionResult::Select { rows, .. } = res {
            assert_eq!(rows.len(), 8);
            let expected = vec![
                vec![DbValue::Int(1), DbValue::string("apple")],
                vec![DbValue::Int(2), DbValue::string("banana")],
                vec![DbValue::Int(2), DbValue::string("banana")],
                vec![DbValue::Int(2), DbValue::string("banana")],
                vec![DbValue::Int(3), DbValue::string("cherry")],
                vec![DbValue::Int(3), DbValue::string("cherry")],
                vec![DbValue::Int(3), DbValue::string("cherry")],
                vec![DbValue::Int(4), DbValue::string("date")],
            ];
            for (i, val) in expected.into_iter().enumerate() {
                assert_eq!(rows[i].values, val);
            }
        } else {
            panic!("Expected Select");
        }

        // EXCEPT DISTINCT (default EXCEPT)
        let res = engine
            .execute(
                "SELECT id, val FROM set_a EXCEPT SELECT id, val FROM set_b ORDER BY id ASC, val ASC",
                &tx10,
            )
            .unwrap();
        if let ExecutionResult::Select { rows, .. } = res {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].values,
                vec![DbValue::Int(1), DbValue::string("apple")]
            );
        } else {
            panic!("Expected Select");
        }

        // EXCEPT ALL
        let res = engine
            .execute(
                "SELECT id, val FROM set_a EXCEPT ALL SELECT id, val FROM set_b ORDER BY id ASC, val ASC",
                &tx10,
            )
            .unwrap();
        if let ExecutionResult::Select { rows, .. } = res {
            // set_a has two (2, 'banana') and set_b has one. So one (2, 'banana') should remain.
            // set_a has one (1, 'apple') and set_b has none. So one (1, 'apple') should remain.
            assert_eq!(rows.len(), 2);
            assert_eq!(
                rows[0].values,
                vec![DbValue::Int(1), DbValue::string("apple")]
            );
            assert_eq!(
                rows[1].values,
                vec![DbValue::Int(2), DbValue::string("banana")]
            );
        } else {
            panic!("Expected Select");
        }

        // INTERSECT DISTINCT (default INTERSECT)
        let res = engine
            .execute(
                "SELECT id, val FROM set_a INTERSECT SELECT id, val FROM set_b ORDER BY id ASC, val ASC",
                &tx10,
            )
            .unwrap();
        if let ExecutionResult::Select { rows, .. } = res {
            assert_eq!(rows.len(), 2);
            assert_eq!(
                rows[0].values,
                vec![DbValue::Int(2), DbValue::string("banana")]
            );
            assert_eq!(
                rows[1].values,
                vec![DbValue::Int(3), DbValue::string("cherry")]
            );
        } else {
            panic!("Expected Select");
        }

        // INTERSECT ALL
        let res = engine
            .execute(
                "SELECT id, val FROM set_a INTERSECT ALL SELECT id, val FROM set_b ORDER BY id ASC, val ASC",
                &tx10,
            )
            .unwrap();
        if let ExecutionResult::Select { rows, .. } = res {
            // (2, banana): left count 2, right count 1. Intersect ALL -> min(2, 1) = 1 row.
            // (3, cherry): left count 1, right count 2. Intersect ALL -> min(1, 2) = 1 row.
            assert_eq!(rows.len(), 2);
            assert_eq!(
                rows[0].values,
                vec![DbValue::Int(2), DbValue::string("banana")]
            );
            assert_eq!(
                rows[1].values,
                vec![DbValue::Int(3), DbValue::string("cherry")]
            );
        } else {
            panic!("Expected Select");
        }

        // Column count mismatch test
        let err_res = engine.execute(
            "SELECT id FROM set_a UNION SELECT id, val FROM set_b",
            &tx10,
        );
        assert!(err_res.is_err());
        assert!(
            err_res
                .unwrap_err()
                .contains("must have the same number of columns")
        );

        // Data type mismatch test
        let err_res2 = engine.execute("SELECT id FROM set_a UNION SELECT val FROM set_b", &tx10);
        assert!(err_res2.is_err());
        assert!(err_res2.unwrap_err().contains("type mismatch"));
    }
}

#[test]
fn test_sql_analyze() {
    let (_temp, db, engine) = setup_engine();

    // 1. Create table and index
    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE items (id INT PRIMARY KEY, name STRING, active BOOL)",
            &tx1,
        )
        .unwrap();
    engine
        .execute("CREATE INDEX idx_name ON items (name)", &tx1)
        .unwrap();
    tx1.commit().unwrap();

    // 2. Insert data
    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO items (id, name, active) VALUES (1, 'apple', true), (2, 'banana', false), (3, 'apple', true)",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();

    // Flush tables to generate SSTables so we actually get non-zero sizes / SSTable count
    let table = db.get_table("items").unwrap();
    table.flush_memtable().unwrap();

    // 3. Execute ANALYZE via SQL
    let tx3 = Transaction::new(3, db.clone());
    let res = engine.execute("ANALYZE TABLE items", &tx3).unwrap();
    assert_eq!(res, ExecutionResult::Analyze);
    tx3.commit().unwrap();

    // 4. Verify statistics
    let stats = db.get_table_statistics("items").unwrap();
    assert_eq!(stats.row_count, 3);
    assert_eq!(stats.table_name, "items");

    // Default locality group stats
    let lg_stats = stats.locality_group_stats.get("").unwrap();
    assert!(lg_stats.entry_count > 0);
    assert!(lg_stats.total_sstable_size > 0);
    assert_eq!(lg_stats.num_sstables, 1);

    // Index stats
    let idx_stats = stats.index_stats.get("idx_name").unwrap();
    assert_eq!(idx_stats.entry_count, 3);
    assert_eq!(idx_stats.unique_values, 2); // 'apple' and 'banana'
    assert_eq!(idx_stats.avg_rows_per_value, 1.5);
}

fn setup_engine_with_options(
    options: dtdb_relational::DatabaseOptions,
) -> (TempDir, Arc<Database>, SqlEngine) {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open_with_options(temp_dir.path(), options).unwrap());
    let engine = SqlEngine::new(db.clone());
    (temp_dir, db, engine)
}

/// Default database options with a configurable per-operator memory budget. A tiny
/// budget (a few hundred bytes) forces the spilling operators down their disk path.
fn opts_with_budget(memory_budget: Option<usize>) -> dtdb_relational::DatabaseOptions {
    dtdb_relational::DatabaseOptions {
        compression: dtdb_storage::CompressionType::Lz4,
        memtable_size_limit: 1024 * 1024,
        block_size_limit: 4096,
        wal_size_limit: 32 * 1024 * 1024,
        flush_interval_ms: None,
        l0_compaction_threshold: None,
        sstable_target_size: None,
        base_level_size_limit: None,
        level_size_multiplier: None,
        max_level: None,
        block_cache_capacity: Some(1000),
        analyze_frequency_ms: None,
        wal_sync_interval_ms: None,
        memory_budget,
        fsync_method: dtdb_storage::FsyncMethod::default(),
    }
}

/// Asserts the `_tmp/` spill directory is empty (all `Run` temp files were cleaned up
/// on Drop after the query finished).
fn assert_tmp_empty(db: &Arc<Database>) {
    let tmp_path = db.dir_path().join("_tmp");
    if tmp_path.exists() {
        let entries: Vec<_> = std::fs::read_dir(&tmp_path).unwrap().collect();
        assert!(
            entries.is_empty(),
            "Temp directory should be empty but has entries: {:?}",
            entries
        );
    }
}

#[test]
fn test_cbo_index_selection_by_cardinality() {
    // Construct database options with block_cache_capacity = Some(0) to disable caching
    // and isolate disk block read diagnostics
    let options = dtdb_relational::DatabaseOptions {
        compression: dtdb_storage::CompressionType::Lz4,
        memtable_size_limit: 1024 * 1024,
        // Small blocks so the 500-row table spans many data blocks regardless of
        // how compact the serialization is: a full scan then reads many blocks
        // while the selective index scan (5 matching rows) reads only a few. With
        // larger blocks postcard's compact rows would fit the whole table in ~3
        // blocks, erasing the index advantage this test checks for.
        block_size_limit: 256,
        wal_size_limit: 32 * 1024 * 1024,
        flush_interval_ms: None,
        l0_compaction_threshold: None,
        sstable_target_size: None,
        base_level_size_limit: None,
        level_size_multiplier: None,
        max_level: None,
        block_cache_capacity: Some(0),
        analyze_frequency_ms: None,
        wal_sync_interval_ms: None,
        memory_budget: None,
        fsync_method: dtdb_storage::FsyncMethod::default(),
    };

    let (_temp, db, engine) = setup_engine_with_options(options);

    // 1. Create table and two indexes
    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE items (id INT PRIMARY KEY, x INT, y INT, z INT)",
            &tx1,
        )
        .unwrap();
    engine
        .execute("CREATE INDEX idx_x ON items (x)", &tx1)
        .unwrap();
    engine
        .execute("CREATE INDEX idx_y ON items (y)", &tx1)
        .unwrap();
    tx1.commit().unwrap();

    // 2. Insert data: x is highly selective (only 5 rows have x=1),
    // y is poorly selective (all 500 rows have y=2).
    let tx2 = Transaction::new(2, db.clone());
    for i in 1..=500 {
        let x_val = if i <= 5 { 1 } else { i };
        let query = format!(
            "INSERT INTO items (id, x, y, z) VALUES ({}, {}, 2, {})",
            i, x_val, i
        );
        engine.execute(&query, &tx2).unwrap();
    }
    tx2.commit().unwrap();

    // Flush tables and indexes to generate SSTables
    let table = db.get_table("items").unwrap();
    table.flush_memtable().unwrap();

    // Execute ANALYZE to collect statistics
    let tx3 = Transaction::new(3, db.clone());
    engine.execute("ANALYZE TABLE items", &tx3).unwrap();
    tx3.commit().unwrap();

    // 3. Verify via EXPLAIN that the physical plan selects idx_x for the query:
    // SELECT id FROM items WHERE x = 1 AND y = 2
    let tx4 = Transaction::new(4, db.clone());
    let explain_res = engine
        .execute("EXPLAIN SELECT id FROM items WHERE x = 1 AND y = 2", &tx4)
        .unwrap();

    let plan_str = if let ExecutionResult::Select { rows, .. } = explain_res {
        match &rows[0].values[0] {
            DbValue::String(s) => s.clone(),
            _ => panic!("Expected string plan representation"),
        }
    } else {
        panic!("Expected ExecutionResult::Select");
    };

    assert!(plan_str.contains("- IndexScan: table=items, index=idx_x"));
    assert!(!plan_str.contains("- IndexScan: table=items, index=idx_y"));

    // 4. Verify blocks read is significantly fewer using CBO (with idx_x) than a query doing full scan
    // First, let's query WHERE x = 1 AND y = 2. It will use idx_x.
    dtdb_storage::reset_physical_blocks_read();
    let res_cbo = engine
        .execute("SELECT id FROM items WHERE x = 1 AND y = 2", &tx4)
        .unwrap();
    let blocks_cbo = dtdb_storage::get_physical_blocks_read();

    if let ExecutionResult::Select { rows, .. } = res_cbo {
        assert_eq!(rows.len(), 5);
    } else {
        panic!("Expected Select result");
    }

    // Now, run a query that forces a full scan (filtering on z, which is not indexed)
    dtdb_storage::reset_physical_blocks_read();
    let res_seq = engine
        .execute("SELECT id FROM items WHERE z = 10", &tx4)
        .unwrap();
    let blocks_seq = dtdb_storage::get_physical_blocks_read();

    if let ExecutionResult::Select { rows, .. } = res_seq {
        assert_eq!(rows.len(), 1);
    } else {
        panic!("Expected Select result");
    }

    println!(
        "Blocks read: CBO = {}, Full Seq Scan = {}",
        blocks_cbo, blocks_seq
    );
    // CBO index scan on idx_x (fetching 5 rows) should read fewer blocks than scanning the whole 500-row table
    assert!(blocks_cbo < blocks_seq);
    assert!(blocks_cbo > 0);
}

#[test]
fn test_cbo_locality_group_pruning_performance() {
    // Construct database options with block_cache_capacity = Some(0) to disable caching
    let options = dtdb_relational::DatabaseOptions {
        compression: dtdb_storage::CompressionType::Lz4,
        memtable_size_limit: 1024 * 1024,
        block_size_limit: 4096,
        wal_size_limit: 32 * 1024 * 1024,
        flush_interval_ms: None,
        l0_compaction_threshold: None,
        sstable_target_size: None,
        base_level_size_limit: None,
        level_size_multiplier: None,
        max_level: None,
        block_cache_capacity: Some(0),
        analyze_frequency_ms: None,
        wal_sync_interval_ms: None,
        memory_budget: None,
        fsync_method: dtdb_storage::FsyncMethod::default(),
    };

    let (_temp, db, engine) = setup_engine_with_options(options);

    // 1. Create table with locality groups
    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE employees (id INT PRIMARY KEY, name STRING, salary INT, bio STRING) WITH (locality_groups = 'lg_finance:salary; lg_bio:bio')",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();

    // 2. Insert data
    let tx2 = Transaction::new(2, db.clone());
    for i in 1..=100 {
        let query = format!(
            "INSERT INTO employees (id, name, salary, bio) VALUES ({}, 'name_{}', {}, 'biography_{}')",
            i,
            i,
            i * 1000,
            i
        );
        engine.execute(&query, &tx2).unwrap();
    }
    tx2.commit().unwrap();

    // Flush tables to generate SSTables for each locality group
    let table = db.get_table("employees").unwrap();
    table.flush_memtable().unwrap();

    // Execute ANALYZE to collect statistics
    let tx3 = Transaction::new(3, db.clone());
    engine.execute("ANALYZE TABLE employees", &tx3).unwrap();
    tx3.commit().unwrap();

    // 3. Query only columns in default locality group ("")
    let tx4 = Transaction::new(4, db.clone());
    dtdb_storage::reset_physical_blocks_read();
    let res_pruned = engine
        .execute("SELECT id, name FROM employees WHERE id > 0", &tx4)
        .unwrap();
    let blocks_pruned = dtdb_storage::get_physical_blocks_read();

    if let ExecutionResult::Select { rows, .. } = res_pruned {
        assert_eq!(rows.len(), 100);
    } else {
        panic!("Expected Select result");
    }

    // 4. Query columns in default group ("") + lg_bio
    dtdb_storage::reset_physical_blocks_read();
    let res_full = engine
        .execute("SELECT id, name, bio FROM employees WHERE id > 0", &tx4)
        .unwrap();
    let blocks_full = dtdb_storage::get_physical_blocks_read();

    if let ExecutionResult::Select { rows, .. } = res_full {
        assert_eq!(rows.len(), 100);
    } else {
        panic!("Expected Select result");
    }

    println!(
        "Blocks read: Pruned = {}, Full = {}",
        blocks_pruned, blocks_full
    );
    // The pruned query (only scanning default group) should read fewer blocks than scanning default + lg_bio groups
    assert!(blocks_pruned < blocks_full);
    assert!(blocks_pruned > 0);
}

#[test]
fn test_sql_optimizer_predicate_pushdown_through_joins() {
    let (_temp, db, engine) = setup_engine();

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE users (id INT PRIMARY KEY, name STRING)", &tx1)
        .unwrap();
    engine
        .execute(
            "CREATE TABLE orders (order_id INT PRIMARY KEY, user_id INT, amount FLOAT)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
            &tx2,
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO orders (order_id, user_id, amount) VALUES (10, 1, 100.5), (20, 2, 250.0)",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();

    let tx3 = Transaction::new(3, db.clone());
    engine.execute("ANALYZE TABLE users", &tx3).unwrap();
    engine.execute("ANALYZE TABLE orders", &tx3).unwrap();
    tx3.commit().unwrap();

    let tx4 = Transaction::new(4, db.clone());

    // 1. Verify EXPLAIN shows predicate pushed down past the Join
    let explain_res = engine
        .execute(
            "EXPLAIN SELECT users.name, orders.amount \
             FROM users JOIN orders ON users.id = orders.user_id \
             WHERE users.id = 2 AND orders.amount > 100.0",
            &tx4,
        )
        .unwrap();

    if let ExecutionResult::Select { rows, .. } = explain_res {
        let plan_text = match &rows[0].values[0] {
            DbValue::String(s) => s.clone(),
            _ => panic!("Expected string plan text"),
        };
        println!("PLAN TEXT IS:\n{}", plan_text);
        // The filter for users.id = 2 should be pushed below the join, selecting a range on table=users Scan.
        // It should scan users with range=[Int(2), Int(2)].
        assert!(
            plan_text.contains("Scan: table=users, range=[Value(Int(2)), Value(Int(2))]")
                || plan_text.contains("Scan: table=users, range=[Int(2), Int(2)]")
                || plan_text.contains("Scan: table=users, range=[2, 2]")
        );
    } else {
        panic!("Expected EXPLAIN Select output");
    }

    // 2. Verify we get correct results
    let res = engine
        .execute(
            "SELECT users.name, orders.amount \
             FROM users JOIN orders ON users.id = orders.user_id \
             WHERE users.id = 2 AND orders.amount > 100.0",
            &tx4,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].values,
            vec![DbValue::string("Bob"), DbValue::Float(250.0)]
        );
    } else {
        panic!("Expected Select result");
    }
}

#[test]
fn test_sql_optimizer_join_order_reordering() {
    let (_temp, db, engine) = setup_engine();

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t1 (id INT PRIMARY KEY, name STRING)", &tx1)
        .unwrap();
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, name STRING)", &tx1)
        .unwrap();
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    // Insert 1 row into t1
    engine
        .execute("INSERT INTO t1 (id, name) VALUES (1, 'one')", &tx2)
        .unwrap();
    // Insert 3 rows into t2
    engine
        .execute(
            "INSERT INTO t2 (id, name) VALUES (1, 'one'), (2, 'two'), (3, 'three')",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();

    // Run ANALYZE to collect row count statistics
    let tx_analyze = Transaction::new(3, db.clone());
    engine.execute("ANALYZE TABLE t1", &tx_analyze).unwrap();
    engine.execute("ANALYZE TABLE t2", &tx_analyze).unwrap();
    tx_analyze.commit().unwrap();

    let tx4 = Transaction::new(4, db.clone());

    let stats1 = db.get_table_statistics("t1");
    let stats2 = db.get_table_statistics("t2");
    println!("STATS FOR T1: {:?}", stats1);
    println!("STATS FOR T2: {:?}", stats2);

    // t1 JOIN t2: left is t1 (1 row), right is t2 (3 rows).
    // Left size (1) < Right size (3).
    // Since left is smaller, they should swap so t2 (larger) is left, t1 (smaller) is right (build side).
    let explain_res = engine
        .execute(
            "EXPLAIN SELECT t1.name, t2.name FROM t1 JOIN t2 ON t1.id = t2.id",
            &tx4,
        )
        .unwrap();

    if let ExecutionResult::Select { rows, .. } = explain_res {
        let plan_text = match &rows[0].values[0] {
            DbValue::String(s) => s.clone(),
            _ => panic!("Expected string plan text"),
        };
        println!("PLAN TEXT IS:\n{}", plan_text);
        // Verify that the Physical Plan swaps the children so that t2 is left and t1 is right.
        // The plan text output for PhysicalSortMergeJoin has left: then right:
        let optimized_part = plan_text.split("--- Physical Plan ---").nth(1).unwrap();
        let left_idx = optimized_part.find("left:").unwrap();
        let right_idx = optimized_part.find("right:").unwrap();
        let left_part = &optimized_part[left_idx..right_idx];
        let right_part = &optimized_part[right_idx..];

        assert!(left_part.contains("t2.id"));
        assert!(right_part.contains("t1.id"));
    } else {
        panic!("Expected EXPLAIN Select output");
    }

    // Verify correct results
    let res = engine
        .execute(
            "SELECT t1.name, t2.name FROM t1 JOIN t2 ON t1.id = t2.id",
            &tx4,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].values,
            vec![DbValue::string("one"), DbValue::string("one")]
        );
    } else {
        panic!("Expected Select result");
    }
}

#[test]
fn test_sql_cross_join_lazy_and_correctness() {
    let (_temp, db, engine) = setup_engine();

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t1 (id INT PRIMARY KEY, val STRING)", &tx1)
        .unwrap();
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, val STRING)", &tx1)
        .unwrap();
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute("INSERT INTO t1 (id, val) VALUES (1, 'a'), (2, 'b')", &tx2)
        .unwrap();
    engine
        .execute("INSERT INTO t2 (id, val) VALUES (10, 'x'), (20, 'y')", &tx2)
        .unwrap();
    tx2.commit().unwrap();

    let tx3 = Transaction::new(3, db.clone());
    let res = engine
        .execute(
            "SELECT t1.val, t2.val FROM t1 CROSS JOIN t2 ORDER BY t1.val ASC, t2.val ASC",
            &tx3,
        )
        .unwrap();

    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 4);
        assert_eq!(
            rows[0].values,
            vec![DbValue::string("a"), DbValue::string("x")]
        );
        assert_eq!(
            rows[1].values,
            vec![DbValue::string("a"), DbValue::string("y")]
        );
        assert_eq!(
            rows[2].values,
            vec![DbValue::string("b"), DbValue::string("x")]
        );
        assert_eq!(
            rows[3].values,
            vec![DbValue::string("b"), DbValue::string("y")]
        );
    } else {
        panic!("Expected Select result");
    }
}

#[test]
fn test_fulltext_search() {
    let (_temp, db, engine) = setup_engine();

    // 1. Setup table and insert rows
    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE posts (id INT PRIMARY KEY, content STRING)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();
    drop(tx1);

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO posts (id, content) VALUES \
             (1, 'Database engines are complex systems'), \
             (2, 'LSM-tree is a data structure for write-heavy workloads'), \
             (3, 'B-trees are widely used in databases'), \
             (4, 'Simple tokenization splits by whitespace')",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();
    drop(tx2);

    // 2. Query WITHOUT FULLTEXT index (Sequential scan fallback)
    let tx3 = Transaction::new(3, db.clone());
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('databases') ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], DbValue::Int(3));
    } else {
        panic!("Expected Select result");
    }

    // Check case insensitivity / tokenizer splits
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('complex') ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
    } else {
        panic!("Expected Select result");
    }
    drop(tx3);

    // 3. Create FULLTEXT index (default simple tokenizer)
    let tx4 = Transaction::new(4, db.clone());
    engine
        .execute("CREATE FULLTEXT INDEX idx_content ON posts (content)", &tx4)
        .unwrap();
    tx4.commit().unwrap();
    drop(tx4);

    // 4. Query WITH FULLTEXT index (Index scan)
    let tx5 = Transaction::new(5, db.clone());
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('complex') ORDER BY id ASC",
            &tx5,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
    } else {
        panic!("Expected Select result");
    }

    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('databases') ORDER BY id ASC",
            &tx5,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], DbValue::Int(3));
    } else {
        panic!("Expected Select result");
    }

    // Query non-existent token
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('nonexistent')",
            &tx5,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 0);
    } else {
        panic!("Expected Select result");
    }
    drop(tx5);

    // 5. Custom tokenizer registration and USING syntax
    // Define a custom tokenizer that splits by comma
    struct CommaTokenizer;
    impl dtdb_relational::Tokenizer for CommaTokenizer {
        fn tokenize(&self, text: &str) -> Vec<String> {
            text.split(',')
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty())
                .collect()
        }
    }

    dtdb_relational::register_global_tokenizer("comma", std::sync::Arc::new(CommaTokenizer));

    let tx6 = Transaction::new(6, db.clone());
    engine
        .execute("CREATE TABLE tags (id INT PRIMARY KEY, list STRING)", &tx6)
        .unwrap();
    tx6.commit().unwrap();
    drop(tx6);

    let tx7 = Transaction::new(7, db.clone());
    engine
        .execute(
            "INSERT INTO tags (id, list) VALUES \
             (1, 'rust,database,lsm'), \
             (2, 'c++,btree,relational'), \
             (3, 'rust,compiler,ast')",
            &tx7,
        )
        .unwrap();
    tx7.commit().unwrap();
    drop(tx7);

    // Create fulltext index with USING clause
    let tx8 = Transaction::new(8, db.clone());
    engine
        .execute(
            "CREATE FULLTEXT INDEX idx_tags ON tags (list) USING comma",
            &tx8,
        )
        .unwrap();
    tx8.commit().unwrap();
    drop(tx8);

    // Query tags with comma tokenizer
    let tx9 = Transaction::new(9, db.clone());
    let res = engine
        .execute(
            "SELECT id FROM tags WHERE MATCH(list) AGAINST('rust') ORDER BY id ASC",
            &tx9,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
        assert_eq!(rows[1].values[0], DbValue::Int(3));
    } else {
        panic!("Expected Select result");
    }

    let res = engine
        .execute(
            "SELECT id FROM tags WHERE MATCH(list) AGAINST('compiler') ORDER BY id ASC",
            &tx9,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], DbValue::Int(3));
    } else {
        panic!("Expected Select result");
    }
    drop(tx9);

    // 6. OCC / Transaction write updates and deletes
    let tx10 = Transaction::new(10, db.clone());
    // Insert new row
    engine
        .execute("INSERT INTO tags (id, list) VALUES (4, 'rust,web')", &tx10)
        .unwrap();
    // Querying within same transaction should see the uncommitted write via write buffer scan
    let res = engine
        .execute(
            "SELECT id FROM tags WHERE MATCH(list) AGAINST('rust') ORDER BY id ASC",
            &tx10,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
        assert_eq!(rows[1].values[0], DbValue::Int(3));
        assert_eq!(rows[2].values[0], DbValue::Int(4));
    } else {
        panic!("Expected Select result");
    }
    tx10.commit().unwrap();
    drop(tx10);

    // Query committed write
    let tx11 = Transaction::new(11, db.clone());
    let res = engine
        .execute(
            "SELECT id FROM tags WHERE MATCH(list) AGAINST('web') ORDER BY id ASC",
            &tx11,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], DbValue::Int(4));
    } else {
        panic!("Expected Select result");
    }
    drop(tx11);
}

#[test]
fn test_fulltext_boolean_search() {
    let (_temp, db, engine) = setup_engine();

    // 1. Setup table and insert rows
    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE posts (id INT PRIMARY KEY, content STRING)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();
    drop(tx1);

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO posts (id, content) VALUES \
             (1, 'Rust database engines are built using LSM-trees'), \
             (2, 'C++ database structures include B-Trees'), \
             (3, 'Rust compilers parse source code into an AST'), \
             (4, 'Java database connection is established via JDBC')",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();
    drop(tx2);

    // 2. Query WITHOUT FULLTEXT index (Sequential scan fallback)
    let tx3 = Transaction::new(3, db.clone());

    // Explicit OR: rust OR c++
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('rust OR c++') ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
        assert_eq!(rows[1].values[0], DbValue::Int(2));
        assert_eq!(rows[2].values[0], DbValue::Int(3));
    } else {
        panic!("Expected Select result");
    }

    // Explicit AND with grouping: (rust OR c++) AND database
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('(rust OR c++) AND database') ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
        assert_eq!(rows[1].values[0], DbValue::Int(2));
    } else {
        panic!("Expected Select result");
    }

    // Implicit AND: rust database
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('rust database') ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
    } else {
        panic!("Expected Select result");
    }

    // Query non-matching query: rust c++ (implicit AND)
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('rust c++') ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 0);
    } else {
        panic!("Expected Select result");
    }
    drop(tx3);

    // 3. Create FULLTEXT index
    let tx4 = Transaction::new(4, db.clone());
    engine
        .execute("CREATE FULLTEXT INDEX idx_content ON posts (content)", &tx4)
        .unwrap();
    tx4.commit().unwrap();
    drop(tx4);

    // 4. Query WITH FULLTEXT index (Index scan)
    let tx5 = Transaction::new(5, db.clone());

    // Verify via EXPLAIN that the physical plan selects PhysicalFullTextScan
    let res = engine
        .execute(
            "EXPLAIN SELECT id FROM posts WHERE MATCH(content) AGAINST('(rust OR c++) AND database')",
            &tx5,
        )
        .unwrap();
    if let ExecutionResult::Select { schema, rows } = res {
        assert_eq!(schema.columns[0].name, "Query Plan");
        assert_eq!(rows.len(), 1);
        let plan_text = match &rows[0].values[0] {
            DbValue::String(s) => s.clone(),
            _ => panic!("Expected string plan text"),
        };
        assert!(plan_text.contains("FullTextScan: table=posts, index=idx_content"));
        assert!(plan_text.contains("PhysicalFullTextScan"));
    } else {
        panic!("Expected EXPLAIN result");
    }

    // Test: rust OR c++ (explicit OR)
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('rust OR c++') ORDER BY id ASC",
            &tx5,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
        assert_eq!(rows[1].values[0], DbValue::Int(2));
        assert_eq!(rows[2].values[0], DbValue::Int(3));
    } else {
        panic!("Expected Select result");
    }

    // Test: (rust OR c++) AND database
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('(rust OR c++) AND database') ORDER BY id ASC",
            &tx5,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
        assert_eq!(rows[1].values[0], DbValue::Int(2));
    } else {
        panic!("Expected Select result");
    }

    // Test: rust database (implicit AND)
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('rust database') ORDER BY id ASC",
            &tx5,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
    } else {
        panic!("Expected Select result");
    }

    // Test: (rust OR c++) (database OR AST) (implicit AND between groupings)
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('(rust OR c++) (database OR AST)') ORDER BY id ASC",
            &tx5,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
        assert_eq!(rows[1].values[0], DbValue::Int(2));
        assert_eq!(rows[2].values[0], DbValue::Int(3));
    } else {
        panic!("Expected Select result");
    }
    drop(tx5);

    // 5. OCC / Transaction write updates and deletes with boolean queries
    let tx6 = Transaction::new(6, db.clone());

    // Insert new row matching: '(rust OR c++) AND database'
    engine
        .execute(
            "INSERT INTO posts (id, content) VALUES (5, 'Rust database query parser')",
            &tx6,
        )
        .unwrap();

    // Query within transaction should scan write buffer and return Row 1, 2, and 5
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('(rust OR c++) AND database') ORDER BY id ASC",
            &tx6,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
        assert_eq!(rows[1].values[0], DbValue::Int(2));
        assert_eq!(rows[2].values[0], DbValue::Int(5));
    } else {
        panic!("Expected Select result");
    }

    // Delete Row 2 within transaction
    engine
        .execute("DELETE FROM posts WHERE id = 2", &tx6)
        .unwrap();

    // Query again - Row 2 should be gone, Row 1 and 5 should be returned
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('(rust OR c++) AND database') ORDER BY id ASC",
            &tx6,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
        assert_eq!(rows[1].values[0], DbValue::Int(5));
    } else {
        panic!("Expected Select result");
    }

    // Update Row 1 to match something else
    engine
        .execute(
            "UPDATE posts SET content = 'Go database engines' WHERE id = 1",
            &tx6,
        )
        .unwrap();

    // Query again - Row 1 should no longer match, only Row 5 should be returned
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('(rust OR c++) AND database') ORDER BY id ASC",
            &tx6,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], DbValue::Int(5));
    } else {
        panic!("Expected Select result");
    }

    tx6.commit().unwrap();
    drop(tx6);
}

#[test]
fn test_fulltext_phrase_search() {
    let (_temp, db, engine) = setup_engine();

    // 1. Setup table and insert rows
    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE posts (id INT PRIMARY KEY, content STRING)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();
    drop(tx1);

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO posts (id, content) VALUES \
             (1, 'the quick brown fox jumps over the lazy dog'), \
             (2, 'the quick lazy dog jumps over the brown fox'), \
             (3, 'quick brown fox is extremely fast'), \
             (4, 'lazy dog is very lazy')",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();
    drop(tx2);

    // 2. Query WITHOUT FULLTEXT index (Sequential scan fallback)
    let tx3 = Transaction::new(3, db.clone());

    // Contiguous Phrase: "quick brown fox"
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('\"quick brown fox\"') ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
        assert_eq!(rows[1].values[0], DbValue::Int(3));
    } else {
        panic!("Expected Select result");
    }

    // Contiguous Phrase: "brown fox jumps"
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('\"brown fox jumps\"') ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
    } else {
        panic!("Expected Select result");
    }

    // Phrase + Boolean AND: "quick brown" AND jumps
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('\"quick brown\" AND jumps') ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
    } else {
        panic!("Expected Select result");
    }

    // Phrase: "lazy dog"
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('\"lazy dog\"') ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
        assert_eq!(rows[1].values[0], DbValue::Int(2));
        assert_eq!(rows[2].values[0], DbValue::Int(4));
    } else {
        panic!("Expected Select result");
    }

    // Non-contiguous Phrase: "lazy jumps" (exist in text but not contiguous)
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('\"lazy jumps\"') ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 0);
    } else {
        panic!("Expected Select result");
    }
    drop(tx3);

    // 3. Create FULLTEXT index
    let tx4 = Transaction::new(4, db.clone());
    engine
        .execute("CREATE FULLTEXT INDEX idx_content ON posts (content)", &tx4)
        .unwrap();
    tx4.commit().unwrap();
    drop(tx4);

    // 4. Query WITH FULLTEXT index (Index scan / PhysicalFullTextScan)
    let tx5 = Transaction::new(5, db.clone());

    // Verify via EXPLAIN that physical plan selects PhysicalFullTextScan
    let res = engine
        .execute(
            "EXPLAIN SELECT id FROM posts WHERE MATCH(content) AGAINST('\"quick brown fox\"')",
            &tx5,
        )
        .unwrap();
    if let ExecutionResult::Select { schema, rows } = res {
        assert_eq!(schema.columns[0].name, "Query Plan");
        let plan_text = match &rows[0].values[0] {
            DbValue::String(s) => s.clone(),
            _ => panic!("Expected string plan text"),
        };
        assert!(plan_text.contains("FullTextScan: table=posts, index=idx_content"));
        assert!(plan_text.contains("PhysicalFullTextScan"));
    } else {
        panic!("Expected EXPLAIN result");
    }

    // Contiguous Phrase: "quick brown fox"
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('\"quick brown fox\"') ORDER BY id ASC",
            &tx5,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
        assert_eq!(rows[1].values[0], DbValue::Int(3));
    } else {
        panic!("Expected Select result");
    }

    // Contiguous Phrase: "brown fox jumps"
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('\"brown fox jumps\"') ORDER BY id ASC",
            &tx5,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
    } else {
        panic!("Expected Select result");
    }

    // Phrase: "lazy dog"
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('\"lazy dog\"') ORDER BY id ASC",
            &tx5,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
        assert_eq!(rows[1].values[0], DbValue::Int(2));
        assert_eq!(rows[2].values[0], DbValue::Int(4));
    } else {
        panic!("Expected Select result");
    }

    // Non-contiguous Phrase: "lazy jumps"
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('\"lazy jumps\"') ORDER BY id ASC",
            &tx5,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 0);
    } else {
        panic!("Expected Select result");
    }
    drop(tx5);

    // 5. OCC / Transaction write updates and deletes with phrase queries
    let tx6 = Transaction::new(6, db.clone());

    // Insert new row matching: "quick brown fox"
    engine
        .execute(
            "INSERT INTO posts (id, content) VALUES (5, 'quick brown fox query phrase')",
            &tx6,
        )
        .unwrap();

    // Query within transaction should scan write buffer and return Row 1, 3, and 5
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('\"quick brown fox\"') ORDER BY id ASC",
            &tx6,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
        assert_eq!(rows[1].values[0], DbValue::Int(3));
        assert_eq!(rows[2].values[0], DbValue::Int(5));
    } else {
        panic!("Expected Select result");
    }

    // Delete Row 3 within transaction
    engine
        .execute("DELETE FROM posts WHERE id = 3", &tx6)
        .unwrap();

    // Query again - Row 3 should be gone, Row 1 and 5 should be returned
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('\"quick brown fox\"') ORDER BY id ASC",
            &tx6,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
        assert_eq!(rows[1].values[0], DbValue::Int(5));
    } else {
        panic!("Expected Select result");
    }

    // Update Row 1 to match something else
    engine
        .execute(
            "UPDATE posts SET content = 'the slow brown fox jumps over the lazy dog' WHERE id = 1",
            &tx6,
        )
        .unwrap();

    // Query again - Row 1 should no longer match, only Row 5 should be returned
    let res = engine
        .execute(
            "SELECT id FROM posts WHERE MATCH(content) AGAINST('\"quick brown fox\"') ORDER BY id ASC",
            &tx6,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], DbValue::Int(5));
    } else {
        panic!("Expected Select result");
    }

    tx6.commit().unwrap();
    drop(tx6);
}

#[test]
fn test_sql_sort_elimination_and_promotion() {
    let (_temp_dir, db, engine) = setup_engine();

    // 1. Create table and insert data
    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE products (id INT PRIMARY KEY, name STRING, price INT)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();
    drop(tx1);

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO products (id, name, price) VALUES (1, 'Apple', 10), (2, 'Banana', 5), (3, 'Orange', 8)",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();
    drop(tx2);

    // Create index on price
    let tx3 = Transaction::new(3, db.clone());
    engine
        .execute("CREATE INDEX idx_price ON products (price)", &tx3)
        .unwrap();
    tx3.commit().unwrap();
    drop(tx3);

    // 2. Test Primary Key Sort Elimination
    let tx4 = Transaction::new(4, db.clone());
    let explain_pk = engine
        .execute("EXPLAIN SELECT name FROM products ORDER BY id ASC", &tx4)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = explain_pk {
        let plan_str = match &rows[0].values[0] {
            DbValue::String(s) => s,
            _ => panic!("Expected string plan representation"),
        };
        // Should use SeqScan and eliminate Sort
        assert!(plan_str.contains("- PhysicalSeqScan"));
        assert!(!plan_str.contains("- PhysicalSort"));
    } else {
        panic!("Expected Select");
    }

    let query_pk = engine
        .execute("SELECT name FROM products ORDER BY id ASC", &tx4)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = query_pk {
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values[0], DbValue::string("Apple"));
        assert_eq!(rows[1].values[0], DbValue::string("Banana"));
        assert_eq!(rows[2].values[0], DbValue::string("Orange"));
    } else {
        panic!("Expected Select");
    }

    // 3. Test Index Scan Promotion and Sort Elimination
    let explain_idx = engine
        .execute("EXPLAIN SELECT name FROM products ORDER BY price ASC", &tx4)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = explain_idx {
        let plan_str = match &rows[0].values[0] {
            DbValue::String(s) => s,
            _ => panic!("Expected string plan representation"),
        };
        // Should promote to IndexScan and eliminate Sort
        assert!(plan_str.contains("- PhysicalIndexScan"));
        assert!(!plan_str.contains("- PhysicalSort"));
    } else {
        panic!("Expected Select");
    }

    let query_idx = engine
        .execute("SELECT name FROM products ORDER BY price ASC", &tx4)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = query_idx {
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values[0], DbValue::string("Banana"));
        assert_eq!(rows[1].values[0], DbValue::string("Orange"));
        assert_eq!(rows[2].values[0], DbValue::string("Apple"));
    } else {
        panic!("Expected Select");
    }

    // 4. Verify sort is NOT eliminated for DESC order
    let explain_desc = engine
        .execute(
            "EXPLAIN SELECT name FROM products ORDER BY price DESC",
            &tx4,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = explain_desc {
        let plan_str = match &rows[0].values[0] {
            DbValue::String(s) => s,
            _ => panic!("Expected string plan representation"),
        };
        // Should still contain PhysicalSort since DESC is not optimized yet
        assert!(plan_str.contains("- PhysicalSort"));
    } else {
        panic!("Expected Select");
    }

    let query_desc = engine
        .execute("SELECT name FROM products ORDER BY price DESC", &tx4)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = query_desc {
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values[0], DbValue::string("Apple"));
        assert_eq!(rows[1].values[0], DbValue::string("Orange"));
        assert_eq!(rows[2].values[0], DbValue::string("Banana"));
    } else {
        panic!("Expected Select");
    }

    drop(tx4);
}

#[test]
fn test_external_sort_basic() {
    // Set a very small sort memory budget (200 bytes) to force spilling to disk
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let options = dtdb_relational::DatabaseOptions {
        compression: dtdb_storage::CompressionType::Lz4,
        memtable_size_limit: 1024 * 1024,
        block_size_limit: 4096,
        wal_size_limit: 32 * 1024 * 1024,
        flush_interval_ms: None,
        l0_compaction_threshold: None,
        sstable_target_size: None,
        base_level_size_limit: None,
        level_size_multiplier: None,
        max_level: None,
        block_cache_capacity: Some(1000),
        analyze_frequency_ms: None,
        wal_sync_interval_ms: None,
        memory_budget: Some(200), // very small budget to force spills
        fsync_method: dtdb_storage::FsyncMethod::default(),
    };

    let db = Arc::new(Database::open_with_options(db_path, options).unwrap());
    let engine = SqlEngine::new(db.clone());

    // Create table
    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t1 (id INT PRIMARY KEY, val INT)", &tx1)
        .unwrap();
    tx1.commit().unwrap();
    drop(tx1);

    // Insert 10 rows in unsorted order
    let tx2 = Transaction::new(2, db.clone());
    engine.execute(
        "INSERT INTO t1 (id, val) VALUES (1, 50), (2, 10), (3, 80), (4, 20), (5, 90), (6, 30), (7, 70), (8, 40), (9, 100), (10, 60)",
        &tx2,
    ).unwrap();
    tx2.commit().unwrap();
    drop(tx2);

    // Query with ORDER BY on unindexed column (forces PhysicalSort)
    let tx3 = Transaction::new(3, db.clone());
    let explain_res = engine
        .execute("EXPLAIN SELECT val FROM t1 ORDER BY val ASC", &tx3)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = explain_res {
        let plan = match &rows[0].values[0] {
            DbValue::String(s) => s,
            _ => panic!("Expected explain plan string"),
        };
        assert!(plan.contains("- PhysicalSort"));
    } else {
        panic!("Expected EXPLAIN SELECT");
    }

    let query_res = engine
        .execute("SELECT val FROM t1 ORDER BY val ASC", &tx3)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = query_res {
        assert_eq!(rows.len(), 10);
        let sorted_vals: Vec<i64> = rows
            .into_iter()
            .map(|r| match r.values[0] {
                DbValue::Int(v) => v,
                _ => panic!("Expected Int value"),
            })
            .collect();
        assert_eq!(sorted_vals, vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100]);
    } else {
        panic!("Expected SELECT result");
    }

    // Verify cleanup: temp dir should be empty after query is finished
    let tmp_path = db.dir_path().join("_tmp");
    if tmp_path.exists() {
        let entries: Vec<_> = std::fs::read_dir(&tmp_path).unwrap().collect();
        assert!(
            entries.is_empty(),
            "Temp directory should be empty but has entries: {:?}",
            entries
        );
    }

    drop(tx3);
}

#[test]
fn test_external_sort_desc() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let options = dtdb_relational::DatabaseOptions {
        compression: dtdb_storage::CompressionType::Lz4,
        memtable_size_limit: 1024 * 1024,
        block_size_limit: 4096,
        wal_size_limit: 32 * 1024 * 1024,
        flush_interval_ms: None,
        l0_compaction_threshold: None,
        sstable_target_size: None,
        base_level_size_limit: None,
        level_size_multiplier: None,
        max_level: None,
        block_cache_capacity: Some(1000),
        analyze_frequency_ms: None,
        wal_sync_interval_ms: None,
        memory_budget: Some(200), // force spills
        fsync_method: dtdb_storage::FsyncMethod::default(),
    };

    let db = Arc::new(Database::open_with_options(db_path, options).unwrap());
    let engine = SqlEngine::new(db.clone());

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, val INT)", &tx1)
        .unwrap();
    tx1.commit().unwrap();
    drop(tx1);

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO t2 (id, val) VALUES (1, 50), (2, 10), (3, 80), (4, 20), (5, 90)",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();
    drop(tx2);

    let tx3 = Transaction::new(3, db.clone());
    let query_res = engine
        .execute("SELECT val FROM t2 ORDER BY val DESC", &tx3)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = query_res {
        assert_eq!(rows.len(), 5);
        let sorted_vals: Vec<i64> = rows
            .into_iter()
            .map(|r| match r.values[0] {
                DbValue::Int(v) => v,
                _ => panic!("Expected Int value"),
            })
            .collect();
        assert_eq!(sorted_vals, vec![90, 80, 50, 20, 10]);
    } else {
        panic!("Expected SELECT result");
    }

    drop(tx3);
}

#[test]
fn test_external_sort_multi_key() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let options = dtdb_relational::DatabaseOptions {
        compression: dtdb_storage::CompressionType::Lz4,
        memtable_size_limit: 1024 * 1024,
        block_size_limit: 4096,
        wal_size_limit: 32 * 1024 * 1024,
        flush_interval_ms: None,
        l0_compaction_threshold: None,
        sstable_target_size: None,
        base_level_size_limit: None,
        level_size_multiplier: None,
        max_level: None,
        block_cache_capacity: Some(1000),
        analyze_frequency_ms: None,
        wal_sync_interval_ms: None,
        memory_budget: Some(100), // force spills
        fsync_method: dtdb_storage::FsyncMethod::default(),
    };

    let db = Arc::new(Database::open_with_options(db_path, options).unwrap());
    let engine = SqlEngine::new(db.clone());

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t3 (id INT PRIMARY KEY, a INT, b INT)", &tx1)
        .unwrap();
    tx1.commit().unwrap();
    drop(tx1);

    let tx2 = Transaction::new(2, db.clone());
    engine.execute(
        "INSERT INTO t3 (id, a, b) VALUES (1, 10, 100), (2, 20, 200), (3, 10, 300), (4, 20, 50), (5, 10, 200)",
        &tx2,
    ).unwrap();
    tx2.commit().unwrap();
    drop(tx2);

    let tx3 = Transaction::new(3, db.clone());
    let query_res = engine
        .execute("SELECT a, b FROM t3 ORDER BY a ASC, b DESC", &tx3)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = query_res {
        assert_eq!(rows.len(), 5);
        let sorted_vals: Vec<(i64, i64)> = rows
            .into_iter()
            .map(|r| match (&r.values[0], &r.values[1]) {
                (DbValue::Int(av), DbValue::Int(bv)) => (*av, *bv),
                _ => panic!("Expected Int values"),
            })
            .collect();
        assert_eq!(
            sorted_vals,
            vec![(10, 300), (10, 200), (10, 100), (20, 200), (20, 50),]
        );
    } else {
        panic!("Expected SELECT result");
    }

    drop(tx3);
}

#[test]
fn test_external_sort_empty_and_single() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let options = dtdb_relational::DatabaseOptions {
        compression: dtdb_storage::CompressionType::Lz4,
        memtable_size_limit: 1024 * 1024,
        block_size_limit: 4096,
        wal_size_limit: 32 * 1024 * 1024,
        flush_interval_ms: None,
        l0_compaction_threshold: None,
        sstable_target_size: None,
        base_level_size_limit: None,
        level_size_multiplier: None,
        max_level: None,
        block_cache_capacity: Some(1000),
        analyze_frequency_ms: None,
        wal_sync_interval_ms: None,
        memory_budget: Some(200),
        fsync_method: dtdb_storage::FsyncMethod::default(),
    };

    let db = Arc::new(Database::open_with_options(db_path, options).unwrap());
    let engine = SqlEngine::new(db.clone());

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t4 (id INT PRIMARY KEY, val INT)", &tx1)
        .unwrap();
    tx1.commit().unwrap();
    drop(tx1);

    // Test 1: Empty table sort
    let tx2 = Transaction::new(2, db.clone());
    let query_empty = engine
        .execute("SELECT val FROM t4 ORDER BY val ASC", &tx2)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = query_empty {
        assert!(rows.is_empty());
    } else {
        panic!("Expected Select");
    }
    drop(tx2);

    // Test 2: Single row sort
    let tx3 = Transaction::new(3, db.clone());
    engine
        .execute("INSERT INTO t4 (id, val) VALUES (1, 42)", &tx3)
        .unwrap();
    tx3.commit().unwrap();
    drop(tx3);

    let tx4 = Transaction::new(4, db.clone());
    let query_single = engine
        .execute("SELECT val FROM t4 ORDER BY val ASC", &tx4)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = query_single {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], DbValue::Int(42));
    } else {
        panic!("Expected Select");
    }
    drop(tx4);
}

#[test]
fn test_sql_sorted_aggregate() {
    let (_temp, db, engine) = setup_engine();

    // 1. Create table and insert data
    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE sales (id INT PRIMARY KEY, dept STRING, amount FLOAT)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();
    drop(tx1);

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO sales (id, dept, amount) VALUES \
             (1, 'Sales', 10.0), \
             (2, 'Sales', 20.0), \
             (3, 'Engineering', 50.0), \
             (4, 'Engineering', 60.0), \
             (5, 'HR', 30.0)",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();
    drop(tx2);

    // 2. Query grouping by Primary Key (id) which guarantees sorted input.
    // This should compile to PhysicalSortedAggregate.
    let tx3 = Transaction::new(3, db.clone());
    let explain_res = engine
        .execute(
            "EXPLAIN SELECT id, SUM(amount) FROM sales GROUP BY id",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = explain_res {
        let plan = match &rows[0].values[0] {
            DbValue::String(s) => s,
            _ => panic!("Expected plan string"),
        };
        assert!(plan.contains("- PhysicalSortedAggregate"));
        assert!(!plan.contains("- PhysicalHashAggregate"));
    } else {
        panic!("Expected Select");
    }

    let query_res = engine
        .execute(
            "SELECT id, SUM(amount) FROM sales GROUP BY id ORDER BY id ASC",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = query_res {
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0].values, vec![DbValue::Int(1), DbValue::Float(10.0)]);
        assert_eq!(rows[1].values, vec![DbValue::Int(2), DbValue::Float(20.0)]);
    } else {
        panic!("Expected Select");
    }

    // 3. Grouping by unindexed column (amount) should fallback to PhysicalHashAggregate.
    let explain_hash = engine
        .execute(
            "EXPLAIN SELECT amount, COUNT(id) FROM sales GROUP BY amount",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = explain_hash {
        let plan = match &rows[0].values[0] {
            DbValue::String(s) => s,
            _ => panic!("Expected plan string"),
        };
        assert!(plan.contains("- PhysicalHashAggregate"));
        assert!(!plan.contains("- PhysicalSortedAggregate"));
    } else {
        panic!("Expected Select");
    }
    tx3.commit().unwrap();
    drop(tx3);

    // 4. Create secondary index on dept, then query grouping by dept (which promotes to IndexScan).
    // This should also compile to PhysicalSortedAggregate.
    let tx4 = Transaction::new(4, db.clone());
    engine
        .execute("CREATE INDEX idx_dept ON sales (dept)", &tx4)
        .unwrap();
    tx4.commit().unwrap();
    drop(tx4);

    let tx5 = Transaction::new(5, db.clone());
    let explain_idx = engine
        .execute(
            "EXPLAIN SELECT dept, SUM(amount) FROM sales GROUP BY dept",
            &tx5,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = explain_idx {
        let plan = match &rows[0].values[0] {
            DbValue::String(s) => s,
            _ => panic!("Expected plan string"),
        };
        assert!(plan.contains("- PhysicalSortedAggregate"));
        assert!(!plan.contains("- PhysicalHashAggregate"));
    } else {
        panic!("Expected Select");
    }

    let query_idx = engine
        .execute(
            "SELECT dept, SUM(amount) FROM sales GROUP BY dept ORDER BY dept ASC",
            &tx5,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = query_idx {
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows[0].values,
            vec![DbValue::string("Engineering"), DbValue::Float(110.0)]
        );
        assert_eq!(
            rows[1].values,
            vec![DbValue::string("HR"), DbValue::Float(30.0)]
        );
        assert_eq!(
            rows[2].values,
            vec![DbValue::string("Sales"), DbValue::Float(30.0)]
        );
    } else {
        panic!("Expected Select");
    }

    drop(tx5);
}

#[test]
fn test_sql_insert_arity_mismatch() {
    let (_temp, db, engine) = setup_engine();

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE users (id INT PRIMARY KEY, name STRING, age INT)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();

    // Arity mismatch: 3 columns but only 2 values provided
    let tx2 = Transaction::new(2, db.clone());
    let err_res = engine.execute(
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice')",
        &tx2,
    );
    assert!(err_res.is_err());
    assert!(err_res.unwrap_err().contains("Column count mismatch"));
}

#[test]
fn test_sql_integer_overflow_prevention() {
    let (_temp, db, engine) = setup_engine();

    let tx_ddl = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, val INT)", &tx_ddl)
        .unwrap();
    tx_ddl.commit().unwrap();

    let tx_insert = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO items (id, val) VALUES (1, 9223372036854775807), (2, -9223372036854775807)",
            &tx_insert,
        )
        .unwrap();
    engine
        .execute("UPDATE items SET val = val - 1 WHERE id = 2", &tx_insert)
        .unwrap();
    tx_insert.commit().unwrap();

    let tx_query = Transaction::new(3, db.clone());
    // 1. SELECT overflow check
    let res_add = engine.execute("SELECT val + 1 FROM items WHERE id = 1", &tx_query);
    assert!(res_add.is_err());
    assert!(res_add.unwrap_err().contains("Integer overflow"));

    let res_div_overflow = engine.execute("SELECT val / -1 FROM items WHERE id = 2", &tx_query);
    assert!(res_div_overflow.is_err());
    assert!(res_div_overflow.unwrap_err().contains("Integer overflow"));

    // 2. Optimizer bound nudging check
    // Lower bound nudging: Lt with i64::MIN should not overflow/panic
    let _ = engine
        .execute(
            "EXPLAIN SELECT id FROM items WHERE val < -9223372036854775808",
            &tx_query,
        )
        .unwrap();
    // Upper bound nudging: Gt with i64::MAX should not overflow/panic
    let _ = engine
        .execute(
            "EXPLAIN SELECT id FROM items WHERE val > 9223372036854775807",
            &tx_query,
        )
        .unwrap();
}

#[test]
fn test_sql_join_order_orientation() {
    let (_temp, db, engine) = setup_engine();

    let tx_ddl = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE users (id INT PRIMARY KEY, name STRING)",
            &tx_ddl,
        )
        .unwrap();
    engine
        .execute(
            "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)",
            &tx_ddl,
        )
        .unwrap();
    tx_ddl.commit().unwrap();

    let tx_insert = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
            &tx_insert,
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO orders (id, user_id, amount) VALUES (10, 1, 100), (20, 2, 200)",
            &tx_insert,
        )
        .unwrap();
    tx_insert.commit().unwrap();

    let tx_query = Transaction::new(3, db.clone());

    // 1. Join with standard order: ON users.id = orders.user_id (left=left, right=right)
    let res1 = engine
        .execute(
            "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id ORDER BY users.id",
            &tx_query,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res1 {
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].values,
            vec![DbValue::string("Alice"), DbValue::Int(100)]
        );
        assert_eq!(
            rows[1].values,
            vec![DbValue::string("Bob"), DbValue::Int(200)]
        );
    } else {
        panic!("Expected SELECT result");
    }

    // 2. Join with reversed order: ON orders.user_id = users.id (left=right, right=left)
    let res2 = engine
        .execute(
            "SELECT users.name, orders.amount FROM users JOIN orders ON orders.user_id = users.id ORDER BY users.id",
            &tx_query,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res2 {
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].values,
            vec![DbValue::string("Alice"), DbValue::Int(100)]
        );
        assert_eq!(
            rows[1].values,
            vec![DbValue::string("Bob"), DbValue::Int(200)]
        );
    } else {
        panic!("Expected SELECT result");
    }
}

#[test]
fn test_sql_aggregate_empty_table() {
    let (_temp, db, engine) = setup_engine();

    let tx_ddl = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE items (id INT PRIMARY KEY, val INT)", &tx_ddl)
        .unwrap();
    tx_ddl.commit().unwrap();

    let tx_query = Transaction::new(2, db.clone());

    let res = engine
        .execute(
            "SELECT COUNT(*), SUM(val), MIN(val), MAX(val), AVG(val) FROM items",
            &tx_query,
        )
        .unwrap();

    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].values,
            vec![
                DbValue::Int(0),
                DbValue::Null,
                DbValue::Null,
                DbValue::Null,
                DbValue::Null
            ]
        );
    } else {
        panic!("Expected SELECT result");
    }
}

#[test]
fn test_sql_nan_group_by_distinct_join() {
    let (_temp, db, engine) = setup_engine();

    let tx_ddl = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE items (id INT PRIMARY KEY, val FLOAT)",
            &tx_ddl,
        )
        .unwrap();
    tx_ddl.commit().unwrap();

    let tx_insert = Transaction::new(2, db.clone());
    tx_insert
        .put(
            "items",
            dtdb_storage::DbKey::Int(1),
            dtdb_relational::Row::new(vec![DbValue::Int(1), DbValue::Float(f64::NAN)]),
        )
        .unwrap();
    tx_insert
        .put(
            "items",
            dtdb_storage::DbKey::Int(2),
            dtdb_relational::Row::new(vec![DbValue::Int(2), DbValue::Float(f64::NAN)]),
        )
        .unwrap();
    tx_insert
        .put(
            "items",
            dtdb_storage::DbKey::Int(3),
            dtdb_relational::Row::new(vec![DbValue::Int(3), DbValue::Float(42.0)]),
        )
        .unwrap();
    tx_insert.commit().unwrap();

    let tx_query = Transaction::new(3, db.clone());

    // 1. SELECT DISTINCT val
    let res_distinct = engine
        .execute("SELECT DISTINCT val FROM items", &tx_query)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res_distinct {
        assert_eq!(rows.len(), 2);
        let mut has_nan = false;
        let mut has_42 = false;
        for r in rows {
            match &r.values[0] {
                DbValue::Float(f) if f.is_nan() => has_nan = true,
                DbValue::Float(f) if *f == 42.0 => has_42 = true,
                _ => {}
            }
        }
        assert!(has_nan);
        assert!(has_42);
    } else {
        panic!("Expected SELECT result");
    }

    // 2. SELECT val, COUNT(*) FROM items GROUP BY val
    let res_group = engine
        .execute("SELECT val, COUNT(*) FROM items GROUP BY val", &tx_query)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res_group {
        assert_eq!(rows.len(), 2);
        let mut nan_count = 0;
        let mut val_42_count = 0;
        for r in rows {
            match &r.values[0] {
                DbValue::Float(f) if f.is_nan() => {
                    assert_eq!(r.values[1], DbValue::Int(2));
                    nan_count += 1;
                }
                DbValue::Float(f) if *f == 42.0 => {
                    assert_eq!(r.values[1], DbValue::Int(1));
                    val_42_count += 1;
                }
                _ => {}
            }
        }
        assert_eq!(nan_count, 1);
        assert_eq!(val_42_count, 1);
    } else {
        panic!("Expected SELECT result");
    }

    // 3. SELECT a.id, b.id FROM items a JOIN items b ON a.val = b.val
    let res_join = engine
        .execute(
            "SELECT a.id, b.id FROM items a JOIN items b ON a.val = b.val",
            &tx_query,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res_join {
        assert_eq!(rows.len(), 5);
        let mut matches = std::collections::HashSet::new();
        for r in rows {
            if let (DbValue::Int(aid), DbValue::Int(bid)) = (&r.values[0], &r.values[1]) {
                matches.insert((*aid, *bid));
            }
        }
        assert!(matches.contains(&(1, 1)));
        assert!(matches.contains(&(1, 2)));
        assert!(matches.contains(&(2, 1)));
        assert!(matches.contains(&(2, 2)));
        assert!(matches.contains(&(3, 3)));
    } else {
        panic!("Expected SELECT result");
    }
}

#[test]
fn test_sql_select_distinct() {
    let (_temp, db, engine) = setup_engine();

    let tx_ddl = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, age INT)",
            &tx_ddl,
        )
        .unwrap();
    tx_ddl.commit().unwrap();

    let tx_insert = Transaction::new(2, db.clone());
    engine.execute(
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 30), (3, 'Alice', 25), (4, 'Charlie', 30)",
        &tx_insert,
    ).unwrap();
    tx_insert.commit().unwrap();

    let tx_query = Transaction::new(3, db.clone());

    // SELECT DISTINCT age
    let res = engine
        .execute("SELECT DISTINCT age FROM users ORDER BY age", &tx_query)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].values, vec![DbValue::Int(25)]);
        assert_eq!(rows[1].values, vec![DbValue::Int(30)]);
    } else {
        panic!("Expected SELECT");
    }

    // SELECT DISTINCT name
    let res = engine
        .execute("SELECT DISTINCT name FROM users ORDER BY name", &tx_query)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].values, vec![DbValue::string("Alice")]);
        assert_eq!(rows[1].values, vec![DbValue::string("Bob")]);
        assert_eq!(rows[2].values, vec![DbValue::string("Charlie")]);
    } else {
        panic!("Expected SELECT");
    }

    // SELECT DISTINCT name, age
    let res = engine
        .execute(
            "SELECT DISTINCT name, age FROM users ORDER BY name, age",
            &tx_query,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 4);
    } else {
        panic!("Expected SELECT");
    }
}

/// Seeds a table `t(id, k, v)` with duplicate and NULL `v` values for exercising
/// DISTINCT aggregates, returning a committed engine ready to query.
fn setup_distinct_agg_table() -> (TempDir, Arc<Database>, SqlEngine) {
    let (temp, db, engine) = setup_engine();

    let tx_ddl = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE t (id INT PRIMARY KEY, k VARCHAR, v INT)",
            &tx_ddl,
        )
        .unwrap();
    tx_ddl.commit().unwrap();

    let tx_insert = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO t (id, k, v) VALUES \
             (1, 'a', 10), (2, 'a', 10), (3, 'a', 20), \
             (4, 'b', 30), (5, 'b', 30), (6, 'b', 30), \
             (7, 'c', NULL), (8, 'c', 5)",
            &tx_insert,
        )
        .unwrap();
    tx_insert.commit().unwrap();

    (temp, db, engine)
}

#[test]
fn test_sql_count_distinct_global() {
    let (_temp, db, engine) = setup_distinct_agg_table();
    let tx = Transaction::new(3, db.clone());

    // Distinct non-null values are {10, 20, 30, 5}; plain COUNT(v) counts all 7
    // non-null rows. SUM/AVG operate over the deduplicated set (65, 65/4).
    let res = engine
        .execute(
            "SELECT COUNT(v), COUNT(DISTINCT v), SUM(DISTINCT v), AVG(DISTINCT v) FROM t",
            &tx,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].values,
            vec![
                DbValue::Int(7),       // COUNT(v)
                DbValue::Int(4),       // COUNT(DISTINCT v)
                DbValue::Int(65),      // SUM(DISTINCT v) = 10+20+30+5
                DbValue::Float(16.25)  // AVG(DISTINCT v) = 65/4
            ]
        );
    } else {
        panic!("Expected SELECT");
    }
}

#[test]
fn test_sql_count_distinct_grouped() {
    let (_temp, db, engine) = setup_distinct_agg_table();
    let tx = Transaction::new(3, db.clone());

    let res = engine
        .execute(
            "SELECT k, COUNT(v), COUNT(DISTINCT v), SUM(DISTINCT v) \
             FROM t GROUP BY k ORDER BY k",
            &tx,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 3);
        // a: v = {10,10,20} -> count=3, distinct=2, sum_distinct=30
        assert_eq!(
            rows[0].values,
            vec![
                DbValue::string("a"),
                DbValue::Int(3),
                DbValue::Int(2),
                DbValue::Int(30),
            ]
        );
        // b: v = {30,30,30} -> count=3, distinct=1, sum_distinct=30
        assert_eq!(
            rows[1].values,
            vec![
                DbValue::string("b"),
                DbValue::Int(3),
                DbValue::Int(1),
                DbValue::Int(30),
            ]
        );
        // c: v = {NULL,5} -> count=1, distinct=1, sum_distinct=5 (NULL ignored)
        assert_eq!(
            rows[2].values,
            vec![
                DbValue::string("c"),
                DbValue::Int(1),
                DbValue::Int(1),
                DbValue::Int(5),
            ]
        );
    } else {
        panic!("Expected SELECT");
    }
}

#[test]
fn test_sql_count_distinct_star_rejected() {
    let (_temp, db, engine) = setup_distinct_agg_table();
    let tx = Transaction::new(3, db.clone());

    let err = engine
        .execute("SELECT COUNT(DISTINCT *) FROM t", &tx)
        .unwrap_err();
    assert!(
        err.to_string().contains("COUNT(DISTINCT *)"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_sql_count_distinct_spilling() {
    // Tiny budget forces the grouped hash aggregate to spill partial groups to
    // disk. Each group's `v` values are inserted across two passes, so a single
    // group's distinct set is split across multiple runs; the merge must union
    // the sets (COUNT(DISTINCT)=1 per group) rather than add partial counts.
    let (_temp, db, engine) = setup_engine_with_options(opts_with_budget(Some(150)));

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE s (id INT PRIMARY KEY, g INT, v INT)", &tx1)
        .unwrap();
    tx1.commit().unwrap();
    drop(tx1);

    // 20 groups, two rows each with the identical value 100, inserted in two
    // interleaved passes (ids 0..20 then 20..40) so spills land between passes.
    let mut values = Vec::new();
    for id in 0..20 {
        values.push(format!("({}, {}, 100)", id, id));
    }
    for id in 0..20 {
        values.push(format!("({}, {}, 100)", id + 20, id));
    }
    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            &format!("INSERT INTO s (id, g, v) VALUES {}", values.join(", ")),
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();
    drop(tx2);

    let tx3 = Transaction::new(3, db.clone());
    let res = engine
        .execute(
            "SELECT g, COUNT(DISTINCT v), SUM(DISTINCT v) FROM s GROUP BY g ORDER BY g",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 20);
        for (g, row) in rows.iter().enumerate() {
            assert_eq!(
                row.values,
                vec![
                    DbValue::Int(g as i64),
                    DbValue::Int(1),   // distinct value count, not 2
                    DbValue::Int(100), // distinct sum, not 200
                ]
            );
        }
    } else {
        panic!("Expected SELECT");
    }
    drop(tx3);

    assert_tmp_empty(&db);
}

#[test]
fn test_sql_binary_short_circuit() {
    let (_temp, db, engine) = setup_engine();

    let tx_ddl = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)", &tx_ddl)
        .unwrap();
    tx_ddl.commit().unwrap();

    let tx_insert = Transaction::new(2, db.clone());
    engine
        .execute("INSERT INTO t (id, val) VALUES (1, 10)", &tx_insert)
        .unwrap();
    tx_insert.commit().unwrap();

    let tx_query = Transaction::new(3, db.clone());

    // 1. OR short-circuits: true OR error => true (no error)
    let res = engine
        .execute("SELECT id FROM t WHERE id = 1 OR val / 0 = 5", &tx_query)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values, vec![DbValue::Int(1)]);
    } else {
        panic!("Expected SELECT");
    }

    // 2. OR does not short-circuit when left is false: false OR error => error
    let res_err = engine.execute("SELECT id FROM t WHERE id = 2 OR val / 0 = 5", &tx_query);
    assert!(res_err.is_err());
    assert!(res_err.unwrap_err().contains("Division by zero"));

    // 3. AND short-circuits: false AND error => false (no error)
    let res = engine
        .execute("SELECT id FROM t WHERE id = 2 AND val / 0 = 5", &tx_query)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 0);
    } else {
        panic!("Expected SELECT");
    }

    // 4. AND does not short-circuit when left is true: true AND error => error
    let res_err2 = engine.execute("SELECT id FROM t WHERE id = 1 AND val / 0 = 5", &tx_query);
    assert!(res_err2.is_err());
    assert!(res_err2.unwrap_err().contains("Division by zero"));

    // 5. AND short-circuits: false AND NULL => false
    let res = engine
        .execute("SELECT id FROM t WHERE id = 2 AND NULL = 5", &tx_query)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 0);
    } else {
        panic!("Expected SELECT");
    }
}

#[test]
fn test_sql_server_side_parameters() {
    let (_temp, db, engine) = setup_engine();

    let tx_ddl = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, age INT)",
            &tx_ddl,
        )
        .unwrap();
    tx_ddl.commit().unwrap();

    let tx_insert = Transaction::new(2, db.clone());
    let mut params = std::collections::HashMap::new();
    params.insert("id".to_string(), DbValue::Int(1));
    params.insert("name".to_string(), DbValue::string("Alice"));
    params.insert("age".to_string(), DbValue::Int(30));

    // Test insert with parameters
    engine
        .execute_with_params(
            "INSERT INTO users (id, name, age) VALUES (:id, :name, :age)",
            &tx_insert,
            &params,
        )
        .unwrap();
    tx_insert.commit().unwrap();

    let tx_query = Transaction::new(3, db.clone());
    let mut q_params = std::collections::HashMap::new();
    q_params.insert("name".to_string(), DbValue::string("Alice"));
    q_params.insert("min_age".to_string(), DbValue::Int(25));

    // Test select with parameters
    let res = engine
        .execute_with_params(
            "SELECT id, age FROM users WHERE name = :name AND age >= :min_age",
            &tx_query,
            &q_params,
        )
        .unwrap();

    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values, vec![DbValue::Int(1), DbValue::Int(30)]);
    } else {
        panic!("Expected SELECT result");
    }
}

#[test]
fn test_prepared_statement_reuse() {
    let (_temp, db, engine) = setup_engine();

    let tx_ddl = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE kv (id INT PRIMARY KEY, v VARCHAR)", &tx_ddl)
        .unwrap();
    tx_ddl.commit().unwrap();

    let tx_ins = Transaction::new(2, db.clone());
    for i in 1..=5i64 {
        engine
            .execute(
                &format!("INSERT INTO kv (id, v) VALUES ({i}, 'v{i}')"),
                &tx_ins,
            )
            .unwrap();
    }
    tx_ins.commit().unwrap();

    // Prepare a parameterized point lookup once, run it many times with
    // different bound keys. Each execution must return the correct row.
    let stmt = engine.prepare("SELECT v FROM kv WHERE id = :id").unwrap();
    assert_eq!(stmt.sql(), "SELECT v FROM kv WHERE id = :id");

    for i in 1..=5i64 {
        let tx = Transaction::new((10 + i) as u64, db.clone());
        let mut params = std::collections::HashMap::new();
        params.insert("id".to_string(), DbValue::Int(i));
        let res = engine.execute_prepared(&stmt, &tx, &params).unwrap();
        tx.commit().unwrap();
        match res {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 1, "id={i}");
                assert_eq!(rows[0].values, vec![DbValue::string(format!("v{i}"))]);
            }
            _ => panic!("expected Select"),
        }
    }

    // A miss returns no rows (same prepared statement, different binding).
    let tx = Transaction::new(100, db.clone());
    let mut params = std::collections::HashMap::new();
    params.insert("id".to_string(), DbValue::Int(999));
    let res = engine.execute_prepared(&stmt, &tx, &params).unwrap();
    match res {
        ExecutionResult::Select { rows, .. } => assert!(rows.is_empty()),
        _ => panic!("expected Select"),
    }
}

#[test]
fn test_prepared_statement_replanned_after_ddl() {
    let (_temp, db, engine) = setup_engine();

    // Each statement runs in its own scoped transaction. The transaction must
    // be dropped (not just committed) before the later DROP TABLE, since DROP
    // spin-waits for active readers and the reader slot is released on Drop.
    let run = |sql: &str, id: u64| {
        let tx = Transaction::new(id, db.clone());
        engine.execute(sql, &tx).unwrap();
        tx.commit().unwrap();
    };

    // Create a two-column table and prepare a `SELECT *` point lookup. The
    // planner expands `*` against the schema at prepare() time, so the cached
    // logical plan projects exactly [id, v].
    run("CREATE TABLE kv (id INT PRIMARY KEY, v VARCHAR)", 1);
    let stmt = engine.prepare("SELECT * FROM kv WHERE id = :id").unwrap();
    run("INSERT INTO kv (id, v) VALUES (1, 'a')", 2);

    let mut params = std::collections::HashMap::new();
    params.insert("id".to_string(), DbValue::Int(1));

    // Against the original schema the prepared plan returns two columns.
    {
        let tx = Transaction::new(3, db.clone());
        let res = engine.execute_prepared(&stmt, &tx, &params).unwrap();
        tx.commit().unwrap();
        match res {
            ExecutionResult::Select { schema, rows } => {
                assert_eq!(schema.columns.len(), 2);
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("expected Select"),
        }
    }

    // DDL: drop and recreate the table with a third column. Each statement
    // bumps the catalog version, so the cached `SELECT *` plan is now stale.
    run("DROP TABLE kv", 4);
    run("CREATE TABLE kv (id INT PRIMARY KEY, v VARCHAR, w INT)", 5);
    run("INSERT INTO kv (id, v, w) VALUES (1, 'a', 42)", 6);

    // Re-executing the same prepared statement must reflect the NEW schema
    // (three columns). A stale plan would still expand `*` to two columns.
    {
        let tx = Transaction::new(7, db.clone());
        let res = engine.execute_prepared(&stmt, &tx, &params).unwrap();
        tx.commit().unwrap();
        match res {
            ExecutionResult::Select { schema, rows } => {
                assert_eq!(
                    schema.columns.len(),
                    3,
                    "stale plan would still project the old two columns"
                );
                assert_eq!(rows.len(), 1);
                assert_eq!(
                    rows[0].values,
                    vec![DbValue::Int(1), DbValue::string("a"), DbValue::Int(42)]
                );
            }
            _ => panic!("expected Select"),
        }
    }
}

#[test]
fn test_prepared_statement_insert_and_errors() {
    let (_temp, db, engine) = setup_engine();

    let tx_ddl = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, n INT)", &tx_ddl)
        .unwrap();
    tx_ddl.commit().unwrap();

    // A prepared INSERT reused with different bound rows.
    let ins = engine
        .prepare("INSERT INTO t (id, n) VALUES (:id, :n)")
        .unwrap();
    for i in 1..=3i64 {
        let tx = Transaction::new((1 + i) as u64, db.clone());
        let mut p = std::collections::HashMap::new();
        p.insert("id".to_string(), DbValue::Int(i));
        p.insert("n".to_string(), DbValue::Int(i * 10));
        engine.execute_prepared(&ins, &tx, &p).unwrap();
        tx.commit().unwrap();
    }

    let tx = Transaction::new(100, db.clone());
    let res = engine.execute("SELECT COUNT(*) FROM t", &tx).unwrap();
    match res {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows[0].values, vec![DbValue::Int(3)])
        }
        _ => panic!("expected Select"),
    }

    // prepare() rejects multi-statement input, like execute().
    assert!(engine.prepare("SELECT 1; SELECT 2").is_err());

    // Executing with a missing parameter binding is an error.
    let sel = engine.prepare("SELECT n FROM t WHERE id = :id").unwrap();
    let tx = Transaction::new(200, db.clone());
    let empty = std::collections::HashMap::new();
    assert!(engine.execute_prepared(&sel, &tx, &empty).is_err());
}

#[test]
fn test_prepared_update_and_delete_with_params() {
    let (_temp, db, engine) = setup_engine();

    let tx_ddl = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, n INT)", &tx_ddl)
        .unwrap();
    tx_ddl.commit().unwrap();

    let tx_ins = Transaction::new(2, db.clone());
    for i in 1..=5i64 {
        engine
            .execute(
                &format!("INSERT INTO t (id, n) VALUES ({i}, {})", i * 10),
                &tx_ins,
            )
            .unwrap();
    }
    tx_ins.commit().unwrap();

    // Prepared UPDATE with parameters in both SET and WHERE, reused with two
    // different bindings (exercises plan-cached substitution of Update exprs).
    let upd = engine
        .prepare("UPDATE t SET n = :n WHERE id = :id")
        .unwrap();
    for (id, n) in [(2i64, 999i64), (4, 888)] {
        let tx = Transaction::new(10 + id as u64, db.clone());
        let mut p = std::collections::HashMap::new();
        p.insert("n".to_string(), DbValue::Int(n));
        p.insert("id".to_string(), DbValue::Int(id));
        engine.execute_prepared(&upd, &tx, &p).unwrap();
        tx.commit().unwrap();
    }

    let tx = Transaction::new(50, db.clone());
    let res = engine
        .execute("SELECT id, n FROM t ORDER BY id", &tx)
        .unwrap();
    match res {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows[1].values, vec![DbValue::Int(2), DbValue::Int(999)]);
            assert_eq!(rows[3].values, vec![DbValue::Int(4), DbValue::Int(888)]);
        }
        _ => panic!("expected Select"),
    }

    // Prepared DELETE with a parameter in WHERE.
    let del = engine.prepare("DELETE FROM t WHERE id = :id").unwrap();
    let tx = Transaction::new(60, db.clone());
    let mut p = std::collections::HashMap::new();
    p.insert("id".to_string(), DbValue::Int(1));
    engine.execute_prepared(&del, &tx, &p).unwrap();
    tx.commit().unwrap();

    let tx = Transaction::new(70, db.clone());
    let res = engine.execute("SELECT COUNT(*) FROM t", &tx).unwrap();
    match res {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows[0].values, vec![DbValue::Int(4)])
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn test_sum_preserves_int_type_and_handles_overflow() {
    // Regression for F31: SUM(int_col) used to finalize as DbValue::Float
    // unconditionally. f64 loses precision past 2^53, so summing many int64
    // values produced wrong totals; worse, the runtime type disagreed with
    // the schema declared by the logical planner (which used the input
    // column's type for SUM).
    let (_temp, db, engine) = setup_engine();

    let tx_ddl = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE nums (id INT PRIMARY KEY, n INT, f FLOAT)",
            &tx_ddl,
        )
        .unwrap();
    tx_ddl.commit().unwrap();

    let tx_insert = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO nums (id, n, f) VALUES (1, 10, 1.5), (2, 20, 2.5), (3, 30, 3.0)",
            &tx_insert,
        )
        .unwrap();
    tx_insert.commit().unwrap();

    // SUM over an INT column must stay Int.
    let tx_q = Transaction::new(3, db.clone());
    let res = engine.execute("SELECT SUM(n) FROM nums", &tx_q).unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows[0].values, vec![DbValue::Int(60)]);
    } else {
        panic!("expected Select");
    }

    // SUM over a FLOAT column stays Float.
    let res = engine.execute("SELECT SUM(f) FROM nums", &tx_q).unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows[0].values, vec![DbValue::Float(7.0)]);
    } else {
        panic!("expected Select");
    }

    // SUM that would overflow i64 promotes to Float instead of panicking
    // (no precision loss for these small magnitudes, but the result type
    // signals that the integer path bailed out).
    let tx_ins2 = Transaction::new(4, db.clone());
    engine
        .execute(
            "INSERT INTO nums (id, n, f) VALUES (4, 9223372036854775807, 0.0), (5, 1, 0.0)",
            &tx_ins2,
        )
        .unwrap();
    tx_ins2.commit().unwrap();

    let tx_q2 = Transaction::new(5, db.clone());
    let res = engine
        .execute("SELECT SUM(n) FROM nums WHERE id IN (4, 5)", &tx_q2)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        match rows[0].values[0] {
            DbValue::Float(_) => {} // promoted on overflow
            ref other => panic!("expected Float (overflow promotion), got {other:?}"),
        }
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_substr_handles_extreme_length() {
    // Regression for F32: SUBSTR(s, start, i64::MAX) used to overflow on
    // `start_idx + length`, panicking in debug and wrapping to a negative
    // value in release. Saturating-add fixes it.
    let (_temp, db, engine) = setup_engine();

    let tx_ddl = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, s STRING)", &tx_ddl)
        .unwrap();
    tx_ddl.commit().unwrap();

    let tx_ins = Transaction::new(2, db.clone());
    engine
        .execute("INSERT INTO t (id, s) VALUES (1, 'hello')", &tx_ins)
        .unwrap();
    tx_ins.commit().unwrap();

    let tx_q = Transaction::new(3, db.clone());
    let res = engine
        .execute(
            "SELECT SUBSTR(s, 1, 9223372036854775807) FROM t WHERE id = 1",
            &tx_q,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows[0].values, vec![DbValue::string("hello")]);
    } else {
        panic!("expected Select");
    }

    // Negative length is now rejected explicitly.
    let res = engine.execute("SELECT SUBSTR(s, 1, -1) FROM t WHERE id = 1", &tx_q);
    assert!(res.is_err());
    assert!(res.unwrap_err().contains("non-negative"));
}

#[test]
fn test_like_handles_adversarial_pattern_in_bounded_time() {
    // Regression for F33: the old recursive-backtracking matcher took
    // exponential time on patterns like `%a%a%a%a%a%a%a%b` against a long
    // non-matching string and could effectively hang a query thread. The
    // regex-backed implementation uses RE2-style NFA simulation and is
    // guaranteed to be linear in the input.
    let (_temp, db, engine) = setup_engine();

    let tx_ddl = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, s STRING)", &tx_ddl)
        .unwrap();
    tx_ddl.commit().unwrap();

    let long_text = "a".repeat(100);
    let tx_ins = Transaction::new(2, db.clone());
    engine
        .execute(
            &format!("INSERT INTO t (id, s) VALUES (1, '{}')", long_text),
            &tx_ins,
        )
        .unwrap();
    tx_ins.commit().unwrap();

    // This pattern is the canonical worst case for naive LIKE. We don't
    // measure wall-clock time here (CI variance), but if we ever regress to
    // backtracking it would take many seconds to minutes on this input.
    let tx_q = Transaction::new(3, db.clone());
    #[cfg(not(miri))]
    let start = std::time::Instant::now();
    let res = engine
        .execute("SELECT id FROM t WHERE s LIKE '%a%a%a%a%a%a%a%b'", &tx_q)
        .unwrap();
    #[cfg(not(miri))]
    {
        let elapsed = start.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "LIKE took too long ({elapsed:?}) — backtracking regression?"
        );
    }
    if let ExecutionResult::Select { rows, .. } = res {
        assert!(
            rows.is_empty(),
            "non-matching pattern should return no rows"
        );
    } else {
        panic!("expected Select");
    }

    // Sanity: positive matches still work, including regex metacharacters
    // that must not leak through the translator.
    let tx_ins2 = Transaction::new(4, db.clone());
    engine
        .execute("INSERT INTO t (id, s) VALUES (2, 'a.b+c')", &tx_ins2)
        .unwrap();
    tx_ins2.commit().unwrap();
    let tx_q2 = Transaction::new(5, db.clone());
    let res = engine
        .execute("SELECT id FROM t WHERE s LIKE 'a.b+c'", &tx_q2)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1, "literal '.' and '+' must match exactly");
        assert_eq!(rows[0].values, vec![DbValue::Int(2)]);
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_ambiguous_column_reference_is_rejected() {
    // Regression for F34: after JOIN-ing two tables that each expose a column
    // with the same trailing name, an unqualified reference (`WHERE name = ?`)
    // used to silently bind to whichever column appeared first in the joined
    // schema. That made queries depend on join order and column ordering —
    // surprising, non-portable, and a frequent silent data bug.
    let (_temp, db, engine) = setup_engine();

    let tx_ddl = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, name STRING)", &tx_ddl)
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, name STRING)", &tx_ddl)
        .unwrap();
    tx_ddl.commit().unwrap();

    let tx_ins = Transaction::new(2, db.clone());
    engine
        .execute("INSERT INTO a (id, name) VALUES (1, 'alice')", &tx_ins)
        .unwrap();
    engine
        .execute("INSERT INTO b (id, name) VALUES (1, 'bob')", &tx_ins)
        .unwrap();
    tx_ins.commit().unwrap();

    let tx_q = Transaction::new(3, db.clone());

    // Unqualified `name` in the SELECT projection is ambiguous after the
    // join (both `a` and `b` expose a `name` column). The projection binds
    // against the joined schema, so predicate-pushdown can't hide the
    // duplicate and the planner must surface the conflict.
    let res = engine.execute("SELECT a.id, name FROM a JOIN b ON a.id = b.id", &tx_q);
    assert!(res.is_err(), "ambiguous column should error, got {res:?}");
    let err = res.unwrap_err();
    assert!(
        err.to_lowercase().contains("ambiguous"),
        "expected an ambiguity message, got: {err}"
    );

    // Qualifying the column makes the query well-formed.
    let res = engine
        .execute("SELECT a.id, a.name FROM a JOIN b ON a.id = b.id", &tx_q)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
    } else {
        panic!("expected Select");
    }
}

#[test]
fn test_optimizer_bound_nudging_handles_int_extremes() {
    // Regression for F35: range pushdown used to nudge bounds with raw `v + 1`
    // or `v - 1` arithmetic, so `WHERE id > i64::MAX` overflowed (and a later
    // patch replaced that with `saturating_add`, which still produced a
    // surprising non-empty [MAX, MAX] scan). Either way, the executor opened
    // a degenerate scan and relied on the original predicate getting
    // re-evaluated to drop the single returned row.
    //
    // The optimizer should now decline the pushdown in those impossible
    // cases. Behaviorally, the query must return zero rows without scanning
    // any data — and crucially must not error.
    let (_temp, db, engine) = setup_engine();

    let tx_ddl = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)", &tx_ddl)
        .unwrap();
    tx_ddl.commit().unwrap();

    let tx_ins = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO t (id, val) VALUES (1, 1), (9223372036854775807, 9999)",
            &tx_ins,
        )
        .unwrap();
    tx_ins.commit().unwrap();

    let tx_q = Transaction::new(3, db.clone());

    // `id > MAX` is the empty set.
    let res = engine
        .execute("SELECT id FROM t WHERE id > 9223372036854775807", &tx_q)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert!(rows.is_empty(), "no row should match id > i64::MAX");
    } else {
        panic!("expected Select");
    }

    // `id < MIN` is also empty.
    let res = engine
        .execute("SELECT id FROM t WHERE id < -9223372036854775808", &tx_q)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert!(rows.is_empty(), "no row should match id < i64::MIN");
    } else {
        panic!("expected Select");
    }

    // Plain `>=` and `<=` against the boundaries still return matching rows.
    let res = engine
        .execute("SELECT id FROM t WHERE id >= 9223372036854775807", &tx_q)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values, vec![DbValue::Int(i64::MAX)]);
    } else {
        panic!("expected Select");
    }

    // EXPLAIN must not panic on the degenerate forms.
    let _ = engine
        .execute(
            "EXPLAIN SELECT id FROM t WHERE id > 9223372036854775807",
            &tx_q,
        )
        .unwrap();
}

// ==========================================
// Disk-spilling parity tests (tiny memory budget forces the external path)
// ==========================================

#[test]
fn test_spilling_hash_aggregate() {
    // Tiny budget forces PhysicalHashAggregate to spill partial groups to disk and
    // merge+combine them at finalize time. Results must match the in-memory path.
    let (_temp, db, engine) = setup_engine_with_options(opts_with_budget(Some(200)));

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, amount INT)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();
    drop(tx1);

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO sales (id, region, amount) VALUES \
             (1, 'east', 10), (2, 'west', 20), (3, 'east', 30), (4, 'north', 40), \
             (5, 'west', 50), (6, 'south', 60), (7, 'east', 70), (8, 'north', 80), \
             (9, 'south', 90), (10, 'west', 100), (11, 'east', 5), (12, 'north', 15)",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();
    drop(tx2);

    let tx3 = Transaction::new(3, db.clone());
    let res = engine
        .execute(
            "SELECT region, COUNT(*), SUM(amount), MIN(amount), MAX(amount), AVG(amount) \
             FROM sales GROUP BY region ORDER BY region",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        // east: 10,30,70,5 -> cnt 4, sum 115, min 5, max 70, avg 28.75
        // north: 40,80,15 -> cnt 3, sum 135, min 15, max 80, avg 45
        // south: 60,90 -> cnt 2, sum 150, min 60, max 90, avg 75
        // west: 20,50,100 -> cnt 3, sum 170, min 20, max 100, avg ~56.666...
        assert_eq!(rows.len(), 4);
        assert_eq!(
            rows[0].values,
            vec![
                DbValue::String("east".into()),
                DbValue::Int(4),
                DbValue::Int(115),
                DbValue::Int(5),
                DbValue::Int(70),
                DbValue::Float(28.75),
            ]
        );
        assert_eq!(
            rows[1].values,
            vec![
                DbValue::String("north".into()),
                DbValue::Int(3),
                DbValue::Int(135),
                DbValue::Int(15),
                DbValue::Int(80),
                DbValue::Float(45.0),
            ]
        );
        assert_eq!(
            rows[2].values,
            vec![
                DbValue::String("south".into()),
                DbValue::Int(2),
                DbValue::Int(150),
                DbValue::Int(60),
                DbValue::Int(90),
                DbValue::Float(75.0),
            ]
        );
        assert_eq!(rows[3].values[0], DbValue::String("west".into()));
        assert_eq!(rows[3].values[1], DbValue::Int(3));
        assert_eq!(rows[3].values[2], DbValue::Int(170));
        assert_eq!(rows[3].values[3], DbValue::Int(20));
        assert_eq!(rows[3].values[4], DbValue::Int(100));
    } else {
        panic!("Expected Select result");
    }
    drop(tx3);

    assert_tmp_empty(&db);
}

#[test]
fn test_spilling_distinct() {
    let (_temp, db, engine) = setup_engine_with_options(opts_with_budget(Some(150)));

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)", &tx1)
        .unwrap();
    tx1.commit().unwrap();
    drop(tx1);

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO t (id, val) VALUES \
             (1, 3), (2, 1), (3, 2), (4, 3), (5, 1), (6, 2), (7, 3), (8, 4), (9, 1), (10, 5)",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();
    drop(tx2);

    let tx3 = Transaction::new(3, db.clone());
    let res = engine
        .execute("SELECT DISTINCT val FROM t ORDER BY val", &tx3)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        let vals: Vec<i64> = rows
            .into_iter()
            .map(|r| match r.values[0] {
                DbValue::Int(v) => v,
                _ => panic!("Expected Int"),
            })
            .collect();
        assert_eq!(vals, vec![1, 2, 3, 4, 5]);
    } else {
        panic!("Expected Select result");
    }
    drop(tx3);

    assert_tmp_empty(&db);
}

#[test]
fn test_spilling_set_ops() {
    let (_temp, db, engine) = setup_engine_with_options(opts_with_budget(Some(150)));

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE a (id INT PRIMARY KEY, val INT)", &tx1)
        .unwrap();
    engine
        .execute("CREATE TABLE b (id INT PRIMARY KEY, val INT)", &tx1)
        .unwrap();
    tx1.commit().unwrap();
    drop(tx1);

    let tx2 = Transaction::new(2, db.clone());
    engine
        .execute(
            "INSERT INTO a (id, val) VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)",
            &tx2,
        )
        .unwrap();
    engine
        .execute(
            "INSERT INTO b (id, val) VALUES (1, 4), (2, 5), (3, 6), (4, 7), (5, 8), (6, 9)",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();
    drop(tx2);

    let collect_vals = |res: ExecutionResult| -> Vec<i64> {
        if let ExecutionResult::Select { rows, .. } = res {
            rows.into_iter()
                .map(|r| match r.values[0] {
                    DbValue::Int(v) => v,
                    _ => panic!("Expected Int"),
                })
                .collect()
        } else {
            panic!("Expected Select result");
        }
    };

    let tx3 = Transaction::new(3, db.clone());
    let inter = engine
        .execute(
            "SELECT val FROM a INTERSECT SELECT val FROM b ORDER BY val",
            &tx3,
        )
        .unwrap();
    assert_eq!(collect_vals(inter), vec![4, 5, 6]);

    let except = engine
        .execute(
            "SELECT val FROM a EXCEPT SELECT val FROM b ORDER BY val",
            &tx3,
        )
        .unwrap();
    assert_eq!(collect_vals(except), vec![1, 2, 3]);

    let union = engine
        .execute(
            "SELECT val FROM a UNION SELECT val FROM b ORDER BY val",
            &tx3,
        )
        .unwrap();
    assert_eq!(collect_vals(union), vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    drop(tx3);

    assert_tmp_empty(&db);
}

#[test]
fn test_spilling_sort_merge_join() {
    let (_temp, db, engine) = setup_engine_with_options(opts_with_budget(Some(150)));

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE l (id INT PRIMARY KEY, k INT, name TEXT)",
            &tx1,
        )
        .unwrap();
    engine
        .execute("CREATE TABLE r (id INT PRIMARY KEY, k INT, tag TEXT)", &tx1)
        .unwrap();
    tx1.commit().unwrap();
    drop(tx1);

    let tx2 = Transaction::new(2, db.clone());
    // Left rows: k=1 (two rows), k=2, k=3 (no right match), k=NULL (never matches).
    engine
        .execute(
            "INSERT INTO l (id, k, name) VALUES \
             (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'c'), (4, 3, 'd'), (5, NULL, 'e')",
            &tx2,
        )
        .unwrap();
    // Right rows: k=1 (two rows -> many-to-many), k=2, k=4 (no left match).
    engine
        .execute(
            "INSERT INTO r (id, k, tag) VALUES \
             (1, 1, 'x'), (2, 1, 'y'), (3, 2, 'z'), (4, 4, 'w')",
            &tx2,
        )
        .unwrap();
    tx2.commit().unwrap();
    drop(tx2);

    let tx3 = Transaction::new(3, db.clone());
    // Inner join: k=1 -> 2 left * 2 right = 4 rows; k=2 -> 1 row. Total 5.
    let inner = engine
        .execute(
            "SELECT l.name, r.tag FROM l JOIN r ON l.k = r.k ORDER BY l.name, r.tag",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = inner {
        let pairs: Vec<(String, String)> = rows
            .into_iter()
            .map(|row| match (&row.values[0], &row.values[1]) {
                (DbValue::String(a), DbValue::String(b)) => (a.to_string(), b.to_string()),
                _ => panic!("Expected strings"),
            })
            .collect();
        assert_eq!(
            pairs,
            vec![
                ("a".into(), "x".into()),
                ("a".into(), "y".into()),
                ("b".into(), "x".into()),
                ("b".into(), "y".into()),
                ("c".into(), "z".into()),
            ]
        );
    } else {
        panic!("Expected Select result");
    }

    // Left outer join: all 5 left rows appear; k=3 and k=NULL get NULL tags.
    let left = engine
        .execute(
            "SELECT l.name, r.tag FROM l LEFT JOIN r ON l.k = r.k ORDER BY l.name, r.tag",
            &tx3,
        )
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = left {
        assert_eq!(rows.len(), 7); // 5 inner matches + d(NULL) + e(NULL)
        let unmatched: Vec<String> = rows
            .iter()
            .filter(|row| row.values[1] == DbValue::Null)
            .map(|row| match &row.values[0] {
                DbValue::String(s) => s.to_string(),
                _ => panic!("Expected string"),
            })
            .collect();
        assert_eq!(unmatched, vec!["d".to_string(), "e".to_string()]);
    } else {
        panic!("Expected Select result");
    }
    drop(tx3);

    assert_tmp_empty(&db);
}

/// Runs a fixed battery of spilling queries over an identical, larger-than-budget
/// dataset under two memory budgets and asserts the results are identical. This is the
/// strongest spill check: the tiny budget forces many runs through the external merge
/// paths, the large budget stays fully in memory, and any divergence (lost rows,
/// mis-merged accumulators, dedup gaps) shows up as a mismatch.
#[test]
fn test_spilling_parity_against_in_memory() {
    fn run_battery(memory_budget: Option<usize>) -> Vec<Vec<dtdb_relational::Row>> {
        let (_temp, db, engine) = setup_engine_with_options(opts_with_budget(memory_budget));

        let tx1 = Transaction::new(1, db.clone());
        engine
            .execute("CREATE TABLE l (id INT PRIMARY KEY, k INT, g TEXT)", &tx1)
            .unwrap();
        engine
            .execute("CREATE TABLE r (id INT PRIMARY KEY, k INT)", &tx1)
            .unwrap();
        tx1.commit().unwrap();
        drop(tx1);

        // 300 left rows; k cycles 0..30 (10 each), g cycles a..j. Right: 150 rows, k 0..15.
        let tx2 = Transaction::new(2, db.clone());
        let mut lvals = Vec::new();
        for i in 0..300 {
            let k = i % 30;
            let g = (b'a' + (i % 10) as u8) as char;
            lvals.push(format!("({i}, {k}, '{g}')"));
        }
        engine
            .execute(
                &format!("INSERT INTO l (id, k, g) VALUES {}", lvals.join(", ")),
                &tx2,
            )
            .unwrap();
        let rvals: Vec<String> = (0..150).map(|i| format!("({i}, {})", i % 15)).collect();
        engine
            .execute(
                &format!("INSERT INTO r (id, k) VALUES {}", rvals.join(", ")),
                &tx2,
            )
            .unwrap();
        tx2.commit().unwrap();
        drop(tx2);

        let queries = [
            "SELECT g, COUNT(*), SUM(k), MIN(k), MAX(k), AVG(k) FROM l GROUP BY g ORDER BY g",
            "SELECT DISTINCT k FROM l ORDER BY k",
            "SELECT k FROM l INTERSECT SELECT k FROM r ORDER BY k",
            "SELECT k FROM l EXCEPT SELECT k FROM r ORDER BY k",
            "SELECT l.g, r.id FROM l JOIN r ON l.k = r.k ORDER BY l.g, l.id, r.id",
            "SELECT l.id, r.id FROM l LEFT JOIN r ON l.k = r.k ORDER BY l.id, r.id",
        ];

        let tx3 = Transaction::new(3, db.clone());
        let mut results = Vec::new();
        for q in queries {
            match engine.execute(q, &tx3).unwrap() {
                ExecutionResult::Select { rows, .. } => results.push(rows),
                _ => panic!("Expected Select for query: {q}"),
            }
        }
        drop(tx3);

        // Spill temp files must be cleaned up regardless of budget.
        assert_tmp_empty(&db);
        results
    }

    let spilled = run_battery(Some(256)); // tiny budget -> heavy spilling
    let in_memory = run_battery(None); // 8 MiB default -> fully in memory
    assert_eq!(
        spilled, in_memory,
        "spilling output diverged from the in-memory path"
    );
}

/// The parse cache keys on SQL text, so re-executing the same parameterized
/// template reuses one parsed AST. This guards correctness: the cached AST keeps
/// its symbolic placeholders (binding happens on a per-execution clone), so
/// repeated executions with different parameters must not leak into each other.
#[test]
fn test_sql_parse_cache_param_isolation() {
    use std::collections::HashMap;
    let (_temp, db, engine) = setup_engine();

    let tx = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v STRING)", &tx)
        .unwrap();
    tx.commit().unwrap();

    // Insert three rows through the SAME template -> one parse, then cache hits.
    let insert_tpl = "INSERT INTO t (id, v) VALUES (@id, @v)";
    let tx = Transaction::new(2, db.clone());
    for (id, v) in [(1i64, "one"), (2, "two"), (3, "three")] {
        let mut params = HashMap::new();
        params.insert("id".to_string(), DbValue::Int(id));
        params.insert("v".to_string(), DbValue::string(v.to_string()));
        let res = engine
            .execute_with_params(insert_tpl, &tx, &params)
            .unwrap();
        assert_eq!(res, ExecutionResult::Insert { count: 1 });
    }
    tx.commit().unwrap();

    // Select via the SAME template with different @id. If the first bind had
    // mutated the cached AST, later calls would return the wrong row.
    let select_tpl = "SELECT v FROM t WHERE id = @id";
    let select_one = |id: i64| -> String {
        let tx = Transaction::new(100 + id as u64, db.clone());
        let mut params = HashMap::new();
        params.insert("id".to_string(), DbValue::Int(id));
        let res = engine
            .execute_with_params(select_tpl, &tx, &params)
            .unwrap();
        tx.commit().unwrap();
        match res {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 1, "expected exactly one row for id={id}");
                match &rows[0].values[0] {
                    DbValue::String(s) => s.to_string(),
                    other => panic!("unexpected value {other:?}"),
                }
            }
            other => panic!("expected Select, got {other:?}"),
        }
    };

    // Out-of-order, with a repeat, to exercise hits against a mutated-AST bug.
    assert_eq!(select_one(3), "three");
    assert_eq!(select_one(1), "one");
    assert_eq!(select_one(2), "two");
    assert_eq!(select_one(3), "three");
}

#[test]
fn test_plan_cache_registry() {
    let (_temp, db, engine) = setup_engine();

    let tx_ddl = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE kv (id INT PRIMARY KEY, v VARCHAR)", &tx_ddl)
        .unwrap();
    tx_ddl.commit().unwrap();

    // Cache starts empty
    assert_eq!(engine.plan_cache_len(), 0);

    // Preparing a query inserts it into the plan cache registry
    let _stmt1 = engine.prepare("SELECT v FROM kv WHERE id = :id").unwrap();
    assert_eq!(engine.plan_cache_len(), 1);

    // Re-preparing the exact same statement hits the cache (cache length stays 1)
    let stmt2 = engine.prepare("SELECT v FROM kv WHERE id = :id").unwrap();
    assert_eq!(engine.plan_cache_len(), 1);

    // Check that we can execute it successfully
    let tx = Transaction::new(2, db.clone());
    let mut params = std::collections::HashMap::new();
    params.insert("id".to_string(), DbValue::Int(1));
    let _ = engine.execute_prepared(&stmt2, &tx, &params).unwrap();
    tx.commit().unwrap();

    // Preparing a different query increases cache length to 2
    let _stmt3 = engine.prepare("SELECT * FROM kv").unwrap();
    assert_eq!(engine.plan_cache_len(), 2);

    // Perform a DDL command to bump catalog/schema version
    let tx_ddl2 = Transaction::new(3, db.clone());
    engine
        .execute("CREATE TABLE kv2 (id INT PRIMARY KEY)", &tx_ddl2)
        .unwrap();
    tx_ddl2.commit().unwrap();

    // Stale plans are replaced when prepared again. Length should still be 2 after preparation
    // since it overwrites the stale cache entries.
    let stmt4 = engine.prepare("SELECT v FROM kv WHERE id = :id").unwrap();
    assert_eq!(engine.plan_cache_len(), 2);
    // Make sure it runs under the new schema
    let tx_new = Transaction::new(4, db.clone());
    let mut params = std::collections::HashMap::new();
    params.insert("id".to_string(), DbValue::Int(1));
    let _res = engine.execute_prepared(&stmt4, &tx_new, &params).unwrap();
    tx_new.commit().unwrap();

    // Eviction test: fill the plan cache beyond capacity (1024)
    // We can prepare 1025 unique queries. Since each unique query is distinct,
    // it will eventually evict the oldest.
    for i in 0..1025 {
        let sql = format!("SELECT v FROM kv WHERE id = :id OR id = {}", i);
        let _ = engine.prepare(&sql).unwrap();
    }
    // Check that the plan cache does not exceed 1024
    assert_eq!(engine.plan_cache_len(), 1024);
}

// ----- ALTER TABLE (lazy ADD / RENAME COLUMN, ADR 0003) -----
//
// Each transaction is scoped in its own block so it is dropped -- releasing its
// `active_table_access` slot -- before a later ALTER, whose stop-the-world
// quiesce spin-waits for active readers of the table. (Real callers go through
// `run_in_transaction`, which drops the transaction promptly; these white-box
// tests construct `Transaction` directly and must scope it themselves.)

/// `ADD COLUMN ... DEFAULT <const>`: rows inserted before the ALTER read back
/// with the default; rows inserted after carry their own value. End-to-end
/// through the SQL engine, exercising parse -> plan -> execute -> reconcile.
#[test]
fn test_alter_table_add_column_with_default() {
    let (_temp, db, engine) = setup_engine();

    {
        let tx = Transaction::new(1, db.clone());
        engine
            .execute("CREATE TABLE users (id INT PRIMARY KEY, name STRING)", &tx)
            .unwrap();
        engine
            .execute("INSERT INTO users (id, name) VALUES (1, 'Alice')", &tx)
            .unwrap();
        tx.commit().unwrap();
    }

    // ADD COLUMN with a default; the existing row predates it.
    {
        let tx = Transaction::new(2, db.clone());
        let res = engine
            .execute("ALTER TABLE users ADD COLUMN active BOOL DEFAULT true", &tx)
            .unwrap();
        assert!(matches!(res, ExecutionResult::AlterTable));
        tx.commit().unwrap();
    }

    // Insert a post-ALTER row that sets the new column explicitly.
    {
        let tx = Transaction::new(3, db.clone());
        engine
            .execute(
                "INSERT INTO users (id, name, active) VALUES (2, 'Bob', false)",
                &tx,
            )
            .unwrap();
        tx.commit().unwrap();
    }

    let tx = Transaction::new(4, db.clone());
    let res = engine
        .execute("SELECT id, name, active FROM users ORDER BY id ASC", &tx)
        .unwrap();
    if let ExecutionResult::Select { schema, rows } = res {
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns[2].name, "active");
        // Old row reconciles to the default; new row keeps its explicit value.
        assert_eq!(
            rows[0].values,
            vec![
                DbValue::Int(1),
                DbValue::string("Alice"),
                DbValue::Bool(true),
            ]
        );
        assert_eq!(
            rows[1].values,
            vec![
                DbValue::Int(2),
                DbValue::string("Bob"),
                DbValue::Bool(false),
            ]
        );
    } else {
        panic!("Expected Select");
    }
}

/// `ADD COLUMN` with no default pads existing rows with NULL.
#[test]
fn test_alter_table_add_column_no_default_is_null() {
    let (_temp, db, engine) = setup_engine();
    {
        let tx = Transaction::new(1, db.clone());
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY)", &tx)
            .unwrap();
        engine
            .execute("INSERT INTO t (id) VALUES (1)", &tx)
            .unwrap();
        tx.commit().unwrap();
    }
    {
        let tx = Transaction::new(2, db.clone());
        engine
            .execute("ALTER TABLE t ADD COLUMN note STRING", &tx)
            .unwrap();
        tx.commit().unwrap();
    }

    let tx = Transaction::new(3, db.clone());
    let res = engine.execute("SELECT id, note FROM t", &tx).unwrap();
    if let ExecutionResult::Select { rows, .. } = res {
        assert_eq!(rows[0].values, vec![DbValue::Int(1), DbValue::Null]);
    } else {
        panic!("Expected Select");
    }
}

/// `ADD COLUMN NOT NULL` without a default is rejected on a non-empty table,
/// but allowed on an empty one.
#[test]
fn test_alter_table_add_not_null_without_default() {
    let (_temp, db, engine) = setup_engine();

    // Empty table: NOT NULL without default is allowed (no rows to violate it).
    {
        let tx = Transaction::new(1, db.clone());
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY)", &tx)
            .unwrap();
        assert!(
            engine
                .execute("ALTER TABLE t ADD COLUMN c INT NOT NULL", &tx)
                .is_ok()
        );
        tx.commit().unwrap();
    }

    // Non-empty table: now rejected.
    {
        let tx = Transaction::new(2, db.clone());
        engine
            .execute("CREATE TABLE u (id INT PRIMARY KEY)", &tx)
            .unwrap();
        engine
            .execute("INSERT INTO u (id) VALUES (1)", &tx)
            .unwrap();
        tx.commit().unwrap();
    }

    let tx = Transaction::new(3, db.clone());
    let err = engine
        .execute("ALTER TABLE u ADD COLUMN c INT NOT NULL", &tx)
        .unwrap_err();
    assert!(err.contains("NOT NULL"), "unexpected error: {err}");
}

/// A duplicate column name is rejected.
#[test]
fn test_alter_table_add_duplicate_column_rejected() {
    let (_temp, db, engine) = setup_engine();
    let tx = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, name STRING)", &tx)
        .unwrap();
    let err = engine
        .execute("ALTER TABLE t ADD COLUMN name STRING", &tx)
        .unwrap_err();
    assert!(err.contains("already exists"), "unexpected error: {err}");
}

/// `RENAME COLUMN` changes the column name; queries use the new name and the
/// old name no longer resolves.
#[test]
fn test_alter_table_rename_column() {
    let (_temp, db, engine) = setup_engine();
    {
        let tx = Transaction::new(1, db.clone());
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, name STRING)", &tx)
            .unwrap();
        engine
            .execute("INSERT INTO t (id, name) VALUES (1, 'Alice')", &tx)
            .unwrap();
        tx.commit().unwrap();
    }

    {
        let tx = Transaction::new(2, db.clone());
        let res = engine
            .execute("ALTER TABLE t RENAME COLUMN name TO full_name", &tx)
            .unwrap();
        assert!(matches!(res, ExecutionResult::AlterTable));
        tx.commit().unwrap();
    }

    // New name resolves and returns the original data.
    {
        let tx = Transaction::new(3, db.clone());
        let res = engine.execute("SELECT id, full_name FROM t", &tx).unwrap();
        if let ExecutionResult::Select { rows, .. } = res {
            assert_eq!(
                rows[0].values,
                vec![DbValue::Int(1), DbValue::string("Alice")]
            );
        } else {
            panic!("Expected Select");
        }
    }

    // Old name no longer resolves.
    let tx = Transaction::new(4, db.clone());
    assert!(engine.execute("SELECT name FROM t", &tx).is_err());
}

/// `DROP COLUMN` drops the column; queries use the new schema and the dropped
/// column no longer resolves.
#[test]
fn test_alter_table_drop_column() {
    let (_temp, db, engine) = setup_engine();
    {
        let tx = Transaction::new(1, db.clone());
        engine
            .execute(
                "CREATE TABLE t (id INT PRIMARY KEY, name STRING, score FLOAT)",
                &tx,
            )
            .unwrap();
        engine
            .execute(
                "INSERT INTO t (id, name, score) VALUES (1, 'Alice', 95.5)",
                &tx,
            )
            .unwrap();
        tx.commit().unwrap();
    }

    {
        let tx = Transaction::new(2, db.clone());
        let res = engine
            .execute("ALTER TABLE t DROP COLUMN score", &tx)
            .unwrap();
        assert!(matches!(res, ExecutionResult::AlterTable));
        tx.commit().unwrap();
    }

    // New schema has only id and name.
    {
        let tx = Transaction::new(3, db.clone());
        let res = engine.execute("SELECT id, name FROM t", &tx).unwrap();
        if let ExecutionResult::Select { rows, .. } = res {
            assert_eq!(
                rows[0].values,
                vec![DbValue::Int(1), DbValue::string("Alice")]
            );
        } else {
            panic!("Expected Select");
        }
    }

    // Dropped column no longer resolves.
    let tx = Transaction::new(4, db.clone());
    assert!(engine.execute("SELECT score FROM t", &tx).is_err());
}

#[test]
fn test_sql_date_time_timestamp_decimal() {
    use chrono::{NaiveDate, NaiveTime};
    use rust_decimal::Decimal;
    use std::str::FromStr;

    let (_temp, db, engine) = setup_engine();

    // 1. Create table with Date, Time, Timestamp, and Decimal columns
    {
        let tx = Transaction::new(1, db.clone());
        engine
            .execute(
                "CREATE TABLE test_types (
                    id INT PRIMARY KEY,
                    d DATE,
                    t TIME,
                    ts TIMESTAMP,
                    dec DECIMAL
                )",
                &tx,
            )
            .unwrap();
        tx.commit().unwrap();
    }

    // 2. Insert values using typed literals
    {
        let tx = Transaction::new(2, db.clone());
        engine
            .execute(
                "INSERT INTO test_types (id, d, t, ts, dec) VALUES (
                    1,
                    DATE '2026-06-02',
                    TIME '12:34:56',
                    TIMESTAMP '2026-06-02 12:34:56',
                    DECIMAL '123.45'
                )",
                &tx,
            )
            .unwrap();
        tx.commit().unwrap();
    }

    // 3. Insert values using typed literals for Row 2
    {
        let tx = Transaction::new(3, db.clone());
        engine
            .execute(
                "INSERT INTO test_types (id, d, t, ts, dec) VALUES (
                    2,
                    DATE '2026-06-03',
                    TIME '13:45:00',
                    TIMESTAMP '2026-06-03 13:45:00',
                    DECIMAL '678.90'
                )",
                &tx,
            )
            .unwrap();
        tx.commit().unwrap();
    }

    // 3b. Test explicit CAST using SELECT
    {
        let tx = Transaction::new(100, db.clone());
        let res = engine
            .execute(
                "SELECT
                    CAST('2026-06-03' AS DATE),
                    CAST('13:45:00' AS TIME),
                    CAST('2026-06-03 13:45:00' AS TIMESTAMP),
                    CAST('678.90' AS DECIMAL)
                 FROM test_types WHERE id = 1",
                &tx,
            )
            .unwrap();
        if let ExecutionResult::Select { rows, .. } = res {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].values[0],
                DbValue::Date(NaiveDate::from_ymd_opt(2026, 6, 3).unwrap())
            );
            assert_eq!(
                rows[0].values[1],
                DbValue::Time(NaiveTime::from_hms_opt(13, 45, 0).unwrap())
            );
            assert_eq!(
                rows[0].values[2],
                DbValue::Timestamp(
                    NaiveDate::from_ymd_opt(2026, 6, 3)
                        .unwrap()
                        .and_hms_opt(13, 45, 0)
                        .unwrap()
                )
            );
            assert_eq!(
                rows[0].values[3],
                DbValue::Decimal(Decimal::from_str("678.90").unwrap())
            );
        } else {
            panic!("Expected Select result");
        }
    }

    // 4. Select and assert the inserted values
    {
        let tx = Transaction::new(4, db.clone());
        let res = engine
            .execute(
                "SELECT id, d, t, ts, dec FROM test_types ORDER BY id ASC",
                &tx,
            )
            .unwrap();
        if let ExecutionResult::Select { rows, .. } = res {
            assert_eq!(rows.len(), 2);

            // Row 1
            assert_eq!(rows[0].values[0], DbValue::Int(1));
            assert_eq!(
                rows[0].values[1],
                DbValue::Date(NaiveDate::from_ymd_opt(2026, 6, 2).unwrap())
            );
            assert_eq!(
                rows[0].values[2],
                DbValue::Time(NaiveTime::from_hms_opt(12, 34, 56).unwrap())
            );
            assert_eq!(
                rows[0].values[3],
                DbValue::Timestamp(
                    NaiveDate::from_ymd_opt(2026, 6, 2)
                        .unwrap()
                        .and_hms_opt(12, 34, 56)
                        .unwrap()
                )
            );
            assert_eq!(
                rows[0].values[4],
                DbValue::Decimal(Decimal::from_str("123.45").unwrap())
            );

            // Row 2
            assert_eq!(rows[1].values[0], DbValue::Int(2));
            assert_eq!(
                rows[1].values[1],
                DbValue::Date(NaiveDate::from_ymd_opt(2026, 6, 3).unwrap())
            );
            assert_eq!(
                rows[1].values[2],
                DbValue::Time(NaiveTime::from_hms_opt(13, 45, 0).unwrap())
            );
            assert_eq!(
                rows[1].values[3],
                DbValue::Timestamp(
                    NaiveDate::from_ymd_opt(2026, 6, 3)
                        .unwrap()
                        .and_hms_opt(13, 45, 0)
                        .unwrap()
                )
            );
            assert_eq!(
                rows[1].values[4],
                DbValue::Decimal(Decimal::from_str("678.90").unwrap())
            );
        } else {
            panic!("Expected Select result");
        }
    }

    // 5. Test rejection of implicit coercion (Date / Decimal vs String)
    {
        let tx = Transaction::new(5, db.clone());

        let res_date = engine.execute("SELECT * FROM test_types WHERE d = '2026-06-02'", &tx);
        assert!(res_date.is_err());
        assert!(res_date.unwrap_err().contains("Type mismatch"));

        let res_dec = engine.execute("SELECT * FROM test_types WHERE dec = '123.45'", &tx);
        assert!(res_dec.is_err());
        assert!(res_dec.unwrap_err().contains("Type mismatch"));
    }

    // 6. Test arithmetic operations on Decimals (eval_arithmetic)
    {
        let tx = Transaction::new(6, db.clone());
        let res = engine
            .execute(
                "SELECT dec + 10, dec * 2, dec - 100, dec / 2 FROM test_types WHERE id = 1",
                &tx,
            )
            .unwrap();
        if let ExecutionResult::Select { rows, .. } = res {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].values[0],
                DbValue::Decimal(Decimal::from_str("133.45").unwrap())
            );
            assert_eq!(
                rows[0].values[1],
                DbValue::Decimal(Decimal::from_str("246.90").unwrap())
            );
            assert_eq!(
                rows[0].values[2],
                DbValue::Decimal(Decimal::from_str("23.45").unwrap())
            );
            assert_eq!(
                rows[0].values[3],
                DbValue::Decimal(Decimal::from_str("61.725").unwrap())
            );
        } else {
            panic!("Expected Select result");
        }
    }

    // 7. Test aggregations (SUM and AVG)
    {
        let tx = Transaction::new(7, db.clone());
        let res = engine
            .execute("SELECT SUM(dec), AVG(dec) FROM test_types", &tx)
            .unwrap();
        if let ExecutionResult::Select { rows, .. } = res {
            assert_eq!(rows.len(), 1);
            // 123.45 + 678.90 = 802.35
            assert_eq!(
                rows[0].values[0],
                DbValue::Decimal(Decimal::from_str("802.35").unwrap())
            );
            // 802.35 / 2 = 401.175
            assert_eq!(
                rows[0].values[1],
                DbValue::Decimal(Decimal::from_str("401.175").unwrap())
            );
        } else {
            panic!("Expected Select result");
        }
    }

    // 8. Test secondary index on Date and Decimal columns
    {
        let tx = Transaction::new(8, db.clone());
        engine
            .execute("CREATE INDEX idx_d ON test_types (d)", &tx)
            .unwrap();
        engine
            .execute("CREATE INDEX idx_dec ON test_types (dec)", &tx)
            .unwrap();
        tx.commit().unwrap();
    }

    // Query using indexes
    {
        let tx = Transaction::new(9, db.clone());
        let res_idx_d = engine
            .execute("SELECT id FROM test_types WHERE d = DATE '2026-06-02'", &tx)
            .unwrap();
        if let ExecutionResult::Select { rows, .. } = res_idx_d {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], DbValue::Int(1));
        } else {
            panic!("Expected Select result");
        }

        let res_idx_dec = engine
            .execute(
                "SELECT id FROM test_types WHERE dec = DECIMAL '678.90'",
                &tx,
            )
            .unwrap();
        if let ExecutionResult::Select { rows, .. } = res_idx_dec {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], DbValue::Int(2));
        } else {
            panic!("Expected Select result");
        }
    }
}

/// Helper: run a SELECT and return its rows, panicking on a non-Select result.
fn select_rows(
    engine: &SqlEngine,
    db: &Arc<Database>,
    txid: u64,
    sql: &str,
) -> Vec<dtdb_relational::Row> {
    let tx = Transaction::new(txid, db.clone());
    match engine.execute(sql, &tx).unwrap() {
        ExecutionResult::Select { rows, .. } => rows,
        other => panic!("Expected Select result, got {:?}", other),
    }
}

/// Range / inequality / BETWEEN scans and ORDER BY over indexed temporal and
/// decimal columns. The commit added bound-extraction code in the optimizer for
/// each of these types, but only equality (`=`) was exercised. ORDER BY also
/// pins numeric (not lexicographic) decimal ordering: string-serialized "123"
/// would sort before "9" if comparison ran on the serialized form.
#[test]
fn test_temporal_decimal_range_and_order() {
    use chrono::NaiveDate;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    let (_temp, db, engine) = setup_engine();

    {
        let tx = Transaction::new(1, db.clone());
        engine
            .execute(
                "CREATE TABLE events (
                    id INT PRIMARY KEY,
                    dec DECIMAL,
                    d DATE,
                    ts TIMESTAMP
                )",
                &tx,
            )
            .unwrap();
        // Index the typed columns so the optimizer's range-bound extraction runs.
        engine
            .execute("CREATE INDEX idx_dec ON events (dec)", &tx)
            .unwrap();
        engine
            .execute("CREATE INDEX idx_d ON events (d)", &tx)
            .unwrap();
        engine
            .execute("CREATE INDEX idx_ts ON events (ts)", &tx)
            .unwrap();
        tx.commit().unwrap();
    }

    {
        let tx = Transaction::new(2, db.clone());
        engine
            .execute(
                "INSERT INTO events (id, dec, d, ts) VALUES
                    (1, DECIMAL '9.00',   DATE '2026-01-09', TIMESTAMP '2026-01-09 08:00:00'),
                    (2, DECIMAL '123.00', DATE '2026-03-15', TIMESTAMP '2026-03-15 12:00:00'),
                    (3, DECIMAL '50.50',  DATE '2026-02-20', TIMESTAMP '2026-02-20 10:00:00')",
                &tx,
            )
            .unwrap();
        tx.commit().unwrap();
    }

    let ids = |rows: &[dtdb_relational::Row]| -> Vec<i64> {
        rows.iter()
            .map(|r| match &r.values[0] {
                DbValue::Int(i) => *i,
                other => panic!("expected Int id, got {:?}", other),
            })
            .collect()
    };

    // Decimal range scans (numeric semantics).
    let rows = select_rows(
        &engine,
        &db,
        10,
        "SELECT id FROM events WHERE dec > DECIMAL '10' ORDER BY id ASC",
    );
    assert_eq!(ids(&rows), vec![2, 3]);

    let rows = select_rows(
        &engine,
        &db,
        11,
        "SELECT id FROM events WHERE dec BETWEEN DECIMAL '10' AND DECIMAL '100' ORDER BY id ASC",
    );
    assert_eq!(ids(&rows), vec![3]);

    let rows = select_rows(
        &engine,
        &db,
        12,
        "SELECT id FROM events WHERE dec <= DECIMAL '50.50' ORDER BY id ASC",
    );
    assert_eq!(ids(&rows), vec![1, 3]);

    // Date range scan.
    let rows = select_rows(
        &engine,
        &db,
        13,
        "SELECT id FROM events WHERE d >= DATE '2026-02-01' ORDER BY id ASC",
    );
    assert_eq!(ids(&rows), vec![2, 3]);

    // Timestamp range scan.
    let rows = select_rows(
        &engine,
        &db,
        14,
        "SELECT id FROM events WHERE ts < TIMESTAMP '2026-02-01 00:00:00' ORDER BY id ASC",
    );
    assert_eq!(ids(&rows), vec![1]);

    // ORDER BY on a decimal column must sort numerically (9 < 50.5 < 123), not
    // lexicographically by the string serialization (which would give 123,50,9).
    let rows = select_rows(
        &engine,
        &db,
        15,
        "SELECT id, dec FROM events ORDER BY dec ASC",
    );
    assert_eq!(ids(&rows), vec![1, 3, 2]);
    assert_eq!(
        rows[2].values[1],
        DbValue::Decimal(Decimal::from_str("123.00").unwrap())
    );

    // ORDER BY DESC on a date column.
    let rows = select_rows(&engine, &db, 16, "SELECT id, d FROM events ORDER BY d DESC");
    assert_eq!(ids(&rows), vec![2, 3, 1]);
    assert_eq!(
        rows[0].values[1],
        DbValue::Date(NaiveDate::from_ymd_opt(2026, 3, 15).unwrap())
    );
}

/// Temporal and decimal columns must work as PRIMARY KEYs: this drives the
/// `value_to_key` / `row_to_key` conversions and the key-type validation in
/// `schema.rs`, plus numeric key ordering and duplicate-key rejection.
#[test]
fn test_temporal_decimal_primary_keys() {
    use rust_decimal::Decimal;
    use std::str::FromStr;

    let (_temp, db, engine) = setup_engine();

    // --- Decimal primary key ---
    {
        let tx = Transaction::new(1, db.clone());
        engine
            .execute("CREATE TABLE pk_dec (k DECIMAL PRIMARY KEY, v STRING)", &tx)
            .unwrap();
        engine
            .execute(
                "INSERT INTO pk_dec (k, v) VALUES
                    (DECIMAL '1.5', 'a'),
                    (DECIMAL '2.25', 'b'),
                    (DECIMAL '0.5', 'c')",
                &tx,
            )
            .unwrap();
        tx.commit().unwrap();
    }

    // Point lookup by decimal key.
    let rows = select_rows(
        &engine,
        &db,
        2,
        "SELECT v FROM pk_dec WHERE k = DECIMAL '2.25'",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], DbValue::string("b"));

    // Keys iterate in numeric order: 0.5, 1.5, 2.25.
    let rows = select_rows(&engine, &db, 3, "SELECT k, v FROM pk_dec ORDER BY k ASC");
    assert_eq!(
        rows.iter().map(|r| r.values[1].clone()).collect::<Vec<_>>(),
        vec![
            DbValue::string("c"),
            DbValue::string("a"),
            DbValue::string("b"),
        ]
    );
    assert_eq!(
        rows[0].values[0],
        DbValue::Decimal(Decimal::from_str("0.5").unwrap())
    );

    // Re-inserting an existing decimal key is rejected.
    {
        let tx = Transaction::new(4, db.clone());
        let dup = engine.execute(
            "INSERT INTO pk_dec (k, v) VALUES (DECIMAL '1.5', 'dup')",
            &tx,
        );
        assert!(dup.is_err(), "duplicate decimal primary key should error");
    }

    // --- Date primary key ---
    {
        let tx = Transaction::new(5, db.clone());
        engine
            .execute("CREATE TABLE pk_date (k DATE PRIMARY KEY, v INT)", &tx)
            .unwrap();
        engine
            .execute(
                "INSERT INTO pk_date (k, v) VALUES
                    (DATE '2026-05-01', 10),
                    (DATE '2026-01-01', 20)",
                &tx,
            )
            .unwrap();
        tx.commit().unwrap();
    }

    let rows = select_rows(
        &engine,
        &db,
        6,
        "SELECT v FROM pk_date WHERE k = DATE '2026-01-01'",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], DbValue::Int(20));

    // Date keys iterate chronologically.
    let rows = select_rows(&engine, &db, 7, "SELECT v FROM pk_date ORDER BY k ASC");
    assert_eq!(
        rows.iter().map(|r| r.values[0].clone()).collect::<Vec<_>>(),
        vec![DbValue::Int(20), DbValue::Int(10)]
    );
}

/// MIN/MAX over temporal and decimal columns, and SUM/AVG/COUNT in the presence
/// of NULLs. The aggregation rewrite added decimal accumulators but only the
/// all-non-null SUM/AVG happy path was tested; MIN/MAX and NULL-skipping for
/// these types were not.
#[test]
fn test_temporal_decimal_aggregates_and_nulls() {
    use chrono::NaiveDate;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    let (_temp, db, engine) = setup_engine();

    {
        let tx = Transaction::new(1, db.clone());
        engine
            .execute(
                "CREATE TABLE measures (id INT PRIMARY KEY, dec DECIMAL, d DATE)",
                &tx,
            )
            .unwrap();
        engine
            .execute(
                "INSERT INTO measures (id, dec, d) VALUES
                    (1, DECIMAL '10.00', DATE '2026-01-10'),
                    (2, NULL,            DATE '2026-03-20'),
                    (3, DECIMAL '30.00', NULL)",
                &tx,
            )
            .unwrap();
        tx.commit().unwrap();
    }

    // SUM/AVG/COUNT skip the NULL decimal; COUNT(*) counts every row.
    let rows = select_rows(
        &engine,
        &db,
        2,
        "SELECT SUM(dec), AVG(dec), COUNT(dec), COUNT(*) FROM measures",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values[0],
        DbValue::Decimal(Decimal::from_str("40.00").unwrap())
    );
    assert_eq!(
        rows[0].values[1],
        DbValue::Decimal(Decimal::from_str("20").unwrap())
    );
    assert_eq!(rows[0].values[2], DbValue::Int(2));
    assert_eq!(rows[0].values[3], DbValue::Int(3));

    // MIN/MAX over decimal and date columns (NULLs ignored).
    let rows = select_rows(
        &engine,
        &db,
        3,
        "SELECT MIN(dec), MAX(dec), MIN(d), MAX(d) FROM measures",
    );
    assert_eq!(
        rows[0].values[0],
        DbValue::Decimal(Decimal::from_str("10.00").unwrap())
    );
    assert_eq!(
        rows[0].values[1],
        DbValue::Decimal(Decimal::from_str("30.00").unwrap())
    );
    assert_eq!(
        rows[0].values[2],
        DbValue::Date(NaiveDate::from_ymd_opt(2026, 1, 10).unwrap())
    );
    assert_eq!(
        rows[0].values[3],
        DbValue::Date(NaiveDate::from_ymd_opt(2026, 3, 20).unwrap())
    );
}

/// CAST conversions through SQL, typed-literal/negated DEFAULTs, ALTER TABLE ADD
/// COLUMN with a temporal type, the NUMERIC/DEC/DECIMAL(p,s) aliases, and NULL
/// storage — all reachable only through end-to-end SQL, not the expr unit tests.
#[test]
fn test_temporal_decimal_cast_defaults_and_aliases() {
    use rust_decimal::Decimal;
    use std::str::FromStr;

    let (_temp, db, engine) = setup_engine();

    // NUMERIC, DEC and DECIMAL(p,s) all map to the Decimal type; DATE/DECIMAL
    // typed-literal DEFAULTs are applied when the column is omitted on INSERT.
    {
        let tx = Transaction::new(1, db.clone());
        engine
            .execute(
                "CREATE TABLE accounts (
                    id INT PRIMARY KEY,
                    balance DECIMAL DEFAULT DECIMAL '-1.50',
                    opened DATE DEFAULT DATE '2026-01-01',
                    n NUMERIC,
                    dc DEC,
                    dp DECIMAL(10, 2)
                )",
                &tx,
            )
            .unwrap();
        engine
            .execute("INSERT INTO accounts (id) VALUES (1)", &tx)
            .unwrap();
        engine
            .execute(
                "INSERT INTO accounts (id, balance, opened, n, dc, dp) VALUES
                    (2, DECIMAL '99.99', DATE '2026-12-31', DECIMAL '1', DECIMAL '2', DECIMAL '3.00')",
                &tx,
            )
            .unwrap();
        tx.commit().unwrap();
    }

    // Row 1 picked up both typed-literal defaults; the alias columns are NULL.
    let rows = select_rows(
        &engine,
        &db,
        2,
        "SELECT balance, opened, n FROM accounts WHERE id = 1",
    );
    assert_eq!(
        rows[0].values[0],
        DbValue::Decimal(Decimal::from_str("-1.50").unwrap())
    );
    assert_eq!(
        rows[0].values[1],
        DbValue::Date(chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap())
    );
    assert_eq!(rows[0].values[2], DbValue::Null);

    // The NUMERIC/DEC/DECIMAL(p,s) alias columns round-trip Decimal values.
    let rows = select_rows(
        &engine,
        &db,
        3,
        "SELECT n, dc, dp FROM accounts WHERE id = 2",
    );
    assert_eq!(
        rows[0].values[0],
        DbValue::Decimal(Decimal::from_str("1").unwrap())
    );
    assert_eq!(
        rows[0].values[2],
        DbValue::Decimal(Decimal::from_str("3.00").unwrap())
    );

    // CAST through SQL: numeric <-> decimal and string -> temporal.
    let rows = select_rows(
        &engine,
        &db,
        4,
        "SELECT
            CAST(balance AS INT),
            CAST(7 AS DECIMAL),
            CAST('2026-06-02' AS DATE)
         FROM accounts WHERE id = 2",
    );
    assert_eq!(rows[0].values[0], DbValue::Int(99)); // 99.99 truncates toward zero
    assert_eq!(
        rows[0].values[1],
        DbValue::Decimal(Decimal::from_str("7").unwrap())
    );
    assert_eq!(
        rows[0].values[2],
        DbValue::Date(chrono::NaiveDate::from_ymd_opt(2026, 6, 2).unwrap())
    );

    // A CAST of an unparseable string surfaces an error rather than panicking.
    {
        let tx = Transaction::new(5, db.clone());
        let err = engine.execute("SELECT CAST('not-a-date' AS DATE) FROM accounts", &tx);
        assert!(err.is_err(), "casting a bad date string should error");
    }

    // ALTER TABLE ADD COLUMN with a temporal type, then write/read it.
    {
        let tx = Transaction::new(6, db.clone());
        engine
            .execute("ALTER TABLE accounts ADD COLUMN last_seen TIMESTAMP", &tx)
            .unwrap();
        engine
            .execute(
                "INSERT INTO accounts (id, last_seen) VALUES (3, TIMESTAMP '2026-06-02 12:00:00')",
                &tx,
            )
            .unwrap();
        tx.commit().unwrap();
    }
    let rows = select_rows(
        &engine,
        &db,
        7,
        "SELECT last_seen FROM accounts WHERE id = 3",
    );
    assert_eq!(
        rows[0].values[0],
        DbValue::Timestamp(
            chrono::NaiveDate::from_ymd_opt(2026, 6, 2)
                .unwrap()
                .and_hms_opt(12, 0, 0)
                .unwrap()
        )
    );
    // Pre-existing rows read NULL for the lazily-added column.
    let rows = select_rows(
        &engine,
        &db,
        8,
        "SELECT last_seen FROM accounts WHERE id = 1",
    );
    assert_eq!(rows[0].values[0], DbValue::Null);
}

/// Typed values (including a decimal primary key) must survive being flushed to
/// SSTables and read back after the database is reopened — this exercises the
/// full postcard storage round-trip (custom decimal serde included) end-to-end,
/// not just the in-memory `to_allocvec` round-trip the unit tests cover.
#[test]
fn test_temporal_decimal_persistence_reopen() {
    use chrono::NaiveDate;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    let temp_dir = TempDir::new().unwrap();

    // Phase 1: write typed rows, then flush the memtable to disk.
    {
        let db = Arc::new(Database::open(temp_dir.path()).unwrap());
        let engine = SqlEngine::new(db.clone());

        let tx = Transaction::new(1, db.clone());
        engine
            .execute(
                "CREATE TABLE snap (k DECIMAL PRIMARY KEY, d DATE, t TIME, ts TIMESTAMP)",
                &tx,
            )
            .unwrap();
        engine
            .execute(
                "INSERT INTO snap (k, d, t, ts) VALUES
                    (DECIMAL '1.25', DATE '2026-06-02', TIME '12:34:56', TIMESTAMP '2026-06-02 12:34:56'),
                    (DECIMAL '99.99', DATE '2026-07-04', TIME '01:02:03', TIMESTAMP '2026-07-04 01:02:03')",
                &tx,
            )
            .unwrap();
        tx.commit().unwrap();

        db.get_table("snap").unwrap().flush_memtable().unwrap();
    }

    // Phase 2: reopen the database from the same directory and read back.
    {
        let db = Arc::new(Database::open(temp_dir.path()).unwrap());
        let engine = SqlEngine::new(db.clone());

        let rows = select_rows(
            &engine,
            &db,
            2,
            "SELECT k, d, t, ts FROM snap ORDER BY k ASC",
        );
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].values[0],
            DbValue::Decimal(Decimal::from_str("1.25").unwrap())
        );
        assert_eq!(
            rows[0].values[1],
            DbValue::Date(NaiveDate::from_ymd_opt(2026, 6, 2).unwrap())
        );
        assert_eq!(
            rows[0].values[2],
            DbValue::Time(chrono::NaiveTime::from_hms_opt(12, 34, 56).unwrap())
        );
        assert_eq!(
            rows[1].values[0],
            DbValue::Decimal(Decimal::from_str("99.99").unwrap())
        );

        // Point lookup by decimal primary key still works after reopen.
        let rows = select_rows(
            &engine,
            &db,
            3,
            "SELECT d FROM snap WHERE k = DECIMAL '99.99'",
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].values[0],
            DbValue::Date(NaiveDate::from_ymd_opt(2026, 7, 4).unwrap())
        );
    }
}

/// Index maintenance on UPDATE and DELETE for a decimal-keyed secondary index.
/// Inserting, then changing or removing the indexed value, must keep the index
/// consistent — the old entry has to be removed, not left dangling. This guards
/// the old-key removal paths (the `Delete` arms of `write_batch`), which had the
/// same missing-variant bug as the insert path and would otherwise leave stale
/// index entries pointing at rows that no longer match.
#[test]
fn test_temporal_decimal_index_update_delete() {
    use rust_decimal::Decimal;
    use std::str::FromStr;

    let (_temp, db, engine) = setup_engine();

    {
        let tx = Transaction::new(1, db.clone());
        engine
            .execute("CREATE TABLE t (id INT PRIMARY KEY, dec DECIMAL)", &tx)
            .unwrap();
        tx.commit().unwrap();
    }
    {
        let tx = Transaction::new(2, db.clone());
        engine
            .execute("CREATE INDEX idx_dec ON t (dec)", &tx)
            .unwrap();
        tx.commit().unwrap();
    }
    {
        let tx = Transaction::new(3, db.clone());
        engine
            .execute(
                "INSERT INTO t (id, dec) VALUES
                    (1, DECIMAL '10.00'),
                    (2, DECIMAL '20.00'),
                    (3, DECIMAL '30.00')",
                &tx,
            )
            .unwrap();
        tx.commit().unwrap();
    }

    // Baseline: the index resolves the seeded value.
    let rows = select_rows(
        &engine,
        &db,
        4,
        "SELECT id FROM t WHERE dec = DECIMAL '20.00'",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], DbValue::Int(2));

    // UPDATE the indexed value: the old key must disappear, the new one appear.
    {
        let tx = Transaction::new(5, db.clone());
        engine
            .execute("UPDATE t SET dec = DECIMAL '25.00' WHERE id = 2", &tx)
            .unwrap();
        tx.commit().unwrap();
    }
    let rows = select_rows(
        &engine,
        &db,
        6,
        "SELECT id FROM t WHERE dec = DECIMAL '20.00'",
    );
    assert!(
        rows.is_empty(),
        "stale index entry for the old decimal value"
    );
    let rows = select_rows(
        &engine,
        &db,
        7,
        "SELECT id FROM t WHERE dec = DECIMAL '25.00'",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], DbValue::Int(2));

    // DELETE a row: its index entry must be removed too.
    {
        let tx = Transaction::new(8, db.clone());
        engine.execute("DELETE FROM t WHERE id = 1", &tx).unwrap();
        tx.commit().unwrap();
    }
    let rows = select_rows(
        &engine,
        &db,
        9,
        "SELECT id FROM t WHERE dec = DECIMAL '10.00'",
    );
    assert!(rows.is_empty(), "deleted row still reachable via the index");

    // A range scan reflects the post-update/delete state: 25.00 and 30.00.
    let rows = select_rows(
        &engine,
        &db,
        10,
        "SELECT id FROM t WHERE dec >= DECIMAL '0' ORDER BY dec ASC",
    );
    assert_eq!(
        rows.iter()
            .map(|r| match &r.values[0] {
                DbValue::Int(i) => *i,
                other => panic!("expected Int, got {:?}", other),
            })
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    let rows = select_rows(&engine, &db, 11, "SELECT dec FROM t WHERE id = 2");
    assert_eq!(
        rows[0].values[0],
        DbValue::Decimal(Decimal::from_str("25.00").unwrap())
    );
}

/// Bound query parameters of the new typed columns must resolve to index/PK
/// keys. The prepared-statement path keeps parameters symbolic (`PlanKey::
/// Parameter`) and converts the bound value to a `DbKey` at compile time; those
/// value→key resolvers handled only Int/String, so a Decimal/Date parameter on
/// a primary-key or indexed column either silently skipped the point-get fast
/// path or errored with "Unsupported key parameter type". This pins all three
/// resolver sites: PK point-get, PK range scan, and secondary IndexScan.
#[test]
fn test_prepared_params_temporal_decimal_keys() {
    use chrono::NaiveDate;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    let (_temp, db, engine) = setup_engine();
    let run = |sql: &str, id: u64| {
        let tx = Transaction::new(id, db.clone());
        engine.execute(sql, &tx).unwrap();
        tx.commit().unwrap();
    };

    // --- Decimal PRIMARY KEY: parameterized point-get and range scan ---
    run("CREATE TABLE pk_dec (k DECIMAL PRIMARY KEY, v STRING)", 1);
    run(
        "INSERT INTO pk_dec (k, v) VALUES
            (DECIMAL '1.50', 'a'),
            (DECIMAL '2.25', 'b'),
            (DECIMAL '10.00', 'c')",
        2,
    );

    // Point lookup by a bound Decimal PK (resolver in the point-get fast path).
    let pt = engine.prepare("SELECT v FROM pk_dec WHERE k = :k").unwrap();
    {
        let tx = Transaction::new(3, db.clone());
        let mut params = std::collections::HashMap::new();
        params.insert(
            "k".to_string(),
            DbValue::Decimal(Decimal::from_str("2.25").unwrap()),
        );
        let res = engine.execute_prepared(&pt, &tx, &params).unwrap();
        tx.commit().unwrap();
        match res {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].values[0], DbValue::string("b"));
            }
            _ => panic!("expected Select"),
        }
    }

    // Range scan over the Decimal PK with two bound parameters (resolver in the
    // primary-key Scan range path).
    let rng = engine
        .prepare("SELECT v FROM pk_dec WHERE k >= :lo AND k <= :hi ORDER BY k ASC")
        .unwrap();
    {
        let tx = Transaction::new(4, db.clone());
        let mut params = std::collections::HashMap::new();
        params.insert(
            "lo".to_string(),
            DbValue::Decimal(Decimal::from_str("2.00").unwrap()),
        );
        params.insert(
            "hi".to_string(),
            DbValue::Decimal(Decimal::from_str("9.99").unwrap()),
        );
        let res = engine.execute_prepared(&rng, &tx, &params).unwrap();
        tx.commit().unwrap();
        match res {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(
                    rows.iter().map(|r| r.values[0].clone()).collect::<Vec<_>>(),
                    vec![DbValue::string("b")] // 2.25 only; 1.50 and 10.00 excluded
                );
            }
            _ => panic!("expected Select"),
        }
    }

    // --- Secondary indexes on Decimal and Date: parameterized IndexScan ---
    run(
        "CREATE TABLE t (id INT PRIMARY KEY, dec DECIMAL, d DATE)",
        5,
    );
    run("CREATE INDEX idx_dec ON t (dec)", 6);
    run("CREATE INDEX idx_d ON t (d)", 7);
    run(
        "INSERT INTO t (id, dec, d) VALUES
            (1, DECIMAL '9.00',   DATE '2026-01-09'),
            (2, DECIMAL '123.00', DATE '2026-03-15'),
            (3, DECIMAL '50.50',  DATE '2026-02-20')",
        8,
    );

    // Equality on the indexed Decimal column via a bound parameter.
    let idx_eq = engine.prepare("SELECT id FROM t WHERE dec = :v").unwrap();
    {
        let tx = Transaction::new(9, db.clone());
        let mut params = std::collections::HashMap::new();
        params.insert(
            "v".to_string(),
            DbValue::Decimal(Decimal::from_str("50.50").unwrap()),
        );
        let res = engine.execute_prepared(&idx_eq, &tx, &params).unwrap();
        tx.commit().unwrap();
        match res {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].values[0], DbValue::Int(3));
            }
            _ => panic!("expected Select"),
        }
    }

    // Range on the indexed Date column with two bound Date parameters.
    let idx_rng = engine
        .prepare("SELECT id FROM t WHERE d >= :lo AND d <= :hi ORDER BY id ASC")
        .unwrap();
    {
        let tx = Transaction::new(10, db.clone());
        let mut params = std::collections::HashMap::new();
        params.insert(
            "lo".to_string(),
            DbValue::Date(NaiveDate::from_ymd_opt(2026, 2, 1).unwrap()),
        );
        params.insert(
            "hi".to_string(),
            DbValue::Date(NaiveDate::from_ymd_opt(2026, 12, 31).unwrap()),
        );
        let res = engine.execute_prepared(&idx_rng, &tx, &params).unwrap();
        tx.commit().unwrap();
        match res {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(
                    rows.iter()
                        .map(|r| match &r.values[0] {
                            DbValue::Int(i) => *i,
                            other => panic!("expected Int, got {:?}", other),
                        })
                        .collect::<Vec<_>>(),
                    vec![2, 3] // 2026-03-15 and 2026-02-20; 2026-01-09 excluded
                );
            }
            _ => panic!("expected Select"),
        }
    }
}

#[test]
fn test_composite_secondary_index_bugs() {
    let (_temp, db, engine) = setup_engine();
    let run = |sql: &str, id: u64| {
        let tx = Transaction::new(id, db.clone());
        engine.execute(sql, &tx).unwrap();
        tx.commit().unwrap();
    };

    // 1. Create table with a composite secondary index and a String primary key.
    // The string primary key will have min_pk = DbKey::String("") which is
    // larger than any DbKey::Int(val) under Rust's enum variant ordering,
    // thereby triggering the bounds vulnerability if not correctly handled.
    run(
        "CREATE TABLE composite_test (id STRING PRIMARY KEY, a INT, b INT)",
        1,
    );
    run("CREATE INDEX idx_a_b ON composite_test (a, b)", 2);

    // Insert some rows.
    run(
        "INSERT INTO composite_test (id, a, b) VALUES
            ('pk1', 5, 20),
            ('pk2', 5, 10),
            ('pk3', 6, 30)",
        3,
    );

    // Verify 1: The bounds bug fix.
    // Querying with `a = 5` must return both rows.
    let rows = select_rows(
        &engine,
        &db,
        4,
        "SELECT id, b FROM composite_test WHERE a = 5",
    );
    assert_eq!(rows.len(), 2, "Expected 2 rows for a=5, got: {:?}", rows);

    // Verify 2: The sorting collapse bug fix.
    // Since the index is on (a, b), a query ordered by index columns must return
    // (5, 10) then (5, 20).
    assert_eq!(rows[0].values[0], DbValue::string("pk2"));
    assert_eq!(rows[0].values[1], DbValue::Int(10));
    assert_eq!(rows[1].values[0], DbValue::string("pk1"));
    assert_eq!(rows[1].values[1], DbValue::Int(20));

    // Verify 3: Write Buffer Null-Checking Bug.
    // In a new transaction, write a row that has a=5 but b=NULL.
    // Since b is NULL, it will NOT generate an index entry on disk.
    // The write buffer scanner must filter it out because it shouldn't be in the index.
    {
        let tx = Transaction::new(5, db.clone());
        engine
            .execute(
                "INSERT INTO composite_test (id, a, b) VALUES ('pk4', 5, NULL)",
                &tx,
            )
            .unwrap();
        // Do the select inside the transaction before committing so it hits the write buffer
        let res = engine
            .execute("SELECT id FROM composite_test WHERE a = 5", &tx)
            .unwrap();
        if let ExecutionResult::Select { rows, .. } = res {
            let ids: Vec<String> = rows
                .iter()
                .map(|r| match &r.values[0] {
                    DbValue::String(s) => s.to_string(),
                    other => panic!("expected string, got {:?}", other),
                })
                .collect();
            assert_eq!(ids, vec!["pk2".to_string(), "pk1".to_string()]);
        } else {
            panic!("expected Select");
        }
        tx.commit().unwrap();
    }
}
