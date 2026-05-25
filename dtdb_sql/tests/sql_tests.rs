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
                DbValue::String("Alice".to_string()),
                DbValue::Float(95.5)
            ]
        );
        assert_eq!(
            rows[1].values,
            vec![
                DbValue::Int(2),
                DbValue::String("Bob".to_string()),
                DbValue::Float(80.0)
            ]
        );
        assert_eq!(
            rows[2].values,
            vec![
                DbValue::Int(3),
                DbValue::String("Charlie".to_string()),
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
        assert_eq!(rows[0].values, vec![DbValue::String("Alice".to_string())]);
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
            vec![DbValue::String("Alice".to_string()), DbValue::Float(15.75)]
        );
        // Row 1: Alice, 100.5
        assert_eq!(
            rows[1].values,
            vec![DbValue::String("Alice".to_string()), DbValue::Float(100.5)]
        );
        // Row 2: Bob, 250.0
        assert_eq!(
            rows[2].values,
            vec![DbValue::String("Bob".to_string()), DbValue::Float(250.0)]
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
                DbValue::String("Engineering".to_string()),
                DbValue::Int(2),
                DbValue::Float(200000.0)
            ]
        );
        // HR: count=1, sum=45000.0
        assert_eq!(
            rows[1].values,
            vec![
                DbValue::String("HR".to_string()),
                DbValue::Int(1),
                DbValue::Float(45000.0)
            ]
        );
        // Sales: count=2, sum=110000.0
        assert_eq!(
            rows[2].values,
            vec![
                DbValue::String("Sales".to_string()),
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
        assert_eq!(rows[0].values[1], DbValue::String("abc-123".to_string()));
        assert_eq!(rows[1].values[1], DbValue::String("abc-789".to_string()));
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
        assert_eq!(rows[0].values[1], DbValue::String("abc-123".to_string()));
        assert_eq!(rows[1].values[1], DbValue::String("abc-789".to_string()));
        assert_eq!(
            rows[2].values[1],
            DbValue::String("xyz-abc-999".to_string())
        );
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
            vec![DbValue::Int(20), DbValue::String("KeepMe".to_string())]
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
        assert_eq!(rows[0].values, vec![DbValue::String("twenty".to_string())]);
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
        assert_eq!(rows[0].values, vec![DbValue::String("twenty".to_string())]);
        assert_eq!(rows[1].values, vec![DbValue::String("thirty".to_string())]);
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

    let tx2 = Transaction::new(2, db.clone());
    let res = engine
        .execute("EXPLAIN SELECT name FROM users WHERE id = 10", &tx2)
        .unwrap();

    if let ExecutionResult::Select { schema, rows } = res {
        assert_eq!(schema.columns[0].name, "Query Plan");
        assert_eq!(rows.len(), 1);
        let plan_text = match &rows[0].values[0] {
            DbValue::String(s) => s.clone(),
            _ => panic!("Expected string plan text"),
        };
        assert!(plan_text.contains("--- Logical Plan ---"));
        assert!(plan_text.contains("--- Optimized Plan ---"));
        assert!(plan_text.contains("--- Physical Plan ---"));
        assert!(plan_text.contains("PhysicalSeqScan"));
    } else {
        panic!("Expected EXPLAIN Select output");
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
        assert_eq!(
            rows[0].values[0],
            DbValue::String("AliceUpdated".to_string())
        );
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
        assert_eq!(rows[1].values[1], DbValue::String("Bob".to_string()));
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
        assert_eq!(rows[0].values[0], DbValue::String("Alice".to_string()));
        assert_eq!(rows[0].values[1], DbValue::Float(99.9));
        // Bob: matched
        assert_eq!(rows[1].values[0], DbValue::String("Bob".to_string()));
        assert_eq!(rows[1].values[1], DbValue::Float(199.9));
        // Charlie: unmatched (padded with NULL)
        assert_eq!(rows[2].values[0], DbValue::String("Charlie".to_string()));
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
        assert_eq!(rows[0].values[0], DbValue::String("Eng".to_string()));
        assert_eq!(rows[0].values[1], DbValue::Float(200.0)); // (100+300)/2
        // Sales
        assert_eq!(rows[1].values[0], DbValue::String("Sales".to_string()));
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
        assert_eq!(rows[0].values[1], DbValue::String("expensive".to_string()));
        assert_eq!(rows[1].values[1], DbValue::String("cheap".to_string()));
        assert_eq!(rows[2].values[1], DbValue::String("cheap".to_string()));
        assert_eq!(rows[3].values[1], DbValue::String("moderate".to_string()));
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
        assert_eq!(rows[0].values[1], DbValue::String("One".to_string()));
        assert_eq!(rows[1].values[1], DbValue::String("Two".to_string()));
        assert_eq!(rows[2].values[1], DbValue::String("Other".to_string()));
        assert_eq!(rows[3].values[1], DbValue::String("Other".to_string()));
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
        assert_eq!(rows[0].values[0], DbValue::String("Lap".to_string()));
        assert_eq!(rows[0].values[1], DbValue::String("top".to_string()));
        assert_eq!(rows[0].values[2], DbValue::String("to".to_string()));
        assert_eq!(rows[0].values[3], DbValue::String("L".to_string()));
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
        assert_eq!(
            rows[0].values[1],
            DbValue::String("Electronics".to_string())
        );
        assert_eq!(rows[1].values[1], DbValue::String("".to_string())); // Mouse category is empty "" -> NOT Null -> no fallback
        assert_eq!(rows[2].values[1], DbValue::String("Furniture".to_string()));
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
        assert_eq!(rows[0].values[1], DbValue::String("Alice".to_string()));
        assert_eq!(rows[0].values[2], DbValue::Null);

        // Bob note is "First Note"
        assert_eq!(rows[1].values[0], DbValue::Int(2));
        assert_eq!(rows[1].values[1], DbValue::String("Bob".to_string()));
        assert_eq!(rows[1].values[2], DbValue::String("First Note".to_string()));

        // Charlie note defaulted to NULL
        assert_eq!(rows[2].values[0], DbValue::Int(3));
        assert_eq!(rows[2].values[1], DbValue::String("Charlie".to_string()));
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
        assert_eq!(rows[0].values[1], DbValue::String("default".to_string()));
        assert_eq!(rows[1].values[1], DbValue::String("First Note".to_string()));
        assert_eq!(rows[2].values[1], DbValue::String("default".to_string()));
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
        assert_eq!(rows[0].values[0], DbValue::String("Alice".to_string()));
        assert_eq!(rows[0].values[1], DbValue::Null);
        // Bob (2): matched order -> amount is 9.99
        assert_eq!(rows[1].values[0], DbValue::String("Bob".to_string()));
        assert_eq!(rows[1].values[1], DbValue::Float(9.99));
        // Charlie (3): unmatched order -> amount is NULL
        assert_eq!(rows[2].values[0], DbValue::String("Charlie".to_string()));
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
                DbValue::String("Alice".to_string()),
                DbValue::Int(100000),
                DbValue::String("Engineering".to_string())
            ]
        );
        assert_eq!(
            rows[1].values,
            vec![
                DbValue::Int(2),
                DbValue::String("Bob".to_string()),
                DbValue::Int(80000),
                DbValue::String("HR".to_string())
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
            vec![DbValue::String("Alice".to_string()), DbValue::Int(110000)]
        );
        assert_eq!(
            rows[1].values,
            vec![DbValue::String("Bob".to_string()), DbValue::Int(80000)]
        );
    } else {
        panic!("Expected Select");
    }

    // 6. Verify directory structure to ensure locality groups were created as subdirectories
    let table_path = temp_dir.path().join("employees");

    // Default group directory should contain manifest and WAL
    let default_dir = table_path.join("default");
    assert!(default_dir.exists());
    assert!(default_dir.join("manifest.bin").exists());

    // lg_name directory should contain manifest and WAL
    let lg_name_dir = table_path.join("lg_lg_name");
    assert!(lg_name_dir.exists());
    assert!(lg_name_dir.join("manifest.bin").exists());

    // lg_finance directory should contain manifest and WAL
    let lg_finance_dir = table_path.join("lg_lg_finance");
    assert!(lg_finance_dir.exists());
    assert!(lg_finance_dir.join("manifest.bin").exists());
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
            bincode::deserialize(&lg_name_bytes).unwrap();
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
            bincode::deserialize(&lg_finance_bytes).unwrap();
        assert_eq!(lg_finance_engine_opts.wal_size_limit, 1048576);
        assert_eq!(lg_finance_engine_opts.max_level, 5);
        assert_eq!(lg_finance_engine_opts.block_cache_capacity, 0);
    }
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
        assert!(
            plan_str
                .contains("- IndexScan: table=students, index=idx_score, range=[Int(85), Int(85)]")
        );
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
        assert_eq!(rows[0].values, vec![DbValue::String("Charlie".to_string())]);
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
        assert_eq!(rows[0].values, vec![DbValue::String("Dave".to_string())]);
    } else {
        panic!("Expected ExecutionResult::Select");
    }

    // Verify Bob's updated score (88) is queryable, and old score (80) returns empty
    let res_bob_new = engine
        .execute("SELECT name FROM students WHERE score = 88", &tx6)
        .unwrap();
    if let ExecutionResult::Select { rows, .. } = res_bob_new {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values, vec![DbValue::String("Bob".to_string())]);
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
        assert!(
            plan_str
                .contains("- IndexScan: table=students, index=idx_score, range=[Int(85), Int(90)]")
        );
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
        assert_eq!(rows[0].values, vec![DbValue::String("Charlie".to_string())]);
        assert_eq!(rows[1].values, vec![DbValue::String("Bob".to_string())]);
        assert_eq!(rows[2].values, vec![DbValue::String("Dave".to_string())]);
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
        assert_eq!(rows[0].values, vec![DbValue::String("Eve".to_string())]);
        assert_eq!(rows[1].values, vec![DbValue::String("Bob".to_string())]);
        assert_eq!(rows[2].values, vec![DbValue::String("Dave".to_string())]);
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
        assert_eq!(rows[0].values[0], DbValue::String("CHARLIE".to_string()));
        assert_eq!(rows[0].values[1], DbValue::String("furniture".to_string()));
        assert_eq!(
            rows[0].values[2],
            DbValue::String("Charlie-Furniture".to_string())
        );
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
        assert_eq!(
            rows[0].values,
            vec![DbValue::String("A".to_string()), DbValue::Int(10)]
        );
        assert_eq!(
            rows[1].values,
            vec![DbValue::String("A".to_string()), DbValue::Int(20)]
        );
        assert_eq!(
            rows[2].values,
            vec![DbValue::String("B".to_string()), DbValue::Int(30)]
        );
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
        assert_eq!(
            rows[0].values,
            vec![DbValue::String("A".to_string()), DbValue::Float(30.0)]
        );
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
        assert_eq!(
            rows[0].values,
            vec![DbValue::String("A".to_string()), DbValue::Int(2)]
        );
        assert_eq!(
            rows[1].values,
            vec![DbValue::String("B".to_string()), DbValue::Int(1)]
        );
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
                vec![
                    DbValue::Int(1),
                    DbValue::Int(10),
                    DbValue::String("admin".to_string())
                ]
            );
            assert_eq!(
                rows[1].values,
                vec![
                    DbValue::Int(1),
                    DbValue::Int(20),
                    DbValue::String("user".to_string())
                ]
            );
            assert_eq!(
                rows[2].values,
                vec![
                    DbValue::Int(2),
                    DbValue::Int(10),
                    DbValue::String("admin".to_string())
                ]
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
                    DbValue::String("Unnamed".to_string()),
                    DbValue::Int(42)
                ]
            );
            assert_eq!(
                rows[1].values,
                vec![
                    DbValue::Int(2),
                    DbValue::String("Bob".to_string()),
                    DbValue::Int(42)
                ]
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
                vec![DbValue::Int(1), DbValue::String("First".to_string())]
            );
            assert_eq!(
                rows[1].values,
                vec![DbValue::Int(2), DbValue::String("Second".to_string())]
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
                vec![DbValue::Int(10), DbValue::String("Ten".to_string())]
            );
            assert_eq!(
                rows[3].values,
                vec![DbValue::Int(11), DbValue::String("Eleven".to_string())]
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
                    DbValue::String("Apple".to_string()),
                    DbValue::String("Cool".to_string())
                ]
            );
            assert_eq!(
                rows[1].values,
                vec![
                    DbValue::Int(2),
                    DbValue::String("Banana".to_string()),
                    DbValue::String("Cool".to_string())
                ]
            );
            assert_eq!(
                rows[2].values,
                vec![
                    DbValue::Int(3),
                    DbValue::String("Cherry".to_string()),
                    DbValue::String("Cool".to_string())
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
                vec![DbValue::Int(1), DbValue::String("apple".to_string())]
            );
            assert_eq!(
                rows[1].values,
                vec![DbValue::Int(2), DbValue::String("banana".to_string())]
            );
            assert_eq!(
                rows[2].values,
                vec![DbValue::Int(3), DbValue::String("cherry".to_string())]
            );
            assert_eq!(
                rows[3].values,
                vec![DbValue::Int(4), DbValue::String("date".to_string())]
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
                vec![DbValue::Int(1), DbValue::String("apple".to_string())],
                vec![DbValue::Int(2), DbValue::String("banana".to_string())],
                vec![DbValue::Int(2), DbValue::String("banana".to_string())],
                vec![DbValue::Int(2), DbValue::String("banana".to_string())],
                vec![DbValue::Int(3), DbValue::String("cherry".to_string())],
                vec![DbValue::Int(3), DbValue::String("cherry".to_string())],
                vec![DbValue::Int(3), DbValue::String("cherry".to_string())],
                vec![DbValue::Int(4), DbValue::String("date".to_string())],
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
                vec![DbValue::Int(1), DbValue::String("apple".to_string())]
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
                vec![DbValue::Int(1), DbValue::String("apple".to_string())]
            );
            assert_eq!(
                rows[1].values,
                vec![DbValue::Int(2), DbValue::String("banana".to_string())]
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
                vec![DbValue::Int(2), DbValue::String("banana".to_string())]
            );
            assert_eq!(
                rows[1].values,
                vec![DbValue::Int(3), DbValue::String("cherry".to_string())]
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
                vec![DbValue::Int(2), DbValue::String("banana".to_string())]
            );
            assert_eq!(
                rows[1].values,
                vec![DbValue::Int(3), DbValue::String("cherry".to_string())]
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
