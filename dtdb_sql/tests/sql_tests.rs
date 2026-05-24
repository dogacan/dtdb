use dtdb_relational::{Database, Transaction};
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
fn test_sql_query_macro_and_interpolation() {
    use dtdb_sql::sql_query;

    // 1. Test basic bindings and escaping
    let query =
        sql_query!("SELECT * FROM users WHERE id = @id AND name = @name AND details = @details")
            .bind("id", 42i64)
            .bind("name", "Alice's Laptop")
            .bind("details", "Some 'escaped' \"value\" @not_a_param");

    let interpolated = query.interpolate().unwrap();
    assert_eq!(
        interpolated,
        "SELECT * FROM users WHERE id = 42 AND name = 'Alice''s Laptop' AND details = 'Some ''escaped'' \"value\" @not_a_param'"
    );

    // 2. Test raw bytes binding
    let query =
        sql_query!("SELECT * FROM items WHERE hash = @hash").bind("hash", vec![1u8, 2u8, 255u8]);
    assert_eq!(
        query.interpolate().unwrap(),
        "SELECT * FROM items WHERE hash = x'0102ff'"
    );

    // 3. Verify that quotes protect placeholders from replacement
    let query = sql_query!("SELECT * FROM users WHERE email = 'support@id.com' AND id = @id")
        .bind("id", 10i64);
    assert_eq!(
        query.interpolate().unwrap(),
        "SELECT * FROM users WHERE email = 'support@id.com' AND id = 10"
    );

    // 4. Verify unbound parameter results in an error
    let query = sql_query!("SELECT * FROM users WHERE id = @id AND age = @age").bind("id", 10i64);
    assert!(query.interpolate().is_err());
}

#[test]
fn test_sql_query_execution_end_to_end() {
    use dtdb_sql::sql_query;
    let (_temp, db, engine) = setup_engine();

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE employees (id INT PRIMARY KEY, name STRING, score FLOAT)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    // Insert using SqlQuery
    let insert_query =
        sql_query!("INSERT INTO employees (id, name, score) VALUES (@id, @name, @score)")
            .bind("id", 1i64)
            .bind("name", "Bob's Team")
            .bind("score", 95.5f64);

    let res = engine.execute_query(&insert_query, &tx2).unwrap();
    assert_eq!(res, ExecutionResult::Insert { count: 1 });
    tx2.commit().unwrap();

    let tx3 = Transaction::new(3, db.clone());
    // Select using SqlQuery
    let select_query = sql_query!(
        "SELECT id, name, score FROM employees WHERE name = @name AND score >= @min_score"
    )
    .bind("name", "Bob's Team")
    .bind("min_score", 90.0f64);

    let select_res = engine.execute_query(&select_query, &tx3).unwrap();
    if let ExecutionResult::Select { rows, .. } = select_res {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], DbValue::Int(1));
        assert_eq!(rows[0].values[1], DbValue::String("Bob's Team".to_string()));
        assert_eq!(rows[0].values[2], DbValue::Float(95.5));
    } else {
        panic!("Expected SELECT results");
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
        assert_eq!(rows[0].values[2], DbValue::Int(1)); // NULL OR 1 = 1
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
