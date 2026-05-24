use dtdb_relational::{Column, DataType, Database, RelationalError, Row, Schema, Transaction};
use dtdb_storage::{DbKey, DbValue};
use std::sync::Arc;
use tempfile::TempDir;

// Helper to create a test schema:
// Users table: id (Int, PK), name (String), active (Bytes - using Bytes as boolean placeholder)
fn create_test_schema() -> Schema {
    Schema::new(vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_nullable: false,
        },
        Column {
            name: "name".to_string(),
            data_type: DataType::String,
            is_primary_key: false,
            is_nullable: true,
        },
        Column {
            name: "score".to_string(),
            data_type: DataType::Float,
            is_primary_key: false,
            is_nullable: true,
        },
    ])
}

fn k_int(val: i64) -> DbKey {
    DbKey::Int(val)
}

fn r_user(id: i64, name: &str, score: f64) -> Row {
    Row::new(vec![
        DbValue::Int(id),
        DbValue::String(name.to_string()),
        DbValue::Float(score),
    ])
}

#[test]
fn test_database_table_crud() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path()).unwrap();

    let schema = create_test_schema();
    db.create_table("users", schema.clone()).unwrap();

    // Verify it exists in table list
    let tables = db.list_tables();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0], "users");

    // Fetch table metadata
    let table = db.get_table("users").unwrap();
    assert_eq!(table.name, "users");
    assert_eq!(table.schema, schema);

    // Can't recreate the same table
    assert!(matches!(
        db.create_table("users", schema),
        Err(RelationalError::TableAlreadyExists(_))
    ));

    // Drop table
    db.drop_table("users").unwrap();
    assert!(db.list_tables().is_empty());
}

#[test]
fn test_schema_validations() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_test_schema()).unwrap();

    let tx = Transaction::new(1, db);

    // 1. Valid insert
    tx.put("users", k_int(1), r_user(1, "alice", 95.5)).unwrap();

    // 2. Schema mismatch: column count mismatch (only 2 values instead of 3)
    let bad_row_cols = Row::new(vec![DbValue::Int(2), DbValue::String("bob".to_string())]);
    assert!(matches!(
        tx.put("users", k_int(2), bad_row_cols),
        Err(RelationalError::SchemaMismatch(_))
    ));

    // 3. Schema mismatch: type mismatch (String instead of Float for score)
    let bad_row_types = Row::new(vec![
        DbValue::Int(2),
        DbValue::String("bob".to_string()),
        DbValue::String("bad_float".to_string()),
    ]);
    assert!(matches!(
        tx.put("users", k_int(2), bad_row_types),
        Err(RelationalError::SchemaMismatch(_))
    ));

    // 4. Primary key validation: Key type mismatch (DbKey::String instead of DbKey::Int)
    assert!(matches!(
        tx.put(
            "users",
            DbKey::String("1".to_string()),
            r_user(1, "alice", 95.5)
        ),
        Err(RelationalError::SchemaMismatch(_))
    ));

    // 5. Primary key validation: Key value mismatch (DbKey::Int(2) vs Row value DbValue::Int(1))
    assert!(matches!(
        tx.put("users", k_int(2), r_user(1, "alice", 95.5)),
        Err(RelationalError::SchemaMismatch(_))
    ));
}

#[test]
fn test_transaction_rollback() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_test_schema()).unwrap();

    let tx = Transaction::new(1, db);
    tx.put("users", k_int(1), r_user(1, "alice", 95.5)).unwrap();

    // Check we can read our own writes
    assert_eq!(
        tx.get("users", &k_int(1)).unwrap(),
        Some(r_user(1, "alice", 95.5))
    );

    // Rollback
    tx.rollback().unwrap();

    // Verify it is gone
    assert_eq!(tx.get("users", &k_int(1)).unwrap(), None);
}

#[test]
fn test_transaction_commit_and_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // 1. Open DB, perform write, and commit.
    {
        let db = Arc::new(Database::open(&db_path).unwrap());
        db.create_table("users", create_test_schema()).unwrap();

        let tx = Transaction::new(1, db);
        tx.put("users", k_int(1), r_user(1, "alice", 95.5)).unwrap();
        tx.put("users", k_int(2), r_user(2, "bob", 80.0)).unwrap();
        tx.commit().unwrap();
    }

    // 2. Re-open DB and verify data is persisted.
    {
        let db = Arc::new(Database::open(&db_path).unwrap());
        let tx = Transaction::new(2, db);

        assert_eq!(
            tx.get("users", &k_int(1)).unwrap(),
            Some(r_user(1, "alice", 95.5))
        );
        assert_eq!(
            tx.get("users", &k_int(2)).unwrap(),
            Some(r_user(2, "bob", 80.0))
        );
        assert_eq!(tx.get("users", &k_int(3)).unwrap(), None);
    }
}

#[test]
fn test_transaction_scans_and_merges() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_test_schema()).unwrap();

    // 1. Write some initial data to disk
    let tx1 = Transaction::new(1, db.clone());
    tx1.put("users", k_int(10), r_user(10, "ten", 10.0))
        .unwrap();
    tx1.put("users", k_int(20), r_user(20, "twenty", 20.0))
        .unwrap();
    tx1.put("users", k_int(30), r_user(30, "thirty", 30.0))
        .unwrap();
    tx1.commit().unwrap();

    // 2. Open a new transaction, modify data, delete data, and scan
    let tx2 = Transaction::new(2, db);

    // Update key 20, delete key 10, add key 25 inside tx buffer
    tx2.put("users", k_int(20), r_user(20, "twenty_new", 22.0))
        .unwrap();
    tx2.delete("users", k_int(10)).unwrap();
    tx2.put("users", k_int(25), r_user(25, "twentyfive", 25.0))
        .unwrap();

    // Scan range [10, 30]
    // Expect:
    // - key 10: omitted (deleted in tx)
    // - key 20: updated value
    // - key 25: new value
    // - key 30: unchanged value
    // Output must be sorted by key (10 < 20 < 25 < 30)
    let scan_res = tx2
        .filtered_scan("users", &k_int(10), &k_int(35), |row| {
            // Filter: only get users with score >= 20.0
            let val = row.get_by_name(&create_test_schema(), "score").unwrap();
            match val {
                DbValue::Float(f) => *f >= 20.0,
                _ => false,
            }
        })
        .unwrap();

    assert_eq!(scan_res.len(), 3);
    assert_eq!(scan_res[0], r_user(20, "twenty_new", 22.0));
    assert_eq!(scan_res[1], r_user(25, "twentyfive", 25.0));
    assert_eq!(scan_res[2], r_user(30, "thirty", 30.0));
}

#[test]
fn test_drop_table_blocks_on_active_transaction() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_test_schema()).unwrap();

    let tx = Transaction::new(1, db.clone());
    tx.put("users", k_int(1), r_user(1, "alice", 95.5)).unwrap();

    // Access the table to register active access
    tx.get("users", &k_int(1)).unwrap();

    let db_clone = db.clone();
    let drop_finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let drop_finished_clone = drop_finished.clone();
    let handle = std::thread::spawn(move || {
        db_clone.drop_table("users").unwrap();
        drop_finished_clone.store(true, std::sync::atomic::Ordering::SeqCst);
    });

    // Sleep to ensure the background drop_table thread has spawned and is waiting on active readers
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert!(!drop_finished.load(std::sync::atomic::Ordering::SeqCst));

    // Drop the transaction to unregister it and release the lock/wait
    drop(tx);

    // Wait for the drop_table thread to finish
    handle.join().unwrap();
    assert!(drop_finished.load(std::sync::atomic::Ordering::SeqCst));

    // Verify directory is deleted from disk
    let table_path = temp_dir.path().join("users");
    assert!(!table_path.exists());
}

#[test]
fn test_drop_table_stranded_cleanup_on_startup() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // 1. Open Database and create a table.
    {
        let db = Database::open(&db_path).unwrap();
        db.create_table("users", create_test_schema()).unwrap();
    }

    // 2. Create a fake stranded ".tmp_drop_" directory inside the database directory.
    let stranded_path = db_path.join(".tmp_drop_users_12345");
    std::fs::create_dir_all(&stranded_path).unwrap();
    std::fs::write(stranded_path.join("some_sst.sst"), b"garbage").unwrap();

    assert!(stranded_path.exists());

    // 3. Re-open Database (which should trigger startup cleanup).
    {
        let _db = Database::open(&db_path).unwrap();
    }

    // 4. Verify the stranded directory was deleted.
    assert!(!stranded_path.exists());
}
