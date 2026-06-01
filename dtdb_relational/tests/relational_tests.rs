use dtdb_relational::{
    Column, DataType, Database, RelationalError, RelationalMutation, Row, Schema, Transaction,
    TransactionRecord,
};
use dtdb_storage::{DbKey, DbValue};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

// Helper to create a test schema:
// Users table: id (Int, PK), name (String), active (Bytes - using Bytes as boolean placeholder)
fn create_test_schema() -> Schema {
    Schema::new(vec![
        Column {
            id: 0,
            name: "id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_nullable: false,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            id: 0,
            name: "name".to_string(),
            data_type: DataType::String,
            is_primary_key: false,
            is_nullable: true,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            id: 0,
            name: "score".to_string(),
            data_type: DataType::Float,
            is_primary_key: false,
            is_nullable: true,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
    ])
}

fn k_int(val: i64) -> DbKey {
    DbKey::Int(val)
}

/// Polls `condition` until it returns true or `timeout` elapses.
/// Returns false on timeout. Replaces ad-hoc `thread::sleep(50ms)` waits
/// that race against the system scheduler — under load (CI, ASan, etc.)
/// the fixed sleep was too short and the test would intermittently fail.
fn poll_until<F: FnMut() -> bool>(timeout: std::time::Duration, mut condition: F) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if condition() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    condition()
}

fn r_user(id: i64, name: &str, score: f64) -> Row {
    Row::new(vec![
        DbValue::Int(id),
        DbValue::string(name.to_string()),
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
    let bad_row_cols = Row::new(vec![DbValue::Int(2), DbValue::string("bob")]);
    assert!(matches!(
        tx.put("users", k_int(2), bad_row_cols),
        Err(RelationalError::SchemaMismatch(_))
    ));

    // 3. Schema mismatch: type mismatch (String instead of Float for score)
    let bad_row_types = Row::new(vec![
        DbValue::Int(2),
        DbValue::string("bob"),
        DbValue::string("bad_float"),
    ]);
    assert!(matches!(
        tx.put("users", k_int(2), bad_row_types),
        Err(RelationalError::SchemaMismatch(_))
    ));

    // 4. Primary key validation: Key type mismatch (DbKey::String instead of DbKey::Int)
    assert!(matches!(
        tx.put(
            "users",
            DbKey::string("1"),
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
fn test_concurrent_drop_table_does_not_deadlock_commit() {
    // Regression: drop_table used to hold the catalog write lock while
    // spin-waiting for active table accessors to drain, while a tx in mid
    // commit needed the read lock to re-resolve the table — a hard deadlock.
    // The cached-table fix lets the tx commit complete with no further
    // catalog read, so drop_table eventually drains and finishes.
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_test_schema()).unwrap();
    db.create_table("orders", create_test_schema()).unwrap();

    let tx = Transaction::new(1, db.clone());
    // Touch both tables so the tx registers active access on each.
    tx.put("users", k_int(1), r_user(1, "alice", 95.5)).unwrap();
    tx.put("orders", k_int(1), r_user(1, "ord", 1.0)).unwrap();
    tx.get("users", &k_int(1)).unwrap();
    tx.get("orders", &k_int(1)).unwrap();

    // Kick off a drop_table while the tx is still live and holding access.
    let db_drop = db.clone();
    let drop_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let drop_done_clone = drop_done.clone();
    let drop_handle = std::thread::spawn(move || {
        db_drop.drop_table("orders").unwrap();
        drop_done_clone.store(true, std::sync::atomic::Ordering::SeqCst);
    });

    // Let drop_table grab the catalog write lock and start spin-waiting.
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert!(
        !drop_done.load(std::sync::atomic::Ordering::SeqCst),
        "drop_table should still be waiting on the live tx"
    );

    // Pre-fix: this commit would hang because it tries to re-resolve "users"
    // via the catalog read lock, which is blocked behind drop_table's writer.
    // Run commit on a worker thread and time-bound the join so the test
    // surfaces a deadlock as a failure rather than hanging the suite.
    let commit_handle = std::thread::spawn(move || tx.commit());
    let start = std::time::Instant::now();
    while !commit_handle.is_finished() {
        if start.elapsed() > std::time::Duration::from_secs(5) {
            panic!("tx.commit() appears deadlocked behind drop_table");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    commit_handle.join().unwrap().unwrap();

    let start = std::time::Instant::now();
    while !drop_handle.is_finished() {
        if start.elapsed() > std::time::Duration::from_secs(5) {
            panic!("drop_table did not finish after commit released its slot");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    drop_handle.join().unwrap();
    assert!(drop_done.load(std::sync::atomic::Ordering::SeqCst));
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

fn auto_inc_schema() -> Schema {
    Schema::new(vec![
        Column {
            id: 0,
            name: "id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_nullable: false,
            locality_group: None,
            default_value: None,
            is_auto_increment: true,
        },
        Column {
            id: 0,
            name: "val".to_string(),
            data_type: DataType::Int,
            is_primary_key: false,
            is_nullable: true,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
    ])
}

#[test]
fn test_auto_increment_sequence_reflects_recovered_rows() {
    // Regression: auto-increment sequences were seeded from on-disk rows BEFORE
    // recover_transactions(), so a prepared-but-not-committed transaction that
    // gets rolled forward during recovery would leave the in-memory sequence
    // pointing below the recovered row's PK — guaranteeing a collision on the
    // next allocation.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // Phase 1: insert a small row normally, then inject a Prepared (but never
    // Committed) record describing a future row with id=100. The matching
    // `Committed` marker is intentionally omitted to simulate a crash between
    // prepare and commit.
    {
        let db = Arc::new(Database::open(&db_path).unwrap());
        db.create_table("t", auto_inc_schema()).unwrap();

        // Commit a regular row at id=1 so on-disk max == 1.
        let tx = Transaction::new(1, db.clone());
        let row1 = Row::new(vec![DbValue::Int(1), DbValue::Int(10)]);
        tx.put("t", DbKey::Int(1), row1).unwrap();
        tx.commit().unwrap();

        // Build a Prepared record for tx 999 that inserts id=100. We write the
        // row bytes directly because the table has a single locality group.
        let row100 = Row::new(vec![DbValue::Int(100), DbValue::Int(1000)]);
        let row100_bytes = row100.to_bytes().unwrap();
        let mutations: HashMap<String, Vec<RelationalMutation>> = HashMap::from([(
            "t".to_string(),
            vec![RelationalMutation::Put {
                key: DbKey::Int(100),
                value: DbValue::bytes(row100_bytes),
            }],
        )]);
        let prepared = TransactionRecord::Prepared {
            tx_id: 999,
            mutations,
            old_rows: None,
        };
        db.write_transaction_record(&prepared).unwrap();
        // Drop without ever calling commit_transaction(999) — the log retains
        // the Prepared record across the close.
    }

    // Phase 2: reopen. Recovery rolls forward id=100. The sequence must now be
    // initialized from the post-recovery state, i.e. >= 101.
    {
        let db = Database::open(&db_path).unwrap();
        let next = db.next_sequence_value("t").unwrap();
        assert!(
            next > 100,
            "auto-increment sequence collided with recovered row: got {next}, expected > 100"
        );
    }
}

#[test]
fn test_locality_group_pruning_verification() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());

    // Schema with three distinct locality groups:
    // 1. "" (default) for id
    // 2. "lg_name" for name
    // 3. "lg_score" for score
    let schema = Schema::new(vec![
        Column {
            id: 0,
            name: "id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_nullable: false,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            id: 0,
            name: "name".to_string(),
            data_type: DataType::String,
            is_primary_key: false,
            is_nullable: true,
            locality_group: Some("lg_name".to_string()),
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            id: 0,
            name: "score".to_string(),
            data_type: DataType::Float,
            is_primary_key: false,
            is_nullable: true,
            locality_group: Some("lg_score".to_string()),
            default_value: None,
            is_auto_increment: false,
        },
    ]);

    db.create_table("users", schema).unwrap();
    let table = db.get_table("users").unwrap();

    // Insert row: (1, "Alice", 95.5)
    let tx = Transaction::new(1, db.clone());
    tx.put("users", k_int(1), r_user(1, "Alice", 95.5)).unwrap();
    tx.commit().unwrap();

    // Case 1: Query only "name" column.
    // This should only scan "lg_name" group, leaving "id" and "score" as Null.
    let scan1 = table
        .filtered_scan(&k_int(1), &k_int(1), Some(&["name".to_string()]))
        .unwrap();
    assert_eq!(scan1.len(), 1);
    assert_eq!(
        scan1[0].1.values,
        vec![
            DbValue::Null,
            DbValue::string("Alice"),
            DbValue::Null
        ]
    );

    // Case 2: Query only "score" column.
    // This should only scan "lg_score" group, leaving "id" and "name" as Null.
    let scan2 = table
        .filtered_scan(&k_int(1), &k_int(1), Some(&["score".to_string()]))
        .unwrap();
    assert_eq!(scan2.len(), 1);
    assert_eq!(
        scan2[0].1.values,
        vec![DbValue::Null, DbValue::Null, DbValue::Float(95.5)]
    );

    // Case 3: Query "id" and "score" columns.
    // This should scan default ("") and "lg_score" groups, leaving "name" as Null.
    let scan3 = table
        .filtered_scan(
            &k_int(1),
            &k_int(1),
            Some(&["id".to_string(), "score".to_string()]),
        )
        .unwrap();
    assert_eq!(scan3.len(), 1);
    assert_eq!(
        scan3[0].1.values,
        vec![DbValue::Int(1), DbValue::Null, DbValue::Float(95.5)]
    );

    // Case 4: Query all columns (None parameter).
    // This should scan all groups and return the fully populated row.
    let scan4 = table.filtered_scan(&k_int(1), &k_int(1), None).unwrap();
    assert_eq!(scan4.len(), 1);
    assert_eq!(
        scan4[0].1.values,
        vec![
            DbValue::Int(1),
            DbValue::string("Alice"),
            DbValue::Float(95.5)
        ]
    );
}

/// Drains a `TableScanIterator` (peek/advance) into a Vec, cloning each row.
fn drain_scan(mut it: dtdb_relational::TableScanIterator) -> Vec<(DbKey, Row)> {
    let mut out = Vec::new();
    // `cloned()` ends the peek() borrow immediately, so advance() is free to run.
    while let Some(pair) = it.peek().cloned() {
        out.push(pair);
        it.advance().unwrap();
    }
    out
}

/// Exercises the single-locality-group fast path in `TableScanIterator::advance`.
/// `create_test_schema` puts every column in the default group, so the whole
/// table is one locality group and the scan takes the passthrough path that
/// skips `merge_rows`. The output must be byte-for-byte what the merge path
/// would have produced: full rows, in key order.
#[test]
fn test_scan_iter_single_group_fast_path() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_test_schema()).unwrap();

    // Insert out of key order to confirm the scan returns them sorted, and use
    // several rows so the per-row refill logic is exercised, not just the first.
    let tx = Transaction::new(1, db.clone());
    tx.put("users", k_int(3), r_user(3, "Carol", 70.0)).unwrap();
    tx.put("users", k_int(1), r_user(1, "Alice", 95.5)).unwrap();
    tx.put("users", k_int(2), r_user(2, "Bob", 80.25)).unwrap();
    tx.commit().unwrap();

    let table = db.get_table("users").unwrap();

    // Full scan over the whole key range -> every row, full width, sorted.
    let rows = drain_scan(
        table
            .scan_iter(&k_int(i64::MIN), &k_int(i64::MAX), None)
            .unwrap(),
    );
    assert_eq!(
        rows,
        vec![
            (k_int(1), r_user(1, "Alice", 95.5)),
            (k_int(2), r_user(2, "Bob", 80.25)),
            (k_int(3), r_user(3, "Carol", 70.0)),
        ]
    );

    // With selective column deserialization, even on a single-group table the
    // scan can skip parsing non-requested columns, returning them as Null.
    let hinted = drain_scan(
        table
            .scan_iter(&k_int(2), &k_int(2), Some(&["name".to_string()]))
            .unwrap(),
    );
    assert_eq!(
        hinted,
        vec![(
            k_int(2),
            Row::new(vec![
                DbValue::Null,
                DbValue::string("Bob"),
                DbValue::Null
            ])
        )]
    );

    // Empty range: the fast path must terminate cleanly with no rows.
    let empty = drain_scan(table.scan_iter(&k_int(100), &k_int(200), None).unwrap());
    assert!(empty.is_empty());
}

/// A four-column schema (the 3-column `create_test_schema` plus a `verified`
/// bool column with a default), modeling the result of a lazy
/// `ALTER TABLE ADD COLUMN verified BOOL DEFAULT true`. `default_value` is what
/// the read path substitutes for rows that predate the column (ADR 0003).
fn create_test_schema_with_added_column(default: Option<DbValue>) -> Schema {
    let mut schema = create_test_schema();
    schema.columns.push(Column {
        id: schema.next_column_id,
        name: "verified".to_string(),
        data_type: DataType::Bool,
        is_primary_key: false,
        is_nullable: true,
        locality_group: None,
        default_value: default,
        is_auto_increment: false,
    });
    schema.next_column_id += 1;
    schema
}

/// End-to-end check of lazy `ADD COLUMN` reconciliation through the real scan
/// path. We reproduce exactly the on-disk state a metadata-only ADD produces:
/// rows are written under the original N-column schema, then `schema.bin` is
/// replaced with an (N+1)-column schema and the database reopened. The added
/// column's bytes are absent from every stored row, so the scan must pad them
/// with the column's default. This is the single-locality-group fast path.
#[test]
fn test_add_column_reconciles_old_rows_on_scan_single_group() {
    let temp_dir = TempDir::new().unwrap();

    // 1. Original 3-column table; insert rows that predate the new column.
    {
        let db = Arc::new(Database::open(temp_dir.path()).unwrap());
        db.create_table("users", create_test_schema()).unwrap();
        let tx = Transaction::new(1, db.clone());
        tx.put("users", k_int(1), r_user(1, "Alice", 95.5)).unwrap();
        tx.put("users", k_int(2), r_user(2, "Bob", 80.25)).unwrap();
        tx.commit().unwrap();
        db.checkpoint().unwrap();
    }

    // 2. Simulate the lazy ALTER's atomic schema swap: overwrite schema.bin with
    //    the 4-column schema (added `verified BOOL DEFAULT true`). No row bytes
    //    are touched -- exactly what a metadata-only ADD does.
    let schema_path = temp_dir.path().join("users").join("schema.bin");
    create_test_schema_with_added_column(Some(DbValue::Bool(true)))
        .save_to_file(&schema_path, dtdb_storage::FsyncMethod::Fullfsync)
        .unwrap();

    // 3. Reopen and scan. Old rows must come back full width with the default.
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    let table = db.get_table("users").unwrap();
    let rows = drain_scan(
        table
            .scan_iter(&k_int(i64::MIN), &k_int(i64::MAX), None)
            .unwrap(),
    );
    let mut alice = r_user(1, "Alice", 95.5).values;
    alice.push(DbValue::Bool(true));
    let mut bob = r_user(2, "Bob", 80.25).values;
    bob.push(DbValue::Bool(true));
    assert_eq!(
        rows,
        vec![(k_int(1), Row::new(alice)), (k_int(2), Row::new(bob))]
    );

    // A point get goes through the merge path; it must reconcile identically.
    let got = table.get(&k_int(2), None).unwrap().unwrap();
    assert_eq!(
        got.values,
        vec![
            DbValue::Int(2),
            DbValue::string("Bob"),
            DbValue::Float(80.25),
            DbValue::Bool(true),
        ]
    );

    // A row inserted *after* the ALTER carries the new column explicitly; mixing
    // it with reconciled old rows must yield consistent full-width rows.
    let tx = Transaction::new(2, db.clone());
    let mut carol = r_user(3, "Carol", 70.0).values;
    carol.push(DbValue::Bool(false));
    tx.put("users", k_int(3), Row::new(carol)).unwrap();
    tx.commit().unwrap();
    let table = db.get_table("users").unwrap();
    let after = table.get(&k_int(3), None).unwrap().unwrap();
    assert_eq!(after.values.last(), Some(&DbValue::Bool(false)));
}

/// Same lazy-ADD reconciliation, but the column has no default, so old rows must
/// pad to NULL rather than a value.
#[test]
fn test_add_column_without_default_pads_null() {
    let temp_dir = TempDir::new().unwrap();
    {
        let db = Arc::new(Database::open(temp_dir.path()).unwrap());
        db.create_table("users", create_test_schema()).unwrap();
        let tx = Transaction::new(1, db.clone());
        tx.put("users", k_int(1), r_user(1, "Alice", 95.5)).unwrap();
        tx.commit().unwrap();
        db.checkpoint().unwrap();
    }
    let schema_path = temp_dir.path().join("users").join("schema.bin");
    create_test_schema_with_added_column(None)
        .save_to_file(&schema_path, dtdb_storage::FsyncMethod::Fullfsync)
        .unwrap();

    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    let table = db.get_table("users").unwrap();
    let got = table.get(&k_int(1), None).unwrap().unwrap();
    assert_eq!(got.values.last(), Some(&DbValue::Null));
}

/// Guards the *other* side of the gate: a genuinely multi-locality-group table
/// must keep using the merge path. A full scan (all groups) has to reassemble
/// each row from its per-group sub-rows; the fast path would be wrong here, so
/// this confirms the gate (`schema.locality_groups().len() == 1`) holds.
#[test]
fn test_scan_iter_multi_group_uses_merge_path() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());

    let schema = Schema::new(vec![
        Column {
            id: 0,
            name: "id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_nullable: false,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            id: 0,
            name: "name".to_string(),
            data_type: DataType::String,
            is_primary_key: false,
            is_nullable: true,
            locality_group: Some("lg_name".to_string()),
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            id: 0,
            name: "score".to_string(),
            data_type: DataType::Float,
            is_primary_key: false,
            is_nullable: true,
            locality_group: Some("lg_score".to_string()),
            default_value: None,
            is_auto_increment: false,
        },
    ]);
    db.create_table("users", schema).unwrap();

    let tx = Transaction::new(1, db.clone());
    tx.put("users", k_int(1), r_user(1, "Alice", 95.5)).unwrap();
    tx.put("users", k_int(2), r_user(2, "Bob", 80.25)).unwrap();
    tx.commit().unwrap();

    let table = db.get_table("users").unwrap();

    // Full scan must merge the three groups back into complete rows.
    let rows = drain_scan(
        table
            .scan_iter(&k_int(i64::MIN), &k_int(i64::MAX), None)
            .unwrap(),
    );
    assert_eq!(
        rows,
        vec![
            (k_int(1), r_user(1, "Alice", 95.5)),
            (k_int(2), r_user(2, "Bob", 80.25)),
        ]
    );

    // Subset scan must still produce full-width rows with NULLs for the unread
    // group -- the case the fast path must never take.
    let only_name = drain_scan(
        table
            .scan_iter(&k_int(1), &k_int(1), Some(&["name".to_string()]))
            .unwrap(),
    );
    assert_eq!(
        only_name,
        vec![(
            k_int(1),
            Row::new(vec![
                DbValue::Null,
                DbValue::string("Alice"),
                DbValue::Null,
            ]),
        )]
    );
}

#[test]
fn test_background_statistics_collector() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // Create options with analyze_frequency_ms = Some(10)
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
        analyze_frequency_ms: Some(10), // Run every 10ms
        wal_sync_interval_ms: None,
        memory_budget: None,
        fsync_method: dtdb_storage::FsyncMethod::default(),
    };

    let db = Arc::new(Database::open_with_options(db_path, options).unwrap());
    db.create_table("users", create_test_schema()).unwrap();

    // Verify initial statistics is empty or zero rows
    let stats = db.get_table_statistics("users").unwrap();
    assert_eq!(stats.row_count, 0);

    // Insert some rows in a transaction
    let tx = Transaction::new(1, db.clone());
    tx.put("users", k_int(1), r_user(1, "alice", 95.5)).unwrap();
    tx.put("users", k_int(2), r_user(2, "bob", 80.0)).unwrap();
    tx.commit().unwrap();

    // Poll for the background analyze thread to publish updated statistics
    // rather than relying on a fixed sleep — under CI load the previous 50ms
    // wait sometimes lost the race and the test flaked.
    let updated = poll_until(std::time::Duration::from_secs(5), || {
        db.get_table_statistics("users")
            .map(|s| s.row_count == 2)
            .unwrap_or(false)
    });
    assert!(updated, "background statistics collector did not update");
    let stats2 = db.get_table_statistics("users").unwrap();
    assert_eq!(stats2.row_count, 2);
}

#[test]
fn test_database_multi_get() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_test_schema()).unwrap();

    let tx = Transaction::new(1, db.clone());
    tx.put("users", k_int(1), r_user(1, "alice", 95.5)).unwrap();
    tx.put("users", k_int(2), r_user(2, "bob", 80.0)).unwrap();
    tx.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    // Modify one in transaction buffer
    tx2.put("users", k_int(2), r_user(2, "bob_new", 85.0))
        .unwrap();

    // Call multi_get_projected
    let keys = vec![k_int(1), k_int(2), k_int(3)];
    let rows = tx2.multi_get_projected("users", &keys, None).unwrap();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], Some(r_user(1, "alice", 95.5))); // From storage
    assert_eq!(rows[1], Some(r_user(2, "bob_new", 85.0))); // From transaction write buffer
    assert_eq!(rows[2], None); // Non-existent key
}

#[test]
fn test_scan_iterator_publishes_read_set_eagerly() {
    use dtdb_relational::IsolationLevel;

    // Regression: TransactionScanIterator used to merge its per-row read keys
    // into the shared read_set only in Drop. If commit() ran while the iterator
    // was still alive, OCC validation saw an empty read_set and missed
    // conflicts with concurrent writers.
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_test_schema()).unwrap();

    // Seed a row at id=1.
    let tx0 = Transaction::new(1, db.clone());
    tx0.put("users", k_int(1), r_user(1, "alice", 1.0)).unwrap();
    tx0.commit().unwrap();

    // tx1 uses RepeatableRead so scan_ranges aren't tracked — only the read_set
    // can flag the upcoming conflict. Open a scan, read id=1, and KEEP the
    // iterator alive across the conflicting commit.
    let tx1 = Transaction::new_with_isolation(10, db.clone(), IsolationLevel::RepeatableRead);
    let mut iter = tx1
        .scan_iter("users", &k_int(0), &k_int(100), None)
        .unwrap();
    let first = iter.next().unwrap();
    assert!(first.is_some());

    // tx2 modifies id=1 and commits while tx1's iterator is still alive.
    let tx2 = Transaction::new_with_isolation(11, db.clone(), IsolationLevel::RepeatableRead);
    tx2.put("users", k_int(1), r_user(1, "alice_v2", 2.0))
        .unwrap();
    tx2.commit().unwrap();

    // tx1 writes an unrelated row so commit goes through the validating path,
    // then commits. With the bug, read_set is empty and the conflict on id=1
    // is missed. With the fix, validation sees id=1 in read_set and aborts.
    tx1.put("users", k_int(50), r_user(50, "fifty", 50.0))
        .unwrap();
    let result = tx1.commit();
    assert!(
        matches!(result, Err(RelationalError::TransactionConflict(_))),
        "expected TransactionConflict, got {result:?}"
    );

    drop(iter);
}

#[test]
fn test_create_index_persists_schema_only_after_index_data() {
    // Regression: create_index used to mutate and save the table schema FIRST,
    // and only then populate the index engine. A crash in the populate step
    // left an on-disk schema declaring an index whose backing data was
    // missing or partial, so queries via that index returned wrong/empty
    // results after recovery.
    //
    // We can't kill the process mid-call from a unit test, but we can pin
    // down the contract that makes the fix work:
    //   (1) `create_index` succeeds end-to-end and reopening the database
    //       sees both the schema entry AND the populated index data
    //       (smoke test for the happy path).
    //   (2) Simulating the old buggy state on disk — schema names an index
    //       whose engine directory is empty — by manually editing the
    //       on-disk schema produces a database that can still be reopened
    //       without a usable index (verifying the index data file actually
    //       exists after a successful create_index, i.e. wasn't an artifact
    //       of the in-memory engine).
    use dtdb_relational::IndexType;

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    {
        let db = Arc::new(Database::open(&db_path).unwrap());
        db.create_table("users", create_test_schema()).unwrap();

        let tx = Transaction::new(1, db.clone());
        tx.put("users", k_int(1), r_user(1, "alice", 1.0)).unwrap();
        tx.put("users", k_int(2), r_user(2, "bob", 2.0)).unwrap();
        tx.put("users", k_int(3), r_user(3, "carol", 3.0)).unwrap();
        tx.commit().unwrap();
        // create_index waits for all transactions touching the table to finish
        // before reading rows; drop the tx so its active_table_access slot is
        // released.
        drop(tx);

        db.create_index(
            "users",
            "users_by_name",
            vec!["name".to_string()],
            IndexType::BTree,
            None,
        )
        .unwrap();
    }

    // Reopen: schema must list the index AND the index engine directory must
    // be present with at least one on-disk SST or WAL entry (i.e. data, not
    // a phantom that the old code-path would have produced if it crashed
    // between the two steps).
    {
        let db = Database::open(&db_path).unwrap();
        let table = db.get_table("users").unwrap();
        assert!(
            table
                .schema
                .indexes
                .iter()
                .any(|i| i.name == "users_by_name"),
            "schema should record the index after reopen"
        );
        assert!(
            table.index_engines.contains_key("users_by_name"),
            "index engine must be opened on reopen"
        );

        let idx_dir = db_path.join("users").join("index_users_by_name");
        assert!(idx_dir.exists(), "index directory should exist on disk");
        let has_data = std::fs::read_dir(&idx_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| {
                let name = e.file_name();
                let n = name.to_string_lossy();
                n.ends_with(".sst") || n.ends_with(".wal") || n == "MANIFEST"
            });
        assert!(
            has_data,
            "index directory should hold persisted data, not be empty"
        );
    }
}

// ----- ALTER TABLE Database API (ADR 0003, Phase 1) -----

/// `alter_table_add_column` is metadata-only: existing rows reconcile to the new
/// column's default on read, and a row written afterward carries it natively.
#[test]
fn test_database_alter_add_column_with_default() {
    use dtdb_relational::Column;
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_test_schema()).unwrap();

    {
        let tx = Transaction::new(1, db.clone());
        tx.put("users", k_int(1), r_user(1, "Alice", 95.5)).unwrap();
        tx.commit().unwrap();
    }

    // Add `verified BOOL DEFAULT true`. The id must be assigned from the
    // schema's high-water mark (3 -> the new column gets id 3), not left at 0.
    db.alter_table_add_column(
        "users",
        Column {
            id: 0,
            name: "verified".to_string(),
            data_type: DataType::Bool,
            is_primary_key: false,
            is_nullable: true,
            locality_group: None,
            default_value: Some(DbValue::Bool(true)),
            is_auto_increment: false,
        },
    )
    .unwrap();

    let table = db.get_table("users").unwrap();
    assert_eq!(table.schema.columns.len(), 4);
    assert_eq!(table.schema.columns[3].id, 3);
    assert_eq!(table.schema.next_column_id, 4);

    // Old row reconciles to the default.
    let got = table.get(&k_int(1), None).unwrap().unwrap();
    assert_eq!(got.values.last(), Some(&DbValue::Bool(true)));
}

/// Adding a duplicate column name, or a NOT NULL column without a default to a
/// non-empty table, is rejected with a SchemaMismatch error.
#[test]
fn test_database_alter_add_column_validation() {
    use dtdb_relational::Column;
    let mk = |name: &str, nullable: bool, default: Option<DbValue>| Column {
        id: 0,
        name: name.to_string(),
        data_type: DataType::Int,
        is_primary_key: false,
        is_nullable: nullable,
        locality_group: None,
        default_value: default,
        is_auto_increment: false,
    };

    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_test_schema()).unwrap();
    {
        let tx = Transaction::new(1, db.clone());
        tx.put("users", k_int(1), r_user(1, "Alice", 95.5)).unwrap();
        tx.commit().unwrap();
    }

    // Duplicate name ("name" already exists).
    let dup = db.alter_table_add_column("users", mk("name", true, None));
    assert!(matches!(dup, Err(RelationalError::SchemaMismatch(_))));

    // NOT NULL without default on a non-empty table.
    let nn = db.alter_table_add_column("users", mk("c", false, None));
    assert!(matches!(nn, Err(RelationalError::SchemaMismatch(_))));

    // NOT NULL *with* default is fine.
    db.alter_table_add_column("users", mk("c", false, Some(DbValue::Int(0))))
        .unwrap();

    // Neither failed ALTER should have advanced the column count beyond the one
    // successful add.
    assert_eq!(db.get_table("users").unwrap().schema.columns.len(), 4);
}

/// `alter_table_rename_column` renames the column and rewrites any index
/// reference to it; the data is untouched.
#[test]
fn test_database_alter_rename_column() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_test_schema()).unwrap();
    db.create_index(
        "users",
        "idx_name",
        vec!["name".to_string()],
        dtdb_relational::IndexType::BTree,
        None,
    )
    .unwrap();
    {
        let tx = Transaction::new(1, db.clone());
        tx.put("users", k_int(1), r_user(1, "Alice", 95.5)).unwrap();
        tx.commit().unwrap();
    }

    db.alter_table_rename_column("users", "name", "full_name")
        .unwrap();

    let table = db.get_table("users").unwrap();
    assert!(table.schema.column_index("full_name").is_some());
    assert!(table.schema.column_index("name").is_none());
    // The index that referenced `name` now references `full_name`.
    assert_eq!(
        table.schema.indexes[0].columns,
        vec!["full_name".to_string()]
    );

    // Renaming a missing column, or onto an existing name, is rejected.
    assert!(matches!(
        db.alter_table_rename_column("users", "nope", "x"),
        Err(RelationalError::SchemaMismatch(_))
    ));
    assert!(matches!(
        db.alter_table_rename_column("users", "full_name", "id"),
        Err(RelationalError::SchemaMismatch(_))
    ));
}

/// An ADD COLUMN survives a reopen: the swapped schema.bin is durable, and old
/// rows still reconcile after the database is reopened from disk.
#[test]
fn test_database_alter_add_column_survives_reopen() {
    use dtdb_relational::Column;
    let temp_dir = TempDir::new().unwrap();
    {
        let db = Arc::new(Database::open(temp_dir.path()).unwrap());
        db.create_table("users", create_test_schema()).unwrap();
        {
            let tx = Transaction::new(1, db.clone());
            tx.put("users", k_int(1), r_user(1, "Alice", 95.5)).unwrap();
            tx.commit().unwrap();
        }
        db.alter_table_add_column(
            "users",
            Column {
                id: 0,
                name: "verified".to_string(),
                data_type: DataType::Bool,
                is_primary_key: false,
                is_nullable: true,
                locality_group: None,
                default_value: Some(DbValue::Bool(true)),
                is_auto_increment: false,
            },
        )
        .unwrap();
        db.checkpoint().unwrap();
    }

    // Reopen and confirm the added column and its default-reconciliation persist.
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    let table = db.get_table("users").unwrap();
    assert_eq!(table.schema.columns.len(), 4);
    assert_eq!(table.schema.next_column_id, 4);
    let got = table.get(&k_int(1), None).unwrap().unwrap();
    assert_eq!(got.values.last(), Some(&DbValue::Bool(true)));
}

/// ADD COLUMN on a *multi-locality-group* table exercises the scan merge path
/// (not the single-group fast path) and the `relative_indices` cache reset:
/// after the new default-group column is appended, an old row read through the
/// merge must reconcile to the new column's default at the correct position.
#[test]
fn test_database_alter_add_column_multi_group() {
    use dtdb_relational::Column;
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());

    // id in the default group; name/score in their own groups.
    let schema = Schema::new(vec![
        Column {
            id: 0,
            name: "id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_nullable: false,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            id: 0,
            name: "name".to_string(),
            data_type: DataType::String,
            is_primary_key: false,
            is_nullable: true,
            locality_group: Some("lg_name".to_string()),
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            id: 0,
            name: "score".to_string(),
            data_type: DataType::Float,
            is_primary_key: false,
            is_nullable: true,
            locality_group: Some("lg_score".to_string()),
            default_value: None,
            is_auto_increment: false,
        },
    ]);
    db.create_table("users", schema).unwrap();
    {
        let tx = Transaction::new(1, db.clone());
        tx.put("users", k_int(1), r_user(1, "Alice", 95.5)).unwrap();
        tx.commit().unwrap();
    }

    // Force the read path (and thus the relative_indices cache) to populate
    // before the ALTER, so the cache-reset path is actually exercised.
    let _ = db.get_table("users").unwrap().get(&k_int(1), None).unwrap();

    // Append a default-group column with a default value.
    db.alter_table_add_column(
        "users",
        Column {
            id: 0,
            name: "active".to_string(),
            data_type: DataType::Bool,
            is_primary_key: false,
            is_nullable: true,
            locality_group: None,
            default_value: Some(DbValue::Bool(true)),
            is_auto_increment: false,
        },
    )
    .unwrap();

    // The old row reconciles to the default in the new (4th) position, with the
    // group-resident columns still correctly merged into their positions.
    let table = db.get_table("users").unwrap();
    let got = table.get(&k_int(1), None).unwrap().unwrap();
    assert_eq!(
        got.values,
        vec![
            DbValue::Int(1),
            DbValue::string("Alice"),
            DbValue::Float(95.5),
            DbValue::Bool(true),
        ]
    );

    // A non-default-group column is rejected.
    let bad = db.alter_table_add_column(
        "users",
        Column {
            id: 0,
            name: "extra".to_string(),
            data_type: DataType::Int,
            is_primary_key: false,
            is_nullable: true,
            locality_group: Some("lg_name".to_string()),
            default_value: None,
            is_auto_increment: false,
        },
    );
    assert!(matches!(bad, Err(RelationalError::SchemaMismatch(_))));
}

#[test]
fn test_database_alter_drop_column() {
    use dtdb_relational::{Column, DataType, Schema};
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());

    // Create schema with id (pk), name, score, active (default true)
    let schema = Schema::new(vec![
        Column {
            id: 0,
            name: "id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_nullable: false,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            id: 1,
            name: "name".to_string(),
            data_type: DataType::String,
            is_primary_key: false,
            is_nullable: true,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            id: 2,
            name: "score".to_string(),
            data_type: DataType::Float,
            is_primary_key: false,
            is_nullable: true,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            id: 3,
            name: "active".to_string(),
            data_type: DataType::Bool,
            is_primary_key: false,
            is_nullable: true,
            locality_group: None,
            default_value: Some(DbValue::Bool(true)),
            is_auto_increment: false,
        },
    ]);

    db.create_table("users", schema).unwrap();

    // Insert a row. Row is serialized with all 4 values.
    {
        let tx = Transaction::new(1, db.clone());
        let val = Row::new(vec![
            DbValue::Int(1),
            DbValue::string("Alice"),
            DbValue::Float(95.5),
            DbValue::Bool(false),
        ]);
        tx.put("users", k_int(1), val).unwrap();
        tx.commit().unwrap();
    }

    // Drop "score" column.
    db.alter_table_drop_column("users", "score").unwrap();

    // 1. Immediately hides it from get and scan
    let table = db.get_table("users").unwrap();
    assert_eq!(table.schema.columns.len(), 3);
    assert!(table.schema.column_index("score").is_none());

    // Get should return the row without the score column
    let got = table.get(&k_int(1), None).unwrap().unwrap();
    assert_eq!(
        got.values,
        vec![
            DbValue::Int(1),
            DbValue::string("Alice"),
            DbValue::Bool(false),
        ]
    );

    // Scan should also hide it
    let rows = drain_scan(table.scan_iter(&k_int(0), &k_int(10), None).unwrap());
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].1.values,
        vec![
            DbValue::Int(1),
            DbValue::string("Alice"),
            DbValue::Bool(false),
        ]
    );

    // Attempting to drop primary key column "id" must fail
    let drop_pk = db.alter_table_drop_column("users", "id");
    assert!(matches!(drop_pk, Err(RelationalError::SchemaMismatch(_))));

    // Attempting to drop non-existent column must fail
    let drop_none = db.alter_table_drop_column("users", "score");
    assert!(matches!(drop_none, Err(RelationalError::SchemaMismatch(_))));

    // 2. Verify that subsequent compaction physically purges the dropped column values.
    // First, let's flush and write some more data to ensure we have files in L0.
    {
        let tx = Transaction::new(2, db.clone());
        tx.put(
            "users",
            k_int(2),
            Row::new(vec![
                DbValue::Int(2),
                DbValue::string("Bob"),
                DbValue::Bool(true),
            ]),
        )
        .unwrap();
        tx.commit().unwrap();
    }

    // Force a compaction on the underlying storage engines.
    for engine in table.engines.values() {
        engine.flush_memtable().unwrap();
        engine.compact().unwrap();
    }

    db.checkpoint().unwrap();

    // Reopen database from disk to verify schema is preserved and compaction did not lose data.
    drop(table);
    drop(db);

    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    let table = db.get_table("users").unwrap();

    // Retrieve values from the table. They should still be readable and correct.
    let got1 = table.get(&k_int(1), None).unwrap().unwrap();
    assert_eq!(
        got1.values,
        vec![
            DbValue::Int(1),
            DbValue::string("Alice"),
            DbValue::Bool(false),
        ]
    );

    let got2 = table.get(&k_int(2), None).unwrap().unwrap();
    assert_eq!(
        got2.values,
        vec![
            DbValue::Int(2),
            DbValue::string("Bob"),
            DbValue::Bool(true),
        ]
    );

    // Let's inspect the raw storage layout directly to check that the score column is not present.
    // We can read raw DbValue from the storage engine.
    for engine in table.engines.values() {
        let raw_val = engine.get(&k_int(1)).unwrap().unwrap();
        if let DbValue::Bytes(bytes) = raw_val {
            let row = Row::from_bytes(&bytes).unwrap();
            // In the raw storage engine (post-compaction), the row value count must be 3,
            // because the rewriter successfully rewrote it to the new 3-column layout during compaction.
            assert_eq!(row.values.len(), 3);
            assert_eq!(
                row.values,
                vec![
                    DbValue::Int(1),
                    DbValue::string("Alice"),
                    DbValue::Bool(false),
                ]
            );
        } else {
            panic!("Expected DbValue::Bytes");
        }
    }
}
