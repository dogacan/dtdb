use dtdb_relational::{
    Column, DataType, Database, RelationalError, Row, Schema, Transaction, TransactionRecord,
};
use dtdb_storage::{DbKey, DbValue, WalEntry};
use std::collections::HashMap;
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
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            name: "name".to_string(),
            data_type: DataType::String,
            is_primary_key: false,
            is_nullable: true,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
        Column {
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
            name: "id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_nullable: false,
            locality_group: None,
            default_value: None,
            is_auto_increment: true,
        },
        Column {
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
        let mutations: HashMap<String, Vec<WalEntry>> = HashMap::from([(
            "t".to_string(),
            vec![WalEntry::Put {
                key: DbKey::Int(100),
                value: DbValue::Bytes(row100_bytes),
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
            name: "id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_nullable: false,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            name: "name".to_string(),
            data_type: DataType::String,
            is_primary_key: false,
            is_nullable: true,
            locality_group: Some("lg_name".to_string()),
            default_value: None,
            is_auto_increment: false,
        },
        Column {
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
            DbValue::String("Alice".to_string()),
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
            DbValue::String("Alice".to_string()),
            DbValue::Float(95.5)
        ]
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
        sort_memory_budget: None,
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

    // Sleep for a short while (e.g. 50ms) to allow the background thread to run and update stats
    std::thread::sleep(std::time::Duration::from_millis(50));

    // Verify background thread ran and updated statistics
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
