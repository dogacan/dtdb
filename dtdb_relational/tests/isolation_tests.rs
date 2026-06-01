use dtdb_relational::schema::{IndexDefinition, IndexType};
use dtdb_relational::{Column, DataType, Database, RelationalError, Row, Schema, Transaction};
use dtdb_storage::{DbKey, DbValue};
use std::sync::Arc;
use tempfile::TempDir;

// Helper to create a user schema
fn create_user_schema() -> Schema {
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
    ])
}

fn k_int(val: i64) -> DbKey {
    DbKey::Int(val)
}

fn r_user(id: i64, name: &str) -> Row {
    Row::new(vec![DbValue::Int(id), DbValue::string(name.to_string())])
}

#[test]
fn test_occ_lost_update_conflict() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_user_schema()).unwrap();

    // Setup initial row
    {
        let tx = Transaction::new(1, db.clone());
        tx.put("users", k_int(42), r_user(42, "Original Name"))
            .unwrap();
        tx.commit().unwrap();
    }

    // 1. Start two concurrent transactions
    let tx1 = Transaction::new(2, db.clone());
    let tx2 = Transaction::new(3, db.clone());

    // 2. Both read the same row
    let row1 = tx1.get("users", &k_int(42)).unwrap().unwrap();
    let row2 = tx2.get("users", &k_int(42)).unwrap().unwrap();
    assert_eq!(row1, r_user(42, "Original Name"));
    assert_eq!(row2, r_user(42, "Original Name"));

    // 3. Both perform concurrent updates
    tx1.put("users", k_int(42), r_user(42, "Tx1 Name")).unwrap();
    tx2.put("users", k_int(42), r_user(42, "Tx2 Name")).unwrap();

    // 4. Tx1 commits successfully
    tx1.commit().unwrap();

    // 5. Tx2 attempts to commit and must fail due to conflict on key 42
    let commit_res = tx2.commit();
    assert!(
        matches!(commit_res, Err(RelationalError::TransactionConflict(_))),
        "Expected TransactionConflict, got {:?}",
        commit_res
    );

    // Verify Tx1's update is the one persisted
    let tx_check = Transaction::new(4, db);
    assert_eq!(
        tx_check.get("users", &k_int(42)).unwrap(),
        Some(r_user(42, "Tx1 Name"))
    );
}

#[test]
fn test_occ_repeatable_read_conflict() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_user_schema()).unwrap();

    // Setup initial row
    {
        let tx = Transaction::new(1, db.clone());
        tx.put("users", k_int(42), r_user(42, "Original")).unwrap();
        tx.commit().unwrap();
    }

    // 1. Start Tx1 and read row
    let tx1 = Transaction::new(2, db.clone());
    assert_eq!(
        tx1.get("users", &k_int(42)).unwrap(),
        Some(r_user(42, "Original"))
    );

    // 2. Start and commit Tx2 updating the row
    let tx2 = Transaction::new(3, db.clone());
    tx2.put("users", k_int(42), r_user(42, "Updated by Tx2"))
        .unwrap();
    tx2.commit().unwrap();

    // 3. Tx1 attempts to commit and must conflict since K=42 was modified since it started
    let commit_res = tx1.commit();
    assert!(
        matches!(commit_res, Err(RelationalError::TransactionConflict(_))),
        "Expected TransactionConflict, got {:?}",
        commit_res
    );
}

#[test]
fn test_occ_phantom_read_conflict() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_user_schema()).unwrap();

    // Setup initial rows
    {
        let tx = Transaction::new(1, db.clone());
        tx.put("users", k_int(5), r_user(5, "Five")).unwrap();
        tx.put("users", k_int(15), r_user(15, "Fifteen")).unwrap();
        tx.commit().unwrap();
    }

    // 1. Tx1 performs range scan over [1, 10]
    let tx1 = Transaction::new(2, db.clone());
    let scan1 = tx1
        .filtered_scan("users", &k_int(1), &k_int(10), |_| true)
        .unwrap();
    assert_eq!(scan1.len(), 1);
    assert_eq!(scan1[0], r_user(5, "Five"));

    // 2. Tx2 inserts a new row at id = 7 (falls within scanned range [1, 10]) and commits
    let tx2 = Transaction::new(3, db.clone());
    tx2.put("users", k_int(7), r_user(7, "Seven")).unwrap();
    tx2.commit().unwrap();

    // 3. Tx1 attempts to commit. Must fail due to phantom insert detection.
    let commit_res = tx1.commit();
    assert!(
        matches!(commit_res, Err(RelationalError::TransactionConflict(_))),
        "Expected TransactionConflict, got {:?}",
        commit_res
    );
}

/// Drains a `TransactionScanIterator` into a Vec of rows.
fn drain_tx_scan(mut it: dtdb_relational::TransactionScanIterator) -> Vec<Row> {
    let mut rows = Vec::new();
    while let Some(row) = it.next().unwrap() {
        rows.push(row);
    }
    rows
}

/// The SQL SELECT scan path is `scan_iter` -> `TransactionScanIterator`, which
/// (unlike `filtered_scan`) no longer publishes per-key read_set entries under
/// SnapshotIsolation -- it relies on the scan range recorded by `scan_iter`.
/// A phantom insert into the scanned range must still conflict at commit.
#[test]
fn test_occ_scan_iter_phantom_conflict_si() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_user_schema()).unwrap();
    {
        let tx = Transaction::new(1, db.clone());
        tx.put("users", k_int(5), r_user(5, "Five")).unwrap();
        tx.put("users", k_int(15), r_user(15, "Fifteen")).unwrap();
        tx.commit().unwrap();
    }

    // Tx1 (SI by default) scans [1, 10] through the iterator and drains it.
    let tx1 = Transaction::new(2, db.clone());
    let rows = drain_tx_scan(tx1.scan_iter("users", &k_int(1), &k_int(10), None).unwrap());
    assert_eq!(rows, vec![r_user(5, "Five")]);

    // Tx2 inserts id = 7 within the scanned range and commits.
    let tx2 = Transaction::new(3, db.clone());
    tx2.put("users", k_int(7), r_user(7, "Seven")).unwrap();
    tx2.commit().unwrap();

    let res = tx1.commit();
    assert!(
        matches!(res, Err(RelationalError::TransactionConflict(_))),
        "Expected phantom conflict via scan range, got {:?}",
        res
    );
}

/// SI iterator scan that *returns* a key must still conflict if that key is
/// concurrently modified -- this is the case the per-key read_set used to
/// catch and the scan range now subsumes (the modified key lies in the range).
#[test]
fn test_occ_scan_iter_modified_key_conflict_si() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_user_schema()).unwrap();
    {
        let tx = Transaction::new(1, db.clone());
        tx.put("users", k_int(5), r_user(5, "Five")).unwrap();
        tx.commit().unwrap();
    }

    let tx1 = Transaction::new(2, db.clone());
    let rows = drain_tx_scan(tx1.scan_iter("users", &k_int(1), &k_int(10), None).unwrap());
    assert_eq!(rows, vec![r_user(5, "Five")]);

    // Tx2 modifies the key Tx1 read, within Tx1's scanned range.
    let tx2 = Transaction::new(3, db.clone());
    tx2.put("users", k_int(5), r_user(5, "FiveModified"))
        .unwrap();
    tx2.commit().unwrap();

    let res = tx1.commit();
    assert!(
        matches!(res, Err(RelationalError::TransactionConflict(_))),
        "Expected conflict on modified scanned key, got {:?}",
        res
    );
}

/// Guard against over-broad detection: an SI iterator scan must NOT conflict
/// with a concurrent modification to a key *outside* the scanned range.
#[test]
fn test_occ_scan_iter_no_conflict_outside_range_si() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_user_schema()).unwrap();
    {
        let tx = Transaction::new(1, db.clone());
        tx.put("users", k_int(5), r_user(5, "Five")).unwrap();
        tx.put("users", k_int(15), r_user(15, "Fifteen")).unwrap();
        tx.commit().unwrap();
    }

    let tx1 = Transaction::new(2, db.clone());
    let rows = drain_tx_scan(tx1.scan_iter("users", &k_int(1), &k_int(10), None).unwrap());
    assert_eq!(rows, vec![r_user(5, "Five")]);

    // Tx2 modifies id = 15, outside Tx1's [1, 10] scan range.
    let tx2 = Transaction::new(3, db.clone());
    tx2.put("users", k_int(15), r_user(15, "FifteenModified"))
        .unwrap();
    tx2.commit().unwrap();

    assert!(
        tx1.commit().is_ok(),
        "SI scan must not conflict with an out-of-range modification"
    );
}

/// RepeatableRead has no scan_ranges, so the iterator still publishes per-key
/// read_set entries: a modification to a key it returned must conflict, but a
/// phantom insert into the range must NOT (RR permits phantoms).
#[test]
fn test_occ_scan_iter_repeatable_read_semantics() {
    use dtdb_relational::IsolationLevel;

    // Case A: modified read key -> conflict.
    {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(Database::open(temp_dir.path()).unwrap());
        db.create_table("users", create_user_schema()).unwrap();
        {
            let tx = Transaction::new(1, db.clone());
            tx.put("users", k_int(5), r_user(5, "Five")).unwrap();
            tx.commit().unwrap();
        }
        let tx1 = Transaction::new_with_isolation(2, db.clone(), IsolationLevel::RepeatableRead);
        let rows = drain_tx_scan(tx1.scan_iter("users", &k_int(1), &k_int(10), None).unwrap());
        assert_eq!(rows, vec![r_user(5, "Five")]);

        let tx2 = Transaction::new(3, db.clone());
        tx2.put("users", k_int(5), r_user(5, "FiveModified"))
            .unwrap();
        tx2.commit().unwrap();

        assert!(
            matches!(tx1.commit(), Err(RelationalError::TransactionConflict(_))),
            "RR must conflict on a modified read key (read_set)"
        );
    }

    // Case B: phantom insert -> no conflict.
    {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(Database::open(temp_dir.path()).unwrap());
        db.create_table("users", create_user_schema()).unwrap();
        {
            let tx = Transaction::new(1, db.clone());
            tx.put("users", k_int(5), r_user(5, "Five")).unwrap();
            tx.commit().unwrap();
        }
        let tx1 = Transaction::new_with_isolation(2, db.clone(), IsolationLevel::RepeatableRead);
        let rows = drain_tx_scan(tx1.scan_iter("users", &k_int(1), &k_int(10), None).unwrap());
        assert_eq!(rows, vec![r_user(5, "Five")]);

        let tx2 = Transaction::new(3, db.clone());
        tx2.put("users", k_int(7), r_user(7, "Seven")).unwrap();
        tx2.commit().unwrap();

        assert!(
            tx1.commit().is_ok(),
            "RR must permit a phantom insert (no scan_ranges)"
        );
    }
}

#[test]
fn test_occ_no_conflict_disjoint_keys() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_user_schema()).unwrap();

    // Setup initial rows
    {
        let tx = Transaction::new(1, db.clone());
        tx.put("users", k_int(1), r_user(1, "One")).unwrap();
        tx.put("users", k_int(2), r_user(2, "Two")).unwrap();
        tx.commit().unwrap();
    }

    // 1. Start concurrent disjoint transactions
    let tx1 = Transaction::new(2, db.clone());
    let tx2 = Transaction::new(3, db.clone());

    // 2. Tx1 reads/writes key 1, Tx2 reads/writes key 2
    assert_eq!(tx1.get("users", &k_int(1)).unwrap(), Some(r_user(1, "One")));
    assert_eq!(tx2.get("users", &k_int(2)).unwrap(), Some(r_user(2, "Two")));

    tx1.put("users", k_int(1), r_user(1, "One Modified"))
        .unwrap();
    tx2.put("users", k_int(2), r_user(2, "Two Modified"))
        .unwrap();

    // 3. Both commit successfully (disjoint read/write sets)
    tx1.commit().unwrap();
    tx2.commit().unwrap();

    // Verify updates
    let tx_check = Transaction::new(4, db);
    assert_eq!(
        tx_check.get("users", &k_int(1)).unwrap(),
        Some(r_user(1, "One Modified"))
    );
    assert_eq!(
        tx_check.get("users", &k_int(2)).unwrap(),
        Some(r_user(2, "Two Modified"))
    );
}

#[test]
fn test_occ_blind_write_write_conflict() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_user_schema()).unwrap();

    // 1. Start two concurrent transactions
    let tx1 = Transaction::new(2, db.clone());
    let tx2 = Transaction::new(3, db.clone());

    // 2. Both perform blind updates to key 42 (without reading it first)
    tx1.put("users", k_int(42), r_user(42, "Tx1 Name")).unwrap();
    tx2.put("users", k_int(42), r_user(42, "Tx2 Name")).unwrap();

    // 3. Tx1 commits successfully
    tx1.commit().unwrap();

    // 4. Tx2 attempts to commit and must fail due to write-write conflict on key 42
    let commit_res = tx2.commit();
    assert!(
        matches!(commit_res, Err(RelationalError::TransactionConflict(_))),
        "Expected TransactionConflict, got {:?}",
        commit_res
    );
}

#[test]
fn test_read_committed_isolation() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_user_schema()).unwrap();

    // Setup initial row
    {
        let tx = Transaction::new(1, db.clone());
        tx.put("users", k_int(42), r_user(42, "Original")).unwrap();
        tx.commit().unwrap();
    }

    // 1. Start Tx1 in Read Committed
    let tx1 = Transaction::new_with_isolation(
        2,
        db.clone(),
        dtdb_relational::IsolationLevel::ReadCommitted,
    );
    assert_eq!(
        tx1.get("users", &k_int(42)).unwrap(),
        Some(r_user(42, "Original"))
    );

    // 2. Start and commit Tx2 updating the row
    let tx2 = Transaction::new(3, db.clone());
    tx2.put("users", k_int(42), r_user(42, "Updated")).unwrap();
    tx2.commit().unwrap();

    // 3. Tx1 reads row again — should see updated value (non-repeatable read!)
    assert_eq!(
        tx1.get("users", &k_int(42)).unwrap(),
        Some(r_user(42, "Updated"))
    );

    // 4. Tx1 commits successfully (no read-write conflict validation)
    tx1.commit().unwrap();
}

#[test]
fn test_repeatable_read_no_phantom_conflict() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_user_schema()).unwrap();

    // Setup initial rows
    {
        let tx = Transaction::new(1, db.clone());
        tx.put("users", k_int(5), r_user(5, "Five")).unwrap();
        tx.put("users", k_int(15), r_user(15, "Fifteen")).unwrap();
        tx.commit().unwrap();
    }

    // 1. Tx1 performs range scan over [1, 10] in Repeatable Read
    let tx1 = Transaction::new_with_isolation(
        2,
        db.clone(),
        dtdb_relational::IsolationLevel::RepeatableRead,
    );
    let scan1 = tx1
        .filtered_scan("users", &k_int(1), &k_int(10), |_| true)
        .unwrap();
    assert_eq!(scan1.len(), 1);
    assert_eq!(scan1[0], r_user(5, "Five"));

    // 2. Tx2 inserts a new row at id = 7 (falls within scanned range [1, 10]) and commits
    let tx2 = Transaction::new(3, db.clone());
    tx2.put("users", k_int(7), r_user(7, "Seven")).unwrap();
    tx2.commit().unwrap();

    // 3. Tx1 attempts to commit. Must succeed under Repeatable Read (allowing phantoms).
    tx1.commit().unwrap();
}

#[test]
fn test_repeatable_read_does_conflict_on_modified_read_key() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_user_schema()).unwrap();

    // Setup initial row
    {
        let tx = Transaction::new(1, db.clone());
        tx.put("users", k_int(42), r_user(42, "Original")).unwrap();
        tx.commit().unwrap();
    }

    // 1. Start Tx1 in Repeatable Read and read row
    let tx1 = Transaction::new_with_isolation(
        2,
        db.clone(),
        dtdb_relational::IsolationLevel::RepeatableRead,
    );
    assert_eq!(
        tx1.get("users", &k_int(42)).unwrap(),
        Some(r_user(42, "Original"))
    );

    // 2. Start and commit Tx2 updating the row
    let tx2 = Transaction::new(3, db.clone());
    tx2.put("users", k_int(42), r_user(42, "Updated")).unwrap();
    tx2.commit().unwrap();

    // 3. Tx1 attempts to commit and must conflict since K=42 was modified since it started
    let commit_res = tx1.commit();
    assert!(
        matches!(commit_res, Err(RelationalError::TransactionConflict(_))),
        "Expected TransactionConflict, got {:?}",
        commit_res
    );
}

#[test]
fn test_occ_commit_history_garbage_collection() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_user_schema()).unwrap();

    // 1. Start a long-running transaction that will pin the min_start_version
    let tx_long = Transaction::new(100, db.clone());
    // Perform a read so that it gets tracked / active version starts
    let _ = tx_long.get("users", &k_int(1)).unwrap();

    // 2. Commit a series of write transactions
    for i in 1..=5 {
        let tx = Transaction::new(i, db.clone());
        tx.put(
            "users",
            k_int(i as i64),
            r_user(i as i64, &format!("User{}", i)),
        )
        .unwrap();
        tx.commit().unwrap();
    }

    // 3. Since tx_long is still active and has start_version = 0, no history can be pruned
    assert_eq!(db.commit_history_len(), 5);

    // 4. Drop the long-running transaction. This should trigger pruning of the history
    drop(tx_long);

    // 5. The history should be pruned up to the global commit version (which is 5).
    // The only remaining record in history should be the one with commit_version >= 5 (i.e. version 5).
    assert_eq!(db.commit_history_len(), 1);
}

#[test]
fn test_occ_concurrency_stress() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_user_schema()).unwrap();

    let thread_count = 8;
    let iterations = 20;

    let mut handles = Vec::new();
    for t in 0..thread_count {
        let db_clone = db.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..iterations {
                let tx_id = (t * 1000 + i + 200) as u64;
                loop {
                    let tx = Transaction::new(tx_id, db_clone.clone());
                    let common_key = k_int(100);
                    let thread_key = k_int(200 + t as i64);

                    let _val_common = tx.get("users", &common_key).unwrap();
                    let _val_thread = tx.get("users", &thread_key).unwrap();

                    let name = format!("Val-{}-{}", t, i);
                    tx.put("users", thread_key.clone(), r_user(200 + t as i64, &name))
                        .unwrap();

                    if i % 3 == 0 {
                        tx.put("users", common_key.clone(), r_user(100, &name))
                            .unwrap();
                    }

                    match tx.commit() {
                        Ok(_) => break,
                        Err(RelationalError::TransactionConflict(_)) => {
                            // Conflict detected, retry!
                            std::thread::yield_now();
                        }
                        Err(e) => panic!("Unexpected error: {:?}", e),
                    }
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_occ_si_race() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_user_schema()).unwrap();

    // 1. Initial write
    {
        let tx = Transaction::new(1, db.clone());
        tx.put("users", k_int(1), r_user(1, "Original")).unwrap();
        tx.commit().unwrap();
    }

    // 2. Spawn concurrent committing and reading threads to stress the commit history
    // and version assignment locks.
    let db_clone = db.clone();
    let h1 = std::thread::spawn(move || {
        for i in 1..50 {
            let tx = Transaction::new(100 + i, db_clone.clone());
            tx.put("users", k_int(1), r_user(1, &format!("Val{}", i)))
                .unwrap();
            let _ = tx.commit();
        }
    });

    let db_clone2 = db.clone();
    let h2 = std::thread::spawn(move || {
        for i in 1..50 {
            let tx = Transaction::new(200 + i, db_clone2.clone());
            if let Some(row) = tx.get("users", &k_int(1)).unwrap() {
                let _name = row.get_by_index(1).unwrap().clone();
            }
            let _ = tx.commit();
        }
    });

    h1.join().unwrap();
    h2.join().unwrap();
}

fn create_user_index_schema() -> Schema {
    let mut s = Schema::new(vec![
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
            name: "age".to_string(),
            data_type: DataType::Int,
            is_primary_key: false,
            is_nullable: true,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
    ]);
    s.indexes.push(IndexDefinition {
        name: "idx_age".to_string(),
        columns: vec!["age".to_string()],
        index_type: IndexType::BTree,
        tokenizer: None,
    });
    s
}

#[test]
fn test_occ_phantom_read_index_scan() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_user_index_schema())
        .unwrap();

    // 1. Initial rows
    {
        let tx = Transaction::new(1, db.clone());
        tx.put(
            "users",
            k_int(1),
            Row::new(vec![DbValue::Int(1), DbValue::Int(20)]),
        )
        .unwrap();
        tx.put(
            "users",
            k_int(3),
            Row::new(vec![DbValue::Int(3), DbValue::Int(40)]),
        )
        .unwrap();
        tx.commit().unwrap();
    }

    // 2. Tx1 starts and performs an index range scan on age in [10, 30]
    let tx1 = Transaction::new(2, db.clone());
    let scan1 = tx1
        .index_scan("users", "idx_age", &k_int(10), &k_int(30), None)
        .unwrap();
    assert_eq!(scan1.len(), 1);
    assert_eq!(scan1[0].values[0], DbValue::Int(1)); // id = 1, age = 20

    // 3. Tx2 starts concurrently, inserts a new user with age = 25 (falls in [10, 30]) and commits
    let tx2 = Transaction::new(3, db.clone());
    tx2.put(
        "users",
        k_int(2),
        Row::new(vec![DbValue::Int(2), DbValue::Int(25)]),
    )
    .unwrap();
    tx2.commit().unwrap();

    // 4. Tx1 attempts to commit and must fail due to phantom read conflict on index scan range
    let commit_res = tx1.commit();
    assert!(
        matches!(commit_res, Err(RelationalError::TransactionConflict(_))),
        "Expected TransactionConflict due to index scan phantom read, got {:?}",
        commit_res
    );
}

fn create_user_fts_schema() -> Schema {
    let mut s = Schema::new(vec![
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
            name: "bio".to_string(),
            data_type: DataType::String,
            is_primary_key: false,
            is_nullable: true,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
    ]);
    s.indexes.push(IndexDefinition {
        name: "idx_bio".to_string(),
        columns: vec!["bio".to_string()],
        index_type: IndexType::FullText,
        tokenizer: Some("simple".to_string()),
    });
    s
}

#[test]
fn test_occ_phantom_read_fts_scan() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    db.create_table("users", create_user_fts_schema()).unwrap();

    // 1. Initial rows
    {
        let tx = Transaction::new(1, db.clone());
        tx.put(
            "users",
            k_int(1),
            Row::new(vec![
                DbValue::Int(1),
                DbValue::string("hello world"),
            ]),
        )
        .unwrap();
        tx.commit().unwrap();
    }

    // 2. Tx1 starts and performs fulltext scan for "rust"
    let tx1 = Transaction::new(2, db.clone());
    let scan1 = tx1.fulltext_scan("users", "idx_bio", "rust", None).unwrap();
    assert_eq!(scan1.len(), 0);

    // 3. Tx2 starts concurrently, inserts a new user with "rust is fast" and commits
    let tx2 = Transaction::new(3, db.clone());
    tx2.put(
        "users",
        k_int(2),
        Row::new(vec![
            DbValue::Int(2),
            DbValue::string("rust is fast"),
        ]),
    )
    .unwrap();
    tx2.commit().unwrap();

    // 4. Tx1 attempts to commit and must fail due to phantom read conflict on fts token
    let commit_res = tx1.commit();
    assert!(
        matches!(commit_res, Err(RelationalError::TransactionConflict(_))),
        "Expected TransactionConflict due to fts scan phantom read, got {:?}",
        commit_res
    );
}
