use dtdb_relational::{Column, DataType, Database, RelationalError, Row, Schema, Transaction};
use dtdb_storage::{DbKey, DbValue};
use std::sync::Arc;
use tempfile::TempDir;

// Helper to create a user schema
fn create_user_schema() -> Schema {
    Schema::new(vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_nullable: false,
            locality_group: None,
        },
        Column {
            name: "name".to_string(),
            data_type: DataType::String,
            is_primary_key: false,
            is_nullable: true,
            locality_group: None,
        },
    ])
}

fn k_int(val: i64) -> DbKey {
    DbKey::Int(val)
}

fn r_user(id: i64, name: &str) -> Row {
    Row::new(vec![DbValue::Int(id), DbValue::String(name.to_string())])
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
