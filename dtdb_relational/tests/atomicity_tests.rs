use dtdb_relational::{Column, DataType, Database, Row, Schema, Transaction, TransactionRecord};
use dtdb_storage::{DbKey, DbValue, WalEntry};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
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
    ])
}

// Helper to create a product schema
fn create_product_schema() -> Schema {
    Schema::new(vec![
        Column {
            name: "sku".to_string(),
            data_type: DataType::String,
            is_primary_key: true,
            is_nullable: false,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            name: "price".to_string(),
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

fn k_str(val: &str) -> DbKey {
    DbKey::String(val.to_string())
}

fn r_user(id: i64, name: &str) -> Row {
    Row::new(vec![DbValue::Int(id), DbValue::String(name.to_string())])
}

fn r_product(sku: &str, price: f64) -> Row {
    Row::new(vec![
        DbValue::String(sku.to_string()),
        DbValue::Float(price),
    ])
}

#[test]
fn test_multi_table_normal_commit() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // 1. Setup DB and write to multiple tables in one transaction
    {
        let db = Arc::new(Database::open(&db_path).unwrap());
        db.create_table("users", create_user_schema()).unwrap();
        db.create_table("products", create_product_schema())
            .unwrap();

        let tx = Transaction::new(1, db);
        tx.put("users", k_int(101), r_user(101, "Alice")).unwrap();
        tx.put("products", k_str("BOOK-01"), r_product("BOOK-01", 29.99))
            .unwrap();
        tx.commit().unwrap();
    }

    // 2. Reopen DB and verify both tables contain the committed records
    {
        let db = Arc::new(Database::open(&db_path).unwrap());
        let tx = Transaction::new(2, db);

        assert_eq!(
            tx.get("users", &k_int(101)).unwrap(),
            Some(r_user(101, "Alice"))
        );
        assert_eq!(
            tx.get("products", &k_str("BOOK-01")).unwrap(),
            Some(r_product("BOOK-01", 29.99))
        );
    }
}

#[test]
fn test_multi_table_rollback() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let db = Arc::new(Database::open(&db_path).unwrap());
    db.create_table("users", create_user_schema()).unwrap();
    db.create_table("products", create_product_schema())
        .unwrap();

    let tx = Transaction::new(1, db);
    tx.put("users", k_int(101), r_user(101, "Alice")).unwrap();
    tx.put("products", k_str("BOOK-01"), r_product("BOOK-01", 29.99))
        .unwrap();

    // Check uncommitted local reads work
    assert_eq!(
        tx.get("users", &k_int(101)).unwrap(),
        Some(r_user(101, "Alice"))
    );
    assert_eq!(
        tx.get("products", &k_str("BOOK-01")).unwrap(),
        Some(r_product("BOOK-01", 29.99))
    );

    // Rollback
    tx.rollback().unwrap();

    // Verify both are gone/empty
    assert_eq!(tx.get("users", &k_int(101)).unwrap(), None);
    assert_eq!(tx.get("products", &k_str("BOOK-01")).unwrap(), None);
}

#[test]
fn test_crash_recovery_roll_forward() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // 1. Create the database and tables initially
    {
        let db = Database::open(&db_path).unwrap();
        db.create_table("users", create_user_schema()).unwrap();
        db.create_table("products", create_product_schema())
            .unwrap();
    }

    // 2. Simulate a crash right after PREPARED log write
    // We do this by manually crafting a `TransactionRecord::Prepared` record
    // and appending it to `transactions.log`, without writing to the tables' storage engines.
    {
        let tx_id = 999u64;
        let mut mutations = HashMap::new();

        // Mutations for users table
        let user_bytes = r_user(42, "Douglas Adams").to_bytes().unwrap();
        let user_entries = vec![WalEntry::Put {
            key: k_int(42),
            value: DbValue::Bytes(user_bytes),
        }];
        mutations.insert("users".to_string(), user_entries);

        // Mutations for products table
        let prod_bytes = r_product("TOWEL", 42.0).to_bytes().unwrap();
        let prod_entries = vec![WalEntry::Put {
            key: k_str("TOWEL"),
            value: DbValue::Bytes(prod_bytes),
        }];
        mutations.insert("products".to_string(), prod_entries);

        let record = TransactionRecord::Prepared {
            tx_id,
            mutations,
            old_rows: None,
        };

        // Write directly to transactions.log
        let log_path = db_path.join("transactions.log");
        let mut file = File::create(&log_path).unwrap();
        let bytes = bincode::serialize(&record).unwrap();
        let len = bytes.len() as u32;
        file.write_all(&len.to_le_bytes()).unwrap();
        file.write_all(&bytes).unwrap();
        file.sync_all().unwrap();
    }

    // 3. Open the database. The recovery protocol should scan the log, see
    // the uncommitted Prepared transaction, roll it forward, and truncate the log.
    {
        let db = Arc::new(Database::open(&db_path).unwrap());
        let tx = Transaction::new(1000, db);

        // Verify the data has been successfully rolled forward!
        assert_eq!(
            tx.get("users", &k_int(42)).unwrap(),
            Some(r_user(42, "Douglas Adams"))
        );
        assert_eq!(
            tx.get("products", &k_str("TOWEL")).unwrap(),
            Some(r_product("TOWEL", 42.0))
        );

        // Verify that the transactions.log has been truncated to 0 bytes
        let log_path = db_path.join("transactions.log");
        let metadata = std::fs::metadata(&log_path).unwrap();
        assert_eq!(metadata.len(), 0);
    }
}

#[test]
fn test_crash_recovery_ignore_corrupt_log() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // 1. Create database and tables
    {
        let db = Database::open(&db_path).unwrap();
        db.create_table("users", create_user_schema()).unwrap();
    }

    // 2. Write a corrupted/truncated transaction record to the log
    {
        let log_path = db_path.join("transactions.log");
        let mut file = File::create(&log_path).unwrap();
        // Write invalid length prefix and random bytes
        let len = 100u32;
        file.write_all(&len.to_le_bytes()).unwrap();
        file.write_all(b"corrupt payload bytes that do not deserialize")
            .unwrap();
        file.sync_all().unwrap();
    }

    // 3. Reopen DB. It should recover fine and truncate the corrupt log without applying anything.
    {
        let db = Arc::new(Database::open(&db_path).unwrap());
        let tx = Transaction::new(1, db);

        // Verify users is empty
        assert_eq!(tx.get("users", &k_int(1)).unwrap(), None);

        // Verify transactions.log is truncated
        let log_path = db_path.join("transactions.log");
        let metadata = std::fs::metadata(&log_path).unwrap();
        assert_eq!(metadata.len(), 0);
    }
}

#[test]
fn test_crash_recovery_secondary_index_maintenance() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // 1. Setup DB, table, index, and write initial row
    {
        let db = Database::open(&db_path).unwrap();
        db.create_table("users", create_user_schema()).unwrap();
        db.create_index(
            "users",
            "idx_name",
            vec!["name".to_string()],
            dtdb_relational::schema::IndexType::BTree,
            None,
        )
        .unwrap();

        let tx = Transaction::new(1, Arc::new(db));
        tx.put("users", k_int(1), r_user(1, "Douglas")).unwrap();
        tx.commit().unwrap();
    }

    // 2. Simulate a crash where the main table engine has the new value "Adams",
    // but the index engine was not updated. We manually insert into the main engine.
    {
        let db = Database::open(&db_path).unwrap();
        let table = db.get_table("users").unwrap();

        // Write the update directly to the main engine (bypassing index engine)
        let main_engine = table.engines.get("").unwrap();
        let new_row_bytes = r_user(1, "Adams").to_bytes().unwrap();
        main_engine
            .put(k_int(1), DbValue::Bytes(new_row_bytes.clone()))
            .unwrap();

        // Write the Prepared record to transactions.log
        let tx_id = 999u64;
        let mut mutations = HashMap::new();
        mutations.insert(
            "users".to_string(),
            vec![WalEntry::Put {
                key: k_int(1),
                value: DbValue::Bytes(new_row_bytes),
            }],
        );

        // Include old_rows pre-image mapping: key 1 was "Douglas"
        let mut old_rows_map = HashMap::new();
        let mut user_old_rows = HashMap::new();
        user_old_rows.insert(k_int(1), r_user(1, "Douglas"));
        old_rows_map.insert("users".to_string(), user_old_rows);

        let record = TransactionRecord::Prepared {
            tx_id,
            mutations,
            old_rows: Some(old_rows_map),
        };

        // Write directly to transactions.log
        let log_path = db_path.join("transactions.log");
        let mut file = File::create(&log_path).unwrap();
        let bytes = bincode::serialize(&record).unwrap();
        let len = bytes.len() as u32;
        file.write_all(&len.to_le_bytes()).unwrap();
        file.write_all(&bytes).unwrap();
        file.sync_all().unwrap();
    }

    // 3. Reopen DB. Recovery runs and should roll forward using the old_rows from the log.
    {
        let db = Arc::new(Database::open(&db_path).unwrap());
        let tx = Transaction::new(1000, db);

        // Verify index works: searching for "Douglas" should yield 0 results
        let douglas_rows = tx
            .index_scan(
                "users",
                "idx_name",
                &DbKey::String("Douglas".to_string()),
                &DbKey::String("Douglas".to_string()),
                None,
            )
            .unwrap();
        assert_eq!(douglas_rows.len(), 0);

        // Searching for "Adams" should yield 1 result
        let adams_rows = tx
            .index_scan(
                "users",
                "idx_name",
                &DbKey::String("Adams".to_string()),
                &DbKey::String("Adams".to_string()),
                None,
            )
            .unwrap();
        assert_eq!(adams_rows.len(), 1);
        assert_eq!(adams_rows[0], r_user(1, "Adams"));
    }
}
