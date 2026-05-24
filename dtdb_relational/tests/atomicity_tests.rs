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
        },
        Column {
            name: "name".to_string(),
            data_type: DataType::String,
            is_primary_key: false,
            is_nullable: true,
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
        },
        Column {
            name: "price".to_string(),
            data_type: DataType::Float,
            is_primary_key: false,
            is_nullable: true,
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

        let record = TransactionRecord::Prepared { tx_id, mutations };

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
