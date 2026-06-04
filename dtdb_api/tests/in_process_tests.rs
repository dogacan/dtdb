use std::fs;
use tempfile::TempDir;

use dtdb_api::in_process::InProcessClient;
use dtdb_relational::DatabaseOptions;
use dtdb_storage::CompressionType;

#[test]
fn test_in_process_client_integration() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    // 1. Create client
    let client = InProcessClient::open(&data_path).unwrap();

    // 2. Test CreateDb with Lz4
    let create_lz4_resp = client.create_db("db_lz4", CompressionType::Lz4).unwrap();
    assert!(create_lz4_resp.success);
    assert!(create_lz4_resp.message.contains("Lz4"));

    // Check directory layout & db_options.bin
    let db_lz4_path = data_path.join("db_lz4");
    assert!(db_lz4_path.exists());
    let db_lz4_options_path = db_lz4_path.join("db_options.bin");
    assert!(db_lz4_options_path.exists());
    let lz4_options_bytes = fs::read(&db_lz4_options_path).unwrap();
    let lz4_comp = postcard::take_from_bytes::<CompressionType>(&lz4_options_bytes)
        .unwrap()
        .0;
    assert_eq!(lz4_comp, CompressionType::Lz4);

    // 3. Test CreateDb with Options
    let options = DatabaseOptions {
        compression: CompressionType::Uncompressed,
        memtable_size_limit: 1024 * 1024,
        block_size_limit: 4096,
        wal_size_limit: 1024 * 1024,
        flush_interval_ms: None,
        l0_compaction_threshold: None,
        sstable_target_size: None,
        base_level_size_limit: None,
        level_size_multiplier: None,
        max_level: None,
        block_cache_capacity: Some(1000),
        analyze_frequency_ms: None,
        wal_sync_interval_ms: None,
        memory_budget: None,
        fsync_method: dtdb_storage::FsyncMethod::default(),
    };
    let create_opt_resp = client.create_db_with_options("db_opt", options).unwrap();
    assert!(create_opt_resp.success);

    // Check directory layout & db_options.bin
    let db_opt_path = data_path.join("db_opt");
    assert!(db_opt_path.exists());
    let db_opt_options_path = db_opt_path.join("db_options.bin");
    assert!(db_opt_options_path.exists());
    let opt_options_bytes = fs::read(&db_opt_options_path).unwrap();
    let opt_comp = postcard::take_from_bytes::<CompressionType>(&opt_options_bytes)
        .unwrap()
        .0;
    assert_eq!(opt_comp, CompressionType::Uncompressed);

    // 4. Test ExecuteQuery on db_lz4
    // Create Table
    {
        let results = client
            .execute_query(
                "db_lz4",
                dtdb_api::sql_query!("CREATE TABLE Users (id int PRIMARY KEY, name varchar(255));"),
            )
            .unwrap();

        let mut payloads = Vec::new();
        for res in results {
            payloads.push(res.unwrap());
        }
        // Since CREATE TABLE is not a query returning rows, it runs immediately and returns
        // InProcessQueryResult::Complete(ExecutionResult::CreateTable), which yields no rows.
        assert_eq!(payloads.len(), 0);
    }

    // Insert rows
    {
        let results = client
            .execute_query(
                "db_lz4",
                dtdb_api::sql_query!("INSERT INTO Users (id, name) VALUES (42, 'Bob');"),
            )
            .unwrap();
        let mut payloads = Vec::new();
        for res in results {
            payloads.push(res.unwrap());
        }
        // INSERT runs immediately and returns Complete, yielding no rows.
        assert_eq!(payloads.len(), 0);
    }

    // Select rows
    {
        let results = client
            .execute_query(
                "db_lz4",
                dtdb_api::sql_query!("SELECT id, name FROM Users;"),
            )
            .unwrap();

        let col_names: Vec<&str> = results
            .schema()
            .unwrap()
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(col_names, vec!["id", "name"]);

        let mut rows = Vec::new();
        for res in results {
            rows.push(res.unwrap());
        }
        assert_eq!(rows.len(), 1);

        // Row validation
        let id_val = rows[0].get_by_index(0).unwrap();
        let name_val = rows[0].get_by_index(1).unwrap();
        assert_eq!(id_val, &dtdb_storage::DbValue::Int(42));
        assert_eq!(name_val, &dtdb_storage::DbValue::string("Bob"));
    }

    // 5. Test Flush Db
    let flush_resp = client.flush_db("db_lz4").unwrap();
    assert!(flush_resp.success);
    assert!(flush_resp.message.contains("Flushed"));

    // 6. Test DropDb
    let drop_resp = client.drop_db("db_lz4").unwrap();
    assert!(drop_resp.success);
    assert!(!db_lz4_path.exists());
}

#[test]
fn test_in_process_open_with_executor() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    let executor = std::sync::Arc::new(dtdb_storage::InlineExecutor);
    let client = InProcessClient::open_with_executor(&data_path, executor).unwrap();

    let create_resp = client
        .create_db("db_exec", CompressionType::Uncompressed)
        .unwrap();
    assert!(create_resp.success);
}

#[test]
fn test_in_process_query_result_helpers() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();
    let client = InProcessClient::open(&data_path).unwrap();

    client
        .create_db("db_helpers", CompressionType::Uncompressed)
        .unwrap();

    // 1. Create Table
    {
        let res = client
            .execute_query(
                "db_helpers",
                dtdb_api::sql_query!("CREATE TABLE Users (id int PRIMARY KEY, name varchar(255));"),
            )
            .unwrap();
        assert_eq!(res.info_message().unwrap(), "Table created successfully.");
        assert!(res.schema().is_none());
    }

    // 2. Create Index
    {
        let res = client
            .execute_query(
                "db_helpers",
                dtdb_api::sql_query!("CREATE INDEX idx_name ON Users(name);"),
            )
            .unwrap();
        assert_eq!(res.info_message().unwrap(), "Index created successfully.");
    }

    // 3. Insert
    {
        let res = client
            .execute_query(
                "db_helpers",
                dtdb_api::sql_query!("INSERT INTO Users (id, name) VALUES (1, 'Alice');"),
            )
            .unwrap();
        assert_eq!(res.info_message().unwrap(), "Inserted 1 row(s).");
    }

    // 4. Update
    {
        let res = client
            .execute_query(
                "db_helpers",
                dtdb_api::sql_query!("UPDATE Users SET name = 'Bob' WHERE id = 1;"),
            )
            .unwrap();
        assert_eq!(res.info_message().unwrap(), "Updated 1 row(s).");
    }

    // 5. Select (returns streaming for execute_query)
    {
        let res = client
            .execute_query(
                "db_helpers",
                dtdb_api::sql_query!("SELECT id, name FROM Users;"),
            )
            .unwrap();
        assert!(res.info_message().is_none());
        assert!(res.schema().is_some());
        assert_eq!(res.schema().unwrap().columns[0].name, "id");
    }

    // 6. Delete
    {
        let res = client
            .execute_query(
                "db_helpers",
                dtdb_api::sql_query!("DELETE FROM Users WHERE id = 1;"),
            )
            .unwrap();
        assert_eq!(res.info_message().unwrap(), "Deleted 1 row(s).");
    }

    // 7. Analyze
    {
        let res = client
            .execute_query("db_helpers", dtdb_api::sql_query!("ANALYZE TABLE Users;"))
            .unwrap();
        assert_eq!(res.info_message().unwrap(), "Table analyzed successfully.");
    }

    // 8. Alter Table
    {
        let res = client
            .execute_query(
                "db_helpers",
                dtdb_api::sql_query!("ALTER TABLE Users ADD COLUMN age int;"),
            )
            .unwrap();
        assert_eq!(res.info_message().unwrap(), "Table altered successfully.");
    }

    // 9. Drop Index
    {
        let res = client
            .execute_query(
                "db_helpers",
                dtdb_api::sql_query!("DROP INDEX idx_name ON Users;"),
            )
            .unwrap();
        assert_eq!(res.info_message().unwrap(), "Index dropped successfully.");
    }

    // 10. Drop Table
    {
        let res = client
            .execute_query("db_helpers", dtdb_api::sql_query!("DROP TABLE Users;"))
            .unwrap();
        assert_eq!(res.info_message().unwrap(), "Table dropped successfully.");
    }
}

#[test]
fn test_in_process_query_parameters() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();
    let client = InProcessClient::open(&data_path).unwrap();

    client
        .create_db("db_params", CompressionType::Uncompressed)
        .unwrap();

    let _ = client
        .execute_query(
            "db_params",
            dtdb_api::sql_query!("CREATE TABLE Users (id int PRIMARY KEY, name varchar(255));"),
        )
        .unwrap();

    // Query with bound parameters (covers line 96-97)
    let q1 = dtdb_api::sql_query!("INSERT INTO Users (id, name) VALUES (@id, @name);")
        .bind("id", 10i64)
        .bind("name", "John");
    let res1 = client.execute_query("db_params", q1).unwrap();
    assert_eq!(res1.info_message().unwrap(), "Inserted 1 row(s).");

    // Transaction query with parameters (covers line 183-184)
    client
        .run_in_transaction("db_params", |tx| {
            let q2 = dtdb_api::sql_query!("INSERT INTO Users (id, name) VALUES (@id, @name);")
                .bind("id", 20i64)
                .bind("name", "Mary");
            let res2 = tx.execute_query(q2)?;
            assert_eq!(res2.info_message().unwrap(), "Inserted 1 row(s).");
            Ok(())
        })
        .unwrap();
}

#[test]
fn test_in_process_streaming_and_drop() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();
    let client = InProcessClient::open(&data_path).unwrap();

    client
        .create_db("db_stream", CompressionType::Uncompressed)
        .unwrap();

    let _ = client
        .execute_query(
            "db_stream",
            dtdb_api::sql_query!("CREATE TABLE Users (id int PRIMARY KEY, name varchar(255));"),
        )
        .unwrap();
    let _ = client
        .execute_query(
            "db_stream",
            dtdb_api::sql_query!("INSERT INTO Users (id, name) VALUES (1, 'Alice');"),
        )
        .unwrap();
    let _ = client
        .execute_query(
            "db_stream",
            dtdb_api::sql_query!("INSERT INTO Users (id, name) VALUES (2, 'Bob');"),
        )
        .unwrap();

    // 1. SELECT inside transaction (covers line 195-202: Streaming in txn)
    client
        .run_in_transaction("db_stream", |tx| {
            let mut res = tx
                .execute_query(dtdb_api::sql_query!(
                    "SELECT id, name FROM Users ORDER BY id ASC;"
                ))
                .unwrap();

            // Iterate and verify row values
            let r1 = res.next().unwrap().unwrap();
            assert_eq!(r1.get_by_index(0).unwrap(), &dtdb_storage::DbValue::Int(1));

            let r2 = res.next().unwrap().unwrap();
            assert_eq!(r2.get_by_index(0).unwrap(), &dtdb_storage::DbValue::Int(2));

            assert!(res.next().is_none());
            Ok(())
        })
        .unwrap();

    // 2. Drop InProcessQueryResult with active RowIterator early to trigger Drop (covers line 306)
    client
        .run_in_transaction("db_stream", |tx| {
            let mut res = tx
                .execute_query(dtdb_api::sql_query!("SELECT id, name FROM Users;"))
                .unwrap();
            let _r1 = res.next().unwrap().unwrap();
            // Drop res early
            drop(res);
            Ok(())
        })
        .unwrap();
}
