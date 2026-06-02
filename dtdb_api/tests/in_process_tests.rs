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
