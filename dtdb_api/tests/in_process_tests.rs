use futures_util::StreamExt;
use std::fs;
use tempfile::TempDir;

use dtdb_api::client::DuctTapeDbClient;
use dtdb_relational::DatabaseOptions;
use dtdb_storage::CompressionType;

#[tokio::test]
async fn test_in_process_client_integration() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    // 1. Create client
    let mut client = DuctTapeDbClient::in_process(&data_path).unwrap();

    // 2. Test CreateDb with Lz4
    let create_lz4_resp = client
        .create_db("db_lz4", CompressionType::Lz4)
        .await
        .unwrap();
    assert!(create_lz4_resp.success);
    assert!(create_lz4_resp.message.contains("Lz4"));

    // Check directory layout & db_options.bin
    let db_lz4_path = data_path.join("db_lz4");
    assert!(db_lz4_path.exists());
    let db_lz4_options_path = db_lz4_path.join("db_options.bin");
    assert!(db_lz4_options_path.exists());
    let lz4_options_bytes = fs::read(&db_lz4_options_path).unwrap();
    let lz4_comp: CompressionType = bincode::deserialize(&lz4_options_bytes).unwrap();
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
    };
    let create_opt_resp = client
        .create_db_with_options("db_opt", options)
        .await
        .unwrap();
    assert!(create_opt_resp.success);

    // Check directory layout & db_options.bin
    let db_opt_path = data_path.join("db_opt");
    assert!(db_opt_path.exists());
    let db_opt_options_path = db_opt_path.join("db_options.bin");
    assert!(db_opt_options_path.exists());
    let opt_options_bytes = fs::read(&db_opt_options_path).unwrap();
    let opt_comp: CompressionType = bincode::deserialize(&opt_options_bytes).unwrap();
    assert_eq!(opt_comp, CompressionType::Uncompressed);

    // 4. Test ExecuteQuery on db_lz4
    // Create Table
    {
        let mut stream = client
            .execute_query(
                "db_lz4",
                dtdb_api::sql_query!("CREATE TABLE Users (id int PRIMARY KEY, name varchar(255));"),
            )
            .await
            .unwrap();

        let mut payloads = Vec::new();
        while let Some(res) = stream.next().await {
            payloads.push(res.unwrap());
        }
        assert_eq!(payloads.len(), 1);
        let payload = payloads[0].payload.as_ref().unwrap();
        match payload {
            dtdb_api::proto::execute_query_response::Payload::InfoMessage(msg) => {
                assert!(msg.contains("Table created"));
            }
            _ => panic!("Expected info message"),
        }
    }

    // Insert rows
    {
        let mut stream = client
            .execute_query(
                "db_lz4",
                dtdb_api::sql_query!("INSERT INTO Users (id, name) VALUES (42, 'Bob');"),
            )
            .await
            .unwrap();
        let mut payloads = Vec::new();
        while let Some(res) = stream.next().await {
            payloads.push(res.unwrap());
        }
        assert_eq!(payloads.len(), 1);
        match payloads[0].payload.as_ref().unwrap() {
            dtdb_api::proto::execute_query_response::Payload::InfoMessage(msg) => {
                assert!(msg.contains("Inserted 1 row"));
            }
            _ => panic!("Expected info message"),
        }
    }

    // Select rows
    {
        let mut stream = client
            .execute_query(
                "db_lz4",
                dtdb_api::sql_query!("SELECT id, name FROM Users;"),
            )
            .await
            .unwrap();
        let mut payloads = Vec::new();
        while let Some(res) = stream.next().await {
            payloads.push(res.unwrap());
        }
        assert_eq!(payloads.len(), 2); // Header + Row

        // Header validation
        match payloads[0].payload.as_ref().unwrap() {
            dtdb_api::proto::execute_query_response::Payload::Header(header) => {
                assert_eq!(header.column_names, vec!["id", "name"]);
            }
            _ => panic!("Expected header payload"),
        }

        // Row validation
        match payloads[1].payload.as_ref().unwrap() {
            dtdb_api::proto::execute_query_response::Payload::Row(row) => {
                assert_eq!(row.values, vec!["42", "Bob"]);
            }
            _ => panic!("Expected row payload"),
        }
    }

    // 5. Test Flush Db
    let flush_resp = client.flush_db("db_lz4").await.unwrap();
    assert!(flush_resp.success);
    assert!(flush_resp.message.contains("Flushed"));

    // 6. Test DropDb
    let drop_resp = client.drop_db("db_lz4").await.unwrap();
    assert!(drop_resp.success);
    assert!(!db_lz4_path.exists());
}
