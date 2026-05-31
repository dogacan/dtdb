use futures_util::StreamExt;
use std::fs;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use dtdb_api::client::RemoteClient;
use dtdb_api::proto::duct_tape_db_service_server::DuctTapeDbServiceServer;
use dtdb_api::server::DuctTapeDbServiceImpl;
use dtdb_relational::DatabaseOptions;
use dtdb_storage::CompressionType;

// Helper to count SSTable files in a directory
fn count_sst_files(dir: &std::path::Path) -> usize {
    let mut count = 0;
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            if entry.path().extension().is_some_and(|ext| ext == "sst") {
                count += 1;
            }
        }
    }
    count
}

#[tokio::test]
async fn test_wal_size_based_flush() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    // Start Server on random port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port();

    let service = DuctTapeDbServiceImpl::new(&data_path).unwrap();

    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(DuctTapeDbServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Let the server spin up
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let client_addr = format!("http://127.0.0.1:{}", port);
    let mut client = RemoteClient::connect(client_addr).await.unwrap();

    // Create a database with a very small WAL size limit (100 bytes)
    let options = DatabaseOptions {
        compression: CompressionType::Lz4,
        memtable_size_limit: 1024 * 1024,
        block_size_limit: 4096,
        wal_size_limit: 100, // 100 bytes limit
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

    let create_resp = client
        .create_db_with_options("db_wal", options)
        .await
        .unwrap();
    assert!(create_resp.success);

    // Create table
    {
        let mut stream = client
            .execute_query(
                "db_wal",
                dtdb_api::sql_query!(
                    "CREATE TABLE TestTable (id int PRIMARY KEY, value varchar(255));"
                ),
            )
            .await
            .unwrap();
        while let Some(res) = stream.next().await {
            res.unwrap();
        }
    }

    let table_dir = data_path.join("db_wal").join("TestTable");
    assert_eq!(count_sst_files(&table_dir), 0);

    // Insert a large string to exceed the 100 bytes WAL size limit
    {
        let mut stream = client
            .execute_query(
                "db_wal",
                dtdb_api::sql_query!("INSERT INTO TestTable (id, value) VALUES (1, 'This is a long string designed to exceed 100 bytes in the WAL, which should trigger an automatic flush to disk.');"),
            )
            .await
            .unwrap();
        while let Some(res) = stream.next().await {
            res.unwrap();
        }
    }

    // Verify that the WAL size limit check triggered an automatic flush and created an SSTable file
    assert_eq!(count_sst_files(&table_dir), 1);

    server_handle.abort();
}

#[tokio::test]
async fn test_periodic_time_based_flush() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    // Start Server on random port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port();

    let service = DuctTapeDbServiceImpl::new(&data_path).unwrap();

    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(DuctTapeDbServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Let the server spin up
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let client_addr = format!("http://127.0.0.1:{}", port);
    let mut client = RemoteClient::connect(client_addr).await.unwrap();

    // Create a database with a 3s periodic flush interval. The interval is kept
    // comfortably longer than the test's setup + insert so the "not flushed
    // yet" check below cannot race a flush tick firing mid-insert — a race that
    // flaked under coverage instrumentation when the interval was 150ms.
    let options = DatabaseOptions {
        compression: CompressionType::Lz4,
        memtable_size_limit: 1024 * 1024,
        block_size_limit: 4096,
        wal_size_limit: 1024 * 1024,
        flush_interval_ms: Some(3000),
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

    let create_resp = client
        .create_db_with_options("db_periodic", options)
        .await
        .unwrap();
    assert!(create_resp.success);

    // Create table
    {
        let mut stream = client
            .execute_query(
                "db_periodic",
                dtdb_api::sql_query!(
                    "CREATE TABLE TestTable (id int PRIMARY KEY, value varchar(255));"
                ),
            )
            .await
            .unwrap();
        while let Some(res) = stream.next().await {
            res.unwrap();
        }
    }

    let table_dir = data_path.join("db_periodic").join("TestTable");
    assert_eq!(count_sst_files(&table_dir), 0);

    // Insert a small row
    {
        let mut stream = client
            .execute_query(
                "db_periodic",
                dtdb_api::sql_query!("INSERT INTO TestTable (id, value) VALUES (1, 'small');"),
            )
            .await
            .unwrap();
        while let Some(res) = stream.next().await {
            res.unwrap();
        }
    }

    // The row is in the memtable; the long flush interval guarantees the
    // periodic flusher hasn't fired yet, so no SSTable exists.
    assert_eq!(count_sst_files(&table_dir), 0);

    // Poll (rather than sleeping a fixed amount) for the periodic flush to land.
    // The background flush thread fires roughly one interval after the row hits
    // the memtable, but instrumented runs stretch wall-clock timing, so we wait
    // up to ~12s.
    let mut flushed = false;
    for _ in 0..80 {
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        if count_sst_files(&table_dir) == 1 {
            flushed = true;
            break;
        }
    }
    assert!(
        flushed,
        "periodic flush should have produced exactly one SSTable within the timeout"
    );

    server_handle.abort();
}

#[tokio::test]
async fn test_manual_rpc_flush() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    // Start Server on random port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port();

    let service = DuctTapeDbServiceImpl::new(&data_path).unwrap();

    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(DuctTapeDbServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Let the server spin up
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let client_addr = format!("http://127.0.0.1:{}", port);
    let mut client = RemoteClient::connect(client_addr).await.unwrap();

    // Create database with no periodic flush
    let options = DatabaseOptions {
        compression: CompressionType::Lz4,
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

    let create_resp = client
        .create_db_with_options("db_manual", options)
        .await
        .unwrap();
    assert!(create_resp.success);

    // Create table
    {
        let mut stream = client
            .execute_query(
                "db_manual",
                dtdb_api::sql_query!(
                    "CREATE TABLE TestTable (id int PRIMARY KEY, value varchar(255));"
                ),
            )
            .await
            .unwrap();
        while let Some(res) = stream.next().await {
            res.unwrap();
        }
    }

    let table_dir = data_path.join("db_manual").join("TestTable");
    assert_eq!(count_sst_files(&table_dir), 0);

    // Insert a small row
    {
        let mut stream = client
            .execute_query(
                "db_manual",
                dtdb_api::sql_query!("INSERT INTO TestTable (id, value) VALUES (1, 'small');"),
            )
            .await
            .unwrap();
        while let Some(res) = stream.next().await {
            res.unwrap();
        }
    }

    // Verify immediately (no flush yet)
    assert_eq!(count_sst_files(&table_dir), 0);

    // Perform manual flush
    let flush_resp = client.flush_db("db_manual").await.unwrap();
    assert!(flush_resp.success);
    assert!(flush_resp.message.contains("Flushed 1 table(s)"));

    // Verify that the table has been flushed
    assert_eq!(count_sst_files(&table_dir), 1);

    server_handle.abort();
}
