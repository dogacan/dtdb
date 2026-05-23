use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use futures_util::StreamExt;
use std::fs;

use dtdb_storage::CompressionType;
use dtdb_rpc::client::DuctTapeDbClient;
use dtdb_rpc::server::DuctTapeDbServiceImpl;
use dtdb_rpc::proto::duct_tape_db_service_server::DuctTapeDbServiceServer;

#[tokio::test]
async fn test_grpc_client_server_integration() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    // 1. Start Server on random port
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

    // 2. Connect client
    let client_addr = format!("http://127.0.0.1:{}", port);
    let mut client = DuctTapeDbClient::connect(client_addr).await.unwrap();

    // 3. Test CreateDb with Lz4
    let create_lz4_resp = client.create_db("db_lz4", CompressionType::Lz4).await.unwrap();
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

    // 4. Test CreateDb with Uncompressed
    let create_uncompressed_resp = client.create_db("db_raw", CompressionType::Uncompressed).await.unwrap();
    assert!(create_uncompressed_resp.success);

    // Check directory layout & db_options.bin
    let db_raw_path = data_path.join("db_raw");
    assert!(db_raw_path.exists());
    let db_raw_options_path = db_raw_path.join("db_options.bin");
    assert!(db_raw_options_path.exists());
    let raw_options_bytes = fs::read(&db_raw_options_path).unwrap();
    let raw_comp: CompressionType = bincode::deserialize(&raw_options_bytes).unwrap();
    assert_eq!(raw_comp, CompressionType::Uncompressed);

    // 5. Test ExecuteQuery on db_lz4
    // Create Table
    {
        let mut stream = client
            .execute_query("db_lz4", "CREATE TABLE Users (id int PRIMARY KEY, name varchar(255));")
            .await
            .unwrap();

        let mut payloads = Vec::new();
        while let Some(res) = stream.next().await {
            payloads.push(res.unwrap());
        }
        assert_eq!(payloads.len(), 1);
        let payload = payloads[0].payload.as_ref().unwrap();
        match payload {
            dtdb_rpc::proto::execute_query_response::Payload::InfoMessage(msg) => {
                assert!(msg.contains("Table created"));
            }
            _ => panic!("Expected info message"),
        }
    }

    // Insert rows
    {
        let mut stream = client
            .execute_query("db_lz4", "INSERT INTO Users (id, name) VALUES (10, 'Alice');")
            .await
            .unwrap();
        let mut payloads = Vec::new();
        while let Some(res) = stream.next().await {
            payloads.push(res.unwrap());
        }
        assert_eq!(payloads.len(), 1);
        match payloads[0].payload.as_ref().unwrap() {
            dtdb_rpc::proto::execute_query_response::Payload::InfoMessage(msg) => {
                assert!(msg.contains("Inserted 1 row"));
            }
            _ => panic!("Expected info message"),
        }
    }

    // Select rows
    {
        let mut stream = client
            .execute_query("db_lz4", "SELECT id, name FROM Users;")
            .await
            .unwrap();
        let mut payloads = Vec::new();
        while let Some(res) = stream.next().await {
            payloads.push(res.unwrap());
        }
        assert_eq!(payloads.len(), 2); // Header + Row

        // Header validation
        match payloads[0].payload.as_ref().unwrap() {
            dtdb_rpc::proto::execute_query_response::Payload::Header(header) => {
                assert_eq!(header.column_names, vec!["id", "name"]);
            }
            _ => panic!("Expected header payload"),
        }

        // Row validation
        match payloads[1].payload.as_ref().unwrap() {
            dtdb_rpc::proto::execute_query_response::Payload::Row(row) => {
                assert_eq!(row.values, vec!["10", "Alice"]);
            }
            _ => panic!("Expected row payload"),
        }
    }

    // 6. Test DropDb
    let drop_resp = client.drop_db("db_lz4").await.unwrap();
    assert!(drop_resp.success);
    assert!(!db_lz4_path.exists());

    // Clean up server task
    server_handle.abort();
}
