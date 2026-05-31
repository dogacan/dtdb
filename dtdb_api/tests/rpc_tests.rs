use futures_util::StreamExt;
use std::fs;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use dtdb_api::client::RemoteClient;
use dtdb_api::proto::duct_tape_db_service_server::DuctTapeDbServiceServer;
use dtdb_api::server::DuctTapeDbServiceImpl;
use dtdb_storage::CompressionType;

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
    let mut client = RemoteClient::connect(client_addr).await.unwrap();

    // 3. Test CreateDb with Lz4
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

    // 4. Test CreateDb with Uncompressed
    let create_uncompressed_resp = client
        .create_db("db_raw", CompressionType::Uncompressed)
        .await
        .unwrap();
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
                dtdb_api::sql_query!("INSERT INTO Users (id, name) VALUES (10, 'Alice');"),
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

#[tokio::test]
async fn test_grpc_server_side_parameters() {
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
    let mut client = RemoteClient::connect(client_addr).await.unwrap();

    // 3. Create database
    client
        .create_db("db_params", CompressionType::Uncompressed)
        .await
        .unwrap();

    // 4. Create table
    {
        let mut stream = client
            .execute_query(
                "db_params",
                dtdb_api::sql_query!("CREATE TABLE Users (id int PRIMARY KEY, name varchar(255));"),
            )
            .await
            .unwrap();
        while let Some(res) = stream.next().await {
            res.unwrap();
        }
    }

    // 5. Insert row with parameters
    {
        let mut stream = client
            .execute_query(
                "db_params",
                dtdb_api::sql_query!("INSERT INTO Users (id, name) VALUES (:id, :name);")
                    .bind("id", 42i64)
                    .bind("name", "Bob"),
            )
            .await
            .unwrap();
        while let Some(res) = stream.next().await {
            res.unwrap();
        }
    }

    // 6. Select row with parameters
    {
        let mut stream = client
            .execute_query(
                "db_params",
                dtdb_api::sql_query!("SELECT id, name FROM Users WHERE id = :id;")
                    .bind("id", 42i64),
            )
            .await
            .unwrap();
        let mut payloads = Vec::new();
        while let Some(res) = stream.next().await {
            payloads.push(res.unwrap());
        }
        assert_eq!(payloads.len(), 2); // Header + Row

        // Row validation
        match payloads[1].payload.as_ref().unwrap() {
            dtdb_api::proto::execute_query_response::Payload::Row(row) => {
                assert_eq!(row.values, vec!["42", "Bob"]);
            }
            _ => panic!("Expected row payload"),
        }
    }

    // Clean up server task
    server_handle.abort();
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn test_grpc_security() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    // 1. Generate self-signed cert & key for TLS
    let ca_key_path = temp_dir.path().join("ca.key");
    let ca_cert_path = temp_dir.path().join("ca.pem");
    let server_key_path = temp_dir.path().join("server.key");
    let server_csr_path = temp_dir.path().join("server.csr");
    let server_cert_path = temp_dir.path().join("server.pem");
    let extfile_path = temp_dir.path().join("extfile.cnf");

    // Generate CA key and cert
    let openssl_status1 = std::process::Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-new",
            "-nodes",
            "-newkey",
            "rsa:2048",
            "-keyout",
            ca_key_path.to_str().unwrap(),
            "-out",
            ca_cert_path.to_str().unwrap(),
            "-sha256",
            "-days",
            "1",
            "-subj",
            "/CN=MyLocalCA",
        ])
        .output()
        .expect("failed to execute openssl req for CA");
    assert!(openssl_status1.status.success(), "openssl status1 failed");

    // Generate server key
    let openssl_status2 = std::process::Command::new("openssl")
        .args(["genrsa", "-out", server_key_path.to_str().unwrap(), "2048"])
        .output()
        .expect("failed to generate server key");
    assert!(openssl_status2.status.success(), "openssl status2 failed");

    // Generate server CSR
    let openssl_status3 = std::process::Command::new("openssl")
        .args([
            "req",
            "-new",
            "-key",
            server_key_path.to_str().unwrap(),
            "-out",
            server_csr_path.to_str().unwrap(),
            "-subj",
            "/CN=localhost",
        ])
        .output()
        .expect("failed to generate server CSR");
    assert!(openssl_status3.status.success(), "openssl status3 failed");

    // Create extfile
    std::fs::write(
        &extfile_path,
        "subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=CA:FALSE\n",
    )
    .unwrap();

    // Sign the server CSR with CA
    let openssl_status4 = std::process::Command::new("openssl")
        .args([
            "x509",
            "-req",
            "-in",
            server_csr_path.to_str().unwrap(),
            "-CA",
            ca_cert_path.to_str().unwrap(),
            "-CAkey",
            ca_key_path.to_str().unwrap(),
            "-CAcreateserial",
            "-out",
            server_cert_path.to_str().unwrap(),
            "-days",
            "1",
            "-sha256",
            "-extfile",
            extfile_path.to_str().unwrap(),
        ])
        .output()
        .expect("failed to sign server cert");
    assert!(openssl_status4.status.success(), "openssl status4 failed");

    // 2. Start Secure Server on random port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port();

    let service = DuctTapeDbServiceImpl::new(&data_path).unwrap();

    let cert_str = fs::read_to_string(&server_cert_path).unwrap();
    let key_str = fs::read_to_string(&server_key_path).unwrap();
    let identity = tonic::transport::Identity::from_pem(cert_str, key_str);
    let tls_config = tonic::transport::ServerTlsConfig::new().identity(identity);

    let auth_token = "secret_token".to_string();
    let auth_token_clone = auth_token.clone();
    let interceptor = move |req: tonic::Request<()>| -> Result<tonic::Request<()>, tonic::Status> {
        match req.metadata().get("authorization") {
            Some(header_val) => {
                let expected_header = format!("Bearer {}", auth_token_clone);
                if header_val.to_str().unwrap_or("") == expected_header {
                    Ok(req)
                } else {
                    Err(tonic::Status::unauthenticated("Invalid auth token"))
                }
            }
            None => Err(tonic::Status::unauthenticated("Missing auth token")),
        }
    };
    let service_server = DuctTapeDbServiceServer::with_interceptor(service, interceptor);

    let server_handle = tokio::spawn(async move {
        Server::builder()
            .tls_config(tls_config)
            .unwrap()
            .add_service(service_server)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Let the server spin up
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Set CA cert path for client
    unsafe {
        std::env::set_var("DTDB_CA_CERT", &ca_cert_path);
    }

    // 3. Connect client with WRONG token
    let client_addr = format!("https://127.0.0.1:{}", port);
    let mut client_wrong_token =
        RemoteClient::connect_with_token(client_addr.clone(), Some("wrong_token".to_string()))
            .await
            .unwrap();

    let err = client_wrong_token
        .create_db("db_secure", CompressionType::Uncompressed)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    // 4. Connect client with MISSING token
    let mut client_missing_token = RemoteClient::connect_with_token(client_addr.clone(), None)
        .await
        .unwrap();

    let err2 = client_missing_token
        .create_db("db_secure2", CompressionType::Uncompressed)
        .await
        .unwrap_err();
    assert_eq!(err2.code(), tonic::Code::Unauthenticated);

    // 5. Connect client with CORRECT token
    let mut client_correct =
        RemoteClient::connect_with_token(client_addr, Some("secret_token".to_string()))
            .await
            .unwrap();

    let res = client_correct
        .create_db("db_secure3", CompressionType::Uncompressed)
        .await
        .unwrap();
    assert!(res.success);

    // Cleanup env var and abort server
    unsafe {
        std::env::remove_var("DTDB_CA_CERT");
    }
    server_handle.abort();

    // 6. Verify loopback bind safety checks
    assert!(dtdb_api::validate_bind_security("127.0.0.1:50051", false, false).is_ok());
    assert!(dtdb_api::validate_bind_security("[::1]:50051", false, false).is_ok());
    assert!(dtdb_api::validate_bind_security("0.0.0.0:50051", true, true).is_ok());

    let err_res = dtdb_api::validate_bind_security("0.0.0.0:50051", false, false);
    assert!(err_res.is_err());
    let err_msg = err_res.unwrap_err();
    assert!(
        err_msg.contains("Security Error") && err_msg.contains("Binding to a non-loopback address")
    );
}

#[tokio::test]
async fn test_drop_db_active_transactions() {
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
    let mut client = RemoteClient::connect(client_addr).await.unwrap();

    // 3. Create database and table
    client
        .create_db("db_drop_test", CompressionType::Uncompressed)
        .await
        .unwrap();

    {
        let mut stream = client
            .execute_query(
                "db_drop_test",
                dtdb_api::sql_query!("CREATE TABLE Users (id int PRIMARY KEY, name varchar(255));"),
            )
            .await
            .unwrap();
        while let Some(res) = stream.next().await {
            res.unwrap();
        }
    }

    // Channels to synchronize transaction and drop_db execution
    let (tx_started_send, tx_started_recv) = tokio::sync::oneshot::channel();
    let (tx_done_send, tx_done_recv) = tokio::sync::oneshot::channel();

    // 4. Start active transaction in another task
    let mut client_tx = client.clone();
    let tx_handle = tokio::spawn(async move {
        client_tx
            .run_in_transaction("db_drop_test", |tx| async move {
                // Signal that we are inside the transaction
                tx_started_send.send(()).unwrap();

                // Wait for the drop_db attempt to be performed on main thread
                tx_done_recv.await.unwrap();

                // Run a statement to keep transaction active/meaningful
                tx.execute_query(dtdb_api::sql_query!(
                    "INSERT INTO Users (id, name) VALUES (1, 'Alice');"
                ))
                .await?;

                Ok(())
            })
            .await
            .unwrap();
    });

    // Wait until transaction is registered as active
    tx_started_recv.await.unwrap();

    // 5. Attempt to drop database with active transaction — should fail
    let drop_fail = client.drop_db("db_drop_test").await.unwrap();
    assert!(!drop_fail.success);
    assert!(drop_fail.message.contains("active transactions"));

    // Check directory still exists
    let db_path = data_path.join("db_drop_test");
    assert!(db_path.exists());

    // 6. Signal the transaction to finish and wait for it
    tx_done_send.send(()).unwrap();
    tx_handle.await.unwrap();

    // 7. Drop the database now that there are no active transactions — should succeed
    let drop_success = client.drop_db("db_drop_test").await.unwrap();
    assert!(drop_success.success);

    // Check directory is deleted
    assert!(!db_path.exists());

    // Cleanup server
    server_handle.abort();
}

#[tokio::test]
async fn test_database_restore_on_startup() {
    use dtdb_api::proto::duct_tape_db_service_server::DuctTapeDbService;
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    // 1. Create a service and a database
    {
        let service = DuctTapeDbServiceImpl::new(&data_path).unwrap();
        let req = tonic::Request::new(dtdb_api::proto::CreateDbRequest {
            db_name: "restore_me".to_string(),
            compression: dtdb_api::proto::CompressionOption::CompressionUncompressed as i32,
            memtable_size_limit: None,
            block_size_limit: None,
            wal_size_limit: None,
            flush_interval_ms: None,
            analyze_frequency_ms: None,
        });
        let resp = service.create_db(req).await.unwrap().into_inner();
        assert!(resp.success);
    } // service is dropped here

    // Create a folder that doesn't have db_options.bin to test skipping
    let dummy_db_path = data_path.join("dummy_no_options");
    fs::create_dir_all(&dummy_db_path).unwrap();

    // 2. Re-create the service on the same path
    let service2 = DuctTapeDbServiceImpl::new(&data_path).unwrap();

    // 3. Verify restore_me was restored, but dummy_no_options was skipped
    let lookup_restored = service2.get_db_and_engine("restore_me");
    assert!(lookup_restored.is_some());
    let (db, _engine) = lookup_restored.unwrap();
    assert_eq!(db.list_tables().len(), 0);

    let lookup_dummy = service2.get_db_and_engine("dummy_no_options");
    assert!(lookup_dummy.is_none());
}

#[tokio::test]
async fn test_invalid_arguments_and_missing_databases() {
    use dtdb_api::proto::duct_tape_db_service_server::DuctTapeDbService;
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();
    let service = DuctTapeDbServiceImpl::new(&data_path).unwrap();

    // Test create_db with empty name
    let create_empty_req = tonic::Request::new(dtdb_api::proto::CreateDbRequest {
        db_name: "   ".to_string(),
        compression: dtdb_api::proto::CompressionOption::CompressionUncompressed as i32,
        memtable_size_limit: None,
        block_size_limit: None,
        wal_size_limit: None,
        flush_interval_ms: None,
        analyze_frequency_ms: None,
    });
    let err = service.create_db(create_empty_req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("Database name cannot be empty"));

    // Test create_db for already existing db
    let create_req1 = tonic::Request::new(dtdb_api::proto::CreateDbRequest {
        db_name: "exists_db".to_string(),
        compression: dtdb_api::proto::CompressionOption::CompressionUncompressed as i32,
        memtable_size_limit: None,
        block_size_limit: None,
        wal_size_limit: None,
        flush_interval_ms: None,
        analyze_frequency_ms: None,
    });
    let resp1 = service.create_db(create_req1).await.unwrap().into_inner();
    assert!(resp1.success);

    let create_req2 = tonic::Request::new(dtdb_api::proto::CreateDbRequest {
        db_name: "exists_db".to_string(),
        compression: dtdb_api::proto::CompressionOption::CompressionUncompressed as i32,
        memtable_size_limit: None,
        block_size_limit: None,
        wal_size_limit: None,
        flush_interval_ms: None,
        analyze_frequency_ms: None,
    });
    let resp2 = service.create_db(create_req2).await.unwrap().into_inner();
    assert!(!resp2.success);
    assert!(resp2.message.contains("already exists"));

    // Test drop_db with empty name
    let drop_empty_req = tonic::Request::new(dtdb_api::proto::DropDbRequest {
        db_name: "  ".to_string(),
    });
    let err = service.drop_db(drop_empty_req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // Test drop_db for non-existent database
    let drop_missing_req = tonic::Request::new(dtdb_api::proto::DropDbRequest {
        db_name: "no_such_db".to_string(),
    });
    let resp = service
        .drop_db(drop_missing_req)
        .await
        .unwrap()
        .into_inner();
    assert!(!resp.success);
    assert!(resp.message.contains("not found"));

    // Test flush_db with empty name
    let flush_empty_req = tonic::Request::new(dtdb_api::proto::FlushDbRequest {
        db_name: "".to_string(),
    });
    let err = service.flush_db(flush_empty_req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // Test flush_db for non-existent database
    let flush_missing_req = tonic::Request::new(dtdb_api::proto::FlushDbRequest {
        db_name: "no_such_db".to_string(),
    });
    let err = service.flush_db(flush_missing_req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);

    // Test execute_query for non-existent database
    let exec_missing_req = tonic::Request::new(dtdb_api::proto::ExecuteQueryRequest {
        db_name: "no_such_db".to_string(),
        sql_query: "SELECT 1;".to_string(),
        parameters: Vec::new(),
    });
    let err = service.execute_query(exec_missing_req).await.err().unwrap();
    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn test_parameter_types_coverage() {
    use dtdb_api::proto::duct_tape_db_service_server::DuctTapeDbService;
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();
    let service = DuctTapeDbServiceImpl::new(&data_path).unwrap();

    // 1. Create a database and a table
    let create_req = tonic::Request::new(dtdb_api::proto::CreateDbRequest {
        db_name: "param_db".to_string(),
        compression: dtdb_api::proto::CompressionOption::CompressionUncompressed as i32,
        memtable_size_limit: None,
        block_size_limit: None,
        wal_size_limit: None,
        flush_interval_ms: None,
        analyze_frequency_ms: None,
    });
    service.create_db(create_req).await.unwrap();

    let create_table_req = tonic::Request::new(dtdb_api::proto::ExecuteQueryRequest {
        db_name: "param_db".to_string(),
        sql_query: "CREATE TABLE ValuesTable (id int PRIMARY KEY, f_val float, b_val bool, s_val varchar(255), bytes_val blob);".to_string(),
        parameters: Vec::new(),
    });
    let mut create_table_stream = service
        .execute_query(create_table_req)
        .await
        .unwrap()
        .into_inner();
    while let Some(res) = create_table_stream.next().await {
        res.unwrap();
    }

    // 2. Insert with float, bool, string, bytes, null, and int parameters
    use dtdb_api::proto::{QueryParam, param_value};

    let p_id = QueryParam {
        name: "id".to_string(),
        value: Some(dtdb_api::proto::ParamValue {
            val: Some(param_value::Val::IntVal(100)),
        }),
    };
    let p_float = QueryParam {
        name: "f".to_string(),
        value: Some(dtdb_api::proto::ParamValue {
            val: Some(param_value::Val::FloatVal(1.23)),
        }),
    };
    let p_bool = QueryParam {
        name: "b".to_string(),
        value: Some(dtdb_api::proto::ParamValue {
            val: Some(param_value::Val::BoolVal(true)),
        }),
    };
    let p_str = QueryParam {
        name: "s".to_string(),
        value: Some(dtdb_api::proto::ParamValue {
            val: Some(param_value::Val::StringVal("hello".to_string())),
        }),
    };
    let p_bytes = QueryParam {
        name: "bytes".to_string(),
        value: Some(dtdb_api::proto::ParamValue {
            val: Some(param_value::Val::BytesVal(vec![1, 2, 3])),
        }),
    };
    let p_null = QueryParam {
        name: "n".to_string(),
        value: Some(dtdb_api::proto::ParamValue {
            val: Some(param_value::Val::NullVal(true)),
        }),
    };

    let insert_req = tonic::Request::new(dtdb_api::proto::ExecuteQueryRequest {
        db_name: "param_db".to_string(),
        sql_query: "INSERT INTO ValuesTable (id, f_val, b_val, s_val, bytes_val) VALUES (:id, :f, :b, :s, :bytes);".to_string(),
        parameters: vec![p_id, p_float, p_bool, p_str, p_bytes, p_null],
    });
    let err = service.execute_query(insert_req).await.err().unwrap();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("Unsupported SQL value type")
            || err.message().contains("HexStringLiteral")
    );
}
