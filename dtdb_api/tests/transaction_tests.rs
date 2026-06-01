#![allow(clippy::result_large_err)]
use futures_util::StreamExt;
use std::path::Path;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Status;
use tonic::transport::Server;

use dtdb_api::client::RemoteClient;
use dtdb_api::proto::duct_tape_db_service_client::DuctTapeDbServiceClient;
use dtdb_api::proto::duct_tape_db_service_server::DuctTapeDbServiceServer;
use dtdb_api::proto::execute_query_response::Payload as EqPayload;
use dtdb_api::proto::transaction_request::Command;
use dtdb_api::proto::transaction_response::Payload as TxPayload;
use dtdb_api::proto::{
    CommitTransaction, ExecuteTxQuery, RollbackTransaction, StartTransaction, TransactionRequest,
};
use dtdb_api::server::DuctTapeDbServiceImpl;
use dtdb_storage::CompressionType;

// Helper to spin up a gRPC server on a random port
async fn start_test_server(data_path: &Path) -> (u16, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port();

    let service = DuctTapeDbServiceImpl::new(data_path).unwrap();

    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(DuctTapeDbServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Let the server spin up
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    (port, server_handle)
}

#[test]
fn test_in_process_transaction_commit() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    let client = dtdb_api::in_process::InProcessClient::open(&data_path).unwrap();
    client
        .create_db("test_db", CompressionType::Uncompressed)
        .unwrap();

    // Create table first
    {
        let results = client
            .execute_query(
                "test_db",
                dtdb_api::sql_query!("CREATE TABLE Users (id int PRIMARY KEY, name varchar(255));"),
            )
            .unwrap();
        for res in results {
            res.unwrap();
        }
    }

    // Run transaction
    let tx_res = client.run_in_transaction("test_db", |tx| {
        let res1 = tx.execute_query(dtdb_api::sql_query!(
            "INSERT INTO Users (id, name) VALUES (1, 'Alice');"
        ))?;
        for res in res1 {
            res.unwrap();
        }
        let res2 = tx.execute_query(dtdb_api::sql_query!(
            "INSERT INTO Users (id, name) VALUES (2, 'Bob');"
        ))?;
        for res in res2 {
            res.unwrap();
        }
        Ok("success_value")
    });

    assert_eq!(tx_res.unwrap(), "success_value");

    // Verify both rows exist
    let results = client
        .execute_query(
            "test_db",
            dtdb_api::sql_query!("SELECT id, name FROM Users ORDER BY id ASC;"),
        )
        .unwrap();

    let mut rows = Vec::new();
    for res in results {
        let row = res.unwrap();
        let id_val = match row.get_by_index(0).unwrap() {
            dtdb_storage::DbValue::Int(v) => v.to_string(),
            _ => panic!("Expected int"),
        };
        let name_val = match row.get_by_index(1).unwrap() {
            dtdb_storage::DbValue::String(s) => s.to_string(),
            _ => panic!("Expected string"),
        };
        rows.push(vec![id_val, name_val]);
    }

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["1", "Alice"]);
    assert_eq!(rows[1], vec!["2", "Bob"]);
}

#[test]
fn test_in_process_transaction_rollback() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    let client = dtdb_api::in_process::InProcessClient::open(&data_path).unwrap();
    client
        .create_db("test_db", CompressionType::Uncompressed)
        .unwrap();

    // Create table first
    {
        let results = client
            .execute_query(
                "test_db",
                dtdb_api::sql_query!("CREATE TABLE Users (id int PRIMARY KEY, name varchar(255));"),
            )
            .unwrap();
        for res in results {
            res.unwrap();
        }
    }

    // Run transaction and return error to trigger rollback
    let tx_res: Result<(), Status> = client.run_in_transaction("test_db", |tx| {
        let res = tx.execute_query(dtdb_api::sql_query!(
            "INSERT INTO Users (id, name) VALUES (1, 'Alice');"
        ))?;
        for r in res {
            r.unwrap();
        }
        Err(Status::aborted("abort transaction"))
    });

    assert!(tx_res.is_err());
    assert_eq!(tx_res.unwrap_err().code(), tonic::Code::Aborted);

    // Verify table is empty
    let results = client
        .execute_query(
            "test_db",
            dtdb_api::sql_query!("SELECT id, name FROM Users;"),
        )
        .unwrap();

    let mut rows = Vec::new();
    for res in results {
        let row = res.unwrap();
        rows.push(row.values);
    }

    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn test_grpc_transaction_commit() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    let (port, server_handle) = start_test_server(&data_path).await;

    let client_addr = format!("http://127.0.0.1:{}", port);
    let mut client = RemoteClient::connect(client_addr).await.unwrap();

    client
        .create_db("test_db", CompressionType::Uncompressed)
        .await
        .unwrap();

    // Create table
    {
        let mut stream = client
            .execute_query(
                "test_db",
                dtdb_api::sql_query!("CREATE TABLE Users (id int PRIMARY KEY, name varchar(255));"),
            )
            .await
            .unwrap();
        while let Some(res) = stream.next().await {
            res.unwrap();
        }
    }

    // Run transaction
    let tx_res = client
        .run_in_transaction("test_db", |tx| async move {
            tx.execute_query(dtdb_api::sql_query!(
                "INSERT INTO Users (id, name) VALUES (10, 'Charlie');"
            ))
            .await?;
            tx.execute_query(dtdb_api::sql_query!(
                "INSERT INTO Users (id, name) VALUES (20, 'Dave');"
            ))
            .await?;
            Ok("success")
        })
        .await;

    assert_eq!(tx_res.unwrap(), "success");

    // Verify rows exist
    let mut stream = client
        .execute_query(
            "test_db",
            dtdb_api::sql_query!("SELECT id, name FROM Users ORDER BY id ASC;"),
        )
        .await
        .unwrap();

    let mut rows = Vec::new();
    while let Some(res) = stream.next().await {
        let resp = res.unwrap();
        if let Some(EqPayload::Row(row)) = resp.payload {
            rows.push(row.values);
        }
    }

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["10", "Charlie"]);
    assert_eq!(rows[1], vec!["20", "Dave"]);

    server_handle.abort();
}

#[tokio::test]
async fn test_grpc_transaction_rollback() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    let (port, server_handle) = start_test_server(&data_path).await;

    let client_addr = format!("http://127.0.0.1:{}", port);
    let mut client = RemoteClient::connect(client_addr).await.unwrap();

    client
        .create_db("test_db", CompressionType::Uncompressed)
        .await
        .unwrap();

    // Create table
    {
        let mut stream = client
            .execute_query(
                "test_db",
                dtdb_api::sql_query!("CREATE TABLE Users (id int PRIMARY KEY, name varchar(255));"),
            )
            .await
            .unwrap();
        while let Some(res) = stream.next().await {
            res.unwrap();
        }
    }

    // Run transaction and return error to trigger rollback
    let tx_res: Result<(), Status> = client
        .run_in_transaction("test_db", |tx| async move {
            tx.execute_query(dtdb_api::sql_query!(
                "INSERT INTO Users (id, name) VALUES (10, 'Charlie');"
            ))
            .await?;
            Err(Status::aborted("abort transaction"))
        })
        .await;

    assert!(tx_res.is_err());

    // Verify table is empty
    let mut stream = client
        .execute_query(
            "test_db",
            dtdb_api::sql_query!("SELECT id, name FROM Users;"),
        )
        .await
        .unwrap();

    let mut rows = Vec::new();
    while let Some(res) = stream.next().await {
        let resp = res.unwrap();
        if let Some(EqPayload::Row(row)) = resp.payload {
            rows.push(row.values);
        }
    }

    assert_eq!(rows.len(), 0);

    server_handle.abort();
}

#[test]
fn test_multi_statement_rejection() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    let client = dtdb_api::in_process::InProcessClient::open(&data_path).unwrap();
    client
        .create_db("test_db", CompressionType::Uncompressed)
        .unwrap();

    // Rejects multi-statement ExecuteQuery
    let err = client
        .execute_query(
            "test_db",
            dtdb_api::sql_query!(
                "CREATE TABLE Users (id int PRIMARY KEY); INSERT INTO Users (id) VALUES (1);"
            ),
        )
        .err()
        .unwrap();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message()
            .contains("Multiple SQL statements in a single execute() call are not allowed")
    );
}

#[tokio::test]
async fn test_grpc_protocol_misuse() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    let (port, server_handle) = start_test_server(&data_path).await;

    let client_addr = format!("http://127.0.0.1:{}", port);

    // Create the database first
    {
        let mut client = RemoteClient::connect(client_addr.clone()).await.unwrap();
        client
            .create_db("test_db", CompressionType::Uncompressed)
            .await
            .unwrap();
    }

    let mut raw_client = DuctTapeDbServiceClient::connect(client_addr).await.unwrap();

    // Scenario A: Send Execute before Start
    {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let mut response = raw_client.transaction(stream).await.unwrap().into_inner();

        tx.send(TransactionRequest {
            command: Some(Command::Execute(ExecuteTxQuery {
                sql_query: "SELECT * FROM Users;".to_string(),
                parameters: Vec::new(),
            })),
        })
        .await
        .unwrap();

        let resp = response.next().await.unwrap().unwrap();
        match resp.payload {
            Some(TxPayload::ErrorMessage(msg)) => {
                assert!(msg.contains("must send StartTransaction before executing queries"));
            }
            other => panic!("Expected ErrorMessage, got {:?}", other),
        }
    }

    // Scenario B: Send Commit before Start
    {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let mut response = raw_client.transaction(stream).await.unwrap().into_inner();

        tx.send(TransactionRequest {
            command: Some(Command::Commit(CommitTransaction {})),
        })
        .await
        .unwrap();

        let resp = response.next().await.unwrap().unwrap();
        match resp.payload {
            Some(TxPayload::ErrorMessage(msg)) => {
                assert!(msg.contains("must send StartTransaction before committing"));
            }
            other => panic!("Expected ErrorMessage, got {:?}", other),
        }
    }

    // Scenario C: Send Rollback before Start
    {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let mut response = raw_client.transaction(stream).await.unwrap().into_inner();

        tx.send(TransactionRequest {
            command: Some(Command::Rollback(RollbackTransaction {})),
        })
        .await
        .unwrap();

        let resp = response.next().await.unwrap().unwrap();
        match resp.payload {
            Some(TxPayload::ErrorMessage(msg)) => {
                assert!(msg.contains("must send StartTransaction before rolling back"));
            }
            other => panic!("Expected ErrorMessage, got {:?}", other),
        }
    }

    // Scenario D: Send multiple StartTransaction commands
    {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let mut response = raw_client.transaction(stream).await.unwrap().into_inner();

        // Send first StartTransaction
        tx.send(TransactionRequest {
            command: Some(Command::Start(StartTransaction {
                db_name: "test_db".to_string(),
                isolation_level: None,
            })),
        })
        .await
        .unwrap();

        let resp1 = response.next().await.unwrap().unwrap();
        assert!(matches!(
            resp1.payload,
            Some(TxPayload::QueryFinished(true))
        ));

        // Send second StartTransaction
        tx.send(TransactionRequest {
            command: Some(Command::Start(StartTransaction {
                db_name: "test_db".to_string(),
                isolation_level: None,
            })),
        })
        .await
        .unwrap();

        let resp2 = response.next().await.unwrap().unwrap();
        match resp2.payload {
            Some(TxPayload::ErrorMessage(msg)) => {
                assert!(msg.contains("StartTransaction already received"));
            }
            other => panic!("Expected ErrorMessage, got {:?}", other),
        }
    }

    // Scenario E: Send commands after Commit
    {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let mut response = raw_client.transaction(stream).await.unwrap().into_inner();

        // 1. Start
        tx.send(TransactionRequest {
            command: Some(Command::Start(StartTransaction {
                db_name: "test_db".to_string(),
                isolation_level: None,
            })),
        })
        .await
        .unwrap();
        let resp = response.next().await.unwrap().unwrap();
        assert!(matches!(resp.payload, Some(TxPayload::QueryFinished(true))));

        // 2. Commit
        tx.send(TransactionRequest {
            command: Some(Command::Commit(CommitTransaction {})),
        })
        .await
        .unwrap();
        let resp = response.next().await.unwrap().unwrap();
        assert!(matches!(resp.payload, Some(TxPayload::CommitResult(_))));

        // 3. Execute query (misuse)
        tx.send(TransactionRequest {
            command: Some(Command::Execute(ExecuteTxQuery {
                sql_query: "SELECT * FROM Users;".to_string(),
                parameters: Vec::new(),
            })),
        })
        .await
        .unwrap();
        let resp = response.next().await.unwrap().unwrap();
        match resp.payload {
            Some(TxPayload::ErrorMessage(msg)) => {
                assert!(msg.contains("Transaction already completed"));
            }
            other => panic!("Expected ErrorMessage, got {:?}", other),
        }
    }

    // Scenario F: Send commands after Rollback
    {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let mut response = raw_client.transaction(stream).await.unwrap().into_inner();

        // 1. Start
        tx.send(TransactionRequest {
            command: Some(Command::Start(StartTransaction {
                db_name: "test_db".to_string(),
                isolation_level: None,
            })),
        })
        .await
        .unwrap();
        let resp = response.next().await.unwrap().unwrap();
        assert!(matches!(resp.payload, Some(TxPayload::QueryFinished(true))));

        // 2. Rollback
        tx.send(TransactionRequest {
            command: Some(Command::Rollback(RollbackTransaction {})),
        })
        .await
        .unwrap();
        let resp = response.next().await.unwrap().unwrap();
        assert!(matches!(resp.payload, Some(TxPayload::CommitResult(_))));

        // 3. Execute query (misuse)
        tx.send(TransactionRequest {
            command: Some(Command::Execute(ExecuteTxQuery {
                sql_query: "SELECT * FROM Users;".to_string(),
                parameters: Vec::new(),
            })),
        })
        .await
        .unwrap();
        let resp = response.next().await.unwrap().unwrap();
        match resp.payload {
            Some(TxPayload::ErrorMessage(msg)) => {
                assert!(msg.contains("Transaction already completed"));
            }
            other => panic!("Expected ErrorMessage, got {:?}", other),
        }
    }

    // Scenario G: Send transaction request with no command specified
    {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let mut response = raw_client.transaction(stream).await.unwrap().into_inner();

        tx.send(TransactionRequest { command: None }).await.unwrap();

        let resp = response.next().await.unwrap().unwrap();
        match resp.payload {
            Some(TxPayload::ErrorMessage(msg)) => {
                assert!(msg.contains("Empty transaction request"));
            }
            other => panic!("Expected ErrorMessage, got {:?}", other),
        }
    }

    // Scenario I: SQL syntax error inside transaction
    {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let mut response = raw_client.transaction(stream).await.unwrap().into_inner();

        // 1. Start
        tx.send(TransactionRequest {
            command: Some(Command::Start(StartTransaction {
                db_name: "test_db".to_string(),
                isolation_level: None,
            })),
        })
        .await
        .unwrap();
        let resp = response.next().await.unwrap().unwrap();
        assert!(matches!(resp.payload, Some(TxPayload::QueryFinished(true))));

        // 2. Execute invalid syntax query
        tx.send(TransactionRequest {
            command: Some(Command::Execute(ExecuteTxQuery {
                sql_query: "SELECT INVALID SYNTAX;".to_string(),
                parameters: Vec::new(),
            })),
        })
        .await
        .unwrap();

        let resp_err = response.next().await.unwrap().unwrap();
        match resp_err.payload {
            Some(TxPayload::ErrorMessage(msg)) => {
                assert!(msg.contains("SQL Error") || msg.contains("Expected"));
            }
            other => panic!("Expected ErrorMessage, got {:?}", other),
        }

        let resp_fin = response.next().await.unwrap().unwrap();
        assert!(matches!(
            resp_fin.payload,
            Some(TxPayload::QueryFinished(true))
        ));
    }

    // Scenario J: Database not found when starting transaction
    {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let mut response = raw_client.transaction(stream).await.unwrap().into_inner();

        tx.send(TransactionRequest {
            command: Some(Command::Start(StartTransaction {
                db_name: "non_existent_db_tx".to_string(),
                isolation_level: None,
            })),
        })
        .await
        .unwrap();

        let resp = response.next().await.unwrap().unwrap();
        match resp.payload {
            Some(TxPayload::ErrorMessage(msg)) => {
                assert!(msg.contains("Database") && msg.contains("not found"));
            }
            other => panic!("Expected ErrorMessage, got {:?}", other),
        }
    }

    server_handle.abort();
}

#[tokio::test]
async fn test_grpc_transaction_disconnect_rollback() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    let (port, server_handle) = start_test_server(&data_path).await;

    let client_addr = format!("http://127.0.0.1:{}", port);
    let mut client = RemoteClient::connect(client_addr.clone()).await.unwrap();

    client
        .create_db("test_disconnect_db", CompressionType::Uncompressed)
        .await
        .unwrap();

    // Create table
    {
        let mut stream = client
            .execute_query(
                "test_disconnect_db",
                dtdb_api::sql_query!("CREATE TABLE Users (id int PRIMARY KEY, name varchar(255));"),
            )
            .await
            .unwrap();
        while let Some(res) = stream.next().await {
            res.unwrap();
        }
    }

    let mut raw_client = DuctTapeDbServiceClient::connect(client_addr).await.unwrap();

    // Start transaction and execute insert, then drop the channel
    {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let mut response = raw_client.transaction(stream).await.unwrap().into_inner();

        tx.send(TransactionRequest {
            command: Some(Command::Start(StartTransaction {
                db_name: "test_disconnect_db".to_string(),
                isolation_level: None,
            })),
        })
        .await
        .unwrap();
        let resp = response.next().await.unwrap().unwrap();
        assert!(matches!(resp.payload, Some(TxPayload::QueryFinished(true))));

        tx.send(TransactionRequest {
            command: Some(Command::Execute(ExecuteTxQuery {
                sql_query: "INSERT INTO Users (id, name) VALUES (999, 'Abandoned');".to_string(),
                parameters: Vec::new(),
            })),
        })
        .await
        .unwrap();

        let resp_query = response.next().await.unwrap().unwrap();
        assert!(matches!(
            resp_query.payload,
            Some(TxPayload::QueryResult(_))
        ));
        let resp_fin = response.next().await.unwrap().unwrap();
        assert!(matches!(
            resp_fin.payload,
            Some(TxPayload::QueryFinished(true))
        ));

        // Drop tx sender here to simulate connection termination / client disconnect
        drop(tx);

        // Wait a short moment for server task to receive disconnect and rollback
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Now query the table to confirm that (999, 'Abandoned') was NOT committed
    {
        let mut stream = client
            .execute_query(
                "test_disconnect_db",
                dtdb_api::sql_query!("SELECT id, name FROM Users WHERE id = 999;"),
            )
            .await
            .unwrap();
        let mut rows = Vec::new();
        while let Some(res) = stream.next().await {
            let resp = res.unwrap();
            if let Some(EqPayload::Row(row)) = resp.payload {
                rows.push(row.values);
            }
        }
        assert_eq!(rows.len(), 0);
    }

    server_handle.abort();
}

#[tokio::test]
async fn test_grpc_ddl_transaction_rejection() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    let (port, server_handle) = start_test_server(&data_path).await;

    let client_addr = format!("http://127.0.0.1:{}", port);
    let mut client = RemoteClient::connect(client_addr).await.unwrap();

    client
        .create_db("test_db", CompressionType::Uncompressed)
        .await
        .unwrap();

    // 1. Remote transaction rejection of DDL
    let tx_res = client
        .run_in_transaction("test_db", |tx| async move {
            let _ = tx
                .execute_query(dtdb_api::sql_query!(
                    "CREATE TABLE Dummy (id int PRIMARY KEY);"
                ))
                .await?;
            Ok(())
        })
        .await;

    assert!(tx_res.is_err());
    let err_msg = tx_res.unwrap_err().message().to_string();
    assert!(err_msg.contains("DDL statements (CREATE TABLE, DROP TABLE) are not supported inside explicit multi-statement transactions."));

    // 2. In-process transaction rejection of DDL
    let ip_client = dtdb_api::in_process::InProcessClient::open(&data_path).unwrap();
    ip_client
        .create_db("test_db_ip", CompressionType::Uncompressed)
        .unwrap();

    let ip_tx_res = ip_client.run_in_transaction("test_db_ip", |tx| {
        let _ = tx.execute_query(dtdb_api::sql_query!(
            "CREATE TABLE Dummy (id int PRIMARY KEY);"
        ))?;
        Ok(())
    });

    assert!(ip_tx_res.is_err());
    let ip_err_msg = ip_tx_res.unwrap_err().message().to_string();
    assert!(ip_err_msg.contains("DDL statements (CREATE TABLE, DROP TABLE) are not supported inside explicit multi-statement transactions."));

    // 3. Confirm DDL still works fine outside explicit transaction (auto-committed)
    {
        let mut stream = client
            .execute_query(
                "test_db",
                dtdb_api::sql_query!("CREATE TABLE Users (id int PRIMARY KEY, name varchar(255));"),
            )
            .await
            .unwrap();
        while let Some(res) = stream.next().await {
            res.unwrap();
        }
    }

    server_handle.abort();
}

#[tokio::test]
async fn test_grpc_transaction_with_isolation_options() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    let (port, server_handle) = start_test_server(&data_path).await;

    let client_addr = format!("http://127.0.0.1:{}", port);
    let mut client = RemoteClient::connect(client_addr).await.unwrap();

    client
        .create_db("test_db", CompressionType::Uncompressed)
        .await
        .unwrap();

    // Create table
    {
        let mut stream = client
            .execute_query(
                "test_db",
                dtdb_api::sql_query!("CREATE TABLE Users (id int PRIMARY KEY, name varchar(255));"),
            )
            .await
            .unwrap();
        while let Some(res) = stream.next().await {
            res.unwrap();
        }
    }

    // Run transaction under ReadCommitted isolation
    let options = dtdb_api::client::TransactionOptions {
        isolation_level: dtdb_api::client::IsolationLevel::ReadCommitted,
    };
    let tx_res = client
        .run_in_transaction_with_options("test_db", options, |tx| async move {
            tx.execute_query(dtdb_api::sql_query!(
                "INSERT INTO Users (id, name) VALUES (100, 'Eve');"
            ))
            .await?;
            Ok("read_committed_done")
        })
        .await;

    assert_eq!(tx_res.unwrap(), "read_committed_done");

    // Verify row exists
    let mut stream = client
        .execute_query(
            "test_db",
            dtdb_api::sql_query!("SELECT id, name FROM Users WHERE id = 100;"),
        )
        .await
        .unwrap();

    let mut rows = Vec::new();
    while let Some(res) = stream.next().await {
        let resp = res.unwrap();
        if let Some(EqPayload::Row(row)) = resp.payload {
            rows.push(row.values);
        }
    }

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec!["100", "Eve"]);

    server_handle.abort();
}
