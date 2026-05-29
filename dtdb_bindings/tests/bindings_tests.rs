use dtdb_bindings::{ffi, new_in_process_client, new_remote_client};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use dtdb_api::proto::duct_tape_db_service_server::DuctTapeDbServiceServer;
use dtdb_api::server::DuctTapeDbServiceImpl;

#[test]
fn test_param_conversions_and_hex_decoding() {
    let tmp = TempDir::new().unwrap();
    let client = new_in_process_client(tmp.path().to_str().unwrap()).unwrap();

    client.create_db("db").unwrap();

    // 1. Create a table
    let create_q = ffi::CxxSqlQuery {
        text: "CREATE TABLE t (id INT PRIMARY KEY, val_null STRING, val_int INT, val_float FLOAT, val_bool BOOLEAN, val_text STRING);".to_string(),
        params: Vec::new(),
    };
    let res = client.execute_query("db", create_q).unwrap();
    assert!(res.success);
    assert_eq!(res.headers, vec!["Info".to_string()]);
    assert!(res.rows[0].contains("Table created"));

    // 2. Insert with parameters covering Null, Int, Float, Bool, Text
    let insert_q = ffi::CxxSqlQuery {
        text: "INSERT INTO t (id, val_null, val_int, val_float, val_bool, val_text) VALUES (:id, :v_null, :v_int, :v_float, :v_bool, :v_text);".to_string(),
        params: vec![
            ffi::QueryParam {
                name: "id".to_string(),
                value: "1".to_string(),
                kind: ffi::ParamKind::Int,
            },
            ffi::QueryParam {
                name: "v_null".to_string(),
                value: "".to_string(),
                kind: ffi::ParamKind::Null,
            },
            ffi::QueryParam {
                name: "v_int".to_string(),
                value: "42".to_string(),
                kind: ffi::ParamKind::Int,
            },
            ffi::QueryParam {
                name: "v_float".to_string(),
                value: "3.14".to_string(),
                kind: ffi::ParamKind::Float,
            },
            ffi::QueryParam {
                name: "v_bool".to_string(),
                value: "true".to_string(),
                kind: ffi::ParamKind::Bool,
            },
            ffi::QueryParam {
                name: "v_text".to_string(),
                value: "hello".to_string(),
                kind: ffi::ParamKind::Text,
            },
        ],
    };
    let res = client.execute_query("db", insert_q).unwrap();
    assert!(res.success);

    // 3. Query back and verify values are correctly stored
    let select_q = ffi::CxxSqlQuery {
        text: "SELECT id, val_null, val_int, val_float, val_bool, val_text FROM t WHERE id = 1;"
            .to_string(),
        params: Vec::new(),
    };
    let res = client.execute_query("db", select_q).unwrap();
    assert!(res.success);
    assert_eq!(
        res.headers,
        vec![
            "id".to_string(),
            "val_null".to_string(),
            "val_int".to_string(),
            "val_float".to_string(),
            "val_bool".to_string(),
            "val_text".to_string()
        ]
    );
    assert_eq!(
        res.rows,
        vec![
            "1".to_string(),
            "NULL".to_string(),
            "42".to_string(),
            "3.14".to_string(),
            "true".to_string(),
            "hello".to_string()
        ]
    );
}

#[test]
fn test_bytes_param_error_path() {
    let tmp = TempDir::new().unwrap();
    let client = new_in_process_client(tmp.path().to_str().unwrap()).unwrap();

    client.create_db("db").unwrap();
    client
        .execute_query(
            "db",
            ffi::CxxSqlQuery {
                text: "CREATE TABLE t (id INT PRIMARY KEY, val STRING);".to_string(),
                params: Vec::new(),
            },
        )
        .unwrap();

    // Binding a valid hex bytes param. It should be decoded to Bytes and then converted to
    // HexStringLiteral in SQL, which returns a query compilation/execution error.
    let res = client.execute_query(
        "db",
        ffi::CxxSqlQuery {
            text: "INSERT INTO t (id, val) VALUES (1, :v);".to_string(),
            params: vec![ffi::QueryParam {
                name: "v".to_string(),
                value: "414243".to_string(),
                kind: ffi::ParamKind::Bytes,
            }],
        },
    );
    assert!(res.is_err());
    let err_msg = match res {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert!(err_msg.contains("Unsupported SQL value type") || err_msg.contains("HexStringLiteral"));
}

#[test]
fn test_param_conversions_fallback() {
    let tmp = TempDir::new().unwrap();
    let client = new_in_process_client(tmp.path().to_str().unwrap()).unwrap();

    client.create_db("db").unwrap();
    client
        .execute_query(
            "db",
            ffi::CxxSqlQuery {
                text: "CREATE TABLE t (id INT PRIMARY KEY, val STRING);".to_string(),
                params: Vec::new(),
            },
        )
        .unwrap();

    // Test invalid Int fallback
    let res = client
        .execute_query(
            "db",
            ffi::CxxSqlQuery {
                text: "INSERT INTO t (id, val) VALUES (1, :v);".to_string(),
                params: vec![ffi::QueryParam {
                    name: "v".to_string(),
                    value: "abc".to_string(),
                    kind: ffi::ParamKind::Int,
                }],
            },
        )
        .unwrap();
    assert!(res.success);

    // Test invalid Float fallback
    let res = client
        .execute_query(
            "db",
            ffi::CxxSqlQuery {
                text: "INSERT INTO t (id, val) VALUES (2, :v);".to_string(),
                params: vec![ffi::QueryParam {
                    name: "v".to_string(),
                    value: "abc".to_string(),
                    kind: ffi::ParamKind::Float,
                }],
            },
        )
        .unwrap();
    assert!(res.success);

    // Test odd-length Bytes fallback
    let res = client
        .execute_query(
            "db",
            ffi::CxxSqlQuery {
                text: "INSERT INTO t (id, val) VALUES (3, :v);".to_string(),
                params: vec![ffi::QueryParam {
                    name: "v".to_string(),
                    value: "123".to_string(),
                    kind: ffi::ParamKind::Bytes,
                }],
            },
        )
        .unwrap();
    assert!(res.success);

    // Test non-hex Bytes fallback
    let res = client
        .execute_query(
            "db",
            ffi::CxxSqlQuery {
                text: "INSERT INTO t (id, val) VALUES (4, :v);".to_string(),
                params: vec![ffi::QueryParam {
                    name: "v".to_string(),
                    value: "zz".to_string(),
                    kind: ffi::ParamKind::Bytes,
                }],
            },
        )
        .unwrap();
    assert!(res.success);

    // Verify all fallback string values were inserted correctly
    let res = client
        .execute_query(
            "db",
            ffi::CxxSqlQuery {
                text: "SELECT id, val FROM t ORDER BY id;".to_string(),
                params: Vec::new(),
            },
        )
        .unwrap();
    assert_eq!(
        res.rows,
        vec![
            "1".to_string(),
            "abc".to_string(),
            "2".to_string(),
            "abc".to_string(),
            "3".to_string(),
            "123".to_string(),
            "4".to_string(),
            "zz".to_string(),
        ]
    );
}

#[test]
fn test_database_operations_and_query_errors() {
    let tmp = TempDir::new().unwrap();
    let client = new_in_process_client(tmp.path().to_str().unwrap()).unwrap();

    // 1. Create DB and verify it is active
    client.create_db("mydb").unwrap();

    // 2. Drop DB
    client.drop_db("mydb").unwrap();

    // 3. Executing a query on dropped DB should fail
    let q = ffi::CxxSqlQuery {
        text: "SELECT 1;".to_string(),
        params: Vec::new(),
    };
    let err_res = client.execute_query("mydb", q);
    assert!(err_res.is_err());

    // 4. Executing invalid SQL syntax should fail
    client.create_db("mydb2").unwrap();
    let bad_q = ffi::CxxSqlQuery {
        text: "INVALID SQL STATEMENT;".to_string(),
        params: Vec::new(),
    };
    let err_res2 = client.execute_query("mydb2", bad_q);
    assert!(err_res2.is_err());
}

#[test]
fn test_transaction_errors_and_drop() {
    let tmp = TempDir::new().unwrap();
    let client = new_in_process_client(tmp.path().to_str().unwrap()).unwrap();

    // 1. Transaction on non-existent database should succeed to start but fail on query execution
    let tx_err = client.start_transaction("non_existent").unwrap();
    let res = client.execute_tx_query(
        &tx_err,
        ffi::CxxSqlQuery {
            text: "SELECT 1;".to_string(),
            params: Vec::new(),
        },
    );
    assert!(res.is_err());

    // 2. Valid transaction but query error
    client.create_db("mydb").unwrap();
    client
        .execute_query(
            "mydb",
            ffi::CxxSqlQuery {
                text: "CREATE TABLE t (id INT PRIMARY KEY);".to_string(),
                params: Vec::new(),
            },
        )
        .unwrap();

    let tx = client.start_transaction("mydb").unwrap();
    // Type error in transaction query
    let tx_err_res = client.execute_tx_query(
        &tx,
        ffi::CxxSqlQuery {
            text: "INSERT INTO t (id) VALUES ('not_an_int');".to_string(),
            params: Vec::new(),
        },
    );
    assert!(tx_err_res.is_err());

    // Clean up transaction
    client.rollback_tx(tx).unwrap();

    // 3. Drop transaction implicitly without calling commit/rollback
    let tx2 = client.start_transaction("mydb").unwrap();
    drop(tx2);
    // Let background task detect drop
    std::thread::sleep(std::time::Duration::from_millis(20));
}

#[test]
fn test_remote_client_integration() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    // Create a temporary runtime just to run the server
    let rt = tokio::runtime::Runtime::new().unwrap();

    // 1. Start Server on random port
    let listener = rt
        .block_on(async { TcpListener::bind("127.0.0.1:0").await })
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port();

    let service = DuctTapeDbServiceImpl::new(&data_path).unwrap();

    let server_handle = rt.spawn(async move {
        Server::builder()
            .add_service(DuctTapeDbServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Let the server spin up
    std::thread::sleep(std::time::Duration::from_millis(50));

    // 2. Connect via remote client
    let server_address = format!("http://127.0.0.1:{}", port);
    let client = new_remote_client(&server_address).unwrap();

    // 3. Test database operations over gRPC
    client.create_db("remote_db").unwrap();

    // Create table
    let create_res = client
        .execute_query(
            "remote_db",
            ffi::CxxSqlQuery {
                text: "CREATE TABLE remote_t (id INT PRIMARY KEY, name STRING);".to_string(),
                params: Vec::new(),
            },
        )
        .unwrap();
    assert!(create_res.success);

    // Insert
    let insert_res = client
        .execute_query(
            "remote_db",
            ffi::CxxSqlQuery {
                text: "INSERT INTO remote_t (id, name) VALUES (1, 'Alice');".to_string(),
                params: Vec::new(),
            },
        )
        .unwrap();
    assert!(insert_res.success);

    // Start Transaction
    let tx = client.start_transaction("remote_db").unwrap();

    // Execute query in transaction
    let tx_res = client
        .execute_tx_query(
            &tx,
            ffi::CxxSqlQuery {
                text: "INSERT INTO remote_t (id, name) VALUES (2, 'Bob');".to_string(),
                params: Vec::new(),
            },
        )
        .unwrap();
    assert!(tx_res.success);

    // Rollback
    client.rollback_tx(tx).unwrap();

    // Verify Bob is not there, but Alice is
    let select_res = client
        .execute_query(
            "remote_db",
            ffi::CxxSqlQuery {
                text: "SELECT id, name FROM remote_t ORDER BY id;".to_string(),
                params: Vec::new(),
            },
        )
        .unwrap();
    assert_eq!(select_res.rows, vec!["1".to_string(), "Alice".to_string()]);

    // Start another transaction and commit
    let tx2 = client.start_transaction("remote_db").unwrap();
    client
        .execute_tx_query(
            &tx2,
            ffi::CxxSqlQuery {
                text: "INSERT INTO remote_t (id, name) VALUES (3, 'Charlie');".to_string(),
                params: Vec::new(),
            },
        )
        .unwrap();
    client.commit_tx(tx2).unwrap();

    // Verify Charlie is there
    let select_res2 = client
        .execute_query(
            "remote_db",
            ffi::CxxSqlQuery {
                text: "SELECT id, name FROM remote_t ORDER BY id;".to_string(),
                params: Vec::new(),
            },
        )
        .unwrap();
    assert_eq!(
        select_res2.rows,
        vec![
            "1".to_string(),
            "Alice".to_string(),
            "3".to_string(),
            "Charlie".to_string()
        ]
    );

    // Drop database
    client.drop_db("remote_db").unwrap();

    // Clean up server task
    server_handle.abort();
    drop(rt);
}

#[test]
fn test_remote_client_connection_failure() {
    // Port 1 is reserved and nothing is listening on it
    let res = new_remote_client("http://127.0.0.1:1");
    assert!(res.is_err());
}
