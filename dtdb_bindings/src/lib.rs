#![allow(clippy::boxed_local)]

use dtdb_api::client::DuctTapeDbClient;
use dtdb_api::proto::execute_query_response::Payload;
use futures_util::StreamExt;
use std::sync::Arc;
use tokio::runtime::Runtime;

#[cxx::bridge]
pub mod ffi {
    // Flattened QueryResult to pass query data cleanly across FFI.
    // Memory layout is optimized using 1D vector rows where:
    // row_count = rows.len() / headers.len()
    struct QueryResult {
        success: bool,
        error_message: String,
        headers: Vec<String>,
        rows: Vec<String>,
    }

    extern "Rust" {
        type CxxClient;
        type CxxTransaction;

        // Factories
        fn new_in_process_client(data_dir: &str) -> Result<Box<CxxClient>>;
        fn new_remote_client(server_address: &str) -> Result<Box<CxxClient>>;

        // Database operations
        fn create_db(self: &CxxClient, db_name: &str) -> Result<()>;
        fn drop_db(self: &CxxClient, db_name: &str) -> Result<()>;
        fn execute_query(self: &CxxClient, db_name: &str, sql: &str) -> Result<QueryResult>;

        // Multi-statement Transaction support
        fn start_transaction(self: &CxxClient, db_name: &str) -> Result<Box<CxxTransaction>>;
        fn execute_tx_query(
            self: &CxxClient,
            tx: &CxxTransaction,
            sql: &str,
        ) -> Result<QueryResult>;
        fn commit_tx(self: &CxxClient, tx: Box<CxxTransaction>) -> Result<()>;
        fn rollback_tx(self: &CxxClient, tx: Box<CxxTransaction>) -> Result<()>;
    }
}

pub struct CxxClient {
    runtime: Arc<Runtime>,
    client: DuctTapeDbClient,
}

enum TxRequest {
    Execute {
        sql: String,
        resp_tx: tokio::sync::oneshot::Sender<Result<ffi::QueryResult, String>>,
    },
    Commit {
        resp_tx: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    Rollback,
}

pub struct CxxTransaction {
    req_tx: tokio::sync::mpsc::Sender<TxRequest>,
}

// --- Factory implementations ---

pub fn new_in_process_client(data_dir: &str) -> Result<Box<CxxClient>, String> {
    let rt = Runtime::new().map_err(|e| e.to_string())?;
    let client = DuctTapeDbClient::in_process(data_dir).map_err(|e| e.to_string())?;
    Ok(Box::new(CxxClient {
        runtime: Arc::new(rt),
        client,
    }))
}

pub fn new_remote_client(server_address: &str) -> Result<Box<CxxClient>, String> {
    let rt = Runtime::new().map_err(|e| e.to_string())?;
    let client = rt
        .block_on(async { DuctTapeDbClient::connect(server_address.to_string()).await })
        .map_err(|e| e.to_string())?;

    Ok(Box::new(CxxClient {
        runtime: Arc::new(rt),
        client,
    }))
}

// --- Client operations implementation ---

impl CxxClient {
    pub fn create_db(&self, db_name: &str) -> Result<(), String> {
        let mut client = self.client.clone();
        self.runtime.block_on(async {
            client
                .create_db(db_name, dtdb_storage::CompressionType::Lz4)
                .await
                .map_err(|e| e.message().to_string())
        })?;
        Ok(())
    }

    pub fn drop_db(&self, db_name: &str) -> Result<(), String> {
        let mut client = self.client.clone();
        self.runtime.block_on(async {
            client
                .drop_db(db_name)
                .await
                .map_err(|e| e.message().to_string())
        })?;
        Ok(())
    }

    pub fn execute_query(&self, db_name: &str, sql: &str) -> Result<ffi::QueryResult, String> {
        let mut client = self.client.clone();
        self.runtime.block_on(async {
            let mut stream = client
                .execute_query(db_name, sql)
                .await
                .map_err(|e| e.message().to_string())?;

            let mut headers = Vec::new();
            let mut rows = Vec::new();

            while let Some(resp_result) = stream.next().await {
                let resp = resp_result.map_err(|e| e.message().to_string())?;
                match resp.payload {
                    Some(Payload::Header(h)) => {
                        headers = h.column_names;
                    }
                    Some(Payload::Row(r)) => {
                        rows.extend(r.values);
                    }
                    Some(Payload::InfoMessage(msg)) => {
                        if headers.is_empty() {
                            headers = vec!["Info".to_string()];
                        }
                        rows.push(msg);
                    }
                    None => {}
                }
            }

            Ok(ffi::QueryResult {
                success: true,
                error_message: String::new(),
                headers,
                rows,
            })
        })
    }

    // --- Stateful Transaction setup ---

    pub fn start_transaction(&self, db_name: &str) -> Result<Box<CxxTransaction>, String> {
        let mut client = self.client.clone();
        let (req_tx, mut req_rx) = tokio::sync::mpsc::channel::<TxRequest>(1);
        let db_name = db_name.to_string();

        let commit_sender = Arc::new(std::sync::Mutex::new(None));
        let commit_sender_clone = commit_sender.clone();

        // Spawn a transaction execution loop inside DuctTapeDbClient's closure-based transaction runner
        self.runtime.spawn(async move {
            let res = client
                .run_in_transaction(&db_name, |tx_client| async move {
                    while let Some(req) = req_rx.recv().await {
                        match req {
                            TxRequest::Execute { sql, resp_tx } => {
                                let responses_result = tx_client.execute_query(&sql).await;
                                let mapped = match responses_result {
                                    Ok(responses) => {
                                        let mut headers = Vec::new();
                                        let mut rows = Vec::new();
                                        for resp in responses {
                                            match resp.payload {
                                                Some(Payload::Header(h)) => {
                                                    headers = h.column_names;
                                                }
                                                Some(Payload::Row(r)) => {
                                                    rows.extend(r.values);
                                                }
                                                Some(Payload::InfoMessage(msg)) => {
                                                    if headers.is_empty() {
                                                        headers = vec!["Info".to_string()];
                                                    }
                                                    rows.push(msg);
                                                }
                                                None => {}
                                            }
                                        }
                                        Ok(ffi::QueryResult {
                                            success: true,
                                            error_message: String::new(),
                                            headers,
                                            rows,
                                        })
                                    }
                                    Err(status) => Err(status.message().to_string()),
                                };
                                let _ = resp_tx.send(mapped);
                            }
                            TxRequest::Commit { resp_tx } => {
                                *commit_sender_clone.lock().unwrap() = Some(resp_tx);
                                return Ok(()); // Returns Ok to commit
                            }
                            TxRequest::Rollback => {
                                return Err(tonic::Status::aborted("Rollback requested"));
                            }
                        }
                    }
                    Err(tonic::Status::aborted("Transaction handle dropped"))
                })
                .await;

            // Wait for the commit to complete and return the final commit result
            let mut guard = commit_sender.lock().unwrap();
            if let Some(resp_tx) = guard.take() {
                match res {
                    Ok(_) => {
                        let _ = resp_tx.send(Ok(()));
                    }
                    Err(status) => {
                        let _ = resp_tx.send(Err(status.message().to_string()));
                    }
                }
            }
        });

        Ok(Box::new(CxxTransaction { req_tx }))
    }

    pub fn execute_tx_query(
        &self,
        tx: &CxxTransaction,
        sql: &str,
    ) -> Result<ffi::QueryResult, String> {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        tx.req_tx
            .try_send(TxRequest::Execute {
                sql: sql.to_string(),
                resp_tx,
            })
            .map_err(|e| e.to_string())?;

        self.runtime.block_on(async {
            resp_rx
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())
        })
    }

    pub fn commit_tx(&self, tx: Box<CxxTransaction>) -> Result<(), String> {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        let _ = tx.req_tx.try_send(TxRequest::Commit { resp_tx });
        self.runtime.block_on(async {
            resp_rx
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())
        })
    }

    pub fn rollback_tx(&self, tx: Box<CxxTransaction>) -> Result<(), String> {
        let _ = tx.req_tx.try_send(TxRequest::Rollback);
        Ok(())
    }
}
