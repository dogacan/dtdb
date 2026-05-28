#![allow(clippy::boxed_local)]

use dtdb_api::client::DuctTapeDbClient;
use dtdb_api::proto::execute_query_response::Payload;
use futures_util::StreamExt;
use std::sync::Arc;
use tokio::runtime::Runtime;

#[cxx::bridge]
pub mod ffi {
    struct QueryParam {
        name: String,
        value: String,
    }

    struct CxxSqlQuery {
        text: String,
        params: Vec<QueryParam>,
    }

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
        fn execute_query(
            self: &CxxClient,
            db_name: &str,
            query: CxxSqlQuery,
        ) -> Result<QueryResult>;

        // Multi-statement Transaction support
        fn start_transaction(self: &CxxClient, db_name: &str) -> Result<Box<CxxTransaction>>;
        fn execute_tx_query(
            self: &CxxClient,
            tx: &CxxTransaction,
            query: CxxSqlQuery,
        ) -> Result<QueryResult>;
        fn commit_tx(self: &CxxClient, tx: Box<CxxTransaction>) -> Result<()>;
        fn rollback_tx(self: &CxxClient, tx: Box<CxxTransaction>) -> Result<()>;
    }
}

fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let mut bytes = Vec::with_capacity(s.len() / 2);
    for chunk in s.as_bytes().chunks_exact(2) {
        let high = char::from(chunk[0]).to_digit(16)? as u8;
        let low = char::from(chunk[1]).to_digit(16)? as u8;
        bytes.push((high << 4) | low);
    }
    Some(bytes)
}

fn parse_cxx_value(val: &str) -> dtdb_storage::DbValue {
    if val.eq_ignore_ascii_case("null") {
        return dtdb_storage::DbValue::Null;
    }
    if let Ok(i) = val.parse::<i64>() {
        return dtdb_storage::DbValue::Int(i);
    }
    if let Ok(f) = val.parse::<f64>() {
        return dtdb_storage::DbValue::Float(f);
    }
    if (val.starts_with("x'") || val.starts_with("X'")) && val.ends_with('\'') && val.len() >= 3 {
        let hex_str = &val[2..val.len() - 1];
        if let Some(bytes) = decode_hex(hex_str) {
            return dtdb_storage::DbValue::Bytes(bytes);
        }
    }
    dtdb_storage::DbValue::String(val.to_string())
}

fn convert_cxx_query(cxx_query: ffi::CxxSqlQuery) -> dtdb_api::SqlQuery {
    let mut query = dtdb_api::SqlQuery::new(cxx_query.text);
    for p in cxx_query.params {
        let val = parse_cxx_value(&p.value);
        query = query.bind(p.name, val);
    }
    query
}

pub struct CxxClient {
    runtime: Arc<Runtime>,
    client: DuctTapeDbClient,
}

enum TxRequest {
    Execute {
        query: dtdb_api::SqlQuery,
        resp_tx: tokio::sync::oneshot::Sender<Result<ffi::QueryResult, String>>,
    },
    Commit {
        resp_tx: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
    Rollback {
        resp_tx: tokio::sync::oneshot::Sender<Result<(), String>>,
    },
}

pub struct CxxTransaction {
    req_tx: tokio::sync::mpsc::Sender<TxRequest>,
}

// --- Factory implementations ---

pub fn new_in_process_client(data_dir: &str) -> Result<Box<CxxClient>, String> {
    let rt = Runtime::new().map_err(|e| e.to_string())?;
    let client = {
        let _guard = rt.enter();
        DuctTapeDbClient::in_process(data_dir).map_err(|e| e.to_string())?
    };
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

    pub fn execute_query(
        &self,
        db_name: &str,
        query: ffi::CxxSqlQuery,
    ) -> Result<ffi::QueryResult, String> {
        let mut client = self.client.clone();
        let rust_query = convert_cxx_query(query);
        self.runtime.block_on(async {
            let mut stream = client
                .execute_query(db_name, rust_query)
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
        let (req_tx, mut req_rx) = tokio::sync::mpsc::channel::<TxRequest>(32);
        let db_name = db_name.to_string();

        // Holds the responder for whichever terminal request (Commit or
        // Rollback) was sent. The spawned task fills this in before exiting
        // the inner closure so it can signal the caller after
        // run_in_transaction completes.
        type TerminalSender =
            std::sync::Mutex<Option<tokio::sync::oneshot::Sender<Result<(), String>>>>;
        let terminal_sender: Arc<TerminalSender> = Arc::new(std::sync::Mutex::new(None));
        let terminal_sender_clone = terminal_sender.clone();

        // Spawn a transaction execution loop inside DuctTapeDbClient's closure-based transaction runner
        self.runtime.spawn(async move {
            let res = client
                .run_in_transaction(&db_name, |tx_client| async move {
                    while let Some(req) = req_rx.recv().await {
                        match req {
                            TxRequest::Execute { query, resp_tx } => {
                                let responses_result = tx_client.execute_query(query).await;
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
                                *terminal_sender_clone.lock().unwrap() = Some(resp_tx);
                                return Ok(()); // Returns Ok to commit
                            }
                            TxRequest::Rollback { resp_tx } => {
                                *terminal_sender_clone.lock().unwrap() = Some(resp_tx);
                                return Err(tonic::Status::aborted("Rollback requested"));
                            }
                        }
                    }
                    Err(tonic::Status::aborted("Transaction handle dropped"))
                })
                .await;

            // Notify whichever caller (commit_tx or rollback_tx) is waiting
            // for the terminal outcome of the transaction.
            let mut guard = terminal_sender.lock().unwrap();
            if let Some(resp_tx) = guard.take() {
                let mapped = match res {
                    Ok(_) => Ok(()),
                    Err(status) => Err(status.message().to_string()),
                };
                let _ = resp_tx.send(mapped);
            }
        });

        Ok(Box::new(CxxTransaction { req_tx }))
    }

    pub fn execute_tx_query(
        &self,
        tx: &CxxTransaction,
        query: ffi::CxxSqlQuery,
    ) -> Result<ffi::QueryResult, String> {
        let rust_query = convert_cxx_query(query);
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        self.runtime.block_on(async {
            tx.req_tx
                .send(TxRequest::Execute {
                    query: rust_query,
                    resp_tx,
                })
                .await
                .map_err(|_| {
                    "transaction worker is no longer running (likely panicked or aborted)"
                        .to_string()
                })?;
            resp_rx
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())
        })
    }

    pub fn commit_tx(&self, tx: Box<CxxTransaction>) -> Result<(), String> {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        self.runtime.block_on(async {
            tx.req_tx
                .send(TxRequest::Commit { resp_tx })
                .await
                .map_err(|_| {
                    "transaction worker is no longer running; commit not applied".to_string()
                })?;
            resp_rx
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())
        })
    }

    pub fn rollback_tx(&self, tx: Box<CxxTransaction>) -> Result<(), String> {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        self.runtime.block_on(async {
            tx.req_tx
                .send(TxRequest::Rollback { resp_tx })
                .await
                .map_err(|_| {
                    "transaction worker is no longer running; rollback not applied".to_string()
                })?;
            // The worker returns Err(Aborted) from run_in_transaction on
            // rollback, then forwards it to us. Treat an Aborted reply as
            // success since that is the expected outcome of a rollback.
            match resp_rx.await.map_err(|e| e.to_string())? {
                Ok(()) => Ok(()),
                Err(msg) if msg.contains("Rollback requested") => Ok(()),
                Err(msg) => Err(msg),
            }
        })
    }
}
