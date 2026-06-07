#![allow(clippy::boxed_local, clippy::result_large_err)]

use dtdb_api::proto::execute_query_response::Payload;
use futures_util::StreamExt;
use std::sync::Arc;
use parking_lot::Mutex;
use tokio::runtime::Runtime;
use tonic::Status;

/// Returns the process-wide Tokio runtime shared by every `CxxClient`.
///
/// Spawning one multi-threaded runtime per client wastes a full scheduler
/// (worker threads + IO driver) per handle, so all clients share one.
fn global_runtime() -> Result<Arc<Runtime>, String> {
    static RUNTIME: Mutex<Option<Arc<Runtime>>> = Mutex::new(None);
    let mut guard = RUNTIME.lock();
    if let Some(rt) = &*guard {
        return Ok(rt.clone());
    }
    let rt = Arc::new(Runtime::new().map_err(|e| e.to_string())?);
    *guard = Some(rt.clone());
    Ok(rt)
}

#[cxx::bridge]
pub mod ffi {
    /// Explicit type tag for a bound parameter.
    ///
    /// The C++ side sets this from the static type at the `bind` call site so
    /// the Rust side never has to guess a value's type from its textual form
    /// (which loses the distinction between e.g. the string "42" and the
    /// integer 42, and made `Bool` unreachable).
    #[repr(u8)]
    enum ParamKind {
        Int,
        Float,
        Bool,
        Text,
        Bytes,
        Null,
    }

    struct QueryParam {
        name: String,
        // Textual carrier for the value. For `Bytes` this is lowercase hex
        // with no delimiters; for `Null` it is ignored.
        value: String,
        kind: ParamKind,
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

fn convert_cxx_param(p: &ffi::QueryParam) -> dtdb_storage::DbValue {
    use dtdb_storage::DbValue;
    match p.kind {
        ffi::ParamKind::Null => DbValue::Null,
        ffi::ParamKind::Int => match p.value.parse::<i64>() {
            Ok(i) => DbValue::Int(i),
            Err(_) => DbValue::string(p.value.clone()),
        },
        ffi::ParamKind::Float => match p.value.parse::<f64>() {
            Ok(f) => DbValue::Float(f),
            Err(_) => DbValue::string(p.value.clone()),
        },
        ffi::ParamKind::Bool => DbValue::Bool(matches!(p.value.as_str(), "1" | "true" | "TRUE")),
        ffi::ParamKind::Bytes => match decode_hex(&p.value) {
            Some(bytes) => DbValue::bytes(bytes),
            None => DbValue::string(p.value.clone()),
        },
        ffi::ParamKind::Text => DbValue::string(p.value.clone()),
        _ => DbValue::string(p.value.clone()),
    }
}

fn convert_cxx_query(cxx_query: ffi::CxxSqlQuery) -> dtdb_api::SqlQuery {
    let mut query = dtdb_api::SqlQuery::new(cxx_query.text);
    for p in cxx_query.params {
        let val = convert_cxx_param(&p);
        query = query.bind(p.name, val);
    }
    query
}

pub enum ClientEnum {
    InProcess(dtdb_api::in_process::InProcessClient),
    Remote(dtdb_api::remote::RemoteClient),
}

pub struct CxxClient {
    runtime: Arc<Runtime>,
    client: ClientEnum,
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

fn handle_in_process_query_result(
    res: dtdb_api::in_process::InProcessQueryResult,
) -> ffi::QueryResult {
    let mut headers = Vec::new();
    if let Some(schema) = res.schema() {
        headers = schema.columns.iter().map(|col| col.name.clone()).collect();
    }

    let mut rows = Vec::new();
    if let Some(msg) = res.info_message() {
        headers = vec!["Info".to_string()];
        rows.push(msg);
    } else {
        for row_res in res {
            match row_res {
                Ok(row) => {
                    let row_vals = row
                        .values
                        .iter()
                        .map(|val| match val {
                            dtdb_storage::DbValue::Int(v) => v.to_string(),
                            dtdb_storage::DbValue::Float(v) => v.to_string(),
                            dtdb_storage::DbValue::String(s) => s.to_string(),
                            dtdb_storage::DbValue::Bytes(b) => format!("{:?}", b),
                            dtdb_storage::DbValue::Bool(b) => b.to_string(),
                            dtdb_storage::DbValue::Null => "NULL".to_string(),
                            dtdb_storage::DbValue::Date(d) => d.to_string(),
                            dtdb_storage::DbValue::Time(t) => t.to_string(),
                            dtdb_storage::DbValue::Timestamp(ts) => ts.to_string(),
                            dtdb_storage::DbValue::Decimal(dec) => dec.to_string(),
                        })
                        .collect::<Vec<_>>();
                    rows.extend(row_vals);
                }
                Err(e) => {
                    return ffi::QueryResult {
                        success: false,
                        error_message: e.message().to_string(),
                        headers: Vec::new(),
                        rows: Vec::new(),
                    };
                }
            }
        }
    }

    ffi::QueryResult {
        success: true,
        error_message: String::new(),
        headers,
        rows,
    }
}

// --- Factory implementations ---

pub fn new_in_process_client(data_dir: &str) -> Result<Box<CxxClient>, String> {
    let rt = global_runtime()?;
    let client =
        dtdb_api::in_process::InProcessClient::open(data_dir).map_err(|e| e.to_string())?;
    Ok(Box::new(CxxClient {
        runtime: rt,
        client: ClientEnum::InProcess(client),
    }))
}

pub fn new_remote_client(server_address: &str) -> Result<Box<CxxClient>, String> {
    let rt = global_runtime()?;
    let client = rt
        .block_on(async {
            dtdb_api::remote::RemoteClient::connect(server_address.to_string()).await
        })
        .map_err(|e| e.to_string())?;

    Ok(Box::new(CxxClient {
        runtime: rt,
        client: ClientEnum::Remote(client),
    }))
}

// --- Client operations implementation ---

impl CxxClient {
    pub fn create_db(&self, db_name: &str) -> Result<(), String> {
        match &self.client {
            ClientEnum::InProcess(client) => {
                client
                    .create_db(db_name, dtdb_storage::CompressionType::Lz4)
                    .map_err(|e| e.message().to_string())?;
                Ok(())
            }
            ClientEnum::Remote(client) => {
                let mut client = client.clone();
                self.runtime.block_on(async {
                    client
                        .create_db(db_name, dtdb_storage::CompressionType::Lz4)
                        .await
                        .map_err(|e| e.message().to_string())
                })?;
                Ok(())
            }
        }
    }

    pub fn drop_db(&self, db_name: &str) -> Result<(), String> {
        match &self.client {
            ClientEnum::InProcess(client) => {
                client
                    .drop_db(db_name)
                    .map_err(|e| e.message().to_string())?;
                Ok(())
            }
            ClientEnum::Remote(client) => {
                let mut client = client.clone();
                self.runtime.block_on(async {
                    client
                        .drop_db(db_name)
                        .await
                        .map_err(|e| e.message().to_string())
                })?;
                Ok(())
            }
        }
    }

    pub fn execute_query(
        &self,
        db_name: &str,
        query: ffi::CxxSqlQuery,
    ) -> Result<ffi::QueryResult, String> {
        let rust_query = convert_cxx_query(query);
        match &self.client {
            ClientEnum::InProcess(client) => {
                let result = client
                    .execute_query(db_name, rust_query)
                    .map_err(|e| e.message().to_string())?;
                Ok(handle_in_process_query_result(result))
            }
            ClientEnum::Remote(client) => {
                let mut client = client.clone();
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
        }
    }

    // --- Stateful Transaction setup ---

    pub fn start_transaction(&self, db_name: &str) -> Result<Box<CxxTransaction>, String> {
        let (req_tx, mut req_rx) = tokio::sync::mpsc::channel::<TxRequest>(32);
        let db_name = db_name.to_string();

        type TerminalSender =
            Mutex<Option<tokio::sync::oneshot::Sender<Result<(), String>>>>;
        let terminal_sender: Arc<TerminalSender> = Arc::new(Mutex::new(None));
        let terminal_sender_clone = terminal_sender.clone();

        match &self.client {
            ClientEnum::InProcess(client) => {
                let client = client.clone();
                self.runtime.spawn_blocking(move || {
                    let res = client.run_in_transaction(&db_name, |tx_client| {
                        while let Some(req) = req_rx.blocking_recv() {
                            match req {
                                TxRequest::Execute { query, resp_tx } => {
                                    let result = tx_client.execute_query(query);
                                    let mapped = match result {
                                        Ok(res) => Ok(handle_in_process_query_result(res)),
                                        Err(status) => Err(status.message().to_string()),
                                    };
                                    let _ = resp_tx.send(mapped);
                                }
                                TxRequest::Commit { resp_tx } => {
                                    *terminal_sender_clone.lock() = Some(resp_tx);
                                    return Ok(());
                                }
                                TxRequest::Rollback { resp_tx } => {
                                    *terminal_sender_clone.lock() = Some(resp_tx);
                                    return Err(Status::aborted("Rollback requested"));
                                }
                            }
                        }
                        Err(Status::aborted("Transaction handle dropped"))
                    });

                    let mut guard = terminal_sender.lock();
                    if let Some(resp_tx) = guard.take() {
                        let mapped = match res {
                            Ok(_) => Ok(()),
                            Err(status) => Err(status.message().to_string()),
                        };
                        let _ = resp_tx.send(mapped);
                    }
                });
            }
            ClientEnum::Remote(client) => {
                let mut client = client.clone();
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
                                        *terminal_sender_clone.lock() = Some(resp_tx);
                                        return Ok(());
                                    }
                                    TxRequest::Rollback { resp_tx } => {
                                        *terminal_sender_clone.lock() = Some(resp_tx);
                                        return Err(tonic::Status::aborted("Rollback requested"));
                                    }
                                }
                            }
                            Err(tonic::Status::aborted("Transaction handle dropped"))
                        })
                        .await;

                    let mut guard = terminal_sender.lock();
                    if let Some(resp_tx) = guard.take() {
                        let mapped = match res {
                            Ok(_) => Ok(()),
                            Err(status) => Err(status.message().to_string()),
                        };
                        let _ = resp_tx.send(mapped);
                    }
                });
            }
        }

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
