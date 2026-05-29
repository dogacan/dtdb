use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use futures_core::Stream;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use dtdb_relational::{Database, DatabaseOptions, Transaction};
use dtdb_sql::SqlEngine;
use dtdb_storage::{CompressionType, DbValue};

use crate::proto::duct_tape_db_service_server::DuctTapeDbService;
use crate::proto::{
    CommitResult, CompressionOption, CreateDbRequest, CreateDbResponse, DropDbRequest,
    DropDbResponse, ExecuteQueryRequest, ExecuteQueryResponse, FlushDbRequest, FlushDbResponse,
    Header, Row, TransactionRequest, TransactionResponse,
};

pub struct DuctTapeDbServiceImpl {
    data_dir: PathBuf,
    databases: Arc<RwLock<HashMap<String, DbState>>>,
    next_tx_id: Arc<AtomicU64>,
    spawner: Arc<dyn dtdb_storage::ThreadSpawner>,
}

struct DbState {
    database: Arc<Database>,
    sql_engine: Arc<SqlEngine>,
}

impl DuctTapeDbServiceImpl {
    pub fn new(data_dir: impl AsRef<Path>) -> Result<Self, String> {
        Self::new_with_spawner(data_dir, Arc::new(dtdb_storage::DefaultSpawner))
    }

    pub fn new_with_spawner(
        data_dir: impl AsRef<Path>,
        spawner: Arc<dyn dtdb_storage::ThreadSpawner>,
    ) -> Result<Self, String> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir).map_err(|e| e.to_string())?;

        let databases = Arc::new(RwLock::new(HashMap::new()));
        let service = Self {
            data_dir: data_dir.clone(),
            databases,
            next_tx_id: Arc::new(AtomicU64::new(1)),
            spawner,
        };

        // Scan and restore databases
        service.restore_databases()?;

        Ok(service)
    }

    fn restore_databases(&self) -> Result<(), String> {
        let mut dbs = self.databases.write().unwrap();
        for entry in fs::read_dir(&self.data_dir).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
            let path = entry.path();
            if path.is_dir()
                && let Some(db_name) = path.file_name().and_then(|s| s.to_str())
            {
                // Check if db_options.bin exists
                if path.join("db_options.bin").exists() {
                    tracing::info!(db = %db_name, "restoring database");
                    let database = Arc::new(
                        Database::open_with_spawner(&path, self.spawner.clone())
                            .map_err(|e| e.to_string())?,
                    );
                    let sql_engine = Arc::new(SqlEngine::new(database.clone()));

                    // Spawn periodic flush if configured
                    if let Some(ms) = database.options.flush_interval_ms {
                        Self::spawn_periodic_flush(database.clone(), ms);
                    }

                    dbs.insert(
                        db_name.to_string(),
                        DbState {
                            database,
                            sql_engine,
                        },
                    );
                }
            }
        }
        Ok(())
    }

    fn spawn_periodic_flush(database: Arc<Database>, interval_ms: u64) {
        let db_weak = Arc::downgrade(&database);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
            // First tick fires immediately, so skip it to wait for the interval
            interval.tick().await;

            loop {
                interval.tick().await;
                if let Some(db) = db_weak.upgrade() {
                    let tables = db.list_tables();
                    for table_name in tables {
                        if let Ok(table) = db.get_table(&table_name)
                            && let Err(e) = table.flush_memtable()
                        {
                            tracing::error!(
                                table = %table_name,
                                error = %e,
                                "periodic flush failed"
                            );
                        }
                    }
                } else {
                    break; // Database was dropped, exit loop
                }
            }
        });
    }

    /// Returns the database and SQL engine for the given database name, if it exists.
    pub fn get_db_and_engine(&self, db_name: &str) -> Option<(Arc<Database>, Arc<SqlEngine>)> {
        let dbs = self.databases.read().unwrap();
        dbs.get(db_name)
            .map(|state| (state.database.clone(), state.sql_engine.clone()))
    }

    /// Allocates the next unique transaction ID.
    pub fn next_tx_id(&self) -> u64 {
        self.next_tx_id.fetch_add(1, Ordering::SeqCst)
    }
}

/// Helper: converts an `ExecutionResult` into a sequence of `ExecuteQueryResponse` messages.
pub(crate) fn execution_result_to_responses(
    result: dtdb_sql::ExecutionResult,
) -> Vec<ExecuteQueryResponse> {
    match result {
        dtdb_sql::ExecutionResult::Analyze => {
            vec![ExecuteQueryResponse {
                payload: Some(crate::proto::execute_query_response::Payload::InfoMessage(
                    "Table analyzed successfully.".to_string(),
                )),
            }]
        }
        dtdb_sql::ExecutionResult::CreateTable => {
            vec![ExecuteQueryResponse {
                payload: Some(crate::proto::execute_query_response::Payload::InfoMessage(
                    "Table created successfully.".to_string(),
                )),
            }]
        }
        dtdb_sql::ExecutionResult::DropTable => {
            vec![ExecuteQueryResponse {
                payload: Some(crate::proto::execute_query_response::Payload::InfoMessage(
                    "Table dropped successfully.".to_string(),
                )),
            }]
        }
        dtdb_sql::ExecutionResult::CreateIndex => {
            vec![ExecuteQueryResponse {
                payload: Some(crate::proto::execute_query_response::Payload::InfoMessage(
                    "Index created successfully.".to_string(),
                )),
            }]
        }
        dtdb_sql::ExecutionResult::DropIndex => {
            vec![ExecuteQueryResponse {
                payload: Some(crate::proto::execute_query_response::Payload::InfoMessage(
                    "Index dropped successfully.".to_string(),
                )),
            }]
        }
        dtdb_sql::ExecutionResult::Insert { count } => {
            vec![ExecuteQueryResponse {
                payload: Some(crate::proto::execute_query_response::Payload::InfoMessage(
                    format!("Inserted {} row(s).", count),
                )),
            }]
        }
        dtdb_sql::ExecutionResult::Delete { count } => {
            vec![ExecuteQueryResponse {
                payload: Some(crate::proto::execute_query_response::Payload::InfoMessage(
                    format!("Deleted {} row(s).", count),
                )),
            }]
        }
        dtdb_sql::ExecutionResult::Update { count } => {
            vec![ExecuteQueryResponse {
                payload: Some(crate::proto::execute_query_response::Payload::InfoMessage(
                    format!("Updated {} row(s).", count),
                )),
            }]
        }
        dtdb_sql::ExecutionResult::Select { schema, rows } => {
            let mut responses = Vec::new();

            // Header
            let column_names = schema.columns.iter().map(|col| col.name.clone()).collect();
            responses.push(ExecuteQueryResponse {
                payload: Some(crate::proto::execute_query_response::Payload::Header(
                    Header { column_names },
                )),
            });

            // Rows
            for row in rows {
                let values = row
                    .values
                    .iter()
                    .map(|val| match val {
                        DbValue::Int(v) => v.to_string(),
                        DbValue::Float(v) => v.to_string(),
                        DbValue::String(s) => s.clone(),
                        DbValue::Bytes(b) => format!("{:?}", b),
                        DbValue::Bool(b) => b.to_string(),
                        DbValue::Null => "NULL".to_string(),
                    })
                    .collect();

                responses.push(ExecuteQueryResponse {
                    payload: Some(crate::proto::execute_query_response::Payload::Row(Row {
                        values,
                    })),
                });
            }

            responses
        }
    }
}

#[tonic::async_trait]
impl DuctTapeDbService for DuctTapeDbServiceImpl {
    type ExecuteQueryStream =
        Pin<Box<dyn Stream<Item = Result<ExecuteQueryResponse, Status>> + Send + 'static>>;
    type TransactionStream =
        Pin<Box<dyn Stream<Item = Result<TransactionResponse, Status>> + Send + 'static>>;

    async fn create_db(
        &self,
        request: Request<CreateDbRequest>,
    ) -> Result<Response<CreateDbResponse>, Status> {
        let req = request.into_inner();
        let db_name = req.db_name.trim();

        if db_name.is_empty() {
            return Err(Status::invalid_argument("Database name cannot be empty"));
        }

        let mut dbs = self.databases.write().unwrap();
        if dbs.contains_key(db_name) {
            return Ok(Response::new(CreateDbResponse {
                success: false,
                message: format!("Database '{}' already exists", db_name),
            }));
        }

        let db_path = self.data_dir.join(db_name);
        let compression = match req.compression() {
            CompressionOption::CompressionLz4 => CompressionType::Lz4,
            CompressionOption::CompressionUncompressed => CompressionType::Uncompressed,
        };

        // Construct DatabaseOptions from gRPC request fields with sensible defaults
        let options = DatabaseOptions {
            compression,
            memtable_size_limit: req.memtable_size_limit.unwrap_or(1024 * 1024) as usize,
            block_size_limit: req.block_size_limit.unwrap_or(4096) as usize,
            wal_size_limit: req.wal_size_limit.unwrap_or(32 * 1024 * 1024) as usize,
            flush_interval_ms: req.flush_interval_ms,
            l0_compaction_threshold: None,
            sstable_target_size: None,
            base_level_size_limit: None,
            level_size_multiplier: None,
            max_level: None,
            block_cache_capacity: Some(1000),
            analyze_frequency_ms: req.analyze_frequency_ms,
            wal_sync_interval_ms: None,
            memory_budget: None,
        };

        let database = Arc::new(
            Database::open_with_options_and_spawner(
                &db_path,
                options.clone(),
                self.spawner.clone(),
            )
            .map_err(|e| Status::internal(format!("Failed to create database: {}", e)))?,
        );
        let sql_engine = Arc::new(SqlEngine::new(database.clone()));

        // Spawn periodic flush task if flush_interval_ms is configured
        if let Some(ms) = options.flush_interval_ms {
            Self::spawn_periodic_flush(database.clone(), ms);
        }

        dbs.insert(
            db_name.to_string(),
            DbState {
                database,
                sql_engine,
            },
        );

        Ok(Response::new(CreateDbResponse {
            success: true,
            message: format!("Database '{}' created with options: {:?}", db_name, options),
        }))
    }

    async fn drop_db(
        &self,
        request: Request<DropDbRequest>,
    ) -> Result<Response<DropDbResponse>, Status> {
        let req = request.into_inner();
        let db_name = req.db_name.trim();

        if db_name.is_empty() {
            return Err(Status::invalid_argument("Database name cannot be empty"));
        }

        let state = {
            let mut dbs = self.databases.write().unwrap();
            if let Some(true) = dbs
                .get(db_name)
                .map(|s| s.database.has_active_transactions())
            {
                return Ok(Response::new(DropDbResponse {
                    success: false,
                    message: format!(
                        "Cannot drop database '{}' because it has active transactions",
                        db_name
                    ),
                }));
            }
            dbs.remove(db_name)
        };

        if let Some(db_state) = state {
            // Drop database references to release file locks
            drop(db_state);

            // Clean up directory from disk
            let db_path = self.data_dir.join(db_name);
            if db_path.exists()
                && let Err(e) = fs::remove_dir_all(&db_path)
            {
                return Ok(Response::new(DropDbResponse {
                    success: false,
                    message: format!(
                        "Database removed from catalog but failed to delete files: {}",
                        e
                    ),
                }));
            }

            Ok(Response::new(DropDbResponse {
                success: true,
                message: format!("Database '{}' dropped successfully", db_name),
            }))
        } else {
            Ok(Response::new(DropDbResponse {
                success: false,
                message: format!("Database '{}' not found", db_name),
            }))
        }
    }

    async fn flush_db(
        &self,
        request: Request<FlushDbRequest>,
    ) -> Result<Response<FlushDbResponse>, Status> {
        let req = request.into_inner();
        let db_name = req.db_name.trim();

        if db_name.is_empty() {
            return Err(Status::invalid_argument("Database name cannot be empty"));
        }

        let database = {
            let dbs = self.databases.read().unwrap();
            if let Some(state) = dbs.get(db_name) {
                state.database.clone()
            } else {
                return Err(Status::not_found(format!(
                    "Database '{}' not found",
                    db_name
                )));
            }
        };

        let flushed_tables = tokio::task::spawn_blocking(move || {
            let tables = database.list_tables();
            let mut flushed_tables = Vec::new();
            for table_name in tables {
                if let Ok(table) = database.get_table(&table_name) {
                    if let Err(e) = table.flush_memtable() {
                        return Err(format!("Failed to flush table {}: {}", table_name, e));
                    }
                    flushed_tables.push(table_name);
                }
            }
            Ok(flushed_tables)
        })
        .await
        .map_err(|e| Status::internal(format!("Worker panicked: {}", e)))?
        .map_err(Status::internal)?;

        Ok(Response::new(FlushDbResponse {
            success: true,
            message: format!(
                "Flushed {} table(s) in database '{}': {:?}",
                flushed_tables.len(),
                db_name,
                flushed_tables
            ),
        }))
    }

    async fn execute_query(
        &self,
        request: Request<ExecuteQueryRequest>,
    ) -> Result<Response<Self::ExecuteQueryStream>, Status> {
        let req = request.into_inner();
        let db_name = req.db_name.trim();
        let sql_query = req.sql_query.trim();
        let params = proto_params_to_db_params(req.parameters)?;

        // 1. Get db state
        let (database, sql_engine) = {
            let dbs = self.databases.read().unwrap();
            if let Some(state) = dbs.get(db_name) {
                (state.database.clone(), state.sql_engine.clone())
            } else {
                return Err(Status::not_found(format!(
                    "Database '{}' not found",
                    db_name
                )));
            }
        };

        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        let tx = Transaction::new(tx_id, database.clone());

        // Create channel for streaming
        let (tx_chan, rx_chan) = mpsc::channel(128);

        // Execute query off the async worker pool — the storage engine is
        // synchronous and may block on cache-miss SSTable reads, fsyncs, and
        // compactions; running it inline would head-of-line block every
        // other in-flight RPC sharing this Tokio worker thread.
        let sql_query_owned = sql_query.to_string();
        let exec_result = tokio::task::spawn_blocking(move || {
            let exec = sql_engine.execute_with_params(&sql_query_owned, &tx, &params);
            match exec {
                Ok(result) => match tx.commit() {
                    Ok(()) => Ok(result),
                    Err(e) => Err((true, e.to_string())),
                },
                Err(e) => {
                    let _ = tx.rollback();
                    Err((false, e))
                }
            }
        })
        .await
        .map_err(|e| Status::internal(format!("Worker panicked: {}", e)))?;

        match exec_result {
            Ok(result) => {
                let responses = execution_result_to_responses(result);
                tokio::spawn(async move {
                    for resp in responses {
                        if tx_chan.send(Ok(resp)).await.is_err() {
                            return;
                        }
                    }
                });
            }
            Err((commit_failure, e)) => {
                if commit_failure {
                    return Err(Status::aborted(format!("Transaction commit failed: {}", e)));
                } else {
                    return Err(Status::invalid_argument(format!("SQL Error: {}", e)));
                }
            }
        }

        let stream = ReceiverStream::new(rx_chan);
        Ok(Response::new(Box::pin(stream) as Self::ExecuteQueryStream))
    }

    /// Bidirectional streaming RPC for multi-statement transactions.
    ///
    /// This implements a state machine with the following valid transitions:
    ///
    ///   [Idle] --Start--> [Active] --Execute--> [Active]
    ///                       |                      |
    ///                       +--Commit/Rollback--> [Done]
    ///
    /// Invalid transitions (e.g. double Start, Execute before Start) return
    /// error messages on the response stream. If the client disconnects at
    /// any point before committing, the transaction is automatically rolled back.
    async fn transaction(
        &self,
        request: Request<tonic::Streaming<TransactionRequest>>,
    ) -> Result<Response<Self::TransactionStream>, Status> {
        let mut in_stream = request.into_inner();
        let (tx_chan, rx_chan) = mpsc::channel::<Result<TransactionResponse, Status>>(128);

        // Clone Arc-wrapped fields so they can be moved into the spawned task.
        let databases = self.databases.clone();
        let next_tx_id = self.next_tx_id.clone();

        tokio::spawn(async move {
            // Transaction state machine.
            // - `None` means we haven't received StartTransaction yet (Idle state).
            // - `Some(...)` means the transaction is active.
            let mut tx_state: Option<(Transaction, Arc<SqlEngine>)> = None;
            let mut finished = false; // True after Commit or Rollback (Done state).

            while let Some(msg_result) = in_stream.next().await {
                let msg = match msg_result {
                    Ok(m) => m,
                    Err(e) => {
                        // Client stream error — roll back if active.
                        if let Some((tx, _)) = tx_state.take() {
                            let _ = tokio::task::spawn_blocking(move || tx.rollback()).await;
                        }
                        let _ = tx_chan
                            .send(Ok(TransactionResponse {
                                payload: Some(
                                    crate::proto::transaction_response::Payload::ErrorMessage(
                                        format!("Stream error: {}", e),
                                    ),
                                ),
                            }))
                            .await;
                        return;
                    }
                };

                let command = match msg.command {
                    Some(c) => c,
                    None => {
                        let _ = tx_chan
                            .send(Ok(TransactionResponse {
                                payload: Some(
                                    crate::proto::transaction_response::Payload::ErrorMessage(
                                        "Empty transaction request (no command specified)"
                                            .to_string(),
                                    ),
                                ),
                            }))
                            .await;
                        continue;
                    }
                };

                // Guard: reject all commands after commit/rollback.
                if finished {
                    let _ = tx_chan
                        .send(Ok(TransactionResponse {
                            payload: Some(
                                crate::proto::transaction_response::Payload::ErrorMessage(
                                    "Transaction already completed (committed or rolled back). \
                             No further commands are accepted."
                                        .to_string(),
                                ),
                            ),
                        }))
                        .await;
                    continue;
                }

                match command {
                    // --- StartTransaction ---
                    crate::proto::transaction_request::Command::Start(start) => {
                        // Guard: reject duplicate StartTransaction.
                        if tx_state.is_some() {
                            let _ = tx_chan
                                .send(Ok(TransactionResponse {
                                    payload: Some(
                                        crate::proto::transaction_response::Payload::ErrorMessage(
                                            "Protocol error: StartTransaction already received. \
                                     Only one StartTransaction is allowed per stream."
                                                .to_string(),
                                        ),
                                    ),
                                }))
                                .await;
                            continue;
                        }

                        let db_name = start.db_name.trim();
                        let lookup = {
                            let dbs = databases.read().unwrap();
                            dbs.get(db_name)
                                .map(|state| (state.database.clone(), state.sql_engine.clone()))
                        };
                        let (database, sql_engine) = match lookup {
                            Some(pair) => pair,
                            None => {
                                let _ = tx_chan.send(Ok(TransactionResponse {
                                    payload: Some(crate::proto::transaction_response::Payload::ErrorMessage(
                                        format!("Database '{}' not found", db_name),
                                    )),
                                })).await;
                                return;
                            }
                        };

                        let proto_level = start
                            .isolation_level
                            .and_then(|v| crate::proto::IsolationLevel::try_from(v).ok())
                            .unwrap_or(crate::proto::IsolationLevel::SnapshotIsolation);

                        let isolation_level = match proto_level {
                            crate::proto::IsolationLevel::ReadUncommitted => {
                                dtdb_relational::IsolationLevel::ReadUncommitted
                            }
                            crate::proto::IsolationLevel::ReadCommitted => {
                                dtdb_relational::IsolationLevel::ReadCommitted
                            }
                            crate::proto::IsolationLevel::RepeatableRead => {
                                dtdb_relational::IsolationLevel::RepeatableRead
                            }
                            crate::proto::IsolationLevel::SnapshotIsolation => {
                                dtdb_relational::IsolationLevel::SnapshotIsolation
                            }
                        };

                        let tx_id = next_tx_id.fetch_add(1, Ordering::SeqCst);
                        let tx = Transaction::new_with_isolation(tx_id, database, isolation_level);
                        tx_state = Some((tx, sql_engine));

                        // Acknowledge the transaction start.
                        let _ = tx_chan
                            .send(Ok(TransactionResponse {
                                payload: Some(
                                    crate::proto::transaction_response::Payload::QueryFinished(
                                        true,
                                    ),
                                ),
                            }))
                            .await;
                    }

                    // --- ExecuteTxQuery ---
                    crate::proto::transaction_request::Command::Execute(exec) => {
                        // Guard: reject Execute before StartTransaction.
                        if tx_state.is_none() {
                            let _ = tx_chan.send(Ok(TransactionResponse {
                                payload: Some(crate::proto::transaction_response::Payload::ErrorMessage(
                                    "Protocol error: must send StartTransaction before executing queries.".to_string(),
                                )),
                            })).await;
                            continue;
                        }
                        let sql_engine_ref = &tx_state.as_ref().expect("checked").1;

                        let sql_query = exec.sql_query.trim();
                        if sql_engine_ref.is_ddl(sql_query) {
                            let _ = tx_chan.send(Ok(TransactionResponse {
                                payload: Some(crate::proto::transaction_response::Payload::ErrorMessage(
                                    "DDL statements (CREATE TABLE, DROP TABLE) are not supported inside explicit multi-statement transactions.".to_string(),
                                )),
                            })).await;
                            let _ = tx_chan
                                .send(Ok(TransactionResponse {
                                    payload: Some(
                                        crate::proto::transaction_response::Payload::QueryFinished(
                                            true,
                                        ),
                                    ),
                                }))
                                .await;
                            continue;
                        }
                        let params = match proto_params_to_db_params(exec.parameters) {
                            Ok(p) => p,
                            Err(e) => {
                                let _ = tx_chan.send(Ok(TransactionResponse {
                                    payload: Some(crate::proto::transaction_response::Payload::ErrorMessage(
                                        format!("Failed to parse query parameters: {}", e),
                                    )),
                                })).await;
                                let _ = tx_chan.send(Ok(TransactionResponse {
                                    payload: Some(crate::proto::transaction_response::Payload::QueryFinished(true)),
                                })).await;
                                continue;
                            }
                        };
                        // Take ownership of tx and engine so we can move them
                        // into spawn_blocking; restore tx_state after.
                        let (tx, sql_engine) = tx_state.take().expect("checked");
                        let sql_query_owned = sql_query.to_string();
                        let blocking_result = tokio::task::spawn_blocking(move || {
                            let r = sql_engine.execute_with_params(&sql_query_owned, &tx, &params);
                            (tx, sql_engine, r)
                        })
                        .await;
                        let (tx, sql_engine, exec_result) = match blocking_result {
                            Ok(triple) => triple,
                            Err(join_err) => {
                                let _ = tx_chan.send(Ok(TransactionResponse {
                                    payload: Some(crate::proto::transaction_response::Payload::ErrorMessage(
                                        format!("Worker panicked: {}", join_err),
                                    )),
                                })).await;
                                return;
                            }
                        };
                        tx_state = Some((tx, sql_engine));
                        match exec_result {
                            Ok(result) => {
                                let responses = execution_result_to_responses(result);
                                for resp in responses {
                                    let tx_resp = TransactionResponse {
                                        payload: Some(crate::proto::transaction_response::Payload::QueryResult(resp)),
                                    };
                                    if tx_chan.send(Ok(tx_resp)).await.is_err() {
                                        return;
                                    }
                                }
                                // Send query_finished marker so the client knows this query's
                                // results are complete and it can send the next command.
                                let _ = tx_chan.send(Ok(TransactionResponse {
                                    payload: Some(crate::proto::transaction_response::Payload::QueryFinished(true)),
                                })).await;
                            }
                            Err(e) => {
                                // SQL error — report the error but keep the transaction alive.
                                // The client can choose to retry, send more queries, or rollback.
                                let _ = tx_chan.send(Ok(TransactionResponse {
                                    payload: Some(crate::proto::transaction_response::Payload::ErrorMessage(
                                        format!("SQL Error: {}", e),
                                    )),
                                })).await;
                                // Still send query_finished so the client can proceed.
                                let _ = tx_chan.send(Ok(TransactionResponse {
                                    payload: Some(crate::proto::transaction_response::Payload::QueryFinished(true)),
                                })).await;
                            }
                        }
                    }

                    // --- CommitTransaction ---
                    crate::proto::transaction_request::Command::Commit(_) => {
                        // Guard: reject Commit before StartTransaction.
                        let (tx, _) = match tx_state.take() {
                            Some(s) => s,
                            None => {
                                let _ = tx_chan.send(Ok(TransactionResponse {
                                    payload: Some(crate::proto::transaction_response::Payload::ErrorMessage(
                                        "Protocol error: must send StartTransaction before committing.".to_string(),
                                    )),
                                })).await;
                                continue;
                            }
                        };

                        let commit_outcome = tokio::task::spawn_blocking(move || {
                            tx.commit().map_err(|e| e.to_string())
                        })
                        .await
                        .unwrap_or_else(|e| Err(format!("Worker panicked: {}", e)));
                        let commit_result = match commit_outcome {
                            Ok(()) => CommitResult {
                                success: true,
                                message: "Transaction committed successfully.".to_string(),
                            },
                            Err(e) => CommitResult {
                                success: false,
                                message: format!("Transaction commit failed: {}", e),
                            },
                        };

                        let _ = tx_chan
                            .send(Ok(TransactionResponse {
                                payload: Some(
                                    crate::proto::transaction_response::Payload::CommitResult(
                                        commit_result,
                                    ),
                                ),
                            }))
                            .await;
                        finished = true;
                    }

                    // --- RollbackTransaction ---
                    crate::proto::transaction_request::Command::Rollback(_) => {
                        // Guard: reject Rollback before StartTransaction.
                        let (tx, _) = match tx_state.take() {
                            Some(s) => s,
                            None => {
                                let _ = tx_chan.send(Ok(TransactionResponse {
                                    payload: Some(crate::proto::transaction_response::Payload::ErrorMessage(
                                        "Protocol error: must send StartTransaction before rolling back.".to_string(),
                                    )),
                                })).await;
                                continue;
                            }
                        };

                        let _ = tokio::task::spawn_blocking(move || tx.rollback()).await;
                        let _ = tx_chan
                            .send(Ok(TransactionResponse {
                                payload: Some(
                                    crate::proto::transaction_response::Payload::CommitResult(
                                        CommitResult {
                                            success: true,
                                            message: "Transaction rolled back.".to_string(),
                                        },
                                    ),
                                ),
                            }))
                            .await;
                        finished = true;
                    }
                }
            }

            // Client stream ended without an explicit Commit or Rollback.
            // Automatically roll back the transaction to prevent dangling state.
            if let Some((tx, _)) = tx_state.take() {
                let _ = tx.rollback();
            }
        });

        let stream = ReceiverStream::new(rx_chan);
        Ok(Response::new(Box::pin(stream) as Self::TransactionStream))
    }
}

#[allow(clippy::result_large_err)]
fn proto_params_to_db_params(
    proto_params: Vec<crate::proto::QueryParam>,
) -> Result<HashMap<String, DbValue>, Status> {
    let mut params = HashMap::new();
    for p in proto_params {
        let name = p.name;
        let value = match p.value {
            Some(pv) => match pv.val {
                Some(crate::proto::param_value::Val::IntVal(i)) => DbValue::Int(i),
                Some(crate::proto::param_value::Val::FloatVal(f)) => DbValue::Float(f),
                Some(crate::proto::param_value::Val::BoolVal(b)) => DbValue::Bool(b),
                Some(crate::proto::param_value::Val::StringVal(s)) => DbValue::String(s),
                Some(crate::proto::param_value::Val::BytesVal(b)) => DbValue::Bytes(b),
                Some(crate::proto::param_value::Val::NullVal(_)) => DbValue::Null,
                None => DbValue::Null,
            },
            None => DbValue::Null,
        };
        params.insert(name, value);
    }
    Ok(params)
}
