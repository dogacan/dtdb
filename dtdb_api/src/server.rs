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
use dtdb_storage::{CompressionType, DbValue, FsyncMethod};

use crate::proto::duct_tape_db_service_server::DuctTapeDbService;
use crate::proto::{
    CommitResult, CompressionOption, CreateDbRequest, CreateDbResponse, DropDbRequest,
    DropDbResponse, ExecuteQueryRequest, ExecuteQueryResponse, FlushDbRequest, FlushDbResponse,
    Header, Row, TransactionRequest, TransactionResponse,
};

pub(crate) struct DbState {
    pub(crate) database: Arc<Database>,
    pub(crate) sql_engine: Arc<SqlEngine>,
}

pub struct DatabaseCatalog {
    data_dir: PathBuf,
    databases: RwLock<HashMap<String, DbState>>,
    next_tx_id: AtomicU64,
    executor: Arc<dyn dtdb_storage::Executor>,
}

impl DatabaseCatalog {
    pub fn new(data_dir: impl AsRef<Path>) -> Result<Self, String> {
        Self::new_with_executor(data_dir, dtdb_storage::default_executor())
    }

    pub fn new_with_executor(
        data_dir: impl AsRef<Path>,
        executor: Arc<dyn dtdb_storage::Executor>,
    ) -> Result<Self, String> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir).map_err(|e| e.to_string())?;

        let databases = RwLock::new(HashMap::new());
        let catalog = Self {
            data_dir: data_dir.clone(),
            databases,
            next_tx_id: AtomicU64::new(1),
            executor,
        };

        // Scan and restore databases
        catalog.restore_databases()?;

        Ok(catalog)
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
                        Database::open_with_executor(&path, self.executor.clone())
                            .map_err(|e| e.to_string())?,
                    );
                    database.start_background_analyze_if_needed(&database);
                    database.start_background_flush_if_needed(&database);
                    let sql_engine = Arc::new(SqlEngine::new(database.clone()));

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

    pub fn get_db_and_engine(&self, db_name: &str) -> Option<(Arc<Database>, Arc<SqlEngine>)> {
        let dbs = self.databases.read().unwrap();
        dbs.get(db_name)
            .map(|state| (state.database.clone(), state.sql_engine.clone()))
    }

    pub fn next_tx_id(&self) -> u64 {
        self.next_tx_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn create_db(
        &self,
        name: &str,
        options: DatabaseOptions,
    ) -> Result<CreateDbResponse, Status> {
        let db_name = name.trim();
        if db_name.is_empty() {
            return Err(Status::invalid_argument("Database name cannot be empty"));
        }

        let mut dbs = self.databases.write().unwrap();
        if dbs.contains_key(db_name) {
            return Ok(CreateDbResponse {
                success: false,
                message: format!("Database '{}' already exists", db_name),
            });
        }

        let db_path = self.data_dir.join(db_name);
        let database = Arc::new(
            Database::open_with_options_and_executor(
                &db_path,
                options.clone(),
                self.executor.clone(),
            )
            .map_err(|e| Status::internal(format!("Failed to create database: {}", e)))?,
        );
        database.start_background_analyze_if_needed(&database);
        database.start_background_flush_if_needed(&database);
        let sql_engine = Arc::new(SqlEngine::new(database.clone()));

        dbs.insert(
            db_name.to_string(),
            DbState {
                database,
                sql_engine,
            },
        );

        Ok(CreateDbResponse {
            success: true,
            message: format!("Database '{}' created with options: {:?}", db_name, options),
        })
    }

    pub fn drop_db(&self, name: &str) -> Result<DropDbResponse, Status> {
        let db_name = name.trim();
        if db_name.is_empty() {
            return Err(Status::invalid_argument("Database name cannot be empty"));
        }

        let state = {
            let mut dbs = self.databases.write().unwrap();
            if let Some(true) = dbs
                .get(db_name)
                .map(|s| s.database.has_active_transactions())
            {
                return Ok(DropDbResponse {
                    success: false,
                    message: format!(
                        "Cannot drop database '{}' because it has active transactions",
                        db_name
                    ),
                });
            }
            dbs.remove(db_name)
        };

        if let Some(db_state) = state {
            drop(db_state);

            let db_path = self.data_dir.join(db_name);
            if db_path.exists()
                && let Err(e) = fs::remove_dir_all(&db_path)
            {
                return Ok(DropDbResponse {
                    success: false,
                    message: format!(
                        "Database removed from catalog but failed to delete files: {}",
                        e
                    ),
                });
            }

            Ok(DropDbResponse {
                success: true,
                message: format!("Database '{}' dropped successfully", db_name),
            })
        } else {
            Ok(DropDbResponse {
                success: false,
                message: format!("Database '{}' not found", db_name),
            })
        }
    }

    pub fn flush_db(&self, name: &str) -> Result<FlushDbResponse, Status> {
        let db_name = name.trim();
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

        let tables = database.list_tables();
        let mut flushed_tables = Vec::new();
        for table_name in tables {
            if let Ok(table) = database.get_table(&table_name) {
                if let Err(e) = table.flush_memtable() {
                    return Err(Status::internal(format!(
                        "Failed to flush table {}: {}",
                        table_name, e
                    )));
                }
                flushed_tables.push(table_name);
            }
        }

        Ok(FlushDbResponse {
            success: true,
            message: format!(
                "Flushed {} table(s) in database '{}': {:?}",
                flushed_tables.len(),
                db_name,
                flushed_tables
            ),
        })
    }
}

pub struct DuctTapeDbServiceImpl {
    pub(crate) catalog: Arc<DatabaseCatalog>,
}

impl DuctTapeDbServiceImpl {
    pub fn new(data_dir: impl AsRef<Path>) -> Result<Self, String> {
        let catalog = Arc::new(DatabaseCatalog::new(data_dir)?);
        Ok(Self { catalog })
    }

    pub fn new_with_executor(
        data_dir: impl AsRef<Path>,
        executor: Arc<dyn dtdb_storage::Executor>,
    ) -> Result<Self, String> {
        let catalog = Arc::new(DatabaseCatalog::new_with_executor(data_dir, executor)?);
        Ok(Self { catalog })
    }

    pub fn get_db_and_engine(&self, db_name: &str) -> Option<(Arc<Database>, Arc<SqlEngine>)> {
        self.catalog.get_db_and_engine(db_name)
    }

    pub fn next_tx_id(&self) -> u64 {
        self.catalog.next_tx_id()
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
        dtdb_sql::ExecutionResult::AlterTable => {
            vec![ExecuteQueryResponse {
                payload: Some(crate::proto::execute_query_response::Payload::InfoMessage(
                    "Table altered successfully.".to_string(),
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
                        DbValue::String(s) => s.to_string(),
                        DbValue::Bytes(b) => format!("{:?}", b),
                        DbValue::Bool(b) => b.to_string(),
                        DbValue::Null => "NULL".to_string(),
                        DbValue::Date(d) => d.to_string(),
                        DbValue::Time(t) => t.to_string(),
                        DbValue::Timestamp(ts) => ts.to_string(),
                        DbValue::Decimal(dec) => dec.to_string(),
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
            fsync_method: FsyncMethod::default(),
        };

        let resp = self.catalog.create_db(db_name, options)?;
        Ok(Response::new(resp))
    }

    async fn drop_db(
        &self,
        request: Request<DropDbRequest>,
    ) -> Result<Response<DropDbResponse>, Status> {
        let req = request.into_inner();
        let db_name = req.db_name.trim();
        let resp = self.catalog.drop_db(db_name)?;
        Ok(Response::new(resp))
    }

    async fn flush_db(
        &self,
        request: Request<FlushDbRequest>,
    ) -> Result<Response<FlushDbResponse>, Status> {
        let req = request.into_inner();
        let db_name = req.db_name.trim();

        let catalog = self.catalog.clone();
        let db_name_owned = db_name.to_string();
        let resp = tokio::task::spawn_blocking(move || catalog.flush_db(&db_name_owned))
            .await
            .map_err(|e| Status::internal(format!("Worker panicked: {}", e)))??;

        Ok(Response::new(resp))
    }

    async fn execute_query(
        &self,
        request: Request<ExecuteQueryRequest>,
    ) -> Result<Response<Self::ExecuteQueryStream>, Status> {
        let req = request.into_inner();
        let db_name = req.db_name.trim();
        let sql_query = req.sql_query.trim();
        let params = proto_params_to_db_params(req.parameters)?;

        let (database, sql_engine) = self
            .catalog
            .get_db_and_engine(db_name)
            .ok_or_else(|| Status::not_found(format!("Database '{}' not found", db_name)))?;

        let tx_id = self.catalog.next_tx_id();
        let tx = Transaction::new(tx_id, database.clone());

        // Create channel for streaming
        let (tx_chan, rx_chan) = mpsc::channel(128);

        // Execute query off the async worker pool — the storage engine is
        // synchronous and may block on cache-miss SSTable reads, fsyncs, and
        // compactions; running it inline would head-of-line block every
        // other in-flight RPC sharing this Tokio worker thread.
        let sql_query_owned = sql_query.to_string();
        let exec_result = tokio::task::spawn_blocking(move || {
            let exec = sql_engine.execute_streaming(&sql_query_owned, &tx, &params);
            match exec {
                Ok(dtdb_sql::ExecutionStreamingResult::Complete(result)) => match tx.commit() {
                    Ok(()) => Ok(crate::server::execution_result_to_responses(result)),
                    Err(e) => Err((true, e.to_string())),
                },
                Ok(dtdb_sql::ExecutionStreamingResult::Streaming {
                    schema,
                    mut operator,
                }) => {
                    let mut responses = Vec::new();
                    let column_names = schema.columns.iter().map(|col| col.name.clone()).collect();
                    responses.push(ExecuteQueryResponse {
                        payload: Some(crate::proto::execute_query_response::Payload::Header(
                            Header { column_names },
                        )),
                    });
                    loop {
                        match operator.next() {
                            Ok(Some(row)) => {
                                let values = row
                                    .values
                                    .iter()
                                    .map(|val| match val {
                                        DbValue::Int(v) => v.to_string(),
                                        DbValue::Float(v) => v.to_string(),
                                        DbValue::String(s) => s.to_string(),
                                        DbValue::Bytes(b) => format!("{:?}", b),
                                        DbValue::Bool(b) => b.to_string(),
                                        DbValue::Null => "NULL".to_string(),
                                        DbValue::Date(d) => d.to_string(),
                                        DbValue::Time(t) => t.to_string(),
                                        DbValue::Timestamp(ts) => ts.to_string(),
                                        DbValue::Decimal(dec) => dec.to_string(),
                                    })
                                    .collect();
                                responses.push(ExecuteQueryResponse {
                                    payload: Some(
                                        crate::proto::execute_query_response::Payload::Row(Row {
                                            values,
                                        }),
                                    ),
                                });
                            }
                            Ok(None) => break,
                            Err(e) => {
                                let _ = tx.rollback();
                                return Err((false, e));
                            }
                        }
                    }
                    match tx.commit() {
                        Ok(()) => Ok(responses),
                        Err(e) => Err((true, e.to_string())),
                    }
                }
                Err(e) => {
                    let _ = tx.rollback();
                    Err((false, e))
                }
            }
        })
        .await
        .map_err(|e| Status::internal(format!("Worker panicked: {}", e)))?;

        match exec_result {
            Ok(responses) => {
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
    async fn transaction(
        &self,
        request: Request<tonic::Streaming<TransactionRequest>>,
    ) -> Result<Response<Self::TransactionStream>, Status> {
        let mut in_stream = request.into_inner();
        let (tx_chan, rx_chan) = mpsc::channel::<Result<TransactionResponse, Status>>(128);

        // Clone catalog so it can be moved into the spawned task.
        let catalog = self.catalog.clone();

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
                        let lookup = catalog.get_db_and_engine(db_name);
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

                        let tx_id = catalog.next_tx_id();
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

                        let sql_query = exec.sql_query.trim();
                        if tx_state.as_ref().unwrap().1.is_ddl(sql_query) {
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
                            let r = sql_engine.execute_streaming(&sql_query_owned, &tx, &params);
                            match r {
                                Ok(dtdb_sql::ExecutionStreamingResult::Complete(result)) => {
                                    let responses = crate::server::execution_result_to_responses(result);
                                    (tx, sql_engine, Ok(responses))
                                }
                                Ok(dtdb_sql::ExecutionStreamingResult::Streaming { schema, mut operator }) => {
                                    let mut responses = Vec::new();
                                    let column_names = schema.columns.iter().map(|col| col.name.clone()).collect();
                                    responses.push(ExecuteQueryResponse {
                                        payload: Some(crate::proto::execute_query_response::Payload::Header(
                                            Header { column_names },
                                        )),
                                    });
                                    loop {
                                        match operator.next() {
                                            Ok(Some(row)) => {
                                                let values = row
                                                    .values
                                                    .iter()
                                                    .map(|val| match val {
                                                        DbValue::Int(v) => v.to_string(),
                                                        DbValue::Float(v) => v.to_string(),
                                                        DbValue::String(s) => s.to_string(),
                                                        DbValue::Bytes(b) => format!("{:?}", b),
                                                        DbValue::Bool(b) => b.to_string(),
                                                        DbValue::Null => "NULL".to_string(),
                                                        DbValue::Date(d) => d.to_string(),
                                                        DbValue::Time(t) => t.to_string(),
                                                        DbValue::Timestamp(ts) => ts.to_string(),
                                                        DbValue::Decimal(dec) => dec.to_string(),
                                                    })
                                                    .collect();
                                                responses.push(ExecuteQueryResponse {
                                                    payload: Some(crate::proto::execute_query_response::Payload::Row(Row {
                                                        values,
                                                    })),
                                                });
                                            }
                                            Ok(None) => break,
                                            Err(e) => {
                                                return (tx, sql_engine, Err(e));
                                            }
                                        }
                                    }
                                    (tx, sql_engine, Ok(responses))
                                }
                                Err(e) => (tx, sql_engine, Err(e)),
                            }
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
                            Ok(responses) => {
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
                Some(crate::proto::param_value::Val::StringVal(s)) => DbValue::string(s),
                Some(crate::proto::param_value::Val::BytesVal(b)) => DbValue::bytes(b),
                Some(crate::proto::param_value::Val::NullVal(_)) => DbValue::Null,
                None => DbValue::Null,
            },
            None => DbValue::Null,
        };
        params.insert(name, value);
    }
    Ok(params)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::duct_tape_db_service_client::DuctTapeDbServiceClient;
    use crate::proto::{
        CommitTransaction, CreateDbRequest, DropDbRequest, ExecuteQueryRequest, ExecuteTxQuery,
        FlushDbRequest, IsolationLevel, ParamValue, QueryParam, RollbackTransaction,
        StartTransaction, param_value::Val, transaction_request, transaction_response,
    };
    use dtdb_relational::DataType;
    use dtdb_storage::DbValue;
    use futures_util::StreamExt;
    use std::path::Path;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::Request;

    async fn start_test_server(data_path: &Path) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client_addr = format!("http://127.0.0.1:{}", addr.port());

        let service = DuctTapeDbServiceImpl::new(data_path).unwrap();

        let server_handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    crate::proto::duct_tape_db_service_server::DuctTapeDbServiceServer::new(
                        service,
                    ),
                )
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        (client_addr, server_handle)
    }

    #[tokio::test]
    async fn test_new_with_executor_and_catalog() {
        let temp_dir = tempfile::tempdir().unwrap();
        let executor = dtdb_storage::default_executor();
        let service = DuctTapeDbServiceImpl::new_with_executor(temp_dir.path(), executor).unwrap();
        assert_eq!(service.next_tx_id(), 1);
        assert_eq!(service.catalog.next_tx_id(), 2);
    }

    #[test]
    fn test_execution_result_to_responses_mapping() {
        use dtdb_relational::{Column, Schema};
        use dtdb_sql::ExecutionResult;

        let map_res = |res| execution_result_to_responses(res);

        // Analyze
        let r = map_res(ExecutionResult::Analyze);
        assert!(
            matches!(r[0].payload, Some(crate::proto::execute_query_response::Payload::InfoMessage(ref m)) if m.contains("analyzed"))
        );

        // CreateTable
        let r = map_res(ExecutionResult::CreateTable);
        assert!(
            matches!(r[0].payload, Some(crate::proto::execute_query_response::Payload::InfoMessage(ref m)) if m.contains("created"))
        );

        // DropTable
        let r = map_res(ExecutionResult::DropTable);
        assert!(
            matches!(r[0].payload, Some(crate::proto::execute_query_response::Payload::InfoMessage(ref m)) if m.contains("dropped"))
        );

        // CreateIndex
        let r = map_res(ExecutionResult::CreateIndex);
        assert!(
            matches!(r[0].payload, Some(crate::proto::execute_query_response::Payload::InfoMessage(ref m)) if m.contains("Index created"))
        );

        // DropIndex
        let r = map_res(ExecutionResult::DropIndex);
        assert!(
            matches!(r[0].payload, Some(crate::proto::execute_query_response::Payload::InfoMessage(ref m)) if m.contains("Index dropped"))
        );

        // AlterTable
        let r = map_res(ExecutionResult::AlterTable);
        assert!(
            matches!(r[0].payload, Some(crate::proto::execute_query_response::Payload::InfoMessage(ref m)) if m.contains("altered"))
        );

        // Insert
        let r = map_res(ExecutionResult::Insert { count: 3 });
        assert!(
            matches!(r[0].payload, Some(crate::proto::execute_query_response::Payload::InfoMessage(ref m)) if m.contains("Inserted 3"))
        );

        // Delete
        let r = map_res(ExecutionResult::Delete { count: 2 });
        assert!(
            matches!(r[0].payload, Some(crate::proto::execute_query_response::Payload::InfoMessage(ref m)) if m.contains("Deleted 2"))
        );

        // Update
        let r = map_res(ExecutionResult::Update { count: 5 });
        assert!(
            matches!(r[0].payload, Some(crate::proto::execute_query_response::Payload::InfoMessage(ref m)) if m.contains("Updated 5"))
        );

        // Select
        let schema = Schema::new(vec![Column {
            id: 1,
            name: "col1".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_nullable: false,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        }]);
        let rows = vec![
            dtdb_relational::Row::new(vec![DbValue::Int(10)]),
            dtdb_relational::Row::new(vec![DbValue::Float(1.5)]),
            dtdb_relational::Row::new(vec![DbValue::bytes(vec![0xAA, 0xBB])]),
            dtdb_relational::Row::new(vec![DbValue::Bool(true)]),
            dtdb_relational::Row::new(vec![DbValue::Null]),
        ];
        let r = map_res(ExecutionResult::Select { schema, rows });
        // Response 0: Header
        assert!(
            matches!(r[0].payload, Some(crate::proto::execute_query_response::Payload::Header(ref h)) if h.column_names[0] == "col1")
        );
        // Response 1: Int Row
        assert!(
            matches!(r[1].payload, Some(crate::proto::execute_query_response::Payload::Row(ref row)) if row.values[0] == "10")
        );
        // Response 2: Float Row
        assert!(
            matches!(r[2].payload, Some(crate::proto::execute_query_response::Payload::Row(ref row)) if row.values[0] == "1.5")
        );
        // Response 3: Bytes Row
        assert!(
            matches!(r[3].payload, Some(crate::proto::execute_query_response::Payload::Row(ref row)) if row.values[0].contains("170"))
        );
        // Response 4: Bool Row
        assert!(
            matches!(r[4].payload, Some(crate::proto::execute_query_response::Payload::Row(ref row)) if row.values[0] == "true")
        );
        // Response 5: Null Row
        assert!(
            matches!(r[5].payload, Some(crate::proto::execute_query_response::Payload::Row(ref row)) if row.values[0] == "NULL")
        );
    }

    #[tokio::test]
    async fn test_db_operations_errors() {
        let temp_dir = tempfile::tempdir().unwrap();
        let service = DuctTapeDbServiceImpl::new(temp_dir.path()).unwrap();

        let test_options = dtdb_relational::DatabaseOptions {
            compression: dtdb_storage::CompressionType::Uncompressed,
            memtable_size_limit: 1024,
            block_size_limit: 4096,
            wal_size_limit: 1024,
            flush_interval_ms: None,
            l0_compaction_threshold: None,
            sstable_target_size: None,
            base_level_size_limit: None,
            level_size_multiplier: None,
            max_level: None,
            block_cache_capacity: None,
            analyze_frequency_ms: None,
            wal_sync_interval_ms: None,
            memory_budget: None,
            fsync_method: dtdb_storage::FsyncMethod::default(),
        };

        // 1. Create DB invalid name
        let req = Request::new(CreateDbRequest {
            db_name: "   ".to_string(),
            ..Default::default()
        });
        let err = service.create_db(req).await.err().unwrap();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        // 2. Drop DB invalid name & not found
        let req = Request::new(DropDbRequest {
            db_name: "   ".to_string(),
        });
        let err = service.drop_db(req).await.err().unwrap();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        let req = Request::new(DropDbRequest {
            db_name: "nonexistent".to_string(),
        });
        let res = service.drop_db(req).await.unwrap();
        assert!(!res.into_inner().success); // Success false but no error code

        // 3. Flush DB invalid name & not found
        let req = Request::new(FlushDbRequest {
            db_name: "   ".to_string(),
        });
        let err = service.flush_db(req).await.err().unwrap();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        let req = Request::new(FlushDbRequest {
            db_name: "nonexistent".to_string(),
        });
        let err = service.flush_db(req).await.err().unwrap();
        assert_eq!(err.code(), tonic::Code::NotFound);

        // Create database and drop database success file delete failure:
        // Not easily simulated unless we make a read-only directory or mock fs.
        // But we can test regular drop success.
        service
            .catalog
            .create_db("to_drop", test_options.clone())
            .unwrap();
        let res_drop = service
            .drop_db(Request::new(DropDbRequest {
                db_name: "to_drop".to_string(),
            }))
            .await
            .unwrap();
        assert!(res_drop.into_inner().success);
    }

    #[tokio::test]
    async fn test_execute_query_errors() {
        let temp_dir = tempfile::tempdir().unwrap();
        let service = DuctTapeDbServiceImpl::new(temp_dir.path()).unwrap();

        let test_options = dtdb_relational::DatabaseOptions {
            compression: dtdb_storage::CompressionType::Uncompressed,
            memtable_size_limit: 1024,
            block_size_limit: 4096,
            wal_size_limit: 1024,
            flush_interval_ms: None,
            l0_compaction_threshold: None,
            sstable_target_size: None,
            base_level_size_limit: None,
            level_size_multiplier: None,
            max_level: None,
            block_cache_capacity: None,
            analyze_frequency_ms: None,
            wal_sync_interval_ms: None,
            memory_budget: None,
            fsync_method: dtdb_storage::FsyncMethod::default(),
        };

        // 1. DB not found
        let req = Request::new(ExecuteQueryRequest {
            db_name: "nonexistent".to_string(),
            sql_query: "SELECT 1".to_string(),
            parameters: vec![],
        });
        let err = service.execute_query(req).await.err().unwrap();
        assert_eq!(err.code(), tonic::Code::NotFound);

        // 2. Database exists but SQL is malformed
        service.catalog.create_db("test_db", test_options).unwrap();

        let req = Request::new(ExecuteQueryRequest {
            db_name: "test_db".to_string(),
            sql_query: "SELECT INVALID SYNTAX".to_string(),
            parameters: vec![],
        });
        let err = service.execute_query(req).await.err().unwrap();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_transaction_errors_and_isolation_mapping() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (client_addr, server_handle) = start_test_server(temp_dir.path()).await;

        let mut client = DuctTapeDbServiceClient::connect(client_addr.clone())
            .await
            .unwrap();

        // 1. Send ExecuteTxQuery before StartTransaction
        let (tx, rx) = mpsc::channel(1);
        let stream = ReceiverStream::new(rx);
        tx.send(crate::proto::TransactionRequest {
            command: Some(transaction_request::Command::Execute(ExecuteTxQuery {
                sql_query: "SELECT 1".to_string(),
                parameters: vec![],
            })),
        })
        .await
        .unwrap();
        drop(tx);

        let mut res_stream = client.transaction(stream).await.unwrap().into_inner();
        let res = res_stream.next().await.unwrap().unwrap();
        assert!(
            matches!(res.payload, Some(transaction_response::Payload::ErrorMessage(ref msg)) if msg.contains("Protocol error"))
        );

        // 2. Duplicate StartTransaction
        let (tx, rx) = mpsc::channel(3);
        let stream = ReceiverStream::new(rx);

        // Setup database first
        let mut client_pre = DuctTapeDbServiceClient::connect(client_addr.clone())
            .await
            .unwrap();
        client_pre
            .create_db(Request::new(CreateDbRequest {
                db_name: "test_db".to_string(),
                ..Default::default()
            }))
            .await
            .unwrap();

        tx.send(crate::proto::TransactionRequest {
            command: Some(transaction_request::Command::Start(StartTransaction {
                db_name: "test_db".to_string(),
                isolation_level: Some(IsolationLevel::ReadUncommitted as i32),
            })),
        })
        .await
        .unwrap();
        tx.send(crate::proto::TransactionRequest {
            command: Some(transaction_request::Command::Start(StartTransaction {
                db_name: "test_db".to_string(),
                isolation_level: Some(IsolationLevel::RepeatableRead as i32),
            })),
        })
        .await
        .unwrap();
        drop(tx);

        let mut res_stream = client.transaction(stream).await.unwrap().into_inner();
        let out1 = res_stream.next().await.unwrap().unwrap();
        let out2 = res_stream.next().await.unwrap().unwrap();
        assert!(matches!(
            out1.payload,
            Some(transaction_response::Payload::QueryFinished(true))
        ));
        assert!(
            matches!(out2.payload, Some(transaction_response::Payload::ErrorMessage(ref msg)) if msg.contains("already received"))
        );

        // 3. Start transaction database not found
        let (tx, rx) = mpsc::channel(1);
        let stream = ReceiverStream::new(rx);
        tx.send(crate::proto::TransactionRequest {
            command: Some(transaction_request::Command::Start(StartTransaction {
                db_name: "nonexistent".to_string(),
                isolation_level: Some(IsolationLevel::RepeatableRead as i32),
            })),
        })
        .await
        .unwrap();
        drop(tx);

        let mut res_stream = client.transaction(stream).await.unwrap().into_inner();
        let out = res_stream.next().await.unwrap().unwrap();
        assert!(
            matches!(out.payload, Some(transaction_response::Payload::ErrorMessage(ref msg)) if msg.contains("not found"))
        );

        // 4. DDL statements inside transaction
        let (tx, rx) = mpsc::channel(2);
        let stream = ReceiverStream::new(rx);
        tx.send(crate::proto::TransactionRequest {
            command: Some(transaction_request::Command::Start(StartTransaction {
                db_name: "test_db".to_string(),
                isolation_level: None,
            })),
        })
        .await
        .unwrap();
        tx.send(crate::proto::TransactionRequest {
            command: Some(transaction_request::Command::Execute(ExecuteTxQuery {
                sql_query: "CREATE TABLE t (id INT)".to_string(),
                parameters: vec![],
            })),
        })
        .await
        .unwrap();
        drop(tx);

        let mut res_stream = client.transaction(stream).await.unwrap().into_inner();
        let out1 = res_stream.next().await.unwrap().unwrap();
        let out2 = res_stream.next().await.unwrap().unwrap();
        assert!(matches!(
            out1.payload,
            Some(transaction_response::Payload::QueryFinished(true))
        ));
        assert!(
            matches!(out2.payload, Some(transaction_response::Payload::ErrorMessage(ref msg)) if msg.contains("DDL statements"))
        );

        // 5. Commit/Rollback protocols
        let (tx, rx) = mpsc::channel(3);
        let stream = ReceiverStream::new(rx);
        tx.send(crate::proto::TransactionRequest {
            command: Some(transaction_request::Command::Start(StartTransaction {
                db_name: "test_db".to_string(),
                isolation_level: None,
            })),
        })
        .await
        .unwrap();
        tx.send(crate::proto::TransactionRequest {
            command: Some(transaction_request::Command::Commit(CommitTransaction {})),
        })
        .await
        .unwrap();
        tx.send(crate::proto::TransactionRequest {
            command: Some(transaction_request::Command::Commit(CommitTransaction {})),
        })
        .await
        .unwrap();
        drop(tx);

        let mut res_stream = client.transaction(stream).await.unwrap().into_inner();
        let out1 = res_stream.next().await.unwrap().unwrap();
        let out2 = res_stream.next().await.unwrap().unwrap();
        let out3 = res_stream.next().await.unwrap().unwrap();
        assert!(matches!(
            out1.payload,
            Some(transaction_response::Payload::QueryFinished(true))
        ));
        assert!(
            matches!(out2.payload, Some(transaction_response::Payload::CommitResult(ref c)) if c.success)
        );
        assert!(
            matches!(out3.payload, Some(transaction_response::Payload::ErrorMessage(ref msg)) if msg.contains("completed"))
        );

        // 6. Test Rollback protocol explicitly
        let (tx, rx) = mpsc::channel(2);
        let stream = ReceiverStream::new(rx);
        tx.send(crate::proto::TransactionRequest {
            command: Some(transaction_request::Command::Start(StartTransaction {
                db_name: "test_db".to_string(),
                isolation_level: None,
            })),
        })
        .await
        .unwrap();
        tx.send(crate::proto::TransactionRequest {
            command: Some(transaction_request::Command::Rollback(
                RollbackTransaction {},
            )),
        })
        .await
        .unwrap();
        drop(tx);

        let mut res_stream = client.transaction(stream).await.unwrap().into_inner();
        let out1 = res_stream.next().await.unwrap().unwrap();
        let out2 = res_stream.next().await.unwrap().unwrap();
        assert!(matches!(
            out1.payload,
            Some(transaction_response::Payload::QueryFinished(true))
        ));
        assert!(
            matches!(out2.payload, Some(transaction_response::Payload::CommitResult(ref c)) if c.success && c.message.contains("rolled back"))
        );

        server_handle.abort();
    }

    #[test]
    fn test_proto_params_to_db_params_all_variants() {
        let proto = vec![
            QueryParam {
                name: "p1".to_string(),
                value: Some(ParamValue {
                    val: Some(Val::IntVal(10)),
                }),
            },
            QueryParam {
                name: "p2".to_string(),
                value: Some(ParamValue {
                    val: Some(Val::FloatVal(1.5)),
                }),
            },
            QueryParam {
                name: "p3".to_string(),
                value: Some(ParamValue {
                    val: Some(Val::BoolVal(true)),
                }),
            },
            QueryParam {
                name: "p4".to_string(),
                value: Some(ParamValue {
                    val: Some(Val::StringVal("hello".to_string())),
                }),
            },
            QueryParam {
                name: "p5".to_string(),
                value: Some(ParamValue {
                    val: Some(Val::BytesVal(vec![1, 2])),
                }),
            },
            QueryParam {
                name: "p6".to_string(),
                value: Some(ParamValue {
                    val: Some(Val::NullVal(true)),
                }),
            },
            QueryParam {
                name: "p7".to_string(),
                value: Some(ParamValue { val: None }),
            },
            QueryParam {
                name: "p8".to_string(),
                value: None,
            },
        ];

        let out = proto_params_to_db_params(proto).unwrap();
        assert_eq!(out.get("p1").unwrap(), &DbValue::Int(10));
        assert_eq!(out.get("p2").unwrap(), &DbValue::Float(1.5));
        assert_eq!(out.get("p3").unwrap(), &DbValue::Bool(true));
        assert_eq!(out.get("p4").unwrap(), &DbValue::string("hello"));
        assert_eq!(out.get("p5").unwrap(), &DbValue::bytes(vec![1, 2]));
        assert_eq!(out.get("p6").unwrap(), &DbValue::Null);
        assert_eq!(out.get("p7").unwrap(), &DbValue::Null);
        assert_eq!(out.get("p8").unwrap(), &DbValue::Null);
    }
}
