use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::pin::Pin;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use futures_core::Stream;
use tonic::{Request, Response, Status};

use dtdb_storage::{CompressionType, DbValue};
use dtdb_relational::{Database, Transaction, DatabaseOptions};
use dtdb_sql::SqlEngine;

use crate::proto::duct_tape_db_service_server::DuctTapeDbService;
use crate::proto::{
    CreateDbRequest, CreateDbResponse, DropDbRequest, DropDbResponse,
    ExecuteQueryRequest, ExecuteQueryResponse, Header, Row, CompressionOption,
    FlushDbRequest, FlushDbResponse,
};

pub struct DuctTapeDbServiceImpl {
    data_dir: PathBuf,
    databases: RwLock<HashMap<String, DbState>>,
    next_tx_id: AtomicU64,
}

struct DbState {
    database: Arc<Database>,
    sql_engine: Arc<SqlEngine>,
}

impl DuctTapeDbServiceImpl {
    pub fn new(data_dir: impl AsRef<Path>) -> Result<Self, String> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir).map_err(|e| e.to_string())?;

        let databases = RwLock::new(HashMap::new());
        let service = Self {
            data_dir: data_dir.clone(),
            databases,
            next_tx_id: AtomicU64::new(1),
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
            if path.is_dir() {
                if let Some(db_name) = path.file_name().and_then(|s| s.to_str()) {
                    // Check if db_options.bin exists
                    if path.join("db_options.bin").exists() {
                        println!("Restoring database: {}", db_name);
                        let database = Arc::new(Database::open(&path).map_err(|e| e.to_string())?);
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
                        if let Ok(table) = db.get_table(&table_name) {
                            if let Err(e) = table.engine.flush_memtable() {
                                eprintln!("Periodic flush failed for table {}: {}", table_name, e);
                            }
                        }
                    }
                } else {
                    break; // Database was dropped, exit loop
                }
            }
        });
    }
}

#[tonic::async_trait]
impl DuctTapeDbService for DuctTapeDbServiceImpl {
    type ExecuteQueryStream = Pin<Box<dyn Stream<Item = Result<ExecuteQueryResponse, Status>> + Send + 'static>>;

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
        };

        let database = Arc::new(Database::open_with_options(&db_path, options.clone())
            .map_err(|e| Status::internal(format!("Failed to create database: {}", e)))?);
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
            dbs.remove(db_name)
        };

        if let Some(db_state) = state {
            // Drop database references to release file locks
            drop(db_state);

            // Clean up directory from disk
            let db_path = self.data_dir.join(db_name);
            if db_path.exists() {
                if let Err(e) = fs::remove_dir_all(&db_path) {
                    return Ok(Response::new(DropDbResponse {
                        success: false,
                        message: format!("Database removed from catalog but failed to delete files: {}", e),
                    }));
                }
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
                return Err(Status::not_found(format!("Database '{}' not found", db_name)));
            }
        };

        let tables = database.list_tables();
        let mut flushed_tables = Vec::new();
        for table_name in tables {
            if let Ok(table) = database.get_table(&table_name) {
                table.engine.flush_memtable()
                    .map_err(|e| Status::internal(format!("Failed to flush table {}: {}", table_name, e)))?;
                flushed_tables.push(table_name);
            }
        }

        Ok(Response::new(FlushDbResponse {
            success: true,
            message: format!("Flushed {} table(s) in database '{}': {:?}", flushed_tables.len(), db_name, flushed_tables),
        }))
    }

    async fn execute_query(
        &self,
        request: Request<ExecuteQueryRequest>,
    ) -> Result<Response<Self::ExecuteQueryStream>, Status> {
        let req = request.into_inner();
        let db_name = req.db_name.trim();
        let sql_query = req.sql_query.trim();

        // 1. Get db state
        let (database, sql_engine) = {
            let dbs = self.databases.read().unwrap();
            if let Some(state) = dbs.get(db_name) {
                (state.database.clone(), state.sql_engine.clone())
            } else {
                return Err(Status::not_found(format!("Database '{}' not found", db_name)));
            }
        };

        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        let tx = Transaction::new(tx_id, database.clone());

        // Create channel for streaming
        let (tx_chan, rx_chan) = mpsc::channel(128);

        // Execute query
        match sql_engine.execute(sql_query, &tx) {
            Ok(result) => {
                if let Err(e) = tx.commit() {
                    return Err(Status::aborted(format!("Transaction commit failed: {}", e)));
                }

                // Handle sending results to stream
                tokio::spawn(async move {
                    match result {
                        dtdb_sql::ExecutionResult::CreateTable => {
                            let _ = tx_chan.send(Ok(ExecuteQueryResponse {
                                payload: Some(crate::proto::execute_query_response::Payload::InfoMessage(
                                    "Table created successfully.".to_string(),
                                )),
                            })).await;
                        }
                        dtdb_sql::ExecutionResult::DropTable => {
                            let _ = tx_chan.send(Ok(ExecuteQueryResponse {
                                payload: Some(crate::proto::execute_query_response::Payload::InfoMessage(
                                    "Table dropped successfully.".to_string(),
                                )),
                            })).await;
                        }
                        dtdb_sql::ExecutionResult::Insert { count } => {
                            let _ = tx_chan.send(Ok(ExecuteQueryResponse {
                                payload: Some(crate::proto::execute_query_response::Payload::InfoMessage(
                                    format!("Inserted {} row(s).", count),
                                )),
                            })).await;
                        }
                        dtdb_sql::ExecutionResult::Select { schema, rows } => {
                            // Send Header first
                            let column_names = schema.columns.iter().map(|col| col.name.clone()).collect();
                            if tx_chan.send(Ok(ExecuteQueryResponse {
                                payload: Some(crate::proto::execute_query_response::Payload::Header(Header {
                                    column_names,
                                })),
                            })).await.is_err() {
                                return;
                            }

                            // Send rows
                            for row in rows {
                                let values = row.values.iter().map(|val| match val {
                                    DbValue::Int(v) => v.to_string(),
                                    DbValue::Float(v) => v.to_string(),
                                    DbValue::String(s) => s.clone(),
                                    DbValue::Bytes(b) => format!("{:?}", b),
                                }).collect();

                                if tx_chan.send(Ok(ExecuteQueryResponse {
                                    payload: Some(crate::proto::execute_query_response::Payload::Row(Row {
                                        values,
                                    })),
                                })).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                });
            }
            Err(e) => {
                let _ = tx.rollback();
                return Err(Status::invalid_argument(format!("SQL Error: {}", e)));
            }
        }

        let stream = ReceiverStream::new(rx_chan);
        Ok(Response::new(Box::pin(stream) as Self::ExecuteQueryStream))
    }
}
