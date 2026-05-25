use futures_core::Stream;
use futures_util::StreamExt;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use tonic::Status;
use tonic::transport::Channel;

use crate::proto::duct_tape_db_service_client::DuctTapeDbServiceClient;
use crate::proto::duct_tape_db_service_server::DuctTapeDbService;
use crate::proto::{
    CommitTransaction, CompressionOption, CreateDbRequest, CreateDbResponse, DropDbRequest,
    DropDbResponse, ExecuteQueryRequest, ExecuteQueryResponse, ExecuteTxQuery, FlushDbRequest,
    FlushDbResponse, RollbackTransaction, StartTransaction, TransactionRequest,
    TransactionResponse,
};
use dtdb_relational::{DatabaseOptions, Transaction};
pub use dtdb_relational::{IsolationLevel, TransactionOptions};
use dtdb_sql::SqlEngine;
use dtdb_storage::CompressionType;

#[derive(Clone)]
pub enum ClientMode {
    Remote(DuctTapeDbServiceClient<Channel>),
    InProcess(Arc<crate::server::DuctTapeDbServiceImpl>),
}

#[derive(Clone)]
pub struct DuctTapeDbClient {
    inner: ClientMode,
}

impl DuctTapeDbClient {
    /// Connects to a remote DuctTapeDB gRPC server at the specified address.
    pub async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
        let client = DuctTapeDbServiceClient::connect(addr).await?;
        Ok(Self {
            inner: ClientMode::Remote(client),
        })
    }

    /// Creates an in-process DuctTapeDB client using the specified data directory.
    pub fn in_process(data_dir: impl AsRef<Path>) -> Result<Self, String> {
        Self::in_process_with_spawner(data_dir, Arc::new(dtdb_storage::DefaultSpawner))
    }

    /// Creates an in-process DuctTapeDB client using the specified data directory and a custom ThreadSpawner.
    pub fn in_process_with_spawner(
        data_dir: impl AsRef<Path>,
        spawner: Arc<dyn dtdb_storage::ThreadSpawner>,
    ) -> Result<Self, String> {
        let service = crate::server::DuctTapeDbServiceImpl::new_with_spawner(data_dir, spawner)?;
        Ok(Self {
            inner: ClientMode::InProcess(Arc::new(service)),
        })
    }

    /// Creates a database with the specified compression configuration.
    pub async fn create_db(
        &mut self,
        name: &str,
        compression: CompressionType,
    ) -> Result<CreateDbResponse, Status> {
        let compression_option = match compression {
            CompressionType::Lz4 => CompressionOption::CompressionLz4,
            CompressionType::Uncompressed => CompressionOption::CompressionUncompressed,
        };

        let request = CreateDbRequest {
            db_name: name.to_string(),
            compression: compression_option as i32,
            memtable_size_limit: None,
            block_size_limit: None,
            wal_size_limit: None,
            flush_interval_ms: None,
            analyze_frequency_ms: None,
        };

        match &mut self.inner {
            ClientMode::Remote(client) => {
                let response = client.create_db(request).await?;
                Ok(response.into_inner())
            }
            ClientMode::InProcess(service) => {
                let response = service.create_db(tonic::Request::new(request)).await?;
                Ok(response.into_inner())
            }
        }
    }

    /// Creates a database with the specified configuration options.
    pub async fn create_db_with_options(
        &mut self,
        name: &str,
        options: DatabaseOptions,
    ) -> Result<CreateDbResponse, Status> {
        let compression_option = match options.compression {
            CompressionType::Lz4 => CompressionOption::CompressionLz4,
            CompressionType::Uncompressed => CompressionOption::CompressionUncompressed,
        };

        let request = CreateDbRequest {
            db_name: name.to_string(),
            compression: compression_option as i32,
            memtable_size_limit: Some(options.memtable_size_limit as u64),
            block_size_limit: Some(options.block_size_limit as u64),
            wal_size_limit: Some(options.wal_size_limit as u64),
            flush_interval_ms: options.flush_interval_ms,
            analyze_frequency_ms: options.analyze_frequency_ms,
        };

        match &mut self.inner {
            ClientMode::Remote(client) => {
                let response = client.create_db(request).await?;
                Ok(response.into_inner())
            }
            ClientMode::InProcess(service) => {
                let response = service.create_db(tonic::Request::new(request)).await?;
                Ok(response.into_inner())
            }
        }
    }

    /// Drops a database and removes all its disk resources.
    pub async fn drop_db(&mut self, name: &str) -> Result<DropDbResponse, Status> {
        let request = DropDbRequest {
            db_name: name.to_string(),
        };

        match &mut self.inner {
            ClientMode::Remote(client) => {
                let response = client.drop_db(request).await?;
                Ok(response.into_inner())
            }
            ClientMode::InProcess(service) => {
                let response = service.drop_db(tonic::Request::new(request)).await?;
                Ok(response.into_inner())
            }
        }
    }

    /// Flushes all memory tables in the specified database to disk.
    pub async fn flush_db(&mut self, name: &str) -> Result<FlushDbResponse, Status> {
        let request = FlushDbRequest {
            db_name: name.to_string(),
        };

        match &mut self.inner {
            ClientMode::Remote(client) => {
                let response = client.flush_db(request).await?;
                Ok(response.into_inner())
            }
            ClientMode::InProcess(service) => {
                let response = service.flush_db(tonic::Request::new(request)).await?;
                Ok(response.into_inner())
            }
        }
    }

    /// Executes a single SQL statement, returning a stream of query response payloads.
    ///
    /// # Single-Statement Restriction
    ///
    /// This method only accepts a single SQL statement. Passing multiple semicolon-
    /// separated statements (e.g. "INSERT ...; INSERT ...;") will return an error.
    ///
    /// To execute multiple statements atomically within a single transaction,
    /// use [`run_in_transaction`](Self::run_in_transaction) instead.
    pub async fn execute_query(
        &mut self,
        db_name: &str,
        query: crate::query::SqlQuery,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<ExecuteQueryResponse, Status>> + Send + 'static>>,
        Status,
    > {
        let sql = query.interpolate().map_err(Status::invalid_argument)?;
        let request = ExecuteQueryRequest {
            db_name: db_name.to_string(),
            sql_query: sql,
        };

        match &mut self.inner {
            ClientMode::Remote(client) => {
                let response = client.execute_query(request).await?;
                let stream = response.into_inner();
                Ok(Box::pin(stream))
            }
            ClientMode::InProcess(service) => {
                let response = service.execute_query(tonic::Request::new(request)).await?;
                Ok(response.into_inner())
            }
        }
    }

    /// Executes a closure within a single database transaction.
    ///
    /// The closure receives a [`TransactionClient`] that can be used to execute
    /// SQL statements. All statements executed through the `TransactionClient`
    /// share the same underlying transaction:
    ///
    /// - If the closure returns `Ok(value)`, the transaction is committed and
    ///   `Ok(value)` is returned to the caller.
    /// - If the closure returns `Err(e)`, the transaction is rolled back and
    ///   `Err(e)` is returned to the caller.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use dtdb_api::sql_query;
    /// let result = client.run_in_transaction("my_db", |tx| async move {
    ///     tx.execute_query(sql_query!("INSERT INTO Users (id, name) VALUES (1, 'Alice');")).await?;
    ///     tx.execute_query(sql_query!("INSERT INTO Users (id, name) VALUES (2, 'Bob');")).await?;
    ///     Ok("inserted two users")
    /// }).await?;
    /// ```
    pub async fn run_in_transaction<F, Fut, T>(
        &mut self,
        db_name: &str,
        func: F,
    ) -> Result<T, Status>
    where
        F: FnOnce(TransactionClient) -> Fut,
        Fut: std::future::Future<Output = Result<T, Status>>,
    {
        self.run_in_transaction_with_options(db_name, TransactionOptions::default(), func)
            .await
    }

    /// Executes a closure within a single database transaction with specific transaction options.
    pub async fn run_in_transaction_with_options<F, Fut, T>(
        &mut self,
        db_name: &str,
        options: TransactionOptions,
        func: F,
    ) -> Result<T, Status>
    where
        F: FnOnce(TransactionClient) -> Fut,
        Fut: std::future::Future<Output = Result<T, Status>>,
    {
        match &mut self.inner {
            ClientMode::InProcess(service) => {
                // In-process mode: directly create a Transaction and SqlEngine,
                // execute the closure, and commit or rollback.
                let (database, sql_engine) =
                    service.get_db_and_engine(db_name).ok_or_else(|| {
                        Status::not_found(format!("Database '{}' not found", db_name))
                    })?;

                let tx_id = service.next_tx_id();
                let tx = Transaction::new_with_isolation(tx_id, database, options.isolation_level);
                let tx_client = TransactionClient {
                    mode: TransactionClientMode::InProcess {
                        tx: Arc::new(tx),
                        sql_engine,
                    },
                };

                match func(tx_client.clone()).await {
                    Ok(value) => {
                        // Commit the transaction.
                        // Consume `tx_client` to drop its reference to the Arc.
                        let tx_arc = match tx_client.mode {
                            TransactionClientMode::InProcess { tx, .. } => tx,
                            _ => unreachable!(),
                        };

                        // We need sole ownership to commit. Since we hold the only
                        // remaining Arc reference, try_unwrap should succeed.
                        match Arc::try_unwrap(tx_arc) {
                            Ok(tx) => {
                                tx.commit().map_err(|e| {
                                    Status::aborted(format!("Commit failed: {}", e))
                                })?;
                                Ok(value)
                            }
                            Err(_) => Err(Status::internal(
                                "Failed to acquire exclusive transaction ownership for commit",
                            )),
                        }
                    }
                    Err(e) => {
                        // Rollback happens automatically when the Transaction is dropped
                        // (its Drop impl calls unregister_transaction). The write buffer
                        // is simply discarded.
                        Err(e)
                    }
                }
            }
            ClientMode::Remote(client) => {
                // Remote mode: use the bidirectional streaming Transaction RPC.
                // We create a channel for sending TransactionRequest messages
                // and receive TransactionResponse messages from the server.
                let (req_tx, req_rx) = tokio::sync::mpsc::channel::<TransactionRequest>(32);

                let proto_level = match options.isolation_level {
                    dtdb_relational::IsolationLevel::ReadUncommitted => {
                        crate::proto::IsolationLevel::ReadUncommitted
                    }
                    dtdb_relational::IsolationLevel::ReadCommitted => {
                        crate::proto::IsolationLevel::ReadCommitted
                    }
                    dtdb_relational::IsolationLevel::RepeatableRead => {
                        crate::proto::IsolationLevel::RepeatableRead
                    }
                    dtdb_relational::IsolationLevel::SnapshotIsolation => {
                        crate::proto::IsolationLevel::SnapshotIsolation
                    }
                };

                // Send StartTransaction.
                req_tx
                    .send(TransactionRequest {
                        command: Some(crate::proto::transaction_request::Command::Start(
                            StartTransaction {
                                db_name: db_name.to_string(),
                                isolation_level: Some(proto_level as i32),
                            },
                        )),
                    })
                    .await
                    .map_err(|_| Status::internal("Failed to send StartTransaction"))?;

                // Open the bidirectional stream.
                let req_stream = tokio_stream::wrappers::ReceiverStream::new(req_rx);
                let response = client.transaction(req_stream).await?;
                let resp_stream = response.into_inner();

                let tx_client = TransactionClient {
                    mode: TransactionClientMode::Remote {
                        req_tx: req_tx.clone(),
                        resp_stream: Arc::new(tokio::sync::Mutex::new(resp_stream)),
                    },
                };

                // Wait for the Start acknowledgment (query_finished = true).
                tx_client.wait_for_query_finished().await?;

                match func(tx_client.clone()).await {
                    Ok(value) => {
                        // Send CommitTransaction.
                        req_tx
                            .send(TransactionRequest {
                                command: Some(crate::proto::transaction_request::Command::Commit(
                                    CommitTransaction {},
                                )),
                            })
                            .await
                            .map_err(|_| Status::internal("Failed to send CommitTransaction"))?;

                        // Read the CommitResult from the response stream.
                        let commit_result = tx_client.read_commit_result().await?;
                        if commit_result.success {
                            Ok(value)
                        } else {
                            Err(Status::aborted(commit_result.message))
                        }
                    }
                    Err(e) => {
                        // Send RollbackTransaction. Best-effort — if the channel is
                        // already closed (e.g. due to a stream error), we just ignore it.
                        let _ = req_tx
                            .send(TransactionRequest {
                                command: Some(
                                    crate::proto::transaction_request::Command::Rollback(
                                        RollbackTransaction {},
                                    ),
                                ),
                            })
                            .await;
                        Err(e)
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// TransactionClient — the handle passed to the run_in_transaction closure
// ---------------------------------------------------------------------------

/// A client handle for executing queries within an active transaction.
///
/// This is the type passed to the closure in
/// [`DuctTapeDbClient::run_in_transaction`]. It should **not** be constructed
/// directly — it is created internally by the client.
#[derive(Clone)]
pub struct TransactionClient {
    mode: TransactionClientMode,
}

#[derive(Clone)]
enum TransactionClientMode {
    InProcess {
        tx: Arc<Transaction>,
        sql_engine: Arc<SqlEngine>,
    },
    Remote {
        req_tx: tokio::sync::mpsc::Sender<TransactionRequest>,
        resp_stream: Arc<tokio::sync::Mutex<tonic::Streaming<TransactionResponse>>>,
    },
}

impl TransactionClient {
    /// Executes a single SQL statement within the transaction.
    ///
    /// Returns a `Vec<ExecuteQueryResponse>` containing all response payloads
    /// (headers, rows, info messages) for the executed statement.
    pub async fn execute_query(
        &self,
        query: crate::query::SqlQuery,
    ) -> Result<Vec<ExecuteQueryResponse>, Status> {
        let sql = query.interpolate().map_err(Status::invalid_argument)?;
        match &self.mode {
            TransactionClientMode::InProcess { tx, sql_engine } => {
                if sql_engine.is_ddl(&sql) {
                    return Err(Status::invalid_argument(
                        "DDL statements (CREATE TABLE, DROP TABLE) are not supported inside explicit multi-statement transactions.",
                    ));
                }
                match sql_engine.execute(&sql, tx) {
                    Ok(result) => Ok(crate::server::execution_result_to_responses(result)),
                    Err(e) => Err(Status::invalid_argument(format!("SQL Error: {}", e))),
                }
            }
            TransactionClientMode::Remote {
                req_tx,
                resp_stream,
            } => {
                // Send the ExecuteTxQuery command.
                req_tx
                    .send(TransactionRequest {
                        command: Some(crate::proto::transaction_request::Command::Execute(
                            ExecuteTxQuery { sql_query: sql },
                        )),
                    })
                    .await
                    .map_err(|_| Status::internal("Failed to send ExecuteTxQuery"))?;

                // Collect all query_result responses until we see query_finished.
                let mut results = Vec::new();
                let mut stream = resp_stream.lock().await;
                loop {
                    let resp = stream
                        .next()
                        .await
                        .ok_or_else(|| Status::internal("Transaction stream ended unexpectedly"))?
                        .map_err(|e| {
                            Status::internal(format!("Transaction stream error: {}", e))
                        })?;

                    match resp.payload {
                        Some(crate::proto::transaction_response::Payload::QueryResult(qr)) => {
                            results.push(qr);
                        }
                        Some(crate::proto::transaction_response::Payload::QueryFinished(_)) => {
                            break;
                        }
                        Some(crate::proto::transaction_response::Payload::ErrorMessage(msg)) => {
                            // Consume the following query_finished marker before returning the error.
                            let _ = stream.next().await;
                            return Err(Status::internal(msg));
                        }
                        _ => {
                            return Err(Status::internal(
                                "Unexpected response during query execution",
                            ));
                        }
                    }
                }
                Ok(results)
            }
        }
    }

    /// Internal helper: waits for the next `query_finished` marker from the remote stream.
    /// Used to consume the StartTransaction acknowledgment.
    async fn wait_for_query_finished(&self) -> Result<(), Status> {
        if let TransactionClientMode::Remote { resp_stream, .. } = &self.mode {
            let mut stream = resp_stream.lock().await;
            let resp = stream
                .next()
                .await
                .ok_or_else(|| Status::internal("Transaction stream ended unexpectedly"))?
                .map_err(|e| Status::internal(format!("Transaction stream error: {}", e)))?;

            match resp.payload {
                Some(crate::proto::transaction_response::Payload::QueryFinished(_)) => Ok(()),
                Some(crate::proto::transaction_response::Payload::ErrorMessage(msg)) => {
                    Err(Status::internal(msg))
                }
                _ => Err(Status::internal("Expected query_finished acknowledgment")),
            }
        } else {
            Ok(())
        }
    }

    /// Internal helper: reads the CommitResult from the remote response stream.
    async fn read_commit_result(&self) -> Result<crate::proto::CommitResult, Status> {
        if let TransactionClientMode::Remote { resp_stream, .. } = &self.mode {
            let mut stream = resp_stream.lock().await;
            let resp = stream
                .next()
                .await
                .ok_or_else(|| Status::internal("Transaction stream ended unexpectedly"))?
                .map_err(|e| Status::internal(format!("Transaction stream error: {}", e)))?;

            match resp.payload {
                Some(crate::proto::transaction_response::Payload::CommitResult(cr)) => Ok(cr),
                Some(crate::proto::transaction_response::Payload::ErrorMessage(msg)) => {
                    Err(Status::internal(msg))
                }
                _ => Err(Status::internal("Expected CommitResult")),
            }
        } else {
            Err(Status::internal(
                "read_commit_result called on non-remote client",
            ))
        }
    }
}
