use std::path::Path;
use std::sync::Arc;
use tonic::Status;

use crate::proto::{CreateDbResponse, DropDbResponse, FlushDbResponse};
use crate::query::SqlQuery;
use crate::server::DatabaseCatalog;
use dtdb_relational::{DatabaseOptions, Row, Schema, Transaction};
pub use dtdb_relational::{IsolationLevel, TransactionOptions};
use dtdb_sql::{ExecutionResult, ExecutionStreamingResult, SqlEngine};
use dtdb_storage::CompressionType;

#[derive(Clone)]
pub struct InProcessClient {
    catalog: Arc<DatabaseCatalog>,
}

impl InProcessClient {
    /// Opens (or creates) an in-process DuctTapeDB client using the specified data directory.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, String> {
        let catalog = Arc::new(DatabaseCatalog::new(data_dir)?);
        Ok(Self { catalog })
    }

    /// Opens (or creates) an in-process DuctTapeDB client using the specified data directory
    /// and a custom [`dtdb_storage::Executor`].
    pub fn open_with_executor(
        data_dir: impl AsRef<Path>,
        executor: Arc<dyn dtdb_storage::Executor>,
    ) -> Result<Self, String> {
        let catalog = Arc::new(DatabaseCatalog::new_with_executor(data_dir, executor)?);
        Ok(Self { catalog })
    }

    /// Creates a database with the specified compression configuration.
    pub fn create_db(
        &self,
        name: &str,
        compression: CompressionType,
    ) -> Result<CreateDbResponse, Status> {
        let options = DatabaseOptions {
            compression,
            memtable_size_limit: 1024 * 1024,
            block_size_limit: 4096,
            wal_size_limit: 32 * 1024 * 1024,
            flush_interval_ms: None,
            l0_compaction_threshold: None,
            sstable_target_size: None,
            base_level_size_limit: None,
            level_size_multiplier: None,
            max_level: None,
            block_cache_capacity: Some(1000),
            analyze_frequency_ms: None,
            wal_sync_interval_ms: None,
            memory_budget: None,
            fsync_method: dtdb_storage::FsyncMethod::default(),
        };
        self.catalog.create_db(name, options)
    }

    /// Creates a database with the specified configuration options.
    pub fn create_db_with_options(
        &self,
        name: &str,
        options: DatabaseOptions,
    ) -> Result<CreateDbResponse, Status> {
        self.catalog.create_db(name, options)
    }

    /// Drops a database and removes all its disk resources.
    pub fn drop_db(&self, name: &str) -> Result<DropDbResponse, Status> {
        self.catalog.drop_db(name)
    }

    /// Flushes all memory tables in the specified database to disk.
    pub fn flush_db(&self, name: &str) -> Result<FlushDbResponse, Status> {
        self.catalog.flush_db(name)
    }

    /// Executes a single SQL statement, returning a streaming query result.
    pub fn execute_query(
        &self,
        db_name: &str,
        query: SqlQuery,
    ) -> Result<InProcessQueryResult, Status> {
        let (database, sql_engine) = self
            .catalog
            .get_db_and_engine(db_name)
            .ok_or_else(|| Status::not_found(format!("Database '{}' not found", db_name)))?;

        let tx_id = self.catalog.next_tx_id();
        let tx = Transaction::new(tx_id, database.clone());

        let mut params = std::collections::HashMap::new();
        for (name, val) in query.bindings() {
            params.insert(name.clone(), val.clone());
        }

        let exec = sql_engine
            .execute_streaming(query.text(), &tx, &params)
            .map_err(|e| Status::invalid_argument(format!("SQL Error: {}", e)))?;

        match exec {
            ExecutionStreamingResult::Complete(result) => {
                tx.commit()
                    .map_err(|e| Status::aborted(format!("Commit failed: {}", e)))?;
                Ok(InProcessQueryResult {
                    inner: QueryResultInner::Complete(result),
                })
            }
            ExecutionStreamingResult::Streaming { schema, operator } => Ok(InProcessQueryResult {
                inner: QueryResultInner::Streaming(RowIterator {
                    physical_op: operator,
                    tx: Some(tx),
                    schema,
                }),
            }),
        }
    }

    /// Executes a closure within a single database transaction.
    pub fn run_in_transaction<F, T>(&self, db_name: &str, func: F) -> Result<T, Status>
    where
        F: FnOnce(InProcessTransactionClient) -> Result<T, Status>,
    {
        self.run_in_transaction_with_options(db_name, TransactionOptions::default(), func)
    }

    /// Executes a closure within a single database transaction with specific options.
    pub fn run_in_transaction_with_options<F, T>(
        &self,
        db_name: &str,
        options: TransactionOptions,
        func: F,
    ) -> Result<T, Status>
    where
        F: FnOnce(InProcessTransactionClient) -> Result<T, Status>,
    {
        let (database, sql_engine) = self
            .catalog
            .get_db_and_engine(db_name)
            .ok_or_else(|| Status::not_found(format!("Database '{}' not found", db_name)))?;

        let tx_id = self.catalog.next_tx_id();
        let tx = Transaction::new_with_isolation(tx_id, database, options.isolation_level);

        let tx_client = InProcessTransactionClient {
            tx: &tx,
            sql_engine,
        };

        match func(tx_client) {
            Ok(value) => {
                tx.commit()
                    .map_err(|e| Status::aborted(format!("Commit failed: {}", e)))?;
                Ok(value)
            }
            Err(e) => {
                let _ = tx.rollback();
                Err(e)
            }
        }
    }
}

/// A client handle for executing queries within an active synchronous transaction.
pub struct InProcessTransactionClient<'a> {
    tx: &'a Transaction,
    sql_engine: Arc<SqlEngine>,
}

impl<'a> InProcessTransactionClient<'a> {
    /// Executes a single SQL statement within the active transaction.
    pub fn execute_query(&self, query: SqlQuery) -> Result<InProcessQueryResult, Status> {
        if self.sql_engine.is_ddl(query.text()) {
            return Err(Status::invalid_argument(
                "DDL statements (CREATE TABLE, DROP TABLE) are not supported inside explicit multi-statement transactions.",
            ));
        }

        let mut params = std::collections::HashMap::new();
        for (name, val) in query.bindings() {
            params.insert(name.clone(), val.clone());
        }

        let exec = self
            .sql_engine
            .execute_streaming(query.text(), self.tx, &params)
            .map_err(|e| Status::invalid_argument(format!("SQL Error: {}", e)))?;

        match exec {
            ExecutionStreamingResult::Complete(result) => Ok(InProcessQueryResult {
                inner: QueryResultInner::Complete(result),
            }),
            ExecutionStreamingResult::Streaming { schema, operator } => {
                Ok(InProcessQueryResult {
                    inner: QueryResultInner::Streaming(RowIterator {
                        physical_op: operator,
                        tx: None, // Transaction is managed externally
                        schema,
                    }),
                })
            }
        }
    }
}

pub struct InProcessQueryResult {
    pub(crate) inner: QueryResultInner,
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum QueryResultInner {
    Complete(ExecutionResult),
    Streaming(RowIterator),
}

impl InProcessQueryResult {
    pub fn schema(&self) -> Option<&Schema> {
        match &self.inner {
            QueryResultInner::Complete(ExecutionResult::Select { schema, .. }) => Some(schema),
            QueryResultInner::Complete(_) => None,
            QueryResultInner::Streaming(iter) => Some(&iter.schema),
        }
    }

    pub fn info_message(&self) -> Option<String> {
        match &self.inner {
            QueryResultInner::Complete(res) => match res {
                ExecutionResult::Analyze => Some("Table analyzed successfully.".to_string()),
                ExecutionResult::CreateTable => Some("Table created successfully.".to_string()),
                ExecutionResult::DropTable => Some("Table dropped successfully.".to_string()),
                ExecutionResult::CreateIndex => Some("Index created successfully.".to_string()),
                ExecutionResult::DropIndex => Some("Index dropped successfully.".to_string()),
                ExecutionResult::Insert { count } => Some(format!("Inserted {} row(s).", count)),
                ExecutionResult::Delete { count } => Some(format!("Deleted {} row(s).", count)),
                ExecutionResult::Update { count } => Some(format!("Updated {} row(s).", count)),
                ExecutionResult::Select { .. } => None,
            },
            QueryResultInner::Streaming(_) => None,
        }
    }
}

impl Iterator for InProcessQueryResult {
    type Item = Result<Row, Status>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            QueryResultInner::Complete(res) => match res {
                ExecutionResult::Select { schema: _, rows } => {
                    if rows.is_empty() {
                        None
                    } else {
                        Some(Ok(rows.remove(0)))
                    }
                }
                _ => None,
            },
            QueryResultInner::Streaming(iter) => iter.next(),
        }
    }
}

pub struct RowIterator {
    physical_op: Box<dyn dtdb_sql::PhysicalOperator>,
    tx: Option<Transaction>,
    schema: Schema,
}

impl RowIterator {
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl Iterator for RowIterator {
    type Item = Result<Row, Status>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.physical_op.next() {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => {
                if let Some(tx) = self.tx.take() {
                    let commit_res = tx.commit();
                    if let Err(e) = commit_res {
                        return Some(Err(Status::aborted(format!("Commit failed: {}", e))));
                    }
                }
                None
            }
            Err(e) => {
                if let Some(tx) = self.tx.take() {
                    let _ = tx.rollback();
                }
                Some(Err(Status::invalid_argument(e)))
            }
        }
    }
}

impl Drop for RowIterator {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.rollback();
        }
    }
}
