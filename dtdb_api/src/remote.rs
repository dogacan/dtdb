use futures_core::Stream;
use futures_util::StreamExt;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use tonic::Status;
use tonic::transport::Channel;

use crate::proto::duct_tape_db_service_client::DuctTapeDbServiceClient;
use crate::proto::{
    CommitTransaction, CompressionOption, CreateDbRequest, CreateDbResponse, DropDbRequest,
    DropDbResponse, ExecuteQueryRequest, ExecuteQueryResponse, ExecuteTxQuery, FlushDbRequest,
    FlushDbResponse, RollbackTransaction, StartTransaction, TransactionRequest,
    TransactionResponse,
};
use dtdb_relational::DatabaseOptions;
pub use dtdb_relational::{IsolationLevel, TransactionOptions};
use dtdb_storage::{CompressionType, DbValue};

#[derive(Clone)]
pub struct AuthClientInterceptor {
    token: Option<String>,
}

impl tonic::service::Interceptor for AuthClientInterceptor {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        if let Some(ref t) = self.token {
            let auth_header = format!("Bearer {}", t);
            if let Ok(meta_val) = tonic::metadata::MetadataValue::from_str(&auth_header) {
                request.metadata_mut().insert("authorization", meta_val);
            }
        }
        Ok(request)
    }
}

#[derive(Clone)]
pub struct RemoteClient {
    inner: DuctTapeDbServiceClient<
        tonic::service::interceptor::InterceptedService<Channel, AuthClientInterceptor>,
    >,
}

impl RemoteClient {
    /// Connects to a remote DuctTapeDB gRPC server at the specified address.
    pub async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
        let token = std::env::var("DTDB_AUTH_TOKEN").ok();
        Self::connect_with_token(addr, token).await
    }

    /// Connects to a remote DuctTapeDB gRPC server with a specific authentication token.
    pub async fn connect_with_token(
        addr: String,
        token: Option<String>,
    ) -> Result<Self, tonic::transport::Error> {
        let mut endpoint = tonic::transport::Endpoint::from_shared(addr.clone())?;

        if addr.starts_with("https://") {
            let mut tls_config = tonic::transport::ClientTlsConfig::new();
            if let Some(pem) = std::env::var("DTDB_CA_CERT")
                .or_else(|_| std::env::var("DTDB_TLS_CERT"))
                .ok()
                .and_then(|ca_path| std::fs::read_to_string(ca_path).ok())
            {
                let ca = tonic::transport::Certificate::from_pem(pem);
                tls_config = tls_config.ca_certificate(ca);
            }
            if let Ok(domain) = std::env::var("DTDB_TLS_DOMAIN") {
                tls_config = tls_config.domain_name(domain);
            } else if addr.contains("127.0.0.1") || addr.contains("localhost") {
                tls_config = tls_config.domain_name("localhost");
            }
            endpoint = endpoint.tls_config(tls_config)?;
        }

        let channel = endpoint.connect().await?;
        let interceptor = AuthClientInterceptor { token };
        let client = DuctTapeDbServiceClient::with_interceptor(channel, interceptor);
        Ok(Self { inner: client })
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

        let response = self.inner.create_db(request).await?;
        Ok(response.into_inner())
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

        let response = self.inner.create_db(request).await?;
        Ok(response.into_inner())
    }

    /// Drops a database and removes all its disk resources.
    pub async fn drop_db(&mut self, name: &str) -> Result<DropDbResponse, Status> {
        let request = DropDbRequest {
            db_name: name.to_string(),
        };

        let response = self.inner.drop_db(request).await?;
        Ok(response.into_inner())
    }

    /// Flushes all memory tables in the specified database to disk.
    pub async fn flush_db(&mut self, name: &str) -> Result<FlushDbResponse, Status> {
        let request = FlushDbRequest {
            db_name: name.to_string(),
        };

        let response = self.inner.flush_db(request).await?;
        Ok(response.into_inner())
    }

    /// Executes a single SQL statement, returning a stream of query response payloads.
    pub async fn execute_query(
        &mut self,
        db_name: &str,
        query: crate::query::SqlQuery,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<ExecuteQueryResponse, Status>> + Send + 'static>>,
        Status,
    > {
        let request = ExecuteQueryRequest {
            db_name: db_name.to_string(),
            sql_query: query.text().to_string(),
            parameters: db_params_to_proto_params(query.bindings()),
        };

        let response = self.inner.execute_query(request).await?;
        let stream = response.into_inner();
        Ok(Box::pin(stream))
    }

    /// Executes a closure within a single database transaction.
    pub async fn run_in_transaction<F, Fut, T>(
        &mut self,
        db_name: &str,
        func: F,
    ) -> Result<T, Status>
    where
        F: FnOnce(RemoteTransactionClient) -> Fut,
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
        F: FnOnce(RemoteTransactionClient) -> Fut,
        Fut: std::future::Future<Output = Result<T, Status>>,
    {
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
        let response = self.inner.transaction(req_stream).await?;
        let resp_stream = response.into_inner();

        let tx_client = RemoteTransactionClient {
            req_tx: req_tx.clone(),
            resp_stream: Arc::new(tokio::sync::Mutex::new(resp_stream)),
        };

        // Wait for the Start acknowledgment.
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
                let _ = req_tx
                    .send(TransactionRequest {
                        command: Some(crate::proto::transaction_request::Command::Rollback(
                            RollbackTransaction {},
                        )),
                    })
                    .await;
                Err(e)
            }
        }
    }
}

/// A client handle for executing queries within an active remote transaction.
#[derive(Clone)]
pub struct RemoteTransactionClient {
    req_tx: tokio::sync::mpsc::Sender<TransactionRequest>,
    resp_stream: Arc<tokio::sync::Mutex<tonic::Streaming<TransactionResponse>>>,
}

impl RemoteTransactionClient {
    /// Executes a single SQL statement within the transaction.
    pub async fn execute_query(
        &self,
        query: crate::query::SqlQuery,
    ) -> Result<Vec<ExecuteQueryResponse>, Status> {
        // Send the ExecuteTxQuery command.
        self.req_tx
            .send(TransactionRequest {
                command: Some(crate::proto::transaction_request::Command::Execute(
                    ExecuteTxQuery {
                        sql_query: query.text().to_string(),
                        parameters: db_params_to_proto_params(query.bindings()),
                    },
                )),
            })
            .await
            .map_err(|_| Status::internal("Failed to send ExecuteTxQuery"))?;

        // Collect all query_result responses until we see query_finished.
        let mut results = Vec::new();
        let mut stream = self.resp_stream.lock().await;
        loop {
            let resp = stream
                .next()
                .await
                .ok_or_else(|| Status::internal("Transaction stream ended unexpectedly"))?
                .map_err(|e| Status::internal(format!("Transaction stream error: {}", e)))?;

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

    /// Internal helper: waits for the next `query_finished` marker from the remote stream.
    async fn wait_for_query_finished(&self) -> Result<(), Status> {
        let mut stream = self.resp_stream.lock().await;
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
    }

    /// Internal helper: reads the CommitResult from the remote response stream.
    async fn read_commit_result(&self) -> Result<crate::proto::CommitResult, Status> {
        let mut stream = self.resp_stream.lock().await;
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
    }
}

fn db_params_to_proto_params(bindings: &[(String, DbValue)]) -> Vec<crate::proto::QueryParam> {
    bindings
        .iter()
        .map(|(name, val)| crate::proto::QueryParam {
            name: name.clone(),
            value: Some(crate::proto::ParamValue {
                val: Some(match val {
                    DbValue::Int(i) => crate::proto::param_value::Val::IntVal(*i),
                    DbValue::Float(f) => crate::proto::param_value::Val::FloatVal(*f),
                    DbValue::Bool(b) => crate::proto::param_value::Val::BoolVal(*b),
                    DbValue::String(s) => crate::proto::param_value::Val::StringVal(s.to_string()),
                    DbValue::Bytes(b) => crate::proto::param_value::Val::BytesVal(b.to_vec()),
                    DbValue::Null => crate::proto::param_value::Val::NullVal(true),
                    DbValue::Date(d) => crate::proto::param_value::Val::StringVal(d.to_string()),
                    DbValue::Time(t) => crate::proto::param_value::Val::StringVal(t.to_string()),
                    DbValue::Timestamp(ts) => {
                        crate::proto::param_value::Val::StringVal(ts.to_string())
                    }
                    DbValue::Decimal(dec) => {
                        crate::proto::param_value::Val::StringVal(dec.to_string())
                    }
                }),
            }),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use rust_decimal::Decimal;
    use std::str::FromStr;

    #[test]
    fn test_db_params_to_proto_params_all_variants() {
        let date = NaiveDate::from_ymd_opt(2026, 6, 3).unwrap();
        let time = NaiveTime::from_hms_opt(14, 30, 0).unwrap();
        let ts = NaiveDateTime::new(date, time);
        let dec = Decimal::from_str("123.45").unwrap();

        let bindings = vec![
            ("i".to_string(), DbValue::Int(42)),
            ("f".to_string(), DbValue::Float(1.23)),
            ("b".to_string(), DbValue::Bool(true)),
            ("s".to_string(), DbValue::string("hello")),
            ("raw".to_string(), DbValue::bytes(vec![1, 2, 3])),
            ("nul".to_string(), DbValue::Null),
            ("d".to_string(), DbValue::Date(date)),
            ("t".to_string(), DbValue::Time(time)),
            ("ts".to_string(), DbValue::Timestamp(ts)),
            ("dec".to_string(), DbValue::Decimal(dec)),
        ];

        let proto_params = db_params_to_proto_params(&bindings);
        assert_eq!(proto_params.len(), 10);

        assert_eq!(proto_params[0].name, "i");
        assert_eq!(
            proto_params[0]
                .value
                .as_ref()
                .unwrap()
                .val
                .as_ref()
                .unwrap(),
            &crate::proto::param_value::Val::IntVal(42)
        );

        assert_eq!(proto_params[1].name, "f");
        assert_eq!(
            proto_params[1]
                .value
                .as_ref()
                .unwrap()
                .val
                .as_ref()
                .unwrap(),
            &crate::proto::param_value::Val::FloatVal(1.23)
        );

        assert_eq!(proto_params[2].name, "b");
        assert_eq!(
            proto_params[2]
                .value
                .as_ref()
                .unwrap()
                .val
                .as_ref()
                .unwrap(),
            &crate::proto::param_value::Val::BoolVal(true)
        );

        assert_eq!(proto_params[3].name, "s");
        assert_eq!(
            proto_params[3]
                .value
                .as_ref()
                .unwrap()
                .val
                .as_ref()
                .unwrap(),
            &crate::proto::param_value::Val::StringVal("hello".to_string())
        );

        assert_eq!(proto_params[4].name, "raw");
        assert_eq!(
            proto_params[4]
                .value
                .as_ref()
                .unwrap()
                .val
                .as_ref()
                .unwrap(),
            &crate::proto::param_value::Val::BytesVal(vec![1, 2, 3])
        );

        assert_eq!(proto_params[5].name, "nul");
        assert_eq!(
            proto_params[5]
                .value
                .as_ref()
                .unwrap()
                .val
                .as_ref()
                .unwrap(),
            &crate::proto::param_value::Val::NullVal(true)
        );

        assert_eq!(proto_params[6].name, "d");
        assert_eq!(
            proto_params[6]
                .value
                .as_ref()
                .unwrap()
                .val
                .as_ref()
                .unwrap(),
            &crate::proto::param_value::Val::StringVal("2026-06-03".to_string())
        );

        assert_eq!(proto_params[7].name, "t");
        assert_eq!(
            proto_params[7]
                .value
                .as_ref()
                .unwrap()
                .val
                .as_ref()
                .unwrap(),
            &crate::proto::param_value::Val::StringVal("14:30:00".to_string())
        );

        assert_eq!(proto_params[8].name, "ts");
        assert_eq!(
            proto_params[8]
                .value
                .as_ref()
                .unwrap()
                .val
                .as_ref()
                .unwrap(),
            &crate::proto::param_value::Val::StringVal("2026-06-03 14:30:00".to_string())
        );

        assert_eq!(proto_params[9].name, "dec");
        assert_eq!(
            proto_params[9]
                .value
                .as_ref()
                .unwrap()
                .val
                .as_ref()
                .unwrap(),
            &crate::proto::param_value::Val::StringVal("123.45".to_string())
        );
    }
}
