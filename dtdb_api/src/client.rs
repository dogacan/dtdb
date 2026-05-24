use std::pin::Pin;
use std::sync::Arc;
use std::path::Path;
use tonic::transport::Channel;
use tonic::Status;
use futures_core::Stream;

use dtdb_storage::CompressionType;
use dtdb_relational::DatabaseOptions;
use crate::proto::duct_tape_db_service_client::DuctTapeDbServiceClient;
use crate::proto::duct_tape_db_service_server::DuctTapeDbService;
use crate::proto::{
    CreateDbRequest, CreateDbResponse, DropDbRequest, DropDbResponse,
    ExecuteQueryRequest, ExecuteQueryResponse, CompressionOption,
    FlushDbRequest, FlushDbResponse,
};

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
        let service = crate::server::DuctTapeDbServiceImpl::new(data_dir)?;
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

    /// Executes a SQL query, returning a stream of query response payloads.
    pub async fn execute_query(
        &mut self,
        db_name: &str,
        sql_query: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<ExecuteQueryResponse, Status>> + Send + 'static>>, Status> {
        let request = ExecuteQueryRequest {
            db_name: db_name.to_string(),
            sql_query: sql_query.to_string(),
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
}
