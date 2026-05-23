use tonic::transport::Channel;
use tonic::Status;
use futures_core::Stream;

use dtdb_storage::CompressionType;
use dtdb_relational::DatabaseOptions;
use crate::proto::duct_tape_db_service_client::DuctTapeDbServiceClient;
use crate::proto::{
    CreateDbRequest, CreateDbResponse, DropDbRequest, DropDbResponse,
    ExecuteQueryRequest, ExecuteQueryResponse, CompressionOption,
    FlushDbRequest, FlushDbResponse,
};

#[derive(Clone)]
pub struct DuctTapeDbClient {
    inner: DuctTapeDbServiceClient<Channel>,
}

impl DuctTapeDbClient {
    /// Connects to a remote DuctTapeDB gRPC server at the specified address.
    pub async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
        let inner = DuctTapeDbServiceClient::connect(addr).await?;
        Ok(Self { inner })
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

    /// Executes a SQL query, returning a stream of query response payloads.
    pub async fn execute_query(
        &mut self,
        db_name: &str,
        sql_query: &str,
    ) -> Result<impl Stream<Item = Result<ExecuteQueryResponse, Status>>, Status> {
        let request = ExecuteQueryRequest {
            db_name: db_name.to_string(),
            sql_query: sql_query.to_string(),
        };

        let response = self.inner.execute_query(request).await?;
        Ok(response.into_inner())
    }
}
