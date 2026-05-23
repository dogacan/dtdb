use tonic::transport::Channel;
use tonic::Status;
use futures_core::Stream;

use dtdb_storage::CompressionType;
use crate::proto::duct_tape_db_service_client::DuctTapeDbServiceClient;
use crate::proto::{
    CreateDbRequest, CreateDbResponse, DropDbRequest, DropDbResponse,
    ExecuteQueryRequest, ExecuteQueryResponse, CompressionOption,
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
