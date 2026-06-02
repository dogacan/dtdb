use dtdb_storage::StorageError;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, RelationalError>;

#[derive(Error, Debug)]
pub enum RelationalError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization/Deserialization error: {0}")]
    Serialization(#[from] postcard::Error),

    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Table already exists: {0}")]
    TableAlreadyExists(String),

    #[error("Transaction conflict: {0}")]
    TransactionConflict(String),
}
