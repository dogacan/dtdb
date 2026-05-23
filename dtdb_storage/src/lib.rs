use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod memtable;
pub mod wal;
pub mod sstable;
pub mod engine;
pub use engine::StorageEngine;

/// Result type wrapper for storage operations.
pub type Result<T> = std::result::Result<T, StorageError>;

/// StorageError defines all errors that can occur in the storage layer.
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization/Deserialization error: {0}")]
    Serialization(#[from] bincode::Error),

    #[error("Lz4 compression error: {0}")]
    Compression(String),

    #[error("Database corrupted: {0}")]
    Corruption(String),

    #[error("Table directory already exists: {0}")]
    AlreadyExists(String),
}

/// DbKey represents strongly typed keys in the database.
///
/// We derive `PartialOrd` and `Ord` so keys can be sorted and stored in `BTreeMap`.
/// Rust's derived ordering compares variants in the order they are declared:
/// `Int` comes before `String`. Within a variant, it compares the inner data.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DbKey {
    Int(i64),
    String(String),
}

/// DbValue represents strongly typed values in the database.
///
/// Note that we do not derive `Ord` or `Eq` because values can contain `Float` (f64),
/// which does not have a total ordering due to NaN.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DbValue {
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
}

/// CompressionType represents the supported block compression algorithms in DuctTapeDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionType {
    Uncompressed,
    Lz4,
}

/// EngineOptions defines the configuration limits and parameters for a StorageEngine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineOptions {
    pub compression: CompressionType,
    pub memtable_size_limit: usize,
    pub block_size_limit: usize,
    pub wal_size_limit: usize,
}

