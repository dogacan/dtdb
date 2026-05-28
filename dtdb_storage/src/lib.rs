use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod block_cache;
pub mod bloom;
pub mod engine;
pub mod manifest;
pub mod memtable;
pub mod merge_iter;
pub mod scan_iter;
pub mod sstable;
pub mod wal;
pub use block_cache::{BlockCache, LruCache};
pub use bloom::BloomFilter;
pub use engine::{StorageEngine, StorageEngineStatistics};
pub use scan_iter::ScanIterator;
pub use wal::WalEntry;

pub trait ThreadSpawner: Send + Sync + 'static {
    fn spawn(&self, f: Box<dyn FnOnce() + Send + 'static>);
}

#[derive(Clone)]
pub struct DefaultSpawner;
impl ThreadSpawner for DefaultSpawner {
    fn spawn(&self, f: Box<dyn FnOnce() + Send + 'static>) {
        std::thread::spawn(f);
    }
}

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
    Bool(bool),
    Composite(Vec<DbKey>),
}

impl DbKey {
    pub fn byte_size(&self) -> usize {
        match self {
            DbKey::Int(_) => 8,
            DbKey::String(s) => s.len(),
            DbKey::Bool(_) => 1,
            DbKey::Composite(keys) => keys.iter().map(|k| k.byte_size()).sum(),
        }
    }
}

/// DbValue represents strongly typed values in the database.
///
/// Note that we do not derive `Ord` or `Eq` because values can contain `Float` (f64),
/// which does not have a total ordering due to NaN.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DbValue {
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Bool(bool),
    Null,
}

impl PartialEq for DbValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DbValue::Int(l), DbValue::Int(r)) => l == r,
            (DbValue::Float(l), DbValue::Float(r)) => {
                if l.is_nan() && r.is_nan() {
                    true
                } else {
                    l == r
                }
            }
            (DbValue::String(l), DbValue::String(r)) => l == r,
            (DbValue::Bytes(l), DbValue::Bytes(r)) => l == r,
            (DbValue::Bool(l), DbValue::Bool(r)) => l == r,
            (DbValue::Null, DbValue::Null) => true,
            _ => false,
        }
    }
}

impl Eq for DbValue {}

impl std::hash::Hash for DbValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            DbValue::Int(v) => {
                0u8.hash(state);
                v.hash(state);
            }
            DbValue::Float(v) => {
                1u8.hash(state);
                if v.is_nan() || *v == 0.0 {
                    0.0f64.to_bits().hash(state);
                } else {
                    v.to_bits().hash(state);
                }
            }
            DbValue::String(v) => {
                2u8.hash(state);
                v.hash(state);
            }
            DbValue::Bytes(v) => {
                3u8.hash(state);
                v.hash(state);
            }
            DbValue::Bool(v) => {
                4u8.hash(state);
                v.hash(state);
            }
            DbValue::Null => {
                5u8.hash(state);
            }
        }
    }
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
    pub l0_compaction_threshold: usize,
    pub sstable_target_size: usize,
    pub base_level_size_limit: usize,
    pub level_size_multiplier: usize,
    pub max_level: usize,
    pub block_cache_capacity: usize,
    #[serde(default)]
    pub wal_sync_interval_ms: Option<u64>,
}

impl From<i64> for DbValue {
    fn from(v: i64) -> Self {
        DbValue::Int(v)
    }
}

impl From<f64> for DbValue {
    fn from(v: f64) -> Self {
        DbValue::Float(v)
    }
}

impl From<String> for DbValue {
    fn from(v: String) -> Self {
        DbValue::String(v)
    }
}

impl From<bool> for DbValue {
    fn from(v: bool) -> Self {
        DbValue::Bool(v)
    }
}

impl<'a> From<&'a str> for DbValue {
    fn from(v: &'a str) -> Self {
        DbValue::String(v.to_string())
    }
}

impl From<Vec<u8>> for DbValue {
    fn from(v: Vec<u8>) -> Self {
        DbValue::Bytes(v)
    }
}

impl<'a> From<&'a [u8]> for DbValue {
    fn from(v: &'a [u8]) -> Self {
        DbValue::Bytes(v.to_vec())
    }
}

/// fsync the parent directory of `path` so that a preceding rename of a child
/// is durable across crashes on filesystems (ext4 data=ordered, XFS, ZFS, many
/// network FSes) that don't otherwise guarantee directory-entry persistence.
///
/// Best-effort: on Windows there is no equivalent and we treat the call as a
/// no-op; everywhere else we silently ignore EINVAL from filesystems whose
/// directory fds can't be fsynced.
pub fn fsync_parent_dir(path: &std::path::Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    // An empty parent ("") means the current directory; map to ".".
    let parent = if parent.as_os_str().is_empty() {
        std::path::Path::new(".")
    } else {
        parent
    };
    #[cfg(not(windows))]
    {
        match std::fs::File::open(parent) {
            Ok(dir) => match dir.sync_all() {
                Ok(()) => Ok(()),
                // EINVAL (22) — some filesystems (e.g. certain FUSE backends)
                // don't allow fsync on a directory fd. Treat as best-effort.
                Err(e) if e.raw_os_error() == Some(22) => Ok(()),
                Err(e) => Err(StorageError::Io(e)),
            },
            Err(e) => Err(StorageError::Io(e)),
        }
    }
    #[cfg(windows)]
    {
        let _ = parent;
        Ok(())
    }
}

pub static PHYSICAL_BLOCKS_READ: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

pub fn reset_physical_blocks_read() {
    PHYSICAL_BLOCKS_READ.store(0, std::sync::atomic::Ordering::SeqCst);
}

pub fn get_physical_blocks_read() -> u64 {
    PHYSICAL_BLOCKS_READ.load(std::sync::atomic::Ordering::SeqCst)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fsync_parent_dir_succeeds_on_existing_file() {
        // The contract isn't about an observable side-effect (a crash is the
        // only way to see the difference), only that the helper succeeds when
        // pointed at a real renamed file and tolerates parentless paths.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.bin");
        std::fs::write(&path, b"data").unwrap();
        fsync_parent_dir(&path).expect("fsync of parent dir should succeed");
        // No parent — should be a no-op rather than an error.
        fsync_parent_dir(std::path::Path::new("")).expect("empty path should be a no-op");
    }

    #[test]
    fn test_composite_key_ordering() {
        let k1 = DbKey::Composite(vec![DbKey::Int(10), DbKey::Int(1)]);
        let k2 = DbKey::Composite(vec![DbKey::Int(10), DbKey::Int(2)]);
        let k3 = DbKey::Composite(vec![DbKey::Int(9), DbKey::Int(20)]);
        let k4 = DbKey::Composite(vec![DbKey::Int(10)]);

        assert!(k1 < k2);
        assert!(k3 < k1);
        assert!(k4 < k1);
    }

    #[test]
    fn test_composite_key_byte_size() {
        let key = DbKey::Composite(vec![DbKey::Int(10), DbKey::String("hello".to_string())]);
        assert_eq!(key.byte_size(), 8 + 5);
    }

    #[test]
    fn test_dbvalue_nan_eq_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let nan1 = DbValue::Float(f64::NAN);
        let nan2 = DbValue::Float(f64::NAN);

        assert_eq!(nan1, nan2);

        let mut h1 = DefaultHasher::new();
        nan1.hash(&mut h1);
        let mut h2 = DefaultHasher::new();
        nan2.hash(&mut h2);

        assert_eq!(h1.finish(), h2.finish());

        let pos_zero = DbValue::Float(0.0);
        let neg_zero = DbValue::Float(-0.0);

        assert_eq!(pos_zero, neg_zero);

        let mut h3 = DefaultHasher::new();
        pos_zero.hash(&mut h3);
        let mut h4 = DefaultHasher::new();
        neg_zero.hash(&mut h4);

        assert_eq!(h3.finish(), h4.finish());
    }
}
