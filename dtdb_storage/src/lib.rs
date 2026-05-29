use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod block_cache;
pub mod bloom;
pub mod engine;
pub mod executor;
pub mod manifest;
pub mod memtable;
pub mod merge_iter;
pub mod scan_iter;
pub mod sstable;
pub mod wal;
pub use block_cache::{BlockCache, LruCache};
pub use bloom::BloomFilter;
pub use engine::{StorageEngine, StorageEngineStatistics};
pub use executor::{
    CoalesceKey, Executor, ExecutorConfig, InlineExecutor, MIN_WORKER_THREADS, PeriodicHandle,
    Priority, ThreadPoolExecutor, default_executor,
};
pub use scan_iter::ScanIterator;
pub use wal::WalEntry;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum FsyncMethod {
    #[default]
    Fsync,
    Fullfsync,
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
    #[serde(default)]
    pub fsync_method: FsyncMethod,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            compression: CompressionType::Lz4,
            memtable_size_limit: 8 * 1024 * 1024,
            block_size_limit: 4096,
            wal_size_limit: 32 * 1024 * 1024,
            l0_compaction_threshold: 4,
            sstable_target_size: 2 * 1024 * 1024,
            base_level_size_limit: 10 * 1024 * 1024,
            level_size_multiplier: 10,
            max_level: 7,
            block_cache_capacity: 1000,
            wal_sync_interval_ms: None,
            fsync_method: FsyncMethod::default(),
        }
    }
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

/// Synchronizes file data to disk using the selected fsync method.
pub fn sync_file(file: &std::fs::File, method: FsyncMethod) -> std::io::Result<()> {
    match method {
        FsyncMethod::Fsync => {
            #[cfg(unix)]
            {
                use std::os::unix::io::AsRawFd;
                let fd = file.as_raw_fd();
                unsafe extern "C" {
                    fn fsync(fd: std::os::raw::c_int) -> std::os::raw::c_int;
                }
                let res = unsafe { fsync(fd) };
                if res == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            }
            #[cfg(not(unix))]
            {
                file.sync_all()
            }
        }
        FsyncMethod::Fullfsync => file.sync_all(),
    }
}

// Counts physical SSTable block reads (cache misses) for the *current thread*.
//
// This is thread-local rather than a process-global atomic so that tests
// asserting on block-read counts (e.g. locality-group pruning) are not
// polluted by reads performed concurrently on other threads — both parallel
// test cases and background compaction/analyze threads. Since query reads
// happen synchronously on the calling thread, a thread-local correctly
// attributes them to the query under measurement.
thread_local! {
    static PHYSICAL_BLOCKS_READ: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Records that one physical SSTable block was read (cache miss) on the
/// current thread.
pub fn record_physical_block_read() {
    PHYSICAL_BLOCKS_READ.with(|c| c.set(c.get() + 1));
}

/// Resets the current thread's physical block-read counter to zero.
pub fn reset_physical_blocks_read() {
    PHYSICAL_BLOCKS_READ.with(|c| c.set(0));
}

/// Returns the number of physical block reads recorded on the current thread
/// since the last reset.
pub fn get_physical_blocks_read() -> u64 {
    PHYSICAL_BLOCKS_READ.with(|c| c.get())
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
    fn test_fsync_parent_dir_relative_path_maps_to_cwd() {
        // A bare filename has an empty ("") parent, which the helper maps to
        // ".". This exercises the empty-parent branch without a crash test.
        // The current directory always exists, so the fsync should succeed.
        fsync_parent_dir(std::path::Path::new("some_relative_file.bin"))
            .expect("relative path should fsync the current directory");
    }

    #[test]
    fn test_dbkey_byte_size_all_variants() {
        assert_eq!(DbKey::Int(0).byte_size(), 8);
        assert_eq!(DbKey::String("abcd".to_string()).byte_size(), 4);
        assert_eq!(DbKey::Bool(true).byte_size(), 1);
        assert_eq!(DbKey::String(String::new()).byte_size(), 0);
    }

    #[test]
    fn test_dbvalue_partial_eq_variants() {
        // Matching variants compare their inner data.
        assert_eq!(DbValue::Int(7), DbValue::Int(7));
        assert_ne!(DbValue::Int(7), DbValue::Int(8));
        assert_eq!(DbValue::String("x".into()), DbValue::String("x".into()));
        assert_ne!(DbValue::String("x".into()), DbValue::String("y".into()));
        assert_eq!(DbValue::Bytes(vec![1, 2]), DbValue::Bytes(vec![1, 2]));
        assert_ne!(DbValue::Bytes(vec![1, 2]), DbValue::Bytes(vec![1, 3]));
        assert_eq!(DbValue::Bool(true), DbValue::Bool(true));
        assert_ne!(DbValue::Bool(true), DbValue::Bool(false));
        assert_eq!(DbValue::Null, DbValue::Null);
        assert_eq!(DbValue::Float(1.5), DbValue::Float(1.5));
        assert_ne!(DbValue::Float(1.5), DbValue::Float(2.5));

        // Mismatched variants are never equal (the `_ => false` arm).
        assert_ne!(DbValue::Int(1), DbValue::Bool(true));
        assert_ne!(DbValue::String("1".into()), DbValue::Int(1));
        assert_ne!(DbValue::Null, DbValue::Bool(false));
    }

    #[test]
    fn test_dbvalue_hash_distinguishes_variants() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        fn hash_of(v: &DbValue) -> u64 {
            let mut h = DefaultHasher::new();
            v.hash(&mut h);
            h.finish()
        }

        // Equal values hash equal across every variant.
        assert_eq!(hash_of(&DbValue::Int(42)), hash_of(&DbValue::Int(42)));
        assert_eq!(
            hash_of(&DbValue::String("k".into())),
            hash_of(&DbValue::String("k".into()))
        );
        assert_eq!(
            hash_of(&DbValue::Bytes(vec![9, 8])),
            hash_of(&DbValue::Bytes(vec![9, 8]))
        );
        assert_eq!(hash_of(&DbValue::Bool(true)), hash_of(&DbValue::Bool(true)));
        assert_eq!(hash_of(&DbValue::Null), hash_of(&DbValue::Null));

        // The variant tag is mixed in, so identical inner bytes in different
        // variants don't collide trivially.
        assert_ne!(hash_of(&DbValue::Bool(false)), hash_of(&DbValue::Null));

        // A non-zero, non-NaN float hashes by its bit pattern.
        assert_eq!(hash_of(&DbValue::Float(3.5)), hash_of(&DbValue::Float(3.5)));
    }

    #[test]
    fn test_dbvalue_from_conversions() {
        assert_eq!(DbValue::from(5i64), DbValue::Int(5));
        assert_eq!(DbValue::from(2.5f64), DbValue::Float(2.5));
        assert_eq!(
            DbValue::from("hi".to_string()),
            DbValue::String("hi".into())
        );
        assert_eq!(DbValue::from(true), DbValue::Bool(true));
        assert_eq!(DbValue::from("slice"), DbValue::String("slice".into()));
        assert_eq!(
            DbValue::from(vec![1u8, 2, 3]),
            DbValue::Bytes(vec![1, 2, 3])
        );
        let bytes: &[u8] = &[7, 8];
        assert_eq!(DbValue::from(bytes), DbValue::Bytes(vec![7, 8]));
    }

    #[test]
    fn test_physical_blocks_read_counter() {
        reset_physical_blocks_read();
        assert_eq!(get_physical_blocks_read(), 0);
        record_physical_block_read();
        record_physical_block_read();
        record_physical_block_read();
        assert_eq!(get_physical_blocks_read(), 3);
        reset_physical_blocks_read();
        assert_eq!(get_physical_blocks_read(), 0);
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
