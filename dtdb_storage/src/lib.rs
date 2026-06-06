use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

/// Serializes a [`rust_decimal::Decimal`] as its decimal string. `rust_decimal`'s
/// default `Deserialize` relies on `deserialize_any`, which non-self-describing
/// formats such as `postcard` reject; stringifying keeps the value portable
/// across every serializer the engine uses (postcard spill files included).
pub mod decimal_serde {
    use rust_decimal::Decimal;
    use serde::{Deserialize, Deserializer, Serializer};
    use std::str::FromStr;

    pub fn serialize<S>(decimal: &Decimal, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&decimal.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Decimal::from_str(&s).map_err(serde::de::Error::custom)
    }
}

/// `Option<Decimal>` counterpart to [`decimal_serde`], for fields like the
/// aggregate accumulators' partial decimal sums that are spilled to disk via
/// postcard. Encodes as `Option<String>` so it never hits `deserialize_any`.
pub mod decimal_serde_option {
    use rust_decimal::Decimal;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::str::FromStr;

    pub fn serialize<S>(value: &Option<Decimal>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value.map(|d| d.to_string()).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Option::<String>::deserialize(deserializer)? {
            Some(s) => Decimal::from_str(&s)
                .map(Some)
                .map_err(serde::de::Error::custom),
            None => Ok(None),
        }
    }
}

pub mod block_cache;
pub mod bloom;
pub mod engine;
pub mod executor;
pub mod framed_log;
pub mod manifest;
pub mod memtable;
pub mod merge_iter;
pub mod scan_iter;
pub mod snapshot_log;
pub mod sstable;
pub mod wal;
pub use block_cache::{BlockCache, LruCache};
pub use bloom::BloomFilter;
pub use engine::{StorageEngine, StorageEngineStatistics};
pub use executor::{
    CoalesceKey, Executor, ExecutorConfig, InlineExecutor, MIN_WORKER_THREADS, PeriodicHandle,
    Priority, ThreadPoolExecutor, default_executor,
};
pub use framed_log::{FramedLog, LogFormat};
pub use manifest::{Manifest, ManifestEdit};
pub use scan_iter::ScanIterator;
pub use snapshot_log::{SnapshotLog, Snapshotable};
pub use wal::WalEntry;

pub trait ValueRewriter: Send + Sync {
    fn rewrite(&self, src_layout: &[u8], dst_layout: &[u8], value: &DbValue) -> Result<DbValue>;
}

#[derive(Debug, Clone)]
pub struct PassthroughValueRewriter;

impl ValueRewriter for PassthroughValueRewriter {
    fn rewrite(&self, _src_layout: &[u8], _dst_layout: &[u8], value: &DbValue) -> Result<DbValue> {
        Ok(value.clone())
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
    Serialization(#[from] postcard::Error),

    #[error("Lz4 compression error: {0}")]
    Compression(String),

    #[error("Database corrupted: {0}")]
    Corruption(String),

    #[error("Table directory already exists: {0}")]
    AlreadyExists(String),

    #[error("Invalid engine options: {0}")]
    InvalidOptions(String),
}

/// DbKey represents strongly typed keys in the database.
///
/// Heap-allocated payloads are wrapped in `Arc` so that snapshotting a key
/// (e.g. when a scan iterator clones the memtable range) is a refcount bump
/// rather than a deep buffer copy. `Arc<T>` delegates `Ord`/`PartialOrd`/`Eq`/
/// `Hash` to the pointee, so ordering and equality semantics are by *value*,
/// not by pointer — `BTreeMap` correctness is unaffected.
///
/// We derive `PartialOrd` and `Ord` so keys can be sorted and stored in `BTreeMap`.
/// Rust's derived ordering compares variants in the order they are declared:
/// `Int` comes before `String`. Within a variant, it compares the inner data.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DbKey {
    Int(i64),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    Timestamp(chrono::NaiveDateTime),
    #[serde(with = "decimal_serde")]
    Decimal(rust_decimal::Decimal),
    String(Arc<str>),
    Bool(bool),
    Composite(Arc<Vec<DbKey>>),
}

impl DbKey {
    /// Construct a `String` key from anything convertible into `Arc<str>`
    /// (`String`, `&str`, `Box<str>`, `Arc<str>`).
    pub fn string(s: impl Into<Arc<str>>) -> Self {
        DbKey::String(s.into())
    }

    /// Construct a `Composite` key from anything convertible into
    /// `Arc<Vec<DbKey>>` (`Vec<DbKey>`, `Arc<Vec<DbKey>>`).
    pub fn composite(keys: impl Into<Arc<Vec<DbKey>>>) -> Self {
        DbKey::Composite(keys.into())
    }

    pub fn byte_size(&self) -> usize {
        match self {
            DbKey::Int(_) => 8,
            DbKey::Date(_) => 4,
            DbKey::Time(_) => 8,
            DbKey::Timestamp(_) => 8,
            DbKey::Decimal(_) => 16,
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
    String(Arc<str>),
    Bytes(Arc<[u8]>),
    Bool(bool),
    Null,
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    Timestamp(chrono::NaiveDateTime),
    #[serde(with = "decimal_serde")]
    Decimal(rust_decimal::Decimal),
}

impl DbValue {
    /// Construct a `String` value from anything convertible into `Arc<str>`
    /// (`String`, `&str`, `Box<str>`, `Arc<str>`).
    pub fn string(s: impl Into<Arc<str>>) -> Self {
        DbValue::String(s.into())
    }

    /// Construct a `Bytes` value from anything convertible into `Arc<[u8]>`
    /// (`Vec<u8>`, `&[u8]`, `Arc<[u8]>`).
    pub fn bytes(b: impl Into<Arc<[u8]>>) -> Self {
        DbValue::Bytes(b.into())
    }

    /// Estimated uncompressed byte size of the value's payload. Used as the
    /// common heuristic behind memtable flush thresholds, block-cache
    /// accounting, and SSTable target sizing, so all those sites agree.
    pub fn byte_size(&self) -> usize {
        match self {
            DbValue::Int(_) => 8,
            DbValue::Float(_) => 8,
            DbValue::String(s) => s.len(),
            DbValue::Bytes(b) => b.len(),
            DbValue::Bool(_) => 1,
            DbValue::Null => 1,
            DbValue::Date(_) => 4,
            DbValue::Time(_) => 8,
            DbValue::Timestamp(_) => 8,
            DbValue::Decimal(_) => 16,
        }
    }
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
            (DbValue::Date(l), DbValue::Date(r)) => l == r,
            (DbValue::Time(l), DbValue::Time(r)) => l == r,
            (DbValue::Timestamp(l), DbValue::Timestamp(r)) => l == r,
            (DbValue::Decimal(l), DbValue::Decimal(r)) => l == r,
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
            DbValue::Date(v) => {
                6u8.hash(state);
                v.hash(state);
            }
            DbValue::Time(v) => {
                7u8.hash(state);
                v.hash(state);
            }
            DbValue::Timestamp(v) => {
                8u8.hash(state);
                v.hash(state);
            }
            DbValue::Decimal(v) => {
                9u8.hash(state);
                v.hash(state);
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

/// Upper bounds for sanity-checking level configuration. These are far above
/// any reasonable value; they exist to reject absurd configs that would
/// otherwise produce pathological compaction behavior or arithmetic overflow.
const MAX_LEVELS: usize = 64;
const MAX_LEVEL_SIZE_MULTIPLIER: usize = 1024;

impl EngineOptions {
    /// Rejects option values that would lead to panics or degenerate behavior:
    /// a zero-capacity block cache (`block_size_limit == 0` with caching on),
    /// zero-sized blocks/memtable/WAL, compaction that never makes progress, or
    /// level sizing that overflows. Called when a `StorageEngine` is opened.
    pub fn validate(&self) -> Result<()> {
        fn require(cond: bool, msg: impl Into<String>) -> Result<()> {
            if cond {
                Ok(())
            } else {
                Err(StorageError::InvalidOptions(msg.into()))
            }
        }

        // Positive lower bounds. A zero `block_size_limit` is the panic the
        // block cache hits (LruCache requires capacity > 0); the rest avoid
        // degenerate "flush/compact on every write" behavior.
        require(
            self.memtable_size_limit > 0,
            "memtable_size_limit must be > 0",
        )?;
        require(self.block_size_limit > 0, "block_size_limit must be > 0")?;
        require(self.wal_size_limit > 0, "wal_size_limit must be > 0")?;
        require(
            self.sstable_target_size > 0,
            "sstable_target_size must be > 0",
        )?;
        require(
            self.base_level_size_limit > 0,
            "base_level_size_limit must be > 0",
        )?;
        require(
            self.l0_compaction_threshold > 0,
            "l0_compaction_threshold must be > 0",
        )?;

        // Level sizing bounds. `level_size_multiplier` feeds a `pow()` over the
        // levels, so both it and `max_level` are bounded to keep the result
        // sane and overflow-free.
        require(
            (1..=MAX_LEVELS).contains(&self.max_level),
            format!("max_level must be between 1 and {MAX_LEVELS}"),
        )?;
        require(
            (1..=MAX_LEVEL_SIZE_MULTIPLIER).contains(&self.level_size_multiplier),
            format!("level_size_multiplier must be between 1 and {MAX_LEVEL_SIZE_MULTIPLIER}"),
        )?;

        Ok(())
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
        DbValue::string(v)
    }
}

impl From<bool> for DbValue {
    fn from(v: bool) -> Self {
        DbValue::Bool(v)
    }
}

impl<'a> From<&'a str> for DbValue {
    fn from(v: &'a str) -> Self {
        DbValue::string(v)
    }
}

impl From<Vec<u8>> for DbValue {
    fn from(v: Vec<u8>) -> Self {
        DbValue::bytes(v)
    }
}

impl<'a> From<&'a [u8]> for DbValue {
    fn from(v: &'a [u8]) -> Self {
        DbValue::bytes(v)
    }
}

impl From<chrono::NaiveDate> for DbValue {
    fn from(v: chrono::NaiveDate) -> Self {
        DbValue::Date(v)
    }
}

impl From<chrono::NaiveTime> for DbValue {
    fn from(v: chrono::NaiveTime) -> Self {
        DbValue::Time(v)
    }
}

impl From<chrono::NaiveDateTime> for DbValue {
    fn from(v: chrono::NaiveDateTime) -> Self {
        DbValue::Timestamp(v)
    }
}

impl From<rust_decimal::Decimal> for DbValue {
    fn from(v: rust_decimal::Decimal) -> Self {
        DbValue::Decimal(v)
    }
}

/// fsync the parent directory of `path` so that a preceding rename of a child
/// is durable across crashes on filesystems (ext4 data=ordered, XFS, ZFS, many
/// network FSes) that don't otherwise guarantee directory-entry persistence.
///
/// `method` selects the fsync strength (matching the file-data sync of the
/// rename it makes durable), so a store configured for plain `fsync` does not
/// silently pay `F_FULLFSYNC` on the directory.
///
/// Best-effort: on Windows there is no equivalent and we treat the call as a
/// no-op; everywhere else we silently ignore EINVAL from filesystems whose
/// directory fds can't be fsynced.
pub fn fsync_parent_dir(path: &std::path::Path, method: FsyncMethod) -> Result<()> {
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
            Ok(dir) => match sync_file(&dir, method) {
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
        let _ = (parent, method);
        Ok(())
    }
}

/// Atomically replaces the file at `path` with `bytes`.
///
/// Writes the new contents to a sibling temp file, fsyncs that temp file,
/// renames it over `path`, then fsyncs the parent directory so the rename
/// itself is durable. After a crash the file is either the complete previous
/// contents or the complete new contents — never a torn or zero-length mix.
///
/// This is the shared "snapshot replacement" primitive for the small,
/// full-rewrite metadata files (statistics, schema, options, and the snapshot
/// half of the manifest). Fsyncing the temp file *before* the rename matters:
/// on some filesystems a rename can otherwise become durable ahead of the data
/// it points at, exposing a zero-length file after a crash.
pub fn atomic_write(path: &std::path::Path, bytes: &[u8], fsync_method: FsyncMethod) -> Result<()> {
    // Hidden, path-derived temp name: concurrent writers of *different* files
    // never collide, and the temp doesn't surface as a stray directory entry.
    let tmp_path = match path.file_name() {
        Some(name) => {
            let mut tmp_name = std::ffi::OsString::from(".");
            tmp_name.push(name);
            tmp_name.push(".tmp");
            path.with_file_name(tmp_name)
        }
        None => path.with_extension("tmp"),
    };

    {
        let mut file = std::fs::File::create(&tmp_path)?;
        std::io::Write::write_all(&mut file, bytes)?;
        sync_file(&file, fsync_method)?;
    }
    std::fs::rename(&tmp_path, path)?;
    fsync_parent_dir(path, fsync_method)?;
    Ok(())
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
    fn test_default_options_validate() {
        EngineOptions::default()
            .validate()
            .expect("default options must be valid");
    }

    #[test]
    fn test_validate_rejects_panic_inducing_options() {
        // The original panic: caching enabled with zero-sized blocks drives the
        // block cache to a zero capacity and trips an assert in LruCache::new.
        let reject = |name: &str, opts: EngineOptions| match opts.validate() {
            Err(StorageError::InvalidOptions(_)) => {}
            other => panic!("expected InvalidOptions for {name}, got {other:?}"),
        };
        let base = EngineOptions::default;

        reject(
            "block_size_limit",
            EngineOptions {
                block_size_limit: 0,
                ..base()
            },
        );
        reject(
            "memtable_size_limit",
            EngineOptions {
                memtable_size_limit: 0,
                ..base()
            },
        );
        reject(
            "wal_size_limit",
            EngineOptions {
                wal_size_limit: 0,
                ..base()
            },
        );
        reject(
            "sstable_target_size",
            EngineOptions {
                sstable_target_size: 0,
                ..base()
            },
        );
        reject(
            "base_level_size_limit",
            EngineOptions {
                base_level_size_limit: 0,
                ..base()
            },
        );
        reject(
            "l0_compaction_threshold",
            EngineOptions {
                l0_compaction_threshold: 0,
                ..base()
            },
        );
        reject(
            "max_level=0",
            EngineOptions {
                max_level: 0,
                ..base()
            },
        );
        reject(
            "max_level too large",
            EngineOptions {
                max_level: MAX_LEVELS + 1,
                ..base()
            },
        );
        reject(
            "multiplier=0",
            EngineOptions {
                level_size_multiplier: 0,
                ..base()
            },
        );
        reject(
            "multiplier too large",
            EngineOptions {
                level_size_multiplier: MAX_LEVEL_SIZE_MULTIPLIER + 1,
                ..base()
            },
        );
    }

    #[test]
    fn test_open_rejects_invalid_options_without_panicking() {
        let dir = tempfile::TempDir::new().unwrap();
        let opts = EngineOptions {
            block_size_limit: 0, // would panic the block cache without validation
            ..EngineOptions::default()
        };
        match StorageEngine::open(dir.path(), opts) {
            Err(StorageError::InvalidOptions(_)) => {}
            Err(other) => panic!("expected InvalidOptions, got {other:?}"),
            Ok(_) => panic!("expected InvalidOptions, got Ok"),
        }
    }

    #[test]
    fn test_fsync_parent_dir_succeeds_on_existing_file() {
        // The contract isn't about an observable side-effect (a crash is the
        // only way to see the difference), only that the helper succeeds when
        // pointed at a real renamed file and tolerates parentless paths.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.bin");
        std::fs::write(&path, b"data").unwrap();
        fsync_parent_dir(&path, FsyncMethod::Fsync).expect("fsync of parent dir should succeed");
        // No parent — should be a no-op rather than an error.
        fsync_parent_dir(std::path::Path::new(""), FsyncMethod::Fsync)
            .expect("empty path should be a no-op");
    }

    #[test]
    fn test_atomic_write_creates_and_overwrites() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("statistics.bin");

        atomic_write(&path, b"first", FsyncMethod::Fullfsync).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"first");

        // Overwriting replaces the contents wholesale.
        atomic_write(&path, b"second-and-longer", FsyncMethod::Fullfsync).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"second-and-longer");

        // The temp file must not linger after a successful write.
        let temp = dir.path().join(".statistics.bin.tmp");
        assert!(
            !temp.exists(),
            "temp file should be renamed away, not left behind"
        );
    }

    #[test]
    fn test_composite_key_ordering() {
        let k1 = DbKey::composite(vec![DbKey::Int(10), DbKey::Int(1)]);
        let k2 = DbKey::composite(vec![DbKey::Int(10), DbKey::Int(2)]);
        let k3 = DbKey::composite(vec![DbKey::Int(9), DbKey::Int(20)]);
        let k4 = DbKey::composite(vec![DbKey::Int(10)]);

        assert!(k1 < k2);
        assert!(k3 < k1);
        assert!(k4 < k1);
    }

    #[test]
    fn test_composite_key_byte_size() {
        let key = DbKey::composite(vec![DbKey::Int(10), DbKey::string("hello")]);
        assert_eq!(key.byte_size(), 8 + 5);
    }

    #[test]
    fn test_fsync_parent_dir_relative_path_maps_to_cwd() {
        // A bare filename has an empty ("") parent, which the helper maps to
        // ".". This exercises the empty-parent branch without a crash test.
        // The current directory always exists, so the fsync should succeed.
        fsync_parent_dir(
            std::path::Path::new("some_relative_file.bin"),
            FsyncMethod::Fsync,
        )
        .expect("relative path should fsync the current directory");
    }

    #[test]
    fn test_dbkey_byte_size_all_variants() {
        assert_eq!(DbKey::Int(0).byte_size(), 8);
        assert_eq!(DbKey::string("abcd").byte_size(), 4);
        assert_eq!(DbKey::Bool(true).byte_size(), 1);
        assert_eq!(DbKey::string("").byte_size(), 0);
    }

    #[test]
    fn test_dbvalue_byte_size_all_variants() {
        // The single source of truth for value byte accounting, shared by the
        // memtable, block cache, SSTable writer, and compaction loop.
        use chrono::{NaiveDate, NaiveTime};
        use rust_decimal::Decimal;

        assert_eq!(DbValue::Int(0).byte_size(), 8);
        assert_eq!(DbValue::Float(1.0).byte_size(), 8);
        assert_eq!(DbValue::string("abcde").byte_size(), 5);
        assert_eq!(DbValue::bytes(vec![0u8; 7]).byte_size(), 7);
        assert_eq!(DbValue::Bool(true).byte_size(), 1);
        assert_eq!(DbValue::Null.byte_size(), 1);
        assert_eq!(
            DbValue::Date(NaiveDate::from_ymd_opt(2026, 6, 4).unwrap()).byte_size(),
            4
        );
        assert_eq!(
            DbValue::Time(NaiveTime::from_hms_opt(1, 2, 3).unwrap()).byte_size(),
            8
        );
        assert_eq!(
            DbValue::Timestamp(
                NaiveDate::from_ymd_opt(2026, 6, 4)
                    .unwrap()
                    .and_hms_opt(1, 2, 3)
                    .unwrap()
            )
            .byte_size(),
            8
        );
        assert_eq!(DbValue::Decimal(Decimal::new(150, 2)).byte_size(), 16);
    }

    #[test]
    fn test_dbvalue_partial_eq_variants() {
        // Matching variants compare their inner data.
        assert_eq!(DbValue::Int(7), DbValue::Int(7));
        assert_ne!(DbValue::Int(7), DbValue::Int(8));
        assert_eq!(DbValue::String("x".into()), DbValue::String("x".into()));
        assert_ne!(DbValue::String("x".into()), DbValue::String("y".into()));
        assert_eq!(DbValue::bytes(vec![1, 2]), DbValue::bytes(vec![1, 2]));
        assert_ne!(DbValue::bytes(vec![1, 2]), DbValue::bytes(vec![1, 3]));
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
            hash_of(&DbValue::bytes(vec![9, 8])),
            hash_of(&DbValue::bytes(vec![9, 8]))
        );
        assert_eq!(hash_of(&DbValue::Bool(true)), hash_of(&DbValue::Bool(true)));
        assert_eq!(hash_of(&DbValue::Null), hash_of(&DbValue::Null));

        // The variant tag is mixed in, so identical inner bytes in different
        // variants don't collide trivially.
        assert_ne!(hash_of(&DbValue::Bool(false)), hash_of(&DbValue::Null));

        // A non-zero, non-NaN float hashes by its bit pattern.
        assert_eq!(hash_of(&DbValue::Float(3.5)), hash_of(&DbValue::Float(3.5)));

        // Temporal and decimal variants hash equal to themselves and carry a
        // distinct variant tag so they don't collide with each other.
        let date = DbValue::Date(chrono::NaiveDate::from_ymd_opt(2026, 6, 4).unwrap());
        let time = DbValue::Time(chrono::NaiveTime::from_hms_opt(1, 2, 3).unwrap());
        let ts = DbValue::Timestamp(
            chrono::NaiveDate::from_ymd_opt(2026, 6, 4)
                .unwrap()
                .and_hms_opt(1, 2, 3)
                .unwrap(),
        );
        let dec = DbValue::Decimal(rust_decimal::Decimal::new(150, 2));
        for v in [&date, &time, &ts, &dec] {
            assert_eq!(hash_of(v), hash_of(v));
        }
        assert_ne!(hash_of(&date), hash_of(&time));
        assert_ne!(hash_of(&ts), hash_of(&dec));
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
            DbValue::bytes(vec![1, 2, 3])
        );
        let bytes: &[u8] = &[7, 8];
        assert_eq!(DbValue::from(bytes), DbValue::bytes(vec![7, 8]));

        let date = chrono::NaiveDate::from_ymd_opt(2026, 6, 4).unwrap();
        assert_eq!(DbValue::from(date), DbValue::Date(date));
        let time = chrono::NaiveTime::from_hms_opt(1, 2, 3).unwrap();
        assert_eq!(DbValue::from(time), DbValue::Time(time));
        let ts = date.and_hms_opt(1, 2, 3).unwrap();
        assert_eq!(DbValue::from(ts), DbValue::Timestamp(ts));
        let dec = rust_decimal::Decimal::new(150, 2);
        assert_eq!(DbValue::from(dec), DbValue::Decimal(dec));
    }

    #[test]
    fn test_passthrough_rewriter_returns_value_unchanged() {
        // The default rewriter ignores both layouts and returns the value
        // verbatim — even when the layouts differ.
        let rw = PassthroughValueRewriter;
        let v = DbValue::string("payload");
        assert_eq!(rw.rewrite(&[], &[1, 2], &v).unwrap(), v);
        assert_eq!(
            rw.rewrite(&[3], &[3], &DbValue::Int(7)).unwrap(),
            DbValue::Int(7)
        );
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

    // ----- Arc-payload invariants (ADR 0004) ---------------------------------

    /// Cloning a `DbValue::String`/`Bytes` must share the underlying buffer via
    /// the `Arc` refcount rather than deep-copying it. This is the core
    /// performance invariant of ADR 0004: if a future change reintroduces a
    /// deep copy on this path (e.g. a stray `to_string()`), `ptr_eq` fails here.
    #[test]
    fn test_dbvalue_clone_shares_arc_buffer() {
        let v = DbValue::string("a reasonably long string payload to copy");
        let DbValue::String(orig) = &v else {
            unreachable!()
        };
        assert_eq!(Arc::strong_count(orig), 1);

        let cloned = v.clone();
        let DbValue::String(copy) = &cloned else {
            unreachable!()
        };
        // Same allocation (no deep copy) and the refcount went up.
        assert!(Arc::ptr_eq(orig, copy));
        assert_eq!(Arc::strong_count(orig), 2);

        let b = DbValue::bytes(vec![0u8; 256]);
        let DbValue::Bytes(orig_b) = &b else {
            unreachable!()
        };
        let cloned_b = b.clone();
        let DbValue::Bytes(copy_b) = &cloned_b else {
            unreachable!()
        };
        assert!(Arc::ptr_eq(orig_b, copy_b));
    }

    /// The same sharing must hold for keys, including the composite vec, so that
    /// snapshotting a scan range is a set of refcount bumps.
    #[test]
    fn test_dbkey_clone_shares_arc_buffer() {
        let k = DbKey::composite(vec![DbKey::Int(1), DbKey::string("payload")]);
        let DbKey::Composite(orig) = &k else {
            unreachable!()
        };
        let cloned = k.clone();
        let DbKey::Composite(copy) = &cloned else {
            unreachable!()
        };
        assert!(Arc::ptr_eq(orig, copy));
    }

    /// `Arc<T>` delegates ordering/equality to the pointee, so wrapping the
    /// payloads in `Arc` must not change key ordering — the entire `BTreeMap`
    /// store depends on by-value comparison, never pointer comparison.
    #[test]
    fn test_arc_keys_order_by_value_not_pointer() {
        // Distinct allocations with equal contents compare equal.
        let a = DbKey::string("dup");
        let b = DbKey::string(String::from("dup"));
        if let (DbKey::String(sa), DbKey::String(sb)) = (&a, &b) {
            assert!(!Arc::ptr_eq(sa, sb), "intended distinct allocations");
        }
        assert_eq!(a, b);

        // Variant order (Int < String) and within-variant ordering hold.
        assert!(DbKey::Int(i64::MAX) < DbKey::string(""));
        assert!(DbKey::string("apple") < DbKey::string("banana"));
        assert!(
            DbKey::composite(vec![DbKey::Int(1), DbKey::string("a")])
                < DbKey::composite(vec![DbKey::Int(1), DbKey::string("b")])
        );

        // Usable as a BTreeMap key with correct sorted iteration order.
        let mut map = std::collections::BTreeMap::new();
        map.insert(DbKey::string("c"), 3);
        map.insert(DbKey::string("a"), 1);
        map.insert(DbKey::string("b"), 2);
        let ordered: Vec<i32> = map.values().copied().collect();
        assert_eq!(ordered, vec![1, 2, 3]);
    }

    /// ADR 0004 claims the on-disk/wire format is unchanged: `Arc<str>` and
    /// `Arc<[u8]>` must serialize byte-for-byte identically to `String`/`Vec<u8>`
    /// (serde `rc` feature), so existing SSTables/WAL stay readable with no
    /// migration. This test makes that claim enforceable.
    #[test]
    fn test_arc_payload_serializes_identically_to_owned() {
        let arc_str: Arc<str> = Arc::from("héllo, wörld");
        let owned_str: String = "héllo, wörld".to_string();
        assert_eq!(
            postcard::to_allocvec(&arc_str).unwrap(),
            postcard::to_allocvec(&owned_str).unwrap(),
        );

        let arc_bytes: Arc<[u8]> = Arc::from(&[0u8, 1, 2, 250, 255][..]);
        let owned_bytes: Vec<u8> = vec![0, 1, 2, 250, 255];
        assert_eq!(
            postcard::to_allocvec(&arc_bytes).unwrap(),
            postcard::to_allocvec(&owned_bytes).unwrap(),
        );
    }

    /// Every `DbKey`/`DbValue` variant must survive a postcard round-trip
    /// unchanged (confirms the serde `rc` feature is wired up correctly). The
    /// `Decimal` variants additionally exercise the custom `decimal_serde`
    /// (string-backed) codec on both `DbKey` and `DbValue`.
    #[test]
    fn test_dbkey_dbvalue_postcard_roundtrip() {
        use rust_decimal::Decimal;
        use std::str::FromStr;
        let date = chrono::NaiveDate::from_ymd_opt(2026, 6, 2).unwrap();
        let time = chrono::NaiveTime::from_hms_opt(12, 34, 56).unwrap();
        let ts = date.and_hms_opt(12, 34, 56).unwrap();
        let dec = Decimal::from_str("123.45").unwrap();

        let keys = [
            DbKey::Int(-42),
            DbKey::string("key"),
            DbKey::Bool(true),
            DbKey::Date(date),
            DbKey::Time(time),
            DbKey::Timestamp(ts),
            DbKey::Decimal(dec),
            DbKey::composite(vec![DbKey::Int(1), DbKey::Decimal(dec)]),
        ];
        for k in keys {
            let back: DbKey = postcard::from_bytes(&postcard::to_allocvec(&k).unwrap()).unwrap();
            assert_eq!(k, back);
        }

        let values = [
            DbValue::Int(7),
            DbValue::Float(2.5),
            DbValue::string("value"),
            DbValue::bytes(vec![9, 8, 7]),
            DbValue::Bool(false),
            DbValue::Null,
            DbValue::Date(date),
            DbValue::Time(time),
            DbValue::Timestamp(ts),
            DbValue::Decimal(dec),
        ];
        for v in values {
            let back: DbValue = postcard::from_bytes(&postcard::to_allocvec(&v).unwrap()).unwrap();
            assert_eq!(v, back);
        }
    }

    /// `decimal_serde` stringifies the value, so trailing-zero scale must be
    /// preserved across the round-trip (e.g. `1.50` must not come back as `1.5`).
    /// Scale matters because it is user-visible in projected results.
    #[test]
    fn test_decimal_serde_preserves_scale() {
        use rust_decimal::Decimal;
        use std::str::FromStr;
        for s in ["1.50", "0.000", "-42.0", "100"] {
            let d = Decimal::from_str(s).unwrap();
            let back: Decimal =
                postcard::from_bytes(&postcard::to_allocvec(&DbValue::Decimal(d)).unwrap())
                    .map(|v: DbValue| match v {
                        DbValue::Decimal(x) => x,
                        other => panic!("expected Decimal, got {:?}", other),
                    })
                    .unwrap();
            assert_eq!(back.to_string(), s, "scale lost for {}", s);
        }
    }

    /// A single-column index over a `Decimal` orders entries by the in-memory
    /// `DbKey::cmp` (numeric `Ord`), NOT by the string serialization. Range
    /// scans would be wrong if `"123"` sorted after `"9"` lexicographically, so
    /// this pins the numeric ordering the LSM range scans depend on.
    #[test]
    fn test_decimal_dbkey_orders_numerically() {
        use rust_decimal::Decimal;
        use std::str::FromStr;
        let d = |s: &str| DbKey::Decimal(Decimal::from_str(s).unwrap());

        // Lexicographic order would put "123" < "9"; numeric order must not.
        assert!(d("9") < d("123"));
        assert!(d("-5") < d("0"));
        assert!(d("1.5") < d("1.50001"));

        // Sorted iteration through a BTreeMap (the memtable's store) is numeric.
        let mut map = std::collections::BTreeMap::new();
        for s in ["9", "123", "0", "-5", "42"] {
            map.insert(d(s), s);
        }
        let ordered: Vec<&str> = map.values().copied().collect();
        assert_eq!(ordered, vec!["-5", "0", "9", "42", "123"]);
    }
}
