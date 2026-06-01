use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

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
    fsync_parent_dir(path)?;
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
        fsync_parent_dir(std::path::Path::new("some_relative_file.bin"))
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
            bincode::serialize(&arc_str).unwrap(),
            bincode::serialize(&owned_str).unwrap(),
        );

        let arc_bytes: Arc<[u8]> = Arc::from(&[0u8, 1, 2, 250, 255][..]);
        let owned_bytes: Vec<u8> = vec![0, 1, 2, 250, 255];
        assert_eq!(
            bincode::serialize(&arc_bytes).unwrap(),
            bincode::serialize(&owned_bytes).unwrap(),
        );
    }

    /// Every `DbKey`/`DbValue` variant must survive a bincode round-trip
    /// unchanged (confirms the serde `rc` feature is wired up correctly).
    #[test]
    fn test_dbkey_dbvalue_bincode_roundtrip() {
        let keys = [
            DbKey::Int(-42),
            DbKey::string("key"),
            DbKey::Bool(true),
            DbKey::composite(vec![DbKey::Int(1), DbKey::string("x")]),
        ];
        for k in keys {
            let back: DbKey = bincode::deserialize(&bincode::serialize(&k).unwrap()).unwrap();
            assert_eq!(k, back);
        }

        let values = [
            DbValue::Int(7),
            DbValue::Float(2.5),
            DbValue::string("value"),
            DbValue::bytes(vec![9, 8, 7]),
            DbValue::Bool(false),
            DbValue::Null,
        ];
        for v in values {
            let back: DbValue = bincode::deserialize(&bincode::serialize(&v).unwrap()).unwrap();
            assert_eq!(v, back);
        }
    }
}
