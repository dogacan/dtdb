use crate::executor::{CoalesceKey, Executor, PeriodicHandle, Priority};
use crate::manifest::{MANIFEST_LOG_FORMAT, Manifest, ManifestEdit};
use crate::memtable::MemTable;
use crate::snapshot_log::SnapshotLog;
use crate::sstable::{SstableReader, SstableWriter};
use crate::wal::{Wal, WalEntry};
use crate::{DbKey, DbValue, EngineOptions, Result, ScanIterator, StorageError, ValueRewriter};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StorageEngineStatistics {
    pub num_sstables: usize,
    pub total_sstable_size: u64,
    pub sstable_entries: u64,
    pub sstable_tombstones: u64,
    pub sstable_uncompressed_bytes: u64,
    pub memtable_entries: u64,
    pub memtable_tombstones: u64,
    pub memtable_uncompressed_bytes: u64,
}

/// Compact the manifest's edit log into a fresh snapshot once it grows past
/// this size. Manifest edits are tiny, so this bounds the log's size without
/// compacting too eagerly.
const MANIFEST_COMPACT_THRESHOLD_BYTES: u64 = 1024 * 1024;

/// Pre-segmentation single-WAL filename. Still recovered on open (replayed
/// before any numbered segment) so older directories migrate transparently.
const LEGACY_WAL_NAME: &str = "active.wal";

/// On-disk name of WAL segment `id`. Each memtable (currently just the active
/// one; an immutable queue lands with background flush) owns exactly one
/// segment; the segment is retired once its memtable is durably flushed. The
/// zero-padded id keeps directory listings sorted lexicographically.
fn wal_segment_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("wal_{id:020}.wal"))
}

/// Parses a WAL segment id out of a path named `wal_<id>.wal`, or `None` if the
/// path is not a numbered segment (e.g. the legacy `active.wal`).
fn parse_wal_segment_id(path: &Path) -> Option<u64> {
    if path.extension()? != "wal" {
        return None;
    }
    path.file_stem()?
        .to_str()?
        .strip_prefix("wal_")?
        .parse::<u64>()
        .ok()
}

pub struct StorageEngine {
    inner: Arc<EngineInner>,
}

/// A sealed memtable awaiting (or undergoing) flush to L0, paired with the id of
/// the WAL segment that holds its writes. The segment is retired once the
/// memtable's data is durable in an SSTable. Cheap to clone (an `Arc` and a
/// `u64`), so a copy can sit in the queue while another is handed to the flush.
#[derive(Clone)]
struct ImmMemtable {
    table: Arc<MemTable>,
    wal_id: u64,
}

/// The in-memory write buffers: one mutable `active` memtable plus a queue of
/// `immutable` memtables that have been sealed and are awaiting flush to L0.
///
/// Writes always land in `active`. Reads merge newest-to-oldest: `active` first,
/// then `immutable` from front (newest) to back (oldest), then the on-disk
/// SSTables. Each sealed memtable corresponds to a sealed WAL segment, so the
/// queue mirrors the segments on disk that have not yet been retired.
///
/// Flush is currently synchronous (it runs inline under `write_mutex`), so
/// `immutable` holds at most one memtable and only transiently — between sealing
/// the active memtable and the flush completing. The queue nonetheless exists so
/// the read path already merges over sealed-but-unflushed memtables. Moving the
/// flush to a background thread (a later change) then needs no read-path work and
/// only has to let the queue grow up to `max_write_buffer_number - 1`.
struct MemSet {
    active: Arc<MemTable>,
    immutable: VecDeque<ImmMemtable>,
}

struct EngineInner {
    dir_path: PathBuf,
    memset: RwLock<MemSet>,
    wal: Mutex<Wal>,
    /// Id of the WAL segment that currently backs the active memtable. Updated
    /// under `write_mutex` when a flush rotates to a fresh segment.
    active_wal_id: AtomicU64,
    /// Allocates WAL segment ids. Independent of `next_sst_id` so the two
    /// lifecycles don't entangle. Initialized past the highest segment id seen
    /// on disk at open.
    next_wal_id: AtomicU64,
    sstables: RwLock<BTreeMap<usize, Vec<Arc<SstableReader>>>>,
    options: EngineOptions,
    write_mutex: Mutex<()>,
    compaction_mutex: Mutex<()>,
    /// The active-SSTable set, persisted as a snapshot + edit log. The mutex
    /// serializes manifest updates (and provides the `&mut` the log needs).
    manifest: Mutex<SnapshotLog<Manifest>>,
    next_sst_id: AtomicU64,
    executor: Arc<dyn Executor>,
    rewriter: Arc<dyn ValueRewriter>,
    target_layout: RwLock<Vec<u8>>,
    /// Keeps the background WAL-sync schedule alive; dropping the engine
    /// cancels it.
    wal_sync_handle: Mutex<Option<PeriodicHandle>>,
    block_cache: Option<Arc<crate::BlockCache>>,
    last_compacted_keys: Mutex<std::collections::HashMap<usize, DbKey>>,
    /// Lifecycle gate for background work (compaction, periodic WAL sync). See
    /// [`BackgroundGate`].
    background: BackgroundGate,
}

/// Tracks in-flight background work so the engine can be *quiesced*.
///
/// Background compaction is submitted to a process-wide, shared [`Executor`]
/// (see [`crate::default_executor`]) and captures an `Arc<EngineInner>`, so it
/// keeps running after the public [`StorageEngine`] handle is dropped. Because
/// compaction deletes SSTable files after recording manifest edits, a task
/// still running when the directory is reopened (or otherwise expected to be
/// quiesced) can delete a manifest-registered file out from under the new
/// open's SSTable discovery, surfacing as a spurious ENOENT.
///
/// The executor is shared across engines and exposes no per-engine drain, so
/// this gate provides that scope. [`EngineInner::quiesce`] flips
/// `shutting_down` and blocks until every task that had already begun finishes;
/// any task that had *not* yet begun observes the flag (via
/// [`EngineInner::enter_background`]) and returns without touching engine state.
/// Together these guarantee that once `quiesce` returns, no background task will
/// mutate engine state — including deleting files.
struct BackgroundGate {
    state: Mutex<BackgroundState>,
    /// Signalled when `active` reaches zero so `quiesce` can wake.
    idle: Condvar,
}

struct BackgroundState {
    /// Number of background tasks that have begun and not yet finished.
    active: usize,
    /// Once set, no new background task is allowed to begin.
    shutting_down: bool,
}

impl BackgroundGate {
    fn new() -> Self {
        BackgroundGate {
            state: Mutex::new(BackgroundState {
                active: 0,
                shutting_down: false,
            }),
            idle: Condvar::new(),
        }
    }
}

/// RAII guard marking a background task as in-flight. Decrements the active
/// count on drop (panic-safe), waking [`EngineInner::quiesce`] when it reaches
/// zero. Obtained from [`EngineInner::enter_background`].
struct BackgroundGuard<'a> {
    inner: &'a EngineInner,
}

impl Drop for BackgroundGuard<'_> {
    fn drop(&mut self) {
        let mut st = self.inner.background.state.lock().unwrap();
        st.active -= 1;
        if st.active == 0 {
            self.inner.background.idle.notify_all();
        }
    }
}

impl StorageEngine {
    /// Opens a StorageEngine directory.
    pub fn open(dir_path: impl AsRef<Path>, options: EngineOptions) -> Result<Self> {
        Self::open_with_executor(
            dir_path,
            options,
            crate::default_executor(),
            Arc::new(crate::PassthroughValueRewriter),
        )
    }

    /// Opens a StorageEngine directory backed by the given [`Executor`].
    pub fn open_with_executor(
        dir_path: impl AsRef<Path>,
        options: EngineOptions,
        executor: Arc<dyn Executor>,
        rewriter: Arc<dyn ValueRewriter>,
    ) -> Result<Self> {
        let inner = Arc::new(EngineInner::open(
            dir_path,
            options,
            executor.clone(),
            rewriter,
        )?);

        if let Some(ms) = inner.options.wal_sync_interval_ms
            && ms > 0
        {
            let inner_weak = Arc::downgrade(&inner);
            let handle = executor.submit_periodic(
                std::time::Duration::from_millis(ms),
                Priority::Normal,
                Box::new(move || {
                    if let Some(engine) = inner_weak.upgrade() {
                        // Honor shutdown: skip the sync once quiescing so no
                        // background task touches engine state after shutdown.
                        let Some(_bg) = engine.enter_background() else {
                            return;
                        };
                        if let Err(e) = engine.sync_wal() {
                            tracing::error!(error = ?e, "background WAL sync failed");
                        }
                    }
                }),
            );
            *inner.wal_sync_handle.lock().unwrap() = Some(handle);
        }

        Ok(Self { inner })
    }

    pub fn set_target_layout(&self, layout: Vec<u8>) {
        self.inner.set_target_layout(layout);
    }

    pub fn target_layout(&self) -> Vec<u8> {
        self.inner.target_layout()
    }

    pub fn put(&self, key: DbKey, value: DbValue) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: DbKey) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn write_batch(&self, entries: Vec<WalEntry>) -> Result<()> {
        self.inner.write_batch(entries)
    }

    pub fn get(&self, key: &DbKey) -> Result<Option<DbValue>> {
        self.inner.get(key)
    }

    pub fn multi_get(&self, keys: &[DbKey]) -> Result<Vec<Option<DbValue>>> {
        self.inner.multi_get(keys)
    }

    pub fn filtered_scan<F>(
        &self,
        start: &DbKey,
        end: &DbKey,
        filter: F,
    ) -> Result<Vec<(DbKey, DbValue)>>
    where
        F: Fn(&DbKey, &DbValue) -> bool,
    {
        self.inner.filtered_scan(start, end, filter)
    }

    pub fn scan_iter(&self, start: &DbKey, end: &DbKey) -> Result<ScanIterator> {
        self.inner.scan_iter(start, end)
    }

    pub fn flush_memtable(&self) -> Result<()> {
        self.inner.flush_memtable()
    }

    /// Forces the engine's WAL to be fsynced to disk. Used by callers that
    /// batch their own durability barrier (e.g. the relational layer's
    /// transaction-log checkpoint) and therefore open the engine with
    /// background WAL syncing rather than fsync-per-write.
    pub fn sync_wal(&self) -> Result<()> {
        self.inner.sync_wal()
    }

    pub fn compact(&self) -> Result<()> {
        self.inner.compact()
    }

    pub fn compact_if_needed(&self) -> Result<()> {
        self.inner.compact_if_needed()
    }

    pub fn get_statistics(&self) -> Result<StorageEngineStatistics> {
        self.inner.get_statistics()
    }

    /// Quiesce the engine: stop scheduling background work and block until any
    /// in-flight background compaction (or WAL sync) has finished. After this
    /// returns, no background task will touch engine state — in particular,
    /// none will delete SSTable files — so the directory can be safely reopened
    /// or removed.
    ///
    /// Idempotent, and also run automatically when the engine is dropped. Call
    /// it explicitly when you need the quiesced state *before* dropping the last
    /// handle (e.g. just before reopening the same directory).
    pub fn shutdown(&self) {
        self.inner.quiesce();
    }
}

impl Drop for StorageEngine {
    /// Quiesce on drop so a background compaction can never outlive the public
    /// handle and race a subsequent reopen of the same directory. Runs only
    /// when the last `StorageEngine` is dropped (the type is not `Clone`;
    /// shared owners use `Arc<StorageEngine>`).
    fn drop(&mut self) {
        self.inner.quiesce();
    }
}

impl EngineInner {
    pub fn open(
        dir_path: impl AsRef<Path>,
        options: EngineOptions,
        executor: Arc<dyn Executor>,
        rewriter: Arc<dyn ValueRewriter>,
    ) -> Result<Self> {
        let dir_path = dir_path.as_ref().to_path_buf();
        fs::create_dir_all(&dir_path)?;

        // Load or save options.bin
        let options_path = dir_path.join("options.bin");
        let active_options = if options_path.exists() {
            let bytes = fs::read(&options_path)?;
            postcard::from_bytes::<EngineOptions>(&bytes)?
        } else {
            // Reject invalid options before persisting them to disk.
            options.validate()?;
            let bytes = postcard::to_allocvec(&options)?;
            crate::atomic_write(&options_path, &bytes, options.fsync_method)?;
            options
        };
        // Re-validate the options actually in effect; this also guards against a
        // corrupted or legacy `options.bin` carrying panic-inducing values.
        active_options.validate()?;

        let block_cache = if active_options.block_cache_capacity > 0 {
            Some(Arc::new(crate::BlockCache::new(
                active_options
                    .block_cache_capacity
                    .saturating_mul(active_options.block_size_limit),
            )))
        } else {
            None
        };

        // 1. Open the manifest (snapshot + edit log). If none exists yet (a
        // fresh directory), rebuild the active-SSTable set from the L*.sst
        // files on disk and record it as the initial manifest.
        let manifest_dir = dir_path.join("manifest");
        let had_manifest = manifest_dir.join("CURRENT").exists();
        let mut manifest = SnapshotLog::<Manifest>::open(
            &manifest_dir,
            MANIFEST_LOG_FORMAT,
            active_options.fsync_method,
            MANIFEST_COMPACT_THRESHOLD_BYTES,
        )?;
        if !had_manifest {
            let mut edits = Vec::new();
            for entry in fs::read_dir(&dir_path)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().is_some_and(|ext| ext == "sst")
                    && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                    && stem.starts_with('L')
                {
                    let parts: Vec<&str> = stem[1..].split('_').collect();
                    if parts.len() == 2
                        && let (Ok(level), Ok(id)) =
                            (parts[0].parse::<usize>(), parts[1].parse::<u64>())
                    {
                        edits.push(ManifestEdit::AddSstable { level, id });
                    }
                }
            }
            if !edits.is_empty() {
                manifest.append_batch(edits)?;
            }
        }

        // 2. Discover active SSTables and clean up orphan/garbage files.
        let mut max_id = 0;
        let mut discovered_ssts = Vec::new();
        let mut files_to_delete = Vec::new();

        for entry in fs::read_dir(&dir_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "tmp") {
                files_to_delete.push(path);
                continue;
            }

            if path.extension().is_some_and(|ext| ext == "sst")
                && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                && stem.starts_with('L')
            {
                let parts: Vec<&str> = stem[1..].split('_').collect();
                if parts.len() == 2
                    && let (Ok(level), Ok(id)) =
                        (parts[0].parse::<usize>(), parts[1].parse::<u64>())
                {
                    if manifest.state().active_sstables.contains(&(level, id)) {
                        max_id = max_id.max(id);
                        discovered_ssts.push((level, id, path));
                    } else {
                        files_to_delete.push(path);
                    }
                }
            }
        }

        // Delete orphan/garbage files
        for p in files_to_delete {
            let _ = fs::remove_file(p);
        }

        let mut sstables_map: BTreeMap<usize, Vec<Arc<SstableReader>>> = BTreeMap::new();
        for (level, id, path) in discovered_ssts {
            let reader = SstableReader::open(&path, id, level, block_cache.clone())?;
            sstables_map
                .entry(level)
                .or_default()
                .push(Arc::new(reader));
        }

        // Sort the files in each level:
        for (level, list) in sstables_map.iter_mut() {
            if *level == 0 {
                list.sort_by(|a, b| {
                    let id_a = a.id;
                    let id_b = b.id;
                    id_b.cmp(&id_a)
                });
            } else {
                list.sort_by(|a, b| {
                    let r_a = a;
                    let r_b = b;
                    match (r_a.first_key(), r_b.first_key()) {
                        (Some(k_a), Some(k_b)) => k_a.cmp(k_b),
                        (Some(_), None) => std::cmp::Ordering::Less,
                        (None, Some(_)) => std::cmp::Ordering::Greater,
                        (None, None) => std::cmp::Ordering::Equal,
                    }
                });
            }
        }

        // 2. Recover from the WAL. Data lives in numbered segments
        // (`wal_<id>.wal`), one per memtable, plus an optional pre-segmentation
        // `active.wal`. Replay them oldest-first into a single memtable so the
        // newest write of each key wins, then flush the result to one SSTable.
        fn apply_entry(mem: &MemTable, ent: WalEntry) {
            match ent {
                WalEntry::Put { key, value } => mem.put(key, value),
                WalEntry::Delete { key } => mem.delete(key),
                WalEntry::Batch(sub) => {
                    for e in sub {
                        apply_entry(mem, e);
                    }
                }
            }
        }

        let mut segments: Vec<(u64, PathBuf)> = Vec::new();
        let mut max_wal_id: Option<u64> = None;
        for entry in fs::read_dir(&dir_path)? {
            let path = entry?.path();
            if let Some(id) = parse_wal_segment_id(&path) {
                max_wal_id = Some(max_wal_id.map_or(id, |m| m.max(id)));
                segments.push((id, path));
            }
        }
        segments.sort_by_key(|(id, _)| *id);

        // The legacy single-file WAL predates every numbered segment, so it
        // replays first.
        let mut replay_paths: Vec<PathBuf> = Vec::new();
        let legacy_wal = dir_path.join(LEGACY_WAL_NAME);
        if legacy_wal.exists() {
            replay_paths.push(legacy_wal);
        }
        replay_paths.extend(segments.into_iter().map(|(_, p)| p));

        let memtable = Arc::new(MemTable::new());
        for path in &replay_paths {
            for entry in Wal::recover(path)? {
                apply_entry(&memtable, entry);
            }
        }

        if memtable.byte_size() > 0 {
            let next_id = max_id + 1;
            max_id = next_id;
            let sst_path = dir_path.join(format!("L0_{:05}.sst", next_id));
            let mut writer = SstableWriter::create(
                &sst_path,
                active_options.block_size_limit,
                active_options.compression,
                memtable.len(),
                vec![],
                active_options.fsync_method,
            )?;
            for (key, val) in memtable.entries() {
                writer.append(&key, val.as_ref())?;
            }
            writer.finish()?;

            // Record the recovered SSTable in the manifest.
            manifest.append(ManifestEdit::AddSstable {
                level: 0,
                id: next_id,
            })?;

            let reader = SstableReader::open(&sst_path, next_id, 0, block_cache.clone())?;
            sstables_map
                .entry(0)
                .or_default()
                .insert(0, Arc::new(reader));
            memtable.clear();
        }

        // The recovered data is now durably in an SSTable, so the segments that
        // held it can be retired. Done after the flush above to preserve the
        // "data durable before WAL discarded" ordering.
        for path in &replay_paths {
            fs::remove_file(path)?;
        }
        // `fsync_parent_dir` fsyncs the *parent* of the path it's given, so pass
        // a segment path (a child of `dir_path`) to make the deletions durable.
        if let Some(first) = replay_paths.first() {
            crate::fsync_parent_dir(first, active_options.fsync_method)?;
        }

        // Open a fresh active segment past every id seen on disk.
        let active_wal_id = max_wal_id.map_or(0, |m| m + 1);
        let next_wal_id = active_wal_id + 1;
        let active_wal_path = wal_segment_path(&dir_path, active_wal_id);
        let wal = Wal::open(
            &active_wal_path,
            active_options.wal_sync_interval_ms,
            active_options.fsync_method,
        )?;
        crate::fsync_parent_dir(&active_wal_path, active_options.fsync_method)?;

        Ok(Self {
            dir_path,
            memset: RwLock::new(MemSet {
                active: memtable,
                immutable: VecDeque::new(),
            }),
            wal: Mutex::new(wal),
            active_wal_id: AtomicU64::new(active_wal_id),
            next_wal_id: AtomicU64::new(next_wal_id),
            sstables: RwLock::new(sstables_map),
            options: active_options,
            write_mutex: Mutex::new(()),
            compaction_mutex: Mutex::new(()),
            manifest: Mutex::new(manifest),
            next_sst_id: AtomicU64::new(max_id + 1),
            executor,
            rewriter,
            target_layout: RwLock::new(Vec::new()),
            wal_sync_handle: Mutex::new(None),
            block_cache,
            last_compacted_keys: Mutex::new(std::collections::HashMap::new()),
            background: BackgroundGate::new(),
        })
    }

    pub fn put(self: &Arc<Self>, key: DbKey, value: DbValue) -> Result<()> {
        let _write_lock = self.write_mutex.lock().unwrap();

        let wal_size = {
            let mut wal = self.wal.lock().unwrap();
            wal.append_put(&key, &value)?;
            wal.size()?
        };

        // Hold the memset read lock only to record the write; drop it before any
        // flush, which needs the write lock to rotate the active memtable.
        // `write_mutex` (held for the whole call) keeps the two steps atomic
        // against other writers.
        let trigger_flush = {
            let memset = self.memset.read().unwrap();
            memset.active.put(key, value);
            let mem_full = memset.active.byte_size() >= self.options.memtable_size_limit;
            let wal_full = wal_size as usize >= self.options.wal_size_limit;
            mem_full || wal_full
        };

        if trigger_flush {
            self.flush_memtable_internal()?;
        }

        Ok(())
    }

    pub fn delete(self: &Arc<Self>, key: DbKey) -> Result<()> {
        let _write_lock = self.write_mutex.lock().unwrap();

        let wal_size = {
            let mut wal = self.wal.lock().unwrap();
            wal.append_delete(&key)?;
            wal.size()?
        };

        let trigger_flush = {
            let memset = self.memset.read().unwrap();
            memset.active.delete(key);
            let mem_full = memset.active.byte_size() >= self.options.memtable_size_limit;
            let wal_full = wal_size as usize >= self.options.wal_size_limit;
            mem_full || wal_full
        };

        if trigger_flush {
            self.flush_memtable_internal()?;
        }

        Ok(())
    }

    pub fn write_batch(self: &Arc<Self>, entries: Vec<WalEntry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let _write_lock = self.write_mutex.lock().unwrap();

        let wal_size = {
            let mut wal = self.wal.lock().unwrap();
            wal.append_batch(&entries)?;
            wal.size()?
        };

        let trigger_flush = {
            let memset = self.memset.read().unwrap();
            let mem = &memset.active;
            for entry in entries {
                match entry {
                    WalEntry::Put { key, value } => mem.put(key, value),
                    WalEntry::Delete { key } => mem.delete(key),
                    WalEntry::Batch(_) => {}
                }
            }
            let mem_full = mem.byte_size() >= self.options.memtable_size_limit;
            let wal_full = wal_size as usize >= self.options.wal_size_limit;
            mem_full || wal_full
        };

        if trigger_flush {
            self.flush_memtable_internal()?;
        }

        Ok(())
    }

    pub fn get(&self, key: &DbKey) -> Result<Option<DbValue>> {
        {
            // Newest-to-oldest: active memtable, then immutable memtables
            // (front = newest). A hit — value or tombstone — shadows everything
            // below it.
            let memset = self.memset.read().unwrap();
            if let Some(res) = memset.active.get(key) {
                return Ok(res);
            }
            for imm in &memset.immutable {
                if let Some(res) = imm.table.get(key) {
                    return Ok(res);
                }
            }
        }

        let sstables_map = self.sstables.read().unwrap();

        if let Some(l0_ssts) = sstables_map.get(&0) {
            for sstable in l0_ssts.iter() {
                if let Some(res) = sstable.get(key)? {
                    if let Some(val) = res {
                        let target_layout = self.target_layout();
                        let src_layout = &sstable.layout;
                        let val = if src_layout != &target_layout {
                            self.rewriter.rewrite(src_layout, &target_layout, &val)?
                        } else {
                            val
                        };
                        return Ok(Some(val));
                    } else {
                        return Ok(None);
                    }
                }
            }
        }

        for (level, ssts) in sstables_map.iter() {
            if *level == 0 {
                continue;
            }
            if ssts.is_empty() {
                continue;
            }

            let mut corruption: Option<u64> = None;
            let idx_res = ssts.binary_search_by(|sstable| {
                let Some(f_key) = sstable.first_key() else {
                    corruption = Some(sstable.id);
                    return std::cmp::Ordering::Equal;
                };
                let l_key = sstable.last_key();
                if key < f_key {
                    std::cmp::Ordering::Greater
                } else if key > l_key {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            });
            if let Some(id) = corruption {
                return Err(StorageError::Corruption(format!(
                    "Level {level} SSTable {id} has an empty index"
                )));
            }

            if let Ok(idx) = idx_res
                && let Some(res) = ssts[idx].get(key)?
            {
                if let Some(val) = res {
                    let target_layout = self.target_layout();
                    let src_layout = &ssts[idx].layout;
                    let val = if src_layout != &target_layout {
                        self.rewriter.rewrite(src_layout, &target_layout, &val)?
                    } else {
                        val
                    };
                    return Ok(Some(val));
                } else {
                    return Ok(None);
                }
            }
        }

        Ok(None)
    }

    pub fn multi_get(&self, keys: &[DbKey]) -> Result<Vec<Option<DbValue>>> {
        let mut results = vec![None; keys.len()];
        let mut remaining_indices: Vec<usize> = (0..keys.len()).collect();

        // 1. Check the in-memory buffers under a single read lock, newest-to-
        // oldest: active memtable first, then immutable memtables (front =
        // newest). The first buffer to hold a key resolves it (a tombstone
        // resolves to `None`), so it is removed from the remaining set.
        {
            let memset = self.memset.read().unwrap();
            let mems = std::iter::once(memset.active.as_ref())
                .chain(memset.immutable.iter().map(|imm| imm.table.as_ref()));
            for mem in mems {
                if remaining_indices.is_empty() {
                    break;
                }
                let mut i = 0;
                while i < remaining_indices.len() {
                    let idx = remaining_indices[i];
                    let key = &keys[idx];
                    if let Some(res) = mem.get(key) {
                        results[idx] = res;
                        remaining_indices.swap_remove(i);
                    } else {
                        i += 1;
                    }
                }
            }
        }

        if remaining_indices.is_empty() {
            return Ok(results);
        }

        // 2. Check SSTables under a single read lock
        let sstables_map = self.sstables.read().unwrap();

        // Check L0
        if let Some(l0_ssts) = sstables_map.get(&0) {
            for sstable in l0_ssts.iter() {
                let mut i = 0;
                while i < remaining_indices.len() {
                    let idx = remaining_indices[i];
                    let key = &keys[idx];
                    if let Some(res) = sstable.get(key)? {
                        if let Some(val) = res {
                            let target_layout = self.target_layout();
                            let src_layout = &sstable.layout;
                            let val = if src_layout != &target_layout {
                                self.rewriter.rewrite(src_layout, &target_layout, &val)?
                            } else {
                                val
                            };
                            results[idx] = Some(val);
                        } else {
                            results[idx] = None;
                        }
                        remaining_indices.swap_remove(i);
                    } else {
                        i += 1;
                    }
                }
                if remaining_indices.is_empty() {
                    return Ok(results);
                }
            }
        }

        // Check Leveled SSTables (Level 1+)
        for (level, ssts) in sstables_map.iter() {
            if *level == 0 || ssts.is_empty() {
                continue;
            }

            let mut i = 0;
            while i < remaining_indices.len() {
                let idx = remaining_indices[i];
                let key = &keys[idx];

                let mut corruption: Option<u64> = None;
                let idx_res = ssts.binary_search_by(|sstable| {
                    let Some(f_key) = sstable.first_key() else {
                        corruption = Some(sstable.id);
                        return std::cmp::Ordering::Equal;
                    };
                    let l_key = sstable.last_key();
                    if key < f_key {
                        std::cmp::Ordering::Greater
                    } else if key > l_key {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Equal
                    }
                });
                if let Some(id) = corruption {
                    return Err(StorageError::Corruption(format!(
                        "Level {level} SSTable {id} has an empty index"
                    )));
                }

                if let Ok(sst_idx) = idx_res
                    && let Some(res) = ssts[sst_idx].get(key)?
                {
                    if let Some(val) = res {
                        let target_layout = self.target_layout();
                        let src_layout = &ssts[sst_idx].layout;
                        let val = if src_layout != &target_layout {
                            self.rewriter.rewrite(src_layout, &target_layout, &val)?
                        } else {
                            val
                        };
                        results[idx] = Some(val);
                    } else {
                        results[idx] = None;
                    }
                    remaining_indices.swap_remove(i);
                } else {
                    i += 1;
                }
            }

            if remaining_indices.is_empty() {
                return Ok(results);
            }
        }

        Ok(results)
    }

    pub fn scan_iter(&self, start: &DbKey, end: &DbKey) -> Result<ScanIterator> {
        let mem_entries = self.snapshot_mem_range(start, end);

        let sstables_map = self.sstables.read().unwrap();
        let mut sst_iters = Vec::new();
        let mut next_priority = 0;

        if let Some(l0_ssts) = sstables_map.get(&0) {
            for sstable in l0_ssts.iter() {
                if let Some(fk) = sstable.first_key() {
                    let lk = sstable.last_key();
                    if fk <= end && lk >= start {
                        sst_iters.push(crate::merge_iter::SstableBlockIterator::new_with_end(
                            sstable.clone(),
                            Some(start),
                            Some(end),
                            next_priority,
                        )?);
                    }
                }
                next_priority += 1;
            }
        }

        for (level, ssts) in sstables_map.iter() {
            if *level == 0 {
                continue;
            }
            // L1+ files are sorted by key and non-overlapping, so binary-search
            // to the first file that can contain `start` and stop at the first
            // file beyond `end` instead of scanning the whole level.
            let start_idx = ssts.partition_point(|s| s.last_key() < start);
            for sstable in ssts[start_idx..].iter() {
                let fk = sstable.first_key().ok_or_else(|| {
                    StorageError::Corruption(format!(
                        "Level {level} SSTable {} has an empty index",
                        sstable.id
                    ))
                })?;
                if fk > end {
                    break;
                }
                // `last_key() >= start` is guaranteed by `partition_point`.
                sst_iters.push(crate::merge_iter::SstableBlockIterator::new_with_end(
                    sstable.clone(),
                    Some(start),
                    Some(end),
                    next_priority,
                )?);
                next_priority += 1;
            }
        }

        // Decision (A): Owned iterator snapshots memtable range on construction.
        // No lifetime parameters are needed, avoiding complex lifetime annotations throughout the SQL engine.
        let target_layout = self.target_layout();
        ScanIterator::new(
            mem_entries,
            sst_iters,
            end.clone(),
            self.rewriter.clone(),
            target_layout,
        )
    }

    pub fn filtered_scan<F>(
        &self,
        start: &DbKey,
        end: &DbKey,
        filter: F,
    ) -> Result<Vec<(DbKey, DbValue)>>
    where
        F: Fn(&DbKey, &DbValue) -> bool,
    {
        let mut seen = HashSet::new();
        let mut results = BTreeMap::new();

        {
            // Newest-to-oldest across the in-memory buffers: active memtable,
            // then immutable memtables (front = newest). `seen.insert` returns
            // false once a key has been resolved by a newer buffer, so older
            // copies — including tombstones that must still shadow the SSTables
            // below — are recorded once and never overwritten.
            let memset = self.memset.read().unwrap();
            let mems = std::iter::once(memset.active.as_ref())
                .chain(memset.immutable.iter().map(|imm| imm.table.as_ref()));
            for mem in mems {
                for (k, v) in mem.scan_range_raw(start, end) {
                    if seen.insert(k.clone())
                        && let Some(val) = v
                        && filter(&k, &val)
                    {
                        results.insert(k, val);
                    }
                }
            }
        }

        let sstables_map = self.sstables.read().unwrap();

        if let Some(l0_ssts) = sstables_map.get(&0) {
            for sstable in l0_ssts.iter() {
                let entries = sstable.scan_raw(start, end)?;
                for (k, v) in entries {
                    if seen.insert(k.clone())
                        && let Some(val) = v
                    {
                        let target_layout = self.target_layout();
                        let src_layout = &sstable.layout;
                        let val = if src_layout != &target_layout {
                            self.rewriter.rewrite(src_layout, &target_layout, &val)?
                        } else {
                            val
                        };
                        if filter(&k, &val) {
                            results.insert(k, val);
                        }
                    }
                }
            }
        }

        for (level, ssts) in sstables_map.iter() {
            if *level == 0 {
                continue;
            }
            // L1+ files are sorted and non-overlapping: binary-search to the
            // first file that can contain `start`, then stop once past `end`.
            let start_idx = ssts.partition_point(|s| s.last_key() < start);
            for sstable in ssts[start_idx..].iter() {
                let f_key = sstable.first_key().ok_or_else(|| {
                    StorageError::Corruption(format!(
                        "Level {level} SSTable {} has an empty index",
                        sstable.id
                    ))
                })?;
                if f_key > end {
                    break;
                }

                let entries = sstable.scan_raw(start, end)?;
                for (k, v) in entries {
                    if seen.insert(k.clone())
                        && let Some(val) = v
                    {
                        let target_layout = self.target_layout();
                        let src_layout = &sstable.layout;
                        let val = if src_layout != &target_layout {
                            self.rewriter.rewrite(src_layout, &target_layout, &val)?
                        } else {
                            val
                        };
                        if filter(&k, &val) {
                            results.insert(k, val);
                        }
                    }
                }
            }
        }

        Ok(results.into_iter().collect())
    }

    pub fn flush_memtable(self: &Arc<Self>) -> Result<()> {
        let _write_lock = self.write_mutex.lock().unwrap();
        self.flush_memtable_internal()?;
        Ok(())
    }

    pub fn compact(self: &Arc<Self>) -> Result<()> {
        // 1. Flush active memtable to disk first so all data is in SSTables (holds write_mutex).
        {
            let _write_lock = self.write_mutex.lock().unwrap();
            self.flush_memtable_internal_no_trigger()?;
        }

        // 2. Run compaction synchronously
        let _compaction_lock = self.compaction_mutex.lock().unwrap();

        let has_l0 = {
            let sstables = self.sstables.read().unwrap();
            sstables.get(&0).is_some_and(|list| !list.is_empty())
        };
        if has_l0 {
            self.compact_level(0)?;
        }

        self.compact_if_needed_locked()?;

        Ok(())
    }

    pub fn compact_if_needed(&self) -> Result<()> {
        let _lock = self.compaction_mutex.lock().unwrap();
        self.compact_if_needed_locked()
    }

    fn compact_if_needed_locked(&self) -> Result<()> {
        while let Some(level) = self.find_level_to_compact() {
            self.compact_level(level)?;
        }
        Ok(())
    }

    pub fn get_statistics(&self) -> Result<StorageEngineStatistics> {
        let sstables_guard = self.sstables.read().unwrap();
        let memset_guard = self.memset.read().unwrap();

        let mut num_sstables = 0;
        let mut total_sstable_size = 0;
        let mut sstable_entries = 0;
        let mut sstable_tombstones = 0;
        let mut sstable_uncompressed_bytes = 0;

        for ssts in sstables_guard.values() {
            for sst in ssts {
                let reader = sst;
                num_sstables += 1;
                total_sstable_size += reader.file_size();
                sstable_entries += reader.stats.num_entries;
                sstable_tombstones += reader.stats.num_tombstones;
                sstable_uncompressed_bytes += reader.stats.total_uncompressed_bytes;
            }
        }

        // Account for every resident buffer: the active memtable plus any
        // immutable memtables still awaiting flush.
        let mut memtable_entries = 0;
        let mut memtable_tombstones = 0;
        let mut memtable_uncompressed_bytes = 0u64;
        for mem in std::iter::once(memset_guard.active.as_ref())
            .chain(memset_guard.immutable.iter().map(|imm| imm.table.as_ref()))
        {
            let (entries, tombstones) = mem.entry_counts();
            memtable_entries += entries;
            memtable_tombstones += tombstones;
            memtable_uncompressed_bytes += mem.byte_size() as u64;
        }

        Ok(StorageEngineStatistics {
            num_sstables,
            total_sstable_size,
            sstable_entries,
            sstable_tombstones,
            sstable_uncompressed_bytes,
            memtable_entries: memtable_entries as u64,
            memtable_tombstones: memtable_tombstones as u64,
            memtable_uncompressed_bytes,
        })
    }

    fn find_level_to_compact(&self) -> Option<usize> {
        let sstables = self.sstables.read().unwrap();

        if let Some(l0_ssts) = sstables.get(&0)
            && l0_ssts.len() >= self.options.l0_compaction_threshold
        {
            return Some(0);
        }

        for level in 1..self.options.max_level {
            if let Some(ssts) = sstables.get(&level) {
                let total_size: u64 = ssts.iter().map(|s| s.file_size()).sum();
                let limit = self.level_size_limit(level);
                if total_size > limit {
                    return Some(level);
                }
            }
        }

        None
    }

    fn level_size_limit(&self, level: usize) -> u64 {
        if level == 0 {
            0
        } else {
            let multiplier =
                (self.options.level_size_multiplier as u64).saturating_pow((level - 1) as u32);
            (self.options.base_level_size_limit as u64).saturating_mul(multiplier)
        }
    }

    fn get_next_id(&self) -> u64 {
        self.next_sst_id.fetch_add(1, Ordering::SeqCst)
    }

    fn get_next_wal_id(&self) -> u64 {
        self.next_wal_id.fetch_add(1, Ordering::SeqCst)
    }

    fn compact_level(&self, source_level: usize) -> Result<()> {
        let target_level = source_level + 1;
        if target_level > self.options.max_level {
            return Ok(());
        }

        let mut source_files = Vec::new();
        let mut overlapping_target_files = Vec::new();

        // 1. Select source files and compute overlapping target files under read lock
        {
            let sstables_guard = self.sstables.read().unwrap();

            if source_level == 0 {
                if let Some(list) = sstables_guard.get(&0) {
                    source_files = list.clone();
                }
            } else if let Some(list) = sstables_guard.get(&source_level)
                && !list.is_empty()
            {
                let last_compacted = self
                    .last_compacted_keys
                    .lock()
                    .unwrap()
                    .get(&source_level)
                    .cloned();
                let selected = if let Some(ref last_key) = last_compacted {
                    list.iter()
                        .find(|sst| sst.first_key().is_some_and(|fk| fk > last_key))
                        .cloned()
                        .unwrap_or_else(|| list[0].clone())
                } else {
                    list[0].clone()
                };
                source_files.push(selected);
            }

            if source_files.is_empty() {
                return Ok(());
            }

            let mut min_key = None;
            let mut max_key = None;
            for sstable in &source_files {
                let reader = sstable;
                if let Some(fk) = reader.first_key()
                    && min_key.as_ref().is_none_or(|k| fk < k)
                {
                    min_key = Some(fk.clone());
                }
                let lk = reader.last_key();
                if max_key.as_ref().is_none_or(|k| lk > k) {
                    max_key = Some(lk.clone());
                }
            }

            if let (Some(min_k), Some(max_k)) = (min_key, max_key)
                && let Some(target_list) = sstables_guard.get(&target_level)
            {
                for sstable in target_list {
                    let overlaps = {
                        let reader = sstable;
                        let fk = reader.first_key().ok_or_else(|| {
                            StorageError::Corruption(format!(
                                "Level {target_level} SSTable {} has an empty index",
                                reader.id
                            ))
                        })?;
                        let lk = reader.last_key();
                        fk <= &max_k && lk >= &min_k
                    };
                    if overlaps {
                        overlapping_target_files.push(sstable.clone());
                    }
                }
            }
        }

        // 2. Merge-sort all selected files (locks released) using streaming k-way merge iterator
        let mut l0_files_sorted = Vec::new();
        let mut other_files = Vec::new();
        for f in source_files.iter().chain(overlapping_target_files.iter()) {
            let reader = f;
            if reader.level == 0 {
                l0_files_sorted.push(f.clone());
            } else {
                other_files.push(f.clone());
            }
        }
        l0_files_sorted.sort_by_key(|f| f.id);

        let mut sources = Vec::new();
        let l0_count = l0_files_sorted.len();
        for (i, f) in other_files.iter().enumerate() {
            sources.push(crate::merge_iter::SstableBlockIterator::new(
                f.clone(),
                None,
                l0_count + i,
            )?);
        }
        for (i, f) in l0_files_sorted.iter().enumerate().rev() {
            let priority = l0_count - 1 - i;
            sources.push(crate::merge_iter::SstableBlockIterator::new(
                f.clone(),
                None,
                priority,
            )?);
        }

        let mut merge_iter = crate::merge_iter::MergeIterator::new(sources)?;

        let expected_entries: usize = source_files
            .iter()
            .map(|f| f.stats.num_entries as usize)
            .sum::<usize>()
            + overlapping_target_files
                .iter()
                .map(|f| f.stats.num_entries as usize)
                .sum::<usize>();

        // 3. Write merged data to new SSTables
        let mut new_sstables = Vec::new();
        let mut current_writer: Option<SstableWriter> = None;
        let mut current_path = None;
        let mut current_id = 0;
        let mut current_writer_uncompressed_bytes = 0;

        // Snapshot the key ranges of every SSTable below the target level once,
        // up front, so the tombstone-drop check below is a lock-free scan over
        // in-memory bounds rather than re-acquiring the `sstables` read lock and
        // re-walking the live map for every tombstone. Levels below the target
        // can't change during this compaction (the compaction_mutex serializes
        // compactions, and flushes only add to L0, which is above any target),
        // so a one-shot snapshot is consistent.
        let lower_level_ranges: Vec<(DbKey, DbKey)> = if target_level < self.options.max_level {
            let sstables_guard = self.sstables.read().unwrap();
            sstables_guard
                .iter()
                .filter(|(level, _)| **level > target_level)
                .flat_map(|(_, ssts)| ssts.iter())
                .filter_map(|sst| {
                    sst.first_key()
                        .map(|fk| (fk.clone(), sst.last_key().clone()))
                })
                .collect()
        } else {
            Vec::new()
        };

        let target_layout = self.target_layout();
        while let Some((k, v, source_idx)) = merge_iter.next()? {
            let mut v = v;
            if let Some(val) = &v {
                let src_reader = merge_iter.get_reader(source_idx);
                let src_layout = &src_reader.layout;
                if src_layout != &target_layout {
                    v = Some(self.rewriter.rewrite(src_layout, &target_layout, val)?);
                }
            }

            if v.is_none() {
                // Decision: To avoid expensive point lookups during compaction, we only
                // check if any SSTable in lower levels has a key range overlapping with the tombstone.
                // If there are no overlapping SSTables in lower levels (or if we are at max_level),
                // the tombstone is safe to drop. This has zero I/O cost as it only uses in-memory range metadata.
                if target_level < self.options.max_level {
                    let overlaps_below = lower_level_ranges
                        .iter()
                        .any(|(fk, lk)| &k >= fk && &k <= lk);
                    if !overlaps_below {
                        continue;
                    }
                } else {
                    continue;
                }
            }

            if current_writer.is_none() {
                current_id = self.get_next_id();
                let path = self
                    .dir_path
                    .join(format!("L{}_{:05}.sst", target_level, current_id));
                current_path = Some(path.clone());
                current_writer = Some(SstableWriter::create(
                    &path,
                    self.options.block_size_limit,
                    self.options.compression,
                    expected_entries,
                    target_layout.clone(),
                    self.options.fsync_method,
                )?);
                current_writer_uncompressed_bytes = 0;
            }

            let entry_sz = k.byte_size() + v.as_ref().map_or(1, DbValue::byte_size);
            current_writer_uncompressed_bytes += entry_sz;

            let writer = current_writer.as_mut().unwrap();
            writer.append(&k, v.as_ref())?;

            if current_writer_uncompressed_bytes >= self.options.sstable_target_size {
                let writer = current_writer.take().unwrap();
                writer.finish()?;
                let reader = SstableReader::open(
                    current_path.take().unwrap(),
                    current_id,
                    target_level,
                    self.block_cache.clone(),
                )?;
                new_sstables.push(Arc::new(reader));
            }
        }

        if let Some(writer) = current_writer.take() {
            writer.finish()?;
            let reader = SstableReader::open(
                current_path.take().unwrap(),
                current_id,
                target_level,
                self.block_cache.clone(),
            )?;
            new_sstables.push(Arc::new(reader));
        }

        // Atomically swap the compacted SSTables for the new ones in the
        // manifest: one batched append (a single fsync) replaces the old
        // full-file rewrite.
        {
            let mut edits = Vec::new();
            for f in source_files.iter().chain(overlapping_target_files.iter()) {
                edits.push(ManifestEdit::RemoveSstable {
                    level: f.level,
                    id: f.id,
                });
            }
            for f in &new_sstables {
                edits.push(ManifestEdit::AddSstable {
                    level: f.level,
                    id: f.id,
                });
            }
            self.manifest.lock().unwrap().append_batch(edits)?;
        }

        // 4. Update the active sstables map
        {
            let mut sstables_guard = self.sstables.write().unwrap();

            let source_ids: HashSet<u64> = source_files.iter().map(|f| f.id).collect();
            if let Some(source_list) = sstables_guard.get_mut(&source_level) {
                source_list.retain(|f| !source_ids.contains(&f.id));
            }

            let target_ids: HashSet<u64> = overlapping_target_files.iter().map(|f| f.id).collect();
            let target_list = sstables_guard.entry(target_level).or_default();
            target_list.retain(|f| !target_ids.contains(&f.id));
            target_list.extend(new_sstables);
            target_list.sort_by(|a, b| {
                let r_a = a;
                let r_b = b;
                match (r_a.first_key(), r_b.first_key()) {
                    (Some(k_a), Some(k_b)) => k_a.cmp(k_b),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                }
            });
        }

        // Delete old files from disk
        for f in source_files.iter().chain(overlapping_target_files.iter()) {
            let reader = f;
            let _ = fs::remove_file(&reader.path);
        }

        if source_level > 0
            && let Some(last_file) = source_files.last()
        {
            let max_k = last_file.last_key();
            self.last_compacted_keys
                .lock()
                .unwrap()
                .insert(source_level, max_k.clone());
        }

        Ok(())
    }

    fn flush_memtable_internal(self: &Arc<Self>) -> Result<()> {
        self.flush_memtable_internal_no_trigger()?;
        self.trigger_compaction();
        Ok(())
    }

    /// Synchronously flush the active memtable to L0: seal it into the immutable
    /// queue (rotating to a fresh active memtable and WAL segment), then write it
    /// out and retire its segment. A no-op when the active memtable is empty.
    ///
    /// Caller must hold `write_mutex`.
    fn flush_memtable_internal_no_trigger(&self) -> Result<()> {
        let Some(imm) = self.seal_active()? else {
            return Ok(());
        };
        self.flush_immutable(&imm)
    }

    /// Seal the active memtable: move it into the immutable queue (newest at the
    /// front) and install a fresh, empty active memtable backed by a new WAL
    /// segment so subsequent writes have somewhere to go. Returns the sealed
    /// memtable paired with its WAL-segment id, or `None` if the active memtable
    /// was empty (nothing to seal).
    ///
    /// Caller must hold `write_mutex`, which serializes sealing against writers
    /// and against other seals.
    ///
    /// Durability note: the freshly created WAL segment's directory entry is
    /// *not* fsynced here. In the synchronous flush path the immediately
    /// following [`Self::flush_immutable`] issues one directory fsync covering
    /// both this segment's creation and the old segment's removal, and no write
    /// can reach the new segment in between because the caller holds
    /// `write_mutex`. A background flush (a later change) that lets writes flow to
    /// the new segment before the old one is retired must fsync the directory
    /// here, at seal time.
    fn seal_active(&self) -> Result<Option<ImmMemtable>> {
        // Fast path: an empty active memtable means an empty WAL segment too —
        // nothing to seal or flush.
        if self.memset.read().unwrap().active.is_empty() {
            return Ok(None);
        }

        let sealed_wal_id = self.active_wal_id.load(Ordering::SeqCst);
        let new_wal_id = self.get_next_wal_id();
        let new_wal_path = wal_segment_path(&self.dir_path, new_wal_id);
        let new_wal = Wal::open(
            &new_wal_path,
            self.options.wal_sync_interval_ms,
            self.options.fsync_method,
        )?;

        // Redirect new writes to the fresh segment, then seal the active memtable
        // and swap in an empty one. Readers never touch the WAL and writers are
        // excluded by `write_mutex`, so the order of these two swaps is
        // immaterial to anyone observing the engine.
        {
            let mut wal_guard = self.wal.lock().unwrap();
            *wal_guard = new_wal;
        }
        self.active_wal_id.store(new_wal_id, Ordering::SeqCst);

        let sealed = {
            let mut memset = self.memset.write().unwrap();
            let table = std::mem::replace(&mut memset.active, Arc::new(MemTable::new()));
            let imm = ImmMemtable {
                table,
                wal_id: sealed_wal_id,
            };
            // The queue keeps its own handle (front = newest) so readers merge
            // over the sealed memtable until the flush removes it; the returned
            // clone shares the same `Arc`, so both observe the same data.
            memset.immutable.push_front(imm.clone());
            imm
        };
        Ok(Some(sealed))
    }

    /// Flush a sealed memtable to a new L0 SSTable, record it in the manifest,
    /// retire the WAL segment that backed it, and drop it from the immutable
    /// queue.
    ///
    /// Crash-safe by ordering: the SSTable and its manifest edit are made durable
    /// before the WAL segment is deleted, so a crash mid-flush leaves the segment
    /// on disk and recovery replays it idempotently into a fresh SSTable. The
    /// single directory fsync below also makes durable the new segment created by
    /// the preceding [`Self::seal_active`].
    fn flush_immutable(&self, imm: &ImmMemtable) -> Result<()> {
        let entries = imm.table.entries();

        let next_id = self.get_next_id();
        let sst_path = self.dir_path.join(format!("L0_{:05}.sst", next_id));

        let target_layout = self.target_layout();
        let mut writer = SstableWriter::create(
            &sst_path,
            self.options.block_size_limit,
            self.options.compression,
            entries.len(),
            target_layout,
            self.options.fsync_method,
        )?;
        for (key, val) in &entries {
            writer.append(key, val.as_ref())?;
        }
        writer.finish()?;

        // Record the newly flushed SSTable in the manifest.
        self.manifest
            .lock()
            .unwrap()
            .append(ManifestEdit::AddSstable {
                level: 0,
                id: next_id,
            })?;

        let reader = SstableReader::open(&sst_path, next_id, 0, self.block_cache.clone())?;
        {
            let mut ssts = self.sstables.write().unwrap();
            ssts.entry(0).or_default().insert(0, Arc::new(reader));
        }

        // The sealed memtable's data is now durably in the SSTable above, so its
        // WAL segment can be retired. A crash before this point leaves the
        // segment on disk; recovery replays it idempotently into a new SSTable.
        let old_wal_path = wal_segment_path(&self.dir_path, imm.wal_id);
        fs::remove_file(&old_wal_path)?;
        // One directory fsync makes both the old segment's removal and the new
        // segment's creation (in `seal_active`) durable.
        crate::fsync_parent_dir(&old_wal_path, self.options.fsync_method)?;

        // Drop the flushed memtable from the immutable queue. Matching on the WAL
        // id (unique and monotonic) removes exactly this entry.
        {
            let mut memset = self.memset.write().unwrap();
            memset.immutable.retain(|m| m.wal_id != imm.wal_id);
        }

        Ok(())
    }

    /// Snapshot the `[start, end]` range of every in-memory buffer (the active
    /// memtable plus any immutable memtables) into one sorted vector, keeping the
    /// newest value (or tombstone) per key. Seeds the scan merge with a stable
    /// view of the memtables whose internal precedence is already resolved.
    fn snapshot_mem_range(&self, start: &DbKey, end: &DbKey) -> Vec<(DbKey, Option<DbValue>)> {
        let memset = self.memset.read().unwrap();
        // Iterate newest-to-oldest and keep the first value seen per key, so the
        // newest write wins. The BTreeMap yields the result already sorted.
        let mut merged: BTreeMap<DbKey, Option<DbValue>> = BTreeMap::new();
        for mem in std::iter::once(memset.active.as_ref())
            .chain(memset.immutable.iter().map(|imm| imm.table.as_ref()))
        {
            for (k, v) in mem.scan_range_raw(start, end) {
                merged.entry(k).or_insert(v);
            }
        }
        merged.into_iter().collect()
    }

    /// Begin a background-task region. Returns a guard that marks the task as
    /// in-flight (decrementing on drop), or `None` if the engine is shutting
    /// down — in which case the caller MUST return without touching engine
    /// state. See [`BackgroundGate`].
    fn enter_background(&self) -> Option<BackgroundGuard<'_>> {
        let mut st = self.background.state.lock().unwrap();
        if st.shutting_down {
            return None;
        }
        st.active += 1;
        Some(BackgroundGuard { inner: self })
    }

    /// Quiesce all background work: stop scheduling, then block until any task
    /// that had already begun has finished. After this returns, no background
    /// task will touch engine state, so the directory can be safely reopened.
    /// Idempotent.
    fn quiesce(&self) {
        // Cancel the periodic WAL-sync schedule so no further ticks are
        // submitted (dropping the handle cancels it).
        *self.wal_sync_handle.lock().unwrap() = None;

        let mut st = self.background.state.lock().unwrap();
        st.shutting_down = true;
        while st.active > 0 {
            st = self.background.idle.wait(st).unwrap();
        }
    }

    fn trigger_compaction(self: &Arc<Self>) {
        // Don't schedule new compaction once the engine is shutting down; a task
        // submitted now would only no-op (see the `enter_background` guard
        // below), and skipping the submit keeps the executor queue clean.
        if self.background.state.lock().unwrap().shutting_down {
            return;
        }
        let inner = self.clone();
        // Coalesce per engine: at most one compaction runs at a time, and a
        // request arriving mid-run schedules exactly one trailing re-run. This
        // preserves the previous CompactionSignal {running, pending} semantics
        // while letting the worker thread be reused between rounds.
        let key = CoalesceKey::new(format!("compact:{}", self.dir_path.display()));
        self.executor.submit(
            Priority::High,
            Some(key),
            Box::new(move || {
                // Register as in-flight so `quiesce` joins us; bail without
                // touching state if the engine was shut down between submit and
                // execution (e.g. the public handle was dropped). This is what
                // prevents a stray compaction from deleting files after the
                // engine is considered closed.
                let Some(_bg) = inner.enter_background() else {
                    return;
                };
                let _compaction_lock = inner.compaction_mutex.lock().unwrap();
                if let Err(e) = inner.compact_if_needed_locked() {
                    tracing::error!(error = ?e, "background compaction failed");
                }
            }),
        );
    }

    pub fn sync_wal(&self) -> Result<()> {
        let wal = self.wal.lock().unwrap();
        wal.sync_all()?;
        Ok(())
    }

    pub fn set_target_layout(&self, layout: Vec<u8>) {
        let mut target = self.target_layout.write().unwrap();
        *target = layout;
    }

    pub fn target_layout(&self) -> Vec<u8> {
        self.target_layout.read().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CompressionType;

    /// Writes a valid-on-disk SSTable whose index has zero entries (so
    /// `first_key()` returns `None`). This passes the magic/footer/checksum
    /// checks at open time but violates the "L1+ SSTable is non-empty"
    /// invariant the read paths rely on.
    fn open_empty_index_sstable(dir: &std::path::Path) -> Arc<SstableReader> {
        let sst_path = dir.join("empty.sst");
        let writer = SstableWriter::create(
            &sst_path,
            4096,
            CompressionType::Uncompressed,
            0,
            Vec::new(),
            crate::FsyncMethod::Fsync,
        )
        .unwrap();
        writer.finish().unwrap();
        let reader = SstableReader::open(&sst_path, 999, 1, None).unwrap();
        assert!(
            reader.first_key().is_none(),
            "test fixture should produce an empty-index SSTable"
        );
        Arc::new(reader)
    }

    fn assert_corruption<T: std::fmt::Debug>(res: Result<T>) {
        match res {
            Err(StorageError::Corruption(_)) => {}
            other => panic!("expected Corruption error, got {other:?}"),
        }
    }

    /// A corrupted empty-index SSTable promoted to L1+ must surface a
    /// `Corruption` error from every read path instead of panicking.
    #[test]
    fn empty_l1_sstable_yields_corruption_not_panic() {
        let engine_dir = tempfile::TempDir::new().unwrap();
        let sst_dir = tempfile::TempDir::new().unwrap();
        let engine = StorageEngine::open(engine_dir.path(), EngineOptions::default()).unwrap();

        // Inject the empty-index SSTable at level 1.
        engine
            .inner
            .sstables
            .write()
            .unwrap()
            .insert(1, vec![open_empty_index_sstable(sst_dir.path())]);

        let key = DbKey::Int(42);
        assert_corruption(engine.get(&key));
        assert_corruption(engine.multi_get(std::slice::from_ref(&key)));
        assert_corruption(engine.inner.filtered_scan(
            &DbKey::Int(i64::MIN),
            &DbKey::Int(i64::MAX),
            |_, _| true,
        ));
        assert_corruption(
            engine
                .scan_iter(&DbKey::Int(i64::MIN), &DbKey::Int(i64::MAX))
                .map(|_| ()),
        );
    }

    /// Options that never auto-flush, so a test controls memtable rotation
    /// explicitly via `seal_active`/`flush_immutable`.
    fn no_autoflush_options() -> EngineOptions {
        EngineOptions {
            memtable_size_limit: 64 * 1024 * 1024,
            wal_size_limit: 64 * 1024 * 1024,
            ..Default::default()
        }
    }

    fn collect_scan(engine: &StorageEngine, lo: i64, hi: i64) -> Vec<(DbKey, DbValue)> {
        let mut it = engine.scan_iter(&DbKey::Int(lo), &DbKey::Int(hi)).unwrap();
        let mut out = Vec::new();
        while let Some(pair) = it.next().unwrap() {
            out.push(pair);
        }
        out
    }

    /// After sealing the active memtable, its data must remain visible to every
    /// read path from the immutable queue, and writes to the fresh active
    /// memtable (overwrites and tombstones) must take precedence over the sealed
    /// copy.
    #[test]
    fn sealed_memtable_is_readable_with_active_precedence() {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = StorageEngine::open(dir.path(), no_autoflush_options()).unwrap();

        engine.put(DbKey::Int(1), DbValue::Int(10)).unwrap();
        engine.put(DbKey::Int(2), DbValue::Int(20)).unwrap();
        engine.put(DbKey::Int(3), DbValue::Int(30)).unwrap();

        // Seal without flushing: the three keys move to the immutable queue and
        // the active memtable is reset.
        let imm = engine
            .inner
            .seal_active()
            .unwrap()
            .expect("active was non-empty");
        {
            let memset = engine.inner.memset.read().unwrap();
            assert_eq!(memset.immutable.len(), 1);
            assert!(memset.active.is_empty());
        }

        // Layer fresh writes over the sealed snapshot: overwrite 2, delete 3,
        // leave 1 untouched (served from the immutable memtable).
        engine.put(DbKey::Int(2), DbValue::Int(222)).unwrap();
        engine.delete(DbKey::Int(3)).unwrap();

        // get: 1 from immutable, 2 from active (newest), 3 shadowed by tombstone.
        assert_eq!(engine.get(&DbKey::Int(1)).unwrap(), Some(DbValue::Int(10)));
        assert_eq!(engine.get(&DbKey::Int(2)).unwrap(), Some(DbValue::Int(222)));
        assert_eq!(engine.get(&DbKey::Int(3)).unwrap(), None);

        // multi_get resolves the same precedence in one pass.
        let got = engine
            .multi_get(&[DbKey::Int(1), DbKey::Int(2), DbKey::Int(3)])
            .unwrap();
        assert_eq!(
            got,
            vec![Some(DbValue::Int(10)), Some(DbValue::Int(222)), None]
        );

        // Both scan paths merge the two memtables, drop the tombstoned key, and
        // keep the active overwrite.
        let expected = vec![
            (DbKey::Int(1), DbValue::Int(10)),
            (DbKey::Int(2), DbValue::Int(222)),
        ];
        assert_eq!(collect_scan(&engine, i64::MIN, i64::MAX), expected);
        let mut filtered = engine
            .inner
            .filtered_scan(&DbKey::Int(i64::MIN), &DbKey::Int(i64::MAX), |_, _| true)
            .unwrap();
        filtered.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(filtered, expected);

        // Keep `imm` alive until here so the queue entry it mirrors is not the
        // only owner being dropped early.
        drop(imm);
    }

    /// Flushing a sealed memtable persists it to an L0 SSTable, retires its WAL
    /// segment, empties the immutable queue, and leaves every key readable from
    /// disk.
    #[test]
    fn flush_immutable_persists_and_empties_queue() {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = StorageEngine::open(dir.path(), no_autoflush_options()).unwrap();

        engine.put(DbKey::Int(1), DbValue::Int(10)).unwrap();
        engine.put(DbKey::Int(2), DbValue::Int(20)).unwrap();

        let imm = engine
            .inner
            .seal_active()
            .unwrap()
            .expect("active was non-empty");
        let sealed_wal = wal_segment_path(dir.path(), imm.wal_id);
        assert!(sealed_wal.exists(), "sealed WAL segment should be on disk");

        engine.inner.flush_immutable(&imm).unwrap();

        // Queue drained, segment retired, data durable in an SSTable.
        assert!(engine.inner.memset.read().unwrap().immutable.is_empty());
        assert!(
            !sealed_wal.exists(),
            "flushed WAL segment should be removed"
        );
        let stats = engine.get_statistics().unwrap();
        assert!(stats.num_sstables >= 1);
        assert_eq!(stats.memtable_entries, 0);

        // Reads now come from the SSTable.
        assert_eq!(engine.get(&DbKey::Int(1)).unwrap(), Some(DbValue::Int(10)));
        assert_eq!(engine.get(&DbKey::Int(2)).unwrap(), Some(DbValue::Int(20)));
        assert_eq!(
            collect_scan(&engine, i64::MIN, i64::MAX),
            vec![
                (DbKey::Int(1), DbValue::Int(10)),
                (DbKey::Int(2), DbValue::Int(20)),
            ]
        );
    }

    /// `get_statistics` must count entries resident in immutable memtables, not
    /// just the active one.
    #[test]
    fn statistics_include_immutable_memtables() {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = StorageEngine::open(dir.path(), no_autoflush_options()).unwrap();

        engine.put(DbKey::Int(1), DbValue::Int(10)).unwrap();
        engine.put(DbKey::Int(2), DbValue::Int(20)).unwrap();
        let imm = engine
            .inner
            .seal_active()
            .unwrap()
            .expect("active was non-empty");
        // A fresh write lands in the new active memtable while the sealed one
        // still holds two entries.
        engine.put(DbKey::Int(3), DbValue::Int(30)).unwrap();

        let stats = engine.get_statistics().unwrap();
        assert_eq!(
            stats.memtable_entries, 3,
            "two sealed + one active entry must all be counted"
        );

        drop(imm);
    }
}
