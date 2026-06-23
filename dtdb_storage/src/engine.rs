use crate::executor::{CoalesceKey, Executor, PeriodicHandle, Priority};
use crate::manifest::{MANIFEST_LOG_FORMAT, Manifest, ManifestEdit};
use crate::memtable::MemTable;
use crate::snapshot_log::SnapshotLog;
use crate::sstable::{SstableReader, SstableWriter};
use crate::wal::{Wal, WalEntry};
use crate::{DbKey, DbValue, EngineOptions, Result, ScanIterator, StorageError, ValueRewriter};
use parking_lot::{Condvar, Mutex};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

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

/// Insert an L0 SSTable reader into `list`, keeping it sorted by id descending
/// (newest first) — the order the read paths rely on to resolve precedence.
///
/// Flushes pre-allocate ids in seal order but run concurrently and may finish
/// out of order, so ids no longer arrive monotonically and the reader can't
/// simply be pushed to the front. `partition_point` finds the slot just past
/// every higher id, preserving the descending invariant regardless of
/// completion order.
fn insert_l0_sorted(list: &mut Vec<Arc<SstableReader>>, reader: Arc<SstableReader>) {
    let pos = list.partition_point(|r| r.id > reader.id);
    list.insert(pos, reader);
}

pub struct StorageEngine {
    inner: Arc<EngineInner>,
}

/// A sealed memtable awaiting (or undergoing) flush to L0, paired with the id of
/// the WAL segment that holds its writes. The segment is retired once the
/// memtable's data is durable in an SSTable. Cheap to clone (a few `Arc`s and
/// `u64`s), so a copy can sit in the queue while another is handed to the flush.
///
/// `sst_id` is pre-allocated at seal time (under `write_mutex`, so in seal
/// order: older memtable → smaller id). Flushes then run concurrently and may
/// complete out of order, but the id — not the completion order — fixes L0
/// precedence (the level is kept sorted by id descending, newest first).
///
/// `flush` guards the flush itself: concurrent flushers (a background task and a
/// synchronous `flush`/`compact` draining the same queue) claim it so the L0
/// write, manifest edit, and WAL retirement happen exactly once per memtable.
#[derive(Clone)]
struct ImmMemtable {
    table: Arc<MemTable>,
    wal_id: u64,
    sst_id: u64,
    flush: Arc<FlushSlot>,
}

/// One-shot claim guarding the flush of a single [`ImmMemtable`]. The first
/// caller to find it `Pending` transitions it to `InFlight` and performs the
/// I/O; concurrent callers wait on `done` and observe `Completed` (returning
/// without redoing the work). On error the owner resets it to `Pending` so a
/// later drain retries — matching the pre-concurrency behavior where a failed
/// flush left the memtable queued.
struct FlushSlot {
    state: Mutex<SlotState>,
    done: Condvar,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SlotState {
    Pending,
    InFlight,
    Completed,
}

impl FlushSlot {
    fn new() -> Self {
        FlushSlot {
            state: Mutex::new(SlotState::Pending),
            done: Condvar::new(),
        }
    }
}

struct WriteQueue {
    tasks: VecDeque<Arc<WriteTask>>,
    writing: bool,
}

struct WriteTask {
    state: Mutex<WriteTaskState>,
    condvar: Condvar,
    /// Whether this task's write has been committed. The sole source of truth
    /// for completion: followers read it (spin path: lock-free; park path: under
    /// the queue lock) and the leader publishes it with Release ordering, which
    /// pairs with followers' Acquire load to make the `state.result` write
    /// visible. The leader sets it + notifies under the queue lock so the
    /// predicate-mutation and wakeup are serialized against a follower parking.
    done: AtomicBool,
}

struct WriteTaskState {
    batch: Vec<WalEntry>,
    /// Pre-built framed WAL record for this task's batch (populated by the
    /// writer thread before enqueuing, reused across writes). The leader reads
    /// this instead of re-serializing the merged batch, moving ~13% of the
    /// serial commit's CPU cost onto the parallel writer threads.
    serialized_frame: Vec<u8>,
    result: Result<()>,
}

thread_local! {
    /// Per-thread cache of one idle [`WriteTask`], reused across writes so the
    /// steady-state put path performs no per-write `Arc`/`Vec` allocation. A
    /// task is only returned here when its `Arc` is uniquely owned (see
    /// [`release_task`]), so reuse never races another holder.
    static WRITE_TASK_POOL: std::cell::RefCell<Option<Arc<WriteTask>>> =
        const { std::cell::RefCell::new(None) };
    /// Reusable scratch the leader collects coalesced tasks into.
    static LEADER_TASKS: std::cell::RefCell<Vec<Arc<WriteTask>>> =
        const { std::cell::RefCell::new(Vec::new()) };
    /// Reusable scratch the leader merges all tasks' entries into before the
    /// single WAL append + memtable apply.
    static LEADER_MERGED: std::cell::RefCell<Vec<WalEntry>> =
        const { std::cell::RefCell::new(Vec::new()) };
    /// Reusable scratch the leader concatenates coalesced tasks' pre-built WAL
    /// frames into for one `write()` syscall. Only used when coalescing actually
    /// batched more than one writer; a lone writer's frame is written directly.
    static LEADER_FRAMES: std::cell::RefCell<Vec<u8>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// Obtain a fresh-stated [`WriteTask`], reusing this thread's pooled one if
/// available (its `batch` allocation is retained) or allocating a new one.
fn acquire_task() -> Arc<WriteTask> {
    WRITE_TASK_POOL.with(|p| {
        if let Some(task) = p.borrow_mut().take() {
            // Uniquely owned (only pooled at strong_count == 1), so this lock is
            // uncontended; reset the state for the new write.
            let mut state = task.state.lock();
            state.batch.clear();
            state.serialized_frame.clear();
            state.result = Ok(());
            drop(state);
            task.done.store(false, Ordering::Relaxed);
            task
        } else {
            Arc::new(WriteTask {
                state: Mutex::new(WriteTaskState {
                    batch: Vec::new(),
                    serialized_frame: Vec::new(),
                    result: Ok(()),
                }),
                condvar: Condvar::new(),
                done: AtomicBool::new(false),
            })
        }
    })
}

/// Return a task to this thread's pool, but only if it is now uniquely owned —
/// otherwise another thread (a leader still holding it in its commit set) could
/// observe a reset mid-flight, so we simply drop it.
fn release_task(task: Arc<WriteTask>) {
    if Arc::strong_count(&task) == 1 {
        WRITE_TASK_POOL.with(|p| {
            *p.borrow_mut() = Some(task);
        });
    }
}

fn apply_entry_recursive(mem: &MemTable, ent: WalEntry) {
    match ent {
        WalEntry::Put { key, value } => mem.put(key, value),
        WalEntry::Delete { key } => mem.delete(key),
        WalEntry::Batch(sub) => {
            for e in sub {
                apply_entry_recursive(mem, e);
            }
        }
    }
}

fn clone_result(res: &Result<()>) -> Result<()> {
    match res {
        Ok(()) => Ok(()),
        Err(e) => Err(match e {
            StorageError::Io(err) => {
                StorageError::Io(std::io::Error::new(err.kind(), err.to_string()))
            }
            StorageError::Serialization(err) => StorageError::Serialization(err.clone()),
            StorageError::Compression(s) => StorageError::Compression(s.clone()),
            StorageError::Corruption(s) => StorageError::Corruption(s.clone()),
            StorageError::AlreadyExists(s) => StorageError::AlreadyExists(s.clone()),
            StorageError::InvalidOptions(s) => StorageError::InvalidOptions(s.clone()),
        }),
    }
}

/// The in-memory write buffers: one mutable `active` memtable plus a queue of
/// `immutable` memtables that have been sealed and are awaiting flush to L0.
///
/// Writes always land in `active`. Reads merge newest-to-oldest: `active` first,
/// then `immutable` from front (newest) to back (oldest), then the on-disk
/// SSTables. Each sealed memtable corresponds to a sealed WAL segment, so the
/// queue mirrors the segments on disk that have not yet been retired.
///
/// A full active memtable is sealed into `immutable` and flushed by a background
/// task, so writes don't block on flush I/O. The queue holds up to
/// `max_write_buffer_number - 1` memtables; a writer that needs to seal when the
/// queue is full stalls (see [`FlushGate`]) until a flush drains a slot.
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
    /// Serializes WAL-segment retirement so it happens strictly oldest-first.
    ///
    /// Concurrent flushers write SSTables in parallel and may finish out of
    /// order, but a segment may only be retired once it *and every older
    /// segment* is durable. This keeps the WAL segments surviving a crash a
    /// contiguous newest-suffix of the seal order — the invariant recovery
    /// relies on when it replays them into a single highest-id SSTable. Retiring
    /// a newer segment while an older one is still live would let recovery
    /// resurrect the older (stale) value over the newer flushed one.
    retire_mutex: Mutex<()>,
    /// Backpressure gate stalling writers when the immutable queue is full.
    flush_gate: FlushGate,
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
    /// Backpressure gate stalling writers when the Level 0 file count is too high.
    l0_gate: L0WriteStallGate,
    write_queue: Mutex<WriteQueue>,
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
        let mut st = self.inner.background.state.lock();
        st.active -= 1;
        if st.active == 0 {
            self.inner.background.idle.notify_all();
        }
    }
}

/// Write-path backpressure for the immutable-memtable queue.
///
/// `immutable_count` mirrors the length of [`MemSet::immutable`] but lives under
/// its own mutex so it can pair with a condvar without serializing the read-side
/// `memset` `RwLock`. A writer that needs to seal a full active memtable blocks
/// on `slot_freed` until `immutable_count` drops below the cap
/// (`max_write_buffer_number - 1`); a completing flush decrements the count and
/// wakes it. This is the LevelDB-style hard stall: writes wait for flushes to
/// catch up rather than letting the queue (and memory) grow without bound.
struct FlushGate {
    state: Mutex<FlushGateState>,
    /// Signalled when a flush frees an immutable slot, or on shutdown.
    slot_freed: Condvar,
}

struct FlushGateState {
    /// Immutable memtables sealed but not yet flushed. Incremented when an active
    /// memtable is sealed, decremented when its flush completes.
    immutable_count: usize,
    /// Set during quiesce so a stalled writer stops waiting and lets shutdown
    /// proceed instead of blocking forever on a flush that will never run.
    shutting_down: bool,
}

impl FlushGate {
    fn new() -> Self {
        FlushGate {
            state: Mutex::new(FlushGateState {
                immutable_count: 0,
                shutting_down: false,
            }),
            slot_freed: Condvar::new(),
        }
    }
}

struct L0WriteStallGate {
    state: Mutex<L0WriteStallGateState>,
    condvar: Condvar,
}

struct L0WriteStallGateState {
    shutting_down: bool,
}

impl L0WriteStallGate {
    fn new() -> Self {
        L0WriteStallGate {
            state: Mutex::new(L0WriteStallGateState {
                shutting_down: false,
            }),
            condvar: Condvar::new(),
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
            *inner.wal_sync_handle.lock() = Some(handle);
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

/// Merges two sorted memtable vectors newest-to-oldest, keeping the first (newest)
/// value seen per key and discarding older duplicates. Runs in linear O(N) time.
fn merge_sorted_mem_vectors(
    newest: Vec<(DbKey, Option<DbValue>)>,
    oldest: Vec<(DbKey, Option<DbValue>)>,
) -> Vec<(DbKey, Option<DbValue>)> {
    let mut res = Vec::with_capacity(newest.len() + oldest.len());
    let mut it_n = newest.into_iter().peekable();
    let mut it_o = oldest.into_iter().peekable();

    while it_n.peek().is_some() || it_o.peek().is_some() {
        match (it_n.peek(), it_o.peek()) {
            (Some(n), Some(o)) => {
                match n.0.cmp(&o.0) {
                    std::cmp::Ordering::Less => {
                        res.push(it_n.next().unwrap());
                    }
                    std::cmp::Ordering::Greater => {
                        res.push(it_o.next().unwrap());
                    }
                    std::cmp::Ordering::Equal => {
                        // Keys are equal. Since `newest` contains newer writes,
                        // keep it and discard the `oldest` copy.
                        res.push(it_n.next().unwrap());
                        let _ = it_o.next();
                    }
                }
            }
            (Some(_), None) => {
                res.push(it_n.next().unwrap());
            }
            (None, Some(_)) => {
                res.push(it_o.next().unwrap());
            }
            (None, None) => break,
        }
    }
    res
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
            let mut opts = postcard::from_bytes::<EngineOptions>(&bytes)?;
            if opts.l0_slowdown_writes_trigger < opts.l0_compaction_threshold {
                opts.l0_slowdown_writes_trigger = opts.l0_compaction_threshold;
            }
            if opts.l0_stop_writes_trigger <= opts.l0_slowdown_writes_trigger {
                opts.l0_stop_writes_trigger = opts.l0_slowdown_writes_trigger + 8;
            }
            opts
        } else {
            let mut opts = options;
            if opts.l0_slowdown_writes_trigger < opts.l0_compaction_threshold {
                opts.l0_slowdown_writes_trigger = opts.l0_compaction_threshold;
            }
            if opts.l0_stop_writes_trigger <= opts.l0_slowdown_writes_trigger {
                opts.l0_stop_writes_trigger = opts.l0_slowdown_writes_trigger + 8;
            }
            // Reject invalid options before persisting them to disk.
            opts.validate()?;
            let bytes = postcard::to_allocvec(&opts)?;
            crate::atomic_write(&options_path, &bytes, opts.fsync_method)?;
            opts
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
                apply_entry_recursive(&memtable, entry);
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
            insert_l0_sorted(sstables_map.entry(0).or_default(), Arc::new(reader));
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
            retire_mutex: Mutex::new(()),
            flush_gate: FlushGate::new(),
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
            l0_gate: L0WriteStallGate::new(),
            write_queue: Mutex::new(WriteQueue {
                tasks: VecDeque::new(),
                writing: false,
            }),
        })
    }

    /// Enqueue a write and run the leader/follower coalescing protocol. `fill`
    /// populates the (reused, empty) batch buffer of this thread's pooled task,
    /// avoiding an allocation per write for the entries themselves.
    fn execute_write(self: &Arc<Self>, fill: impl FnOnce(&mut Vec<WalEntry>)) -> Result<()> {
        let task = acquire_task();
        // Populate the batch and pre-serialize the WAL frame while we have
        // unique access (task not yet enqueued). This moves postcard work out
        // of the leader's serial section onto parallel writer threads.
        {
            let mut state = task.state.lock();
            fill(&mut state.batch);
            let frame_buf = std::mem::take(&mut state.serialized_frame);
            match Wal::build_frame(&state.batch, frame_buf) {
                Ok(frame) => state.serialized_frame = frame,
                Err(e) => {
                    state.batch.clear();
                    drop(state);
                    release_task(task);
                    return Err(e);
                }
            }
        }

        let mut queue = self.write_queue.lock();
        queue.tasks.push_back(task.clone());

        // Spin briefly before committing to a kernel park, but only when we are
        // a follower (a commit is already in flight and we're not at the front).
        // If we're immediately leader-eligible we skip the spin entirely — there
        // is nothing to wait for and the extra mutex drop/reacquire would be pure
        // overhead on uncontended single-threaded writes.
        //
        // parking_lot's Condvar has no built-in spin phase, so without this
        // every follower pays a full kernel round-trip even for sub-µs waits.
        //
        // The spin can't lose a wakeup: it's pure optimization layered on top of
        // the park loop below. After spinning we re-acquire the queue lock and
        // re-check `done` there, against which the leader publishes `done` (also
        // under the queue lock). So we either observe `done` and skip the wait,
        // or we park and the leader's later notify — serialized after our
        // registration by that same lock — wakes us.
        let is_follower =
            queue.writing || !queue.tasks.front().is_some_and(|f| Arc::ptr_eq(f, &task));
        if is_follower {
            drop(queue);
            const SPIN_LIMIT: u32 = 200;
            for _ in 0..SPIN_LIMIT {
                if task.done.load(Ordering::Acquire) {
                    break;
                }
                std::hint::spin_loop();
            }
            queue = self.write_queue.lock();
        }

        while !task.done.load(Ordering::Acquire)
            && (!queue.tasks.front().is_some_and(|f| Arc::ptr_eq(f, &task)) || queue.writing)
        {
            task.condvar.wait(&mut queue);
        }

        if task.done.load(Ordering::Acquire) {
            let result = clone_result(&task.state.lock().result);
            release_task(task);
            return result;
        }

        // Leader path: claim writing and collect all queued tasks into a reused
        // buffer.
        queue.writing = true;
        let mut tasks_to_commit = LEADER_TASKS.with(|c| std::mem::take(&mut *c.borrow_mut()));
        while let Some(t) = queue.tasks.pop_front() {
            tasks_to_commit.push(t);
        }
        drop(queue);

        let write_result = self.commit_coalesced(&tasks_to_commit);

        // Publish results and wake followers, then clear `writing` and wake the
        // next leader — all under the queue lock. Holding the queue lock here is
        // what makes the wakeup correct: a follower checks `done` and parks on
        // its condvar under this same lock, so serializing the `done` store +
        // `notify_one` against it closes the lost-wakeup window (a follower can't
        // read `done == false` and then park between our store and our notify).
        // `result` is written under the per-task `state` lock; the Release store
        // of `done` pairs with the follower's Acquire load to make it visible.
        let mut queue = self.write_queue.lock();
        for t in &tasks_to_commit {
            t.state.lock().result = clone_result(&write_result);
            t.done.store(true, Ordering::Release);
            t.condvar.notify_one();
        }
        queue.writing = false;
        if let Some(next_leader) = queue.tasks.front() {
            next_leader.condvar.notify_one();
        }
        drop(queue);

        // Drop our references to the committed tasks (so our own becomes
        // uniquely owned and poolable) and return the buffer for reuse.
        tasks_to_commit.clear();
        LEADER_TASKS.with(|c| *c.borrow_mut() = tasks_to_commit);
        release_task(task);

        write_result
    }

    /// Write all coalesced tasks' entries as one WAL batch and apply them to the
    /// active memtable. Drains each task's `batch` into a reused merge buffer,
    /// so neither the merge nor the memtable apply allocates.
    fn commit_coalesced(self: &Arc<Self>, tasks: &[Arc<WriteTask>]) -> Result<()> {
        let _write_lock = self.write_mutex.lock();
        self.await_l0_limit();
        self.maybe_slowdown_writes();

        // WAL write from the pre-built frames the writer threads serialized off
        // this critical path. The common single-writer case writes its frame
        // directly — no copy, no temporary allocation. When coalescing batched
        // multiple writers, concatenate their frames into a reused buffer for
        // one `write()` syscall; that copy is paid only when the parallel
        // pre-serialization already saved more than it costs.
        let wal_result = (|| -> Result<u64> {
            let mut wal = self.wal.lock();
            if let [only] = tasks {
                wal.append_prebuilt_bytes(&only.state.lock().serialized_frame)?;
            } else {
                let mut frames = LEADER_FRAMES.with(|c| std::mem::take(&mut *c.borrow_mut()));
                frames.clear();
                for t in tasks {
                    frames.extend_from_slice(&t.state.lock().serialized_frame);
                }
                let res = wal.append_prebuilt_bytes(&frames);
                frames.clear();
                LEADER_FRAMES.with(|c| *c.borrow_mut() = frames);
                res?;
            }
            wal.size()
        })();

        // Memtable apply: drain each task's batch into the merged buffer.
        let mut merged = LEADER_MERGED.with(|c| std::mem::take(&mut *c.borrow_mut()));
        merged.clear();
        for t in tasks {
            merged.append(&mut t.state.lock().batch);
        }

        let trigger_flush = match wal_result {
            Ok(wal_size) => {
                let memset = self.memset.read().unwrap();
                let mem = &memset.active;
                mem.apply_drain(&mut merged);
                let mem_full = mem.byte_size() >= self.options.memtable_size_limit;
                let wal_full = wal_size as usize >= self.options.wal_size_limit;
                mem_full || wal_full
            }
            Err(_) => false,
        };

        // `merged` is now empty (drained on success, or containing the failed
        // entries which we discard on error); return its allocation for reuse.
        merged.clear();
        LEADER_MERGED.with(|c| *c.borrow_mut() = merged);

        wal_result?;
        if trigger_flush {
            self.flush_active_in_background()?;
        }

        Ok(())
    }

    pub fn put(self: &Arc<Self>, key: DbKey, value: DbValue) -> Result<()> {
        self.execute_write(|batch| batch.push(WalEntry::Put { key, value }))
    }

    pub fn delete(self: &Arc<Self>, key: DbKey) -> Result<()> {
        self.execute_write(|batch| batch.push(WalEntry::Delete { key }))
    }

    pub fn write_batch(self: &Arc<Self>, mut entries: Vec<WalEntry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.execute_write(|batch| batch.append(&mut entries))
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
        // Seal under the write lock, then drain synchronously so the data is on
        // disk by the time this returns (callers rely on that). Draining also
        // sweeps up any immutable still queued from a background flush.
        {
            let _write_lock = self.write_mutex.lock();
            self.seal_active()?;
        }
        self.flush_pending()?;
        self.trigger_compaction();
        Ok(())
    }

    pub fn compact(self: &Arc<Self>) -> Result<()> {
        // 1. Flush active memtable to disk first so all data is in SSTables.
        {
            let _write_lock = self.write_mutex.lock();
            self.seal_active()?;
        }
        self.flush_pending()?;

        // 2. Run compaction synchronously
        let _compaction_lock = self.compaction_mutex.lock();

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
        let _lock = self.compaction_mutex.lock();
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
                let last_compacted = self.last_compacted_keys.lock().get(&source_level).cloned();
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
            self.manifest.lock().append_batch(edits)?;
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
                .insert(source_level, max_k.clone());
        }

        self.l0_gate.condvar.notify_all();
        Ok(())
    }

    /// Write-path flush: rotate the full active memtable into the immutable
    /// queue and hand it to a background drain, instead of writing it out inline.
    /// First applies backpressure so a slow flush can't let the queue grow
    /// without bound.
    ///
    /// Caller must hold `write_mutex`.
    fn flush_active_in_background(self: &Arc<Self>) -> Result<()> {
        // Block (still holding `write_mutex`, so all writers stall together)
        // until the immutable queue has a free slot.
        self.await_flush_slot();
        // Seal the active memtable into the queue, then schedule a background
        // flush for *this* memtable. `seal_active` returns `None` only if the
        // active memtable was empty, which a just-triggered flush won't be —
        // guard anyway.
        if let Some(imm) = self.seal_active()? {
            self.schedule_flush_imm(imm);
        }
        Ok(())
    }

    /// Maximum number of immutable memtables allowed in the queue before writers
    /// stall: `max_write_buffer_number - 1` (the rest being the one active
    /// memtable). `saturating_sub` is defensive — `validate` already requires
    /// `max_write_buffer_number >= 2`, so this is at least 1.
    fn immutable_cap(&self) -> usize {
        self.options.max_write_buffer_number.saturating_sub(1)
    }

    /// Block until the immutable queue has room for another sealed memtable
    /// (fewer than [`Self::immutable_cap`]). Returns immediately once the engine
    /// is shutting down so a stalled writer can't outlive the flusher.
    ///
    /// Caller holds `write_mutex`; this keeps holding it across the wait, which
    /// is the intended backpressure — every writer stalls until a slot frees.
    fn await_flush_slot(&self) {
        let cap = self.immutable_cap();
        let mut st = self.flush_gate.state.lock();
        while st.immutable_count >= cap && !st.shutting_down {
            self.flush_gate.slot_freed.wait(&mut st);
        }
    }

    /// Drain the immutable queue to L0, oldest first, until it is empty.
    ///
    /// Each memtable's flush is guarded by its own [`FlushSlot`], so this is safe
    /// to run concurrently with the background flush tasks: a memtable already
    /// being flushed by a background task is awaited (not re-flushed), and one
    /// not yet started is claimed and flushed here. Used by the synchronous
    /// `flush_memtable`/`compact` paths, which must guarantee the queue is on
    /// disk by the time they return.
    fn flush_pending(&self) -> Result<()> {
        loop {
            // Re-read the back (oldest) each iteration: a concurrent seal may have
            // appended a newer memtable to the front while we flushed.
            let imm = self.memset.read().unwrap().immutable.back().cloned();
            let Some(imm) = imm else { break };
            self.flush_immutable(&imm)?;
        }
        Ok(())
    }

    /// Schedule a background task to flush a single sealed memtable, then kick
    /// compaction (L0 will have grown).
    ///
    /// Keyed per memtable (by WAL id), so distinct memtables flush concurrently
    /// on separate executor workers; a duplicate submit for the *same* memtable
    /// coalesces. The [`FlushSlot`] is the real guard against double I/O — the
    /// coalesce key only avoids a redundant queued task.
    fn schedule_flush_imm(self: &Arc<Self>, imm: ImmMemtable) {
        if self.background.state.lock().shutting_down {
            return;
        }
        let inner = self.clone();
        let key = CoalesceKey::new(format!("flush:{}:{}", self.dir_path.display(), imm.wal_id));
        self.executor.submit(
            Priority::High,
            Some(key),
            Box::new(move || {
                // Register as in-flight so `quiesce` joins us; bail without
                // touching state if the engine shut down between submit and run.
                let Some(_bg) = inner.enter_background() else {
                    return;
                };
                if let Err(e) = inner.flush_immutable(&imm) {
                    tracing::error!(error = ?e, "background flush failed");
                }
                inner.trigger_compaction();
            }),
        );
    }

    /// Seal the active memtable: move it into the immutable queue (newest at the
    /// front) and install a fresh, empty active memtable backed by a new WAL
    /// segment so subsequent writes have somewhere to go. Returns the sealed
    /// memtable paired with its WAL-segment id, or `None` if the active memtable
    /// was empty (nothing to seal).
    ///
    /// Caller must hold `write_mutex`, which serializes sealing against writers
    /// and against other seals.
    fn seal_active(&self) -> Result<Option<ImmMemtable>> {
        // Fast path: an empty active memtable means an empty WAL segment too —
        // nothing to seal or flush.
        if self.memset.read().unwrap().active.is_empty() {
            return Ok(None);
        }

        let sealed_wal_id = self.active_wal_id.load(Ordering::SeqCst);
        // Allocate the L0 id now, under `write_mutex`, so ids follow seal order
        // (older memtable → smaller id). Flushes run concurrently and may finish
        // out of order, but this id — not completion order — fixes L0 precedence.
        let sst_id = self.get_next_id();
        let new_wal_id = self.get_next_wal_id();
        let new_wal_path = wal_segment_path(&self.dir_path, new_wal_id);
        let new_wal = Wal::open(
            &new_wal_path,
            self.options.wal_sync_interval_ms,
            self.options.fsync_method,
        )?;
        // Make the new segment's directory entry durable *now*, before any write
        // reaches it. The flush that retires the old segment runs in the
        // background, so unlike a fully synchronous flush we can't defer this to
        // the old segment's removal: a crash could otherwise lose the new
        // segment's directory entry along with every write logged to it.
        crate::fsync_parent_dir(&new_wal_path, self.options.fsync_method)?;

        // Redirect new writes to the fresh segment, then seal the active memtable
        // and swap in an empty one. Readers never touch the WAL and writers are
        // excluded by `write_mutex`, so the order of these two swaps is
        // immaterial to anyone observing the engine.
        {
            let mut wal_guard = self.wal.lock();
            *wal_guard = new_wal;
        }
        self.active_wal_id.store(new_wal_id, Ordering::SeqCst);

        let sealed = {
            let mut memset = self.memset.write().unwrap();
            let table = std::mem::replace(&mut memset.active, Arc::new(MemTable::new()));
            let imm = ImmMemtable {
                table,
                wal_id: sealed_wal_id,
                sst_id,
                flush: Arc::new(FlushSlot::new()),
            };
            // The queue keeps its own handle (front = newest) so readers merge
            // over the sealed memtable until the flush removes it; the returned
            // clone shares the same `Arc`, so both observe the same data.
            memset.immutable.push_front(imm.clone());
            imm
        };
        // Account for the newly occupied immutable slot so backpressure sees it.
        self.flush_gate.state.lock().immutable_count += 1;
        Ok(Some(sealed))
    }

    /// Flush a sealed memtable to L0, claiming its [`FlushSlot`] so the work runs
    /// exactly once even when a background task and a synchronous
    /// `flush`/`compact` target the same memtable concurrently.
    ///
    /// The first caller to find the slot `Pending` performs the flush; others
    /// wait until it is `Completed` and return. On error the slot resets to
    /// `Pending` so a later drain retries, and any waiters are woken to take
    /// over — matching the pre-concurrency behavior where a failed flush left the
    /// memtable queued.
    fn flush_immutable(&self, imm: &ImmMemtable) -> Result<()> {
        // Claim the SSTable write, or wait for whoever owns it. `Completed` means
        // the SSTable is durable and registered — but the memtable may still be
        // in the queue awaiting in-order WAL retirement, so even a waiter falls
        // through to `retire_durable_prefix` below to help the queue drain.
        let we_own = {
            let mut state = imm.flush.state.lock();
            loop {
                match *state {
                    SlotState::Completed => break false,
                    SlotState::InFlight => {
                        imm.flush.done.wait(&mut state);
                    }
                    SlotState::Pending => {
                        *state = SlotState::InFlight;
                        break true;
                    }
                }
            }
        };

        if we_own {
            // Write the SSTable (the expensive, parallelizable part). Publish the
            // outcome and wake waiters: `Completed` on success, `Pending` on
            // error so a waiter — or a later drain — retries. On error, skip
            // retirement: nothing new became durable.
            let result = self.write_immutable_sstable(imm);
            {
                let mut state = imm.flush.state.lock();
                *state = if result.is_ok() {
                    SlotState::Completed
                } else {
                    SlotState::Pending
                };
            }
            imm.flush.done.notify_all();
            result?;
        }

        // This memtable's SSTable is durable. Retire WAL segments for the
        // contiguous run of oldest immutables whose SSTables are now durable.
        self.retire_durable_prefix()
    }

    /// Write a sealed memtable to its pre-allocated L0 SSTable and register it
    /// (manifest + in-memory level). Caller owns the [`FlushSlot`] (see
    /// [`Self::flush_immutable`]), so this never runs concurrently for the same
    /// memtable, but distinct memtables run it in parallel.
    ///
    /// This does *not* retire the WAL segment or dequeue the memtable; that is
    /// [`Self::retire_durable_prefix`]'s job, kept separate so retirement stays
    /// ordered even though SSTable writes finish out of order.
    fn write_immutable_sstable(&self, imm: &ImmMemtable) -> Result<()> {
        let entries = imm.table.entries();

        // The L0 id was pre-allocated at seal time (in seal order); use it so L0
        // precedence is fixed regardless of which flush finishes first.
        let sst_id = imm.sst_id;
        let sst_path = self.dir_path.join(format!("L0_{:05}.sst", sst_id));

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

        // Record the newly flushed SSTable in the manifest. AddSstable edits form
        // a set, so recording them out of id order across concurrent flushers is
        // fine.
        self.manifest.lock().append(ManifestEdit::AddSstable {
            level: 0,
            id: sst_id,
        })?;

        let reader = SstableReader::open(&sst_path, sst_id, 0, self.block_cache.clone())?;
        {
            let mut ssts = self.sstables.write().unwrap();
            // Concurrent flushers finish out of order, so keep L0 sorted by id
            // (newest first) rather than assuming this is the newest.
            insert_l0_sorted(ssts.entry(0).or_default(), Arc::new(reader));
        }

        Ok(())
    }

    /// Retire WAL segments for the longest run of *oldest* immutable memtables
    /// whose SSTables are durable, dropping each from the queue and freeing its
    /// backpressure slot.
    ///
    /// Retirement is strictly oldest-first (and serialized by `retire_mutex`), so
    /// the segments left on disk are always a contiguous newest-suffix of the
    /// seal order — the invariant recovery relies on. A memtable whose SSTable is
    /// durable but which sits behind an older, not-yet-flushed one stays queued
    /// until that gap fills; whichever flush closes the gap cascades the retire.
    ///
    /// Crash-safe by ordering: each SSTable and its manifest edit are durable
    /// (via [`Self::write_immutable_sstable`]) before its segment is deleted here,
    /// so a crash mid-retire leaves the segment on disk and recovery replays it.
    fn retire_durable_prefix(&self) -> Result<()> {
        let _retire_lock = self.retire_mutex.lock();
        loop {
            // The oldest immutable is at the back. Peek it without holding the
            // write lock across the (slow) fsync below.
            let oldest = self.memset.read().unwrap().immutable.back().cloned();
            let Some(oldest) = oldest else { break };

            // Stop at the first immutable whose SSTable is not yet durable: we
            // must not retire past it, or a crash would strand an older live
            // segment behind a retired newer one.
            let durable = matches!(*oldest.flush.state.lock(), SlotState::Completed);
            if !durable {
                break;
            }

            // The data is durably in an SSTable, so retire the segment. A crash
            // before this completes leaves the segment on disk; recovery replays
            // it idempotently into a new SSTable.
            let seg_path = wal_segment_path(&self.dir_path, oldest.wal_id);
            fs::remove_file(&seg_path)?;
            crate::fsync_parent_dir(&seg_path, self.options.fsync_method)?;

            // Drop it from the queue (still the back — only this method pops, and
            // it is serialized; seals only push to the front).
            {
                let mut memset = self.memset.write().unwrap();
                memset.immutable.pop_back();
            }

            // Free the slot and wake any writer stalled on backpressure. Done
            // after the dequeue so a woken writer never observes more immutables
            // than the count claims.
            {
                let mut st = self.flush_gate.state.lock();
                st.immutable_count = st.immutable_count.saturating_sub(1);
            }
            self.flush_gate.slot_freed.notify_all();
        }
        Ok(())
    }

    /// Snapshot the `[start, end]` range of every in-memory buffer (the active
    /// memtable plus any immutable memtables) into one sorted vector, keeping the
    /// newest value (or tombstone) per key. Seeds the scan merge with a stable
    /// view of the memtables whose internal precedence is already resolved.
    fn snapshot_mem_range(&self, start: &DbKey, end: &DbKey) -> Vec<(DbKey, Option<DbValue>)> {
        let (active_entries, imm_entries) = {
            let memset = self.memset.read().unwrap();
            if memset.immutable.is_empty() {
                return memset.active.scan_range_raw(start, end);
            }
            let active_entries = memset.active.scan_range_raw(start, end);
            let imm_entries: Vec<Vec<(DbKey, Option<DbValue>)>> = memset
                .immutable
                .iter()
                .map(|imm| imm.table.scan_range_raw(start, end))
                .collect();
            (active_entries, imm_entries)
        };

        let mut merged = active_entries;
        for imm in imm_entries {
            merged = merge_sorted_mem_vectors(merged, imm);
        }
        merged
    }

    /// Begin a background-task region. Returns a guard that marks the task as
    /// in-flight (decrementing on drop), or `None` if the engine is shutting
    /// down — in which case the caller MUST return without touching engine
    /// state. See [`BackgroundGate`].
    fn enter_background(&self) -> Option<BackgroundGuard<'_>> {
        let mut st = self.background.state.lock();
        if st.shutting_down {
            return None;
        }
        st.active += 1;
        Some(BackgroundGuard { inner: self })
    }

    fn await_l0_limit(&self) {
        let stop_trigger = self.options.l0_stop_writes_trigger;
        let mut st = self.l0_gate.state.lock();
        while !st.shutting_down {
            let l0_count = {
                let ssts = self.sstables.read().unwrap();
                ssts.get(&0).map_or(0, |list| list.len())
            };
            if l0_count < stop_trigger {
                break;
            }
            self.l0_gate.condvar.wait(&mut st);
        }
    }

    fn maybe_slowdown_writes(&self) {
        let slowdown_trigger = self.options.l0_slowdown_writes_trigger;
        let stop_trigger = self.options.l0_stop_writes_trigger;
        let l0_count = {
            let ssts = self.sstables.read().unwrap();
            ssts.get(&0).map_or(0, |list| list.len())
        };

        if l0_count >= slowdown_trigger && l0_count < stop_trigger {
            let diff = l0_count - slowdown_trigger;
            let sleep_ms = (1u64 << diff).min(self.options.l0_slowdown_max_sleep_ms as u64);
            std::thread::sleep(std::time::Duration::from_millis(sleep_ms));
        }
    }

    /// Quiesce all background work: stop scheduling, then block until any task
    /// that had already begun has finished. After this returns, no background
    /// task will touch engine state, so the directory can be safely reopened.
    /// Idempotent.
    fn quiesce(&self) {
        // Cancel the periodic WAL-sync schedule so no further ticks are
        // submitted (dropping the handle cancels it).
        *self.wal_sync_handle.lock() = None;

        // Release any writer stalled on backpressure: a flush that would have
        // freed its slot may now no-op (shutting down), so without this the
        // writer could wait forever and the process couldn't drain.
        {
            let mut fg = self.flush_gate.state.lock();
            fg.shutting_down = true;
        }
        self.flush_gate.slot_freed.notify_all();

        {
            let mut st = self.l0_gate.state.lock();
            st.shutting_down = true;
        }
        self.l0_gate.condvar.notify_all();

        let mut st = self.background.state.lock();
        st.shutting_down = true;
        while st.active > 0 {
            self.background.idle.wait(&mut st);
        }
    }

    fn trigger_compaction(self: &Arc<Self>) {
        // Don't schedule new compaction once the engine is shutting down; a task
        // submitted now would only no-op (see the `enter_background` guard
        // below), and skipping the submit keeps the executor queue clean.
        if self.background.state.lock().shutting_down {
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
                let _compaction_lock = inner.compaction_mutex.lock();
                if let Err(e) = inner.compact_if_needed_locked() {
                    tracing::error!(error = ?e, "background compaction failed");
                }
            }),
        );
    }

    pub fn sync_wal(&self) -> Result<()> {
        let wal = self.wal.lock();
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

    /// An [`Executor`] that *stores* submitted tasks instead of running them, so
    /// a test drives the background flush (and compaction) deterministically via
    /// `run_all`. Periodic work is delegated to an [`InlineExecutor`] (unused —
    /// these tests disable WAL syncing).
    struct ManualExecutor {
        tasks: Mutex<Vec<crate::executor::Task>>,
        inline: crate::InlineExecutor,
    }

    impl ManualExecutor {
        fn new() -> Self {
            ManualExecutor {
                tasks: Mutex::new(Vec::new()),
                inline: crate::InlineExecutor,
            }
        }

        fn pending(&self) -> usize {
            self.tasks.lock().len()
        }

        /// Run every currently-stored task. Tasks submitted *during* a run (e.g. a
        /// flush scheduling compaction, or a woken writer scheduling its own
        /// flush) are left for a later `run_all`.
        fn run_all(&self) {
            let tasks = std::mem::take(&mut *self.tasks.lock());
            for task in tasks {
                task();
            }
        }

        /// Run every currently-stored task in reverse submission order. With one
        /// flush task queued per seal (newest submitted last), this drives the
        /// flushes to *complete* newest-first — the out-of-order case that the L0
        /// id ordering must tolerate. Tasks submitted during the run are left for
        /// later.
        fn run_reversed(&self) {
            let mut tasks = std::mem::take(&mut *self.tasks.lock());
            tasks.reverse();
            for task in tasks {
                task();
            }
        }

        /// Run just the oldest queued task (the first submitted). With one flush
        /// task per seal, this completes the oldest immutable's flush — the one
        /// whose retirement frees a backpressure slot.
        fn run_one(&self) {
            let task = {
                let mut tasks = self.tasks.lock();
                if tasks.is_empty() {
                    None
                } else {
                    Some(tasks.remove(0))
                }
            };
            if let Some(task) = task {
                task();
            }
        }
    }

    impl Executor for ManualExecutor {
        fn submit(
            &self,
            _priority: Priority,
            _key: Option<CoalesceKey>,
            task: crate::executor::Task,
        ) {
            self.tasks.lock().push(task);
        }

        fn submit_periodic(
            &self,
            every: std::time::Duration,
            priority: Priority,
            task: Box<dyn Fn() + Send + Sync + 'static>,
        ) -> PeriodicHandle {
            self.inline.submit_periodic(every, priority, task)
        }
    }

    /// Options whose memtable is so small that any single write overflows it and
    /// triggers a flush, with WAL syncing off so the manual executor stays idle
    /// until a flush is scheduled. `max_write_buffer_number` sets the immutable
    /// cap (`max_write_buffer_number - 1`).
    fn tiny_memtable_options(max_write_buffer_number: usize) -> EngineOptions {
        EngineOptions {
            memtable_size_limit: 5,
            wal_size_limit: 64 * 1024 * 1024,
            wal_sync_interval_ms: None,
            max_write_buffer_number,
            ..Default::default()
        }
    }

    fn open_manual(
        dir: &std::path::Path,
        exec: Arc<ManualExecutor>,
        max_write_buffer_number: usize,
    ) -> StorageEngine {
        StorageEngine::open_with_executor(
            dir,
            tiny_memtable_options(max_write_buffer_number),
            exec,
            Arc::new(crate::PassthroughValueRewriter),
        )
        .unwrap()
    }

    fn immutable_count(engine: &StorageEngine) -> usize {
        engine.inner.flush_gate.state.lock().immutable_count
    }

    /// The ids of the L0 SSTables in list order. The read paths require this to be
    /// sorted descending (newest first), so tests assert on it directly.
    fn l0_ids(engine: &StorageEngine) -> Vec<u64> {
        engine
            .inner
            .sstables
            .read()
            .unwrap()
            .get(&0)
            .map(|list| list.iter().map(|r| r.id).collect())
            .unwrap_or_default()
    }

    /// A write-path flush is deferred to a background task: the active memtable is
    /// sealed immediately (data readable from the immutable queue) but the SSTable
    /// only appears once the background drain runs, which then empties the queue.
    #[test]
    fn background_flush_is_deferred_and_completes_on_drain() {
        let dir = tempfile::TempDir::new().unwrap();
        let exec = Arc::new(ManualExecutor::new());
        let engine = open_manual(dir.path(), exec.clone(), 2);

        // Overflows the tiny memtable: seals it and schedules a flush, but the
        // ManualExecutor hasn't run anything yet.
        engine.put(DbKey::Int(1), DbValue::Int(10)).unwrap();

        assert_eq!(
            immutable_count(&engine),
            1,
            "active memtable should be sealed"
        );
        assert!(
            exec.pending() >= 1,
            "a background flush should be scheduled"
        );
        assert_eq!(
            engine.get_statistics().unwrap().num_sstables,
            0,
            "the flush must not have run yet"
        );
        // Served from the immutable memtable in the meantime.
        assert_eq!(engine.get(&DbKey::Int(1)).unwrap(), Some(DbValue::Int(10)));

        // Run the background flush.
        exec.run_all();

        assert_eq!(immutable_count(&engine), 0, "the slot should be freed");
        assert!(engine.inner.memset.read().unwrap().immutable.is_empty());
        assert_eq!(engine.get_statistics().unwrap().num_sstables, 1);
        // Now served from the SSTable.
        assert_eq!(engine.get(&DbKey::Int(1)).unwrap(), Some(DbValue::Int(10)));
    }

    /// With the single immutable slot full and no flush run, a second
    /// flush-triggering write must stall until a flush frees the slot.
    #[test]
    fn backpressure_stalls_writer_until_flush_frees_a_slot() {
        let dir = tempfile::TempDir::new().unwrap();
        let exec = Arc::new(ManualExecutor::new());
        let engine = open_manual(dir.path(), exec.clone(), 2);

        // Fill the only immutable slot (cap = 1); leave the flush un-run.
        engine.put(DbKey::Int(1), DbValue::Int(10)).unwrap();
        assert_eq!(immutable_count(&engine), 1);

        std::thread::scope(|s| {
            // This write needs to seal, but the queue is full — it must block.
            let writer = s.spawn(|| {
                engine.put(DbKey::Int(2), DbValue::Int(20)).unwrap();
            });

            // Let it reach the backpressure wait, then confirm it is still stuck.
            std::thread::sleep(std::time::Duration::from_millis(100));
            assert!(
                !writer.is_finished(),
                "writer should stall while the immutable queue is full"
            );

            // Draining the queue frees a slot and must unblock the writer.
            exec.run_all();
            writer.join().unwrap();
        });

        // Both writes are durable/visible regardless of where they now live.
        assert_eq!(engine.get(&DbKey::Int(1)).unwrap(), Some(DbValue::Int(10)));
        assert_eq!(engine.get(&DbKey::Int(2)).unwrap(), Some(DbValue::Int(20)));
    }

    /// Shutting down must release a writer stalled on backpressure rather than
    /// hang: the queued flush that would free its slot may never run once the
    /// engine is shutting down.
    #[test]
    fn shutdown_releases_writer_stalled_on_backpressure() {
        let dir = tempfile::TempDir::new().unwrap();
        let exec = Arc::new(ManualExecutor::new());
        let engine = open_manual(dir.path(), exec.clone(), 2);

        engine.put(DbKey::Int(1), DbValue::Int(10)).unwrap();
        assert_eq!(immutable_count(&engine), 1);

        std::thread::scope(|s| {
            let writer = s.spawn(|| {
                engine.put(DbKey::Int(2), DbValue::Int(20)).unwrap();
            });

            std::thread::sleep(std::time::Duration::from_millis(100));
            assert!(!writer.is_finished(), "writer should be stalled");

            // Without waking stalled writers, this would block forever.
            engine.shutdown();
            writer.join().unwrap();
        });
    }

    /// `max_write_buffer_number` raises the cap: with N=4 the write path can seal
    /// three immutable memtables before it stalls on the fourth, where cap=1
    /// would stall on the second.
    #[test]
    fn larger_cap_buffers_multiple_seals_before_stalling() {
        let dir = tempfile::TempDir::new().unwrap();
        let exec = Arc::new(ManualExecutor::new());
        // max_write_buffer_number = 4 -> immutable cap = 3.
        let engine = open_manual(dir.path(), exec.clone(), 4);

        // Three flush-triggering writes fill the three slots without stalling,
        // even though no flush has run yet.
        engine.put(DbKey::Int(1), DbValue::Int(10)).unwrap();
        engine.put(DbKey::Int(2), DbValue::Int(20)).unwrap();
        engine.put(DbKey::Int(3), DbValue::Int(30)).unwrap();
        assert_eq!(
            immutable_count(&engine),
            3,
            "three slots should fill freely"
        );
        assert_eq!(
            engine.get_statistics().unwrap().num_sstables,
            0,
            "no flush has run yet"
        );

        std::thread::scope(|s| {
            // The fourth write exceeds the cap and must stall.
            let writer = s.spawn(|| {
                engine.put(DbKey::Int(4), DbValue::Int(40)).unwrap();
            });

            std::thread::sleep(std::time::Duration::from_millis(100));
            assert!(
                !writer.is_finished(),
                "fourth write should stall at the cap of three"
            );

            // Draining frees slots and unblocks the writer.
            exec.run_all();
            writer.join().unwrap();
        });

        // All four writes are visible regardless of where they ended up.
        for (k, v) in [(1, 10), (2, 20), (3, 30), (4, 40)] {
            assert_eq!(engine.get(&DbKey::Int(k)).unwrap(), Some(DbValue::Int(v)));
        }
    }

    // --- Concurrent flush: out-of-order completion (test set #1) ---------------

    /// Two seals of the *same* key flushed newest-first must still read the
    /// newest value. L0 precedence is fixed by the id pre-allocated at seal time
    /// (older seal → smaller id), not by which flush finishes first, and the
    /// level is kept sorted by id descending so the newer SSTable is consulted
    /// first.
    #[test]
    fn newest_seal_flushed_first_still_reads_newest() {
        let dir = tempfile::TempDir::new().unwrap();
        let exec = Arc::new(ManualExecutor::new());
        // cap = 2 so both seals fit without stalling.
        let engine = open_manual(dir.path(), exec.clone(), 3);

        // Each write overflows the tiny memtable, sealing a single-key memtable
        // and queuing its own flush task. The second overwrites key 1.
        engine.put(DbKey::Int(1), DbValue::Int(100)).unwrap();
        engine.put(DbKey::Int(1), DbValue::Int(200)).unwrap();
        assert_eq!(immutable_count(&engine), 2, "both seals should be queued");
        // Newest value already wins from the immutable queue (front = newest).
        assert_eq!(engine.get(&DbKey::Int(1)).unwrap(), Some(DbValue::Int(200)));

        // Flush newest-first (reverse submission order): the newer memtable's
        // SSTable lands on disk before the older one's.
        exec.run_reversed();

        assert_eq!(immutable_count(&engine), 0, "queue should be drained");
        let ids = l0_ids(&engine);
        assert_eq!(ids.len(), 2, "two L0 SSTables expected");
        assert!(
            ids[0] > ids[1],
            "L0 must stay sorted newest-first by id, got {ids:?}"
        );
        // The newest write wins even though its flush completed first.
        assert_eq!(engine.get(&DbKey::Int(1)).unwrap(), Some(DbValue::Int(200)));
    }

    /// As above, but the newer write is a tombstone: flushing it out of order
    /// (before the older put) must still shadow the put, yielding `None`.
    #[test]
    fn newer_tombstone_flushed_first_still_shadows_older_put() {
        let dir = tempfile::TempDir::new().unwrap();
        let exec = Arc::new(ManualExecutor::new());
        let engine = open_manual(dir.path(), exec.clone(), 3);

        engine.put(DbKey::Int(1), DbValue::Int(100)).unwrap();
        engine.delete(DbKey::Int(1)).unwrap();
        assert_eq!(immutable_count(&engine), 2);
        assert_eq!(engine.get(&DbKey::Int(1)).unwrap(), None);

        // Flush the tombstone's memtable before the put's.
        exec.run_reversed();

        assert_eq!(immutable_count(&engine), 0);
        let ids = l0_ids(&engine);
        assert_eq!(ids.len(), 2);
        assert!(ids[0] > ids[1], "L0 must stay newest-first, got {ids:?}");
        // Tombstone (newer) still wins over the older put.
        assert_eq!(engine.get(&DbKey::Int(1)).unwrap(), None);
    }

    // --- Concurrent flush: FlushSlot claim protocol (test set #2) --------------

    /// Flushing the same sealed memtable twice is idempotent: the second call
    /// observes the slot already `Completed` and does no I/O — no duplicate
    /// SSTable, no double WAL removal (which would error), no double slot
    /// decrement.
    #[test]
    fn double_flush_of_same_immutable_is_idempotent() {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = StorageEngine::open(dir.path(), no_autoflush_options()).unwrap();

        engine.put(DbKey::Int(1), DbValue::Int(10)).unwrap();
        let imm = engine
            .inner
            .seal_active()
            .unwrap()
            .expect("active was non-empty");
        assert_eq!(immutable_count(&engine), 1);

        engine.inner.flush_immutable(&imm).unwrap();
        assert_eq!(immutable_count(&engine), 0);
        assert_eq!(l0_ids(&engine).len(), 1);

        // Second flush must be a clean no-op rather than re-running the I/O (the
        // WAL segment is already gone, so a re-run would fail on remove_file).
        engine.inner.flush_immutable(&imm).unwrap();
        assert_eq!(immutable_count(&engine), 0, "count must not go negative");
        assert_eq!(l0_ids(&engine).len(), 1, "no duplicate SSTable");
        assert_eq!(engine.get(&DbKey::Int(1)).unwrap(), Some(DbValue::Int(10)));
    }

    /// Two threads flushing the same sealed memtable concurrently: the slot lets
    /// exactly one perform the I/O while the other waits and observes
    /// completion. Both return `Ok`, and the engine shows a single SSTable and a
    /// single slot decrement.
    #[test]
    fn concurrent_flush_of_same_immutable_runs_io_once() {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = StorageEngine::open(dir.path(), no_autoflush_options()).unwrap();

        engine.put(DbKey::Int(1), DbValue::Int(10)).unwrap();
        engine.put(DbKey::Int(2), DbValue::Int(20)).unwrap();
        let imm = engine
            .inner
            .seal_active()
            .unwrap()
            .expect("active was non-empty");
        assert_eq!(immutable_count(&engine), 1);

        let barrier = Arc::new(std::sync::Barrier::new(2));
        std::thread::scope(|s| {
            for _ in 0..2 {
                let engine = &engine;
                let imm = imm.clone();
                let barrier = barrier.clone();
                s.spawn(move || {
                    // Maximize the race: both claim at the same instant.
                    barrier.wait();
                    engine.inner.flush_immutable(&imm).unwrap();
                });
            }
        });

        // I/O ran exactly once: one SSTable, one decrement, segment retired.
        assert_eq!(immutable_count(&engine), 0);
        assert_eq!(l0_ids(&engine).len(), 1, "exactly one SSTable written");
        assert!(
            !wal_segment_path(dir.path(), imm.wal_id).exists(),
            "WAL segment should be retired once"
        );
        assert_eq!(engine.get(&DbKey::Int(1)).unwrap(), Some(DbValue::Int(10)));
        assert_eq!(engine.get(&DbKey::Int(2)).unwrap(), Some(DbValue::Int(20)));
    }

    // --- Concurrent flush: synchronous + background coexistence (test set #3) --

    /// A synchronous `compact()` draining the queue while a background flush task
    /// for the same memtable is still pending: `compact` claims the slot and does
    /// the flush, and the later background task observes `Completed` and no-ops.
    /// No duplicate SSTable, no error, slot freed exactly once.
    #[test]
    fn compact_drains_queue_then_pending_background_task_noops() {
        let dir = tempfile::TempDir::new().unwrap();
        let exec = Arc::new(ManualExecutor::new());
        let engine = open_manual(dir.path(), exec.clone(), 3);

        // Seal a memtable; its flush task is queued but not run.
        engine.put(DbKey::Int(1), DbValue::Int(10)).unwrap();
        assert_eq!(immutable_count(&engine), 1);
        assert!(exec.pending() >= 1, "a background flush should be queued");

        // Synchronous compaction flushes the queue itself (claiming the slot),
        // then compacts. The data is durable when it returns.
        engine.inner.compact().unwrap();
        assert_eq!(immutable_count(&engine), 0, "compact drained the queue");
        assert!(engine.inner.memset.read().unwrap().immutable.is_empty());
        let sstables_after_compact = engine.get_statistics().unwrap().num_sstables;
        assert_eq!(engine.get(&DbKey::Int(1)).unwrap(), Some(DbValue::Int(10)));

        // The still-queued background flush task now runs: it must see the slot
        // already Completed and do nothing — no second SSTable, no panic.
        exec.run_all();
        assert_eq!(immutable_count(&engine), 0, "no double decrement");
        assert_eq!(
            engine.get_statistics().unwrap().num_sstables,
            sstables_after_compact,
            "stale background task must not add an SSTable"
        );
        assert_eq!(engine.get(&DbKey::Int(1)).unwrap(), Some(DbValue::Int(10)));
    }

    // --- Concurrent flush: backpressure per slot (test set #5) -----------------

    /// With concurrent per-memtable flush tasks, completing a *single* flush
    /// frees exactly one backpressure slot (not the whole queue) and wakes one
    /// stalled writer. Running the oldest immutable's task is what frees a slot,
    /// since retirement is strictly oldest-first.
    #[test]
    fn one_flush_frees_one_slot_and_wakes_one_writer() {
        let dir = tempfile::TempDir::new().unwrap();
        let exec = Arc::new(ManualExecutor::new());
        // max_write_buffer_number = 4 -> cap = 3.
        let engine = open_manual(dir.path(), exec.clone(), 4);

        // Fill all three slots; three flush tasks queued, none run.
        engine.put(DbKey::Int(1), DbValue::Int(10)).unwrap();
        engine.put(DbKey::Int(2), DbValue::Int(20)).unwrap();
        engine.put(DbKey::Int(3), DbValue::Int(30)).unwrap();
        assert_eq!(immutable_count(&engine), 3);

        std::thread::scope(|s| {
            // Fourth write exceeds the cap and stalls.
            let writer = s.spawn(|| {
                engine.put(DbKey::Int(4), DbValue::Int(40)).unwrap();
            });

            std::thread::sleep(std::time::Duration::from_millis(100));
            assert!(
                !writer.is_finished(),
                "fourth write should stall at the cap"
            );

            // Completing just the oldest flush frees one slot — enough to admit
            // the stalled writer, which then seals its own memtable (back to 3).
            exec.run_one();
            writer.join().unwrap();
        });

        // One SSTable on disk (the one flush we ran); the queue is full again
        // because the woken writer sealed a fresh immutable.
        assert_eq!(l0_ids(&engine).len(), 1, "only one flush should have run");
        assert_eq!(
            immutable_count(&engine),
            3,
            "woken writer refilled the slot"
        );

        for (k, v) in [(1, 10), (2, 20), (3, 30), (4, 40)] {
            assert_eq!(engine.get(&DbKey::Int(k)).unwrap(), Some(DbValue::Int(v)));
        }
    }

    /// A newer memtable's flush completing while an older one is still pending
    /// must *not* free a slot: retirement is oldest-first, so the newer one stays
    /// queued (its data durable in an SSTable, but its WAL segment retained)
    /// until the older gap fills. This is the crash-safety invariant that keeps
    /// surviving WAL segments a contiguous newest-suffix.
    #[test]
    fn newer_flush_does_not_retire_past_older_pending_one() {
        let dir = tempfile::TempDir::new().unwrap();
        let exec = Arc::new(ManualExecutor::new());
        let engine = open_manual(dir.path(), exec.clone(), 4); // cap = 3

        engine.put(DbKey::Int(1), DbValue::Int(10)).unwrap();
        engine.put(DbKey::Int(2), DbValue::Int(20)).unwrap();
        assert_eq!(immutable_count(&engine), 2);

        // Run only the newest task (last submitted). Its SSTable becomes durable,
        // but the older immutable is still Pending, so nothing retires.
        let newest_task = exec.tasks.lock().pop().unwrap();
        newest_task();

        assert_eq!(
            immutable_count(&engine),
            2,
            "newer flush must not retire while an older one is pending"
        );
        assert_eq!(l0_ids(&engine).len(), 1, "newer SSTable is durable");
        assert_eq!(
            engine.inner.memset.read().unwrap().immutable.len(),
            2,
            "both immutables remain queued until the older one flushes"
        );

        // Now run the remaining (older) task: it retires, then cascades to the
        // already-durable newer one. Queue drains fully.
        exec.run_all();
        assert_eq!(
            immutable_count(&engine),
            0,
            "older flush cascades the retire"
        );
        assert_eq!(l0_ids(&engine).len(), 2);
        assert_eq!(engine.get(&DbKey::Int(1)).unwrap(), Some(DbValue::Int(10)));
        assert_eq!(engine.get(&DbKey::Int(2)).unwrap(), Some(DbValue::Int(20)));
    }

    // --- Concurrent flush: shutdown with writers stalled (test set #8) ---------

    /// Shutdown must release *every* writer stalled on backpressure, not just
    /// one: with the queue full and several writers waiting, the queued flushes
    /// may never run once shutting down, so each stalled writer must be woken.
    #[test]
    fn shutdown_releases_all_writers_stalled_on_backpressure() {
        let dir = tempfile::TempDir::new().unwrap();
        let exec = Arc::new(ManualExecutor::new());
        let engine = open_manual(dir.path(), exec.clone(), 4); // cap = 3

        // Fill the three slots; no flush runs.
        engine.put(DbKey::Int(1), DbValue::Int(10)).unwrap();
        engine.put(DbKey::Int(2), DbValue::Int(20)).unwrap();
        engine.put(DbKey::Int(3), DbValue::Int(30)).unwrap();
        assert_eq!(immutable_count(&engine), 3);

        std::thread::scope(|s| {
            // Three writers all need to seal but the queue is full — all stall.
            let engine = &engine;
            let writers: Vec<_> = (4..7)
                .map(|k| {
                    s.spawn(move || {
                        engine.put(DbKey::Int(k), DbValue::Int(k * 10)).unwrap();
                    })
                })
                .collect();

            std::thread::sleep(std::time::Duration::from_millis(100));
            assert!(
                writers.iter().all(|w| !w.is_finished()),
                "all writers should be stalled while the queue is full"
            );

            // Without waking every stalled writer, some joins would hang forever.
            engine.shutdown();
            for w in writers {
                w.join().unwrap();
            }
        });
    }

    #[test]
    fn test_l0_slowdown_exponential_sleep() {
        let engine_dir = tempfile::TempDir::new().unwrap();
        let sst_dir = tempfile::TempDir::new().unwrap();
        let opts = EngineOptions {
            l0_compaction_threshold: 2,
            l0_slowdown_writes_trigger: 2,
            l0_stop_writes_trigger: 6,
            l0_slowdown_max_sleep_ms: 16,
            ..EngineOptions::default()
        };

        let engine = StorageEngine::open(engine_dir.path(), opts).unwrap();
        let sst = open_empty_index_sstable(sst_dir.path());

        // 0 L0 files: no slowdown
        let start = std::time::Instant::now();
        engine.put(DbKey::Int(1), DbValue::Int(10)).unwrap();
        let elapsed = start.elapsed();
        assert!(elapsed < std::time::Duration::from_millis(100));

        // Inject 2 L0 files -> slowdown sleep = 2^0 = 1ms
        {
            let mut ssts = engine.inner.sstables.write().unwrap();
            ssts.insert(0, vec![sst.clone(), sst.clone()]);
        }
        let start = std::time::Instant::now();
        engine.put(DbKey::Int(2), DbValue::Int(20)).unwrap();
        let elapsed = start.elapsed();
        assert!(elapsed >= std::time::Duration::from_millis(1));

        // Inject 4 L0 files -> slowdown sleep = 2^2 = 4ms
        {
            let mut ssts = engine.inner.sstables.write().unwrap();
            ssts.insert(0, vec![sst.clone(), sst.clone(), sst.clone(), sst.clone()]);
        }
        let start = std::time::Instant::now();
        engine.put(DbKey::Int(3), DbValue::Int(30)).unwrap();
        let elapsed = start.elapsed();
        assert!(elapsed >= std::time::Duration::from_millis(4));

        // Inject 5 L0 files -> slowdown sleep = 2^3 = 8ms
        {
            let mut ssts = engine.inner.sstables.write().unwrap();
            ssts.insert(
                0,
                vec![
                    sst.clone(),
                    sst.clone(),
                    sst.clone(),
                    sst.clone(),
                    sst.clone(),
                ],
            );
        }
        let start = std::time::Instant::now();
        engine.put(DbKey::Int(4), DbValue::Int(40)).unwrap();
        let elapsed = start.elapsed();
        assert!(elapsed >= std::time::Duration::from_millis(8));
    }

    #[test]
    fn test_l0_stop_writes_blocks_writer_until_compaction() {
        let engine_dir = tempfile::TempDir::new().unwrap();
        let sst_dir = tempfile::TempDir::new().unwrap();
        let opts = EngineOptions {
            l0_compaction_threshold: 2,
            l0_slowdown_writes_trigger: 2,
            l0_stop_writes_trigger: 3,
            ..EngineOptions::default()
        };

        let engine = StorageEngine::open(engine_dir.path(), opts).unwrap();
        let sst = open_empty_index_sstable(sst_dir.path());

        // Inject 3 L0 files (reaches stop trigger)
        {
            let mut ssts = engine.inner.sstables.write().unwrap();
            ssts.insert(0, vec![sst.clone(), sst.clone(), sst.clone()]);
        }

        std::thread::scope(|s| {
            let engine = &engine;
            let writer = s.spawn(move || {
                engine.put(DbKey::Int(1), DbValue::Int(10)).unwrap();
            });

            // Wait a bit and verify writer is blocked/stalled
            std::thread::sleep(std::time::Duration::from_millis(50));
            assert!(!writer.is_finished());

            // Clear L0 files to simulate compaction completion
            {
                let mut ssts = engine.inner.sstables.write().unwrap();
                ssts.insert(0, vec![]);
            }
            engine.inner.l0_gate.condvar.notify_all();

            // The writer should now unblock and finish
            std::thread::sleep(std::time::Duration::from_millis(50));
            assert!(writer.is_finished());
            writer.join().unwrap();
        });
    }

    #[test]
    fn test_l0_stop_writes_unblocks_on_shutdown() {
        let engine_dir = tempfile::TempDir::new().unwrap();
        let sst_dir = tempfile::TempDir::new().unwrap();
        let opts = EngineOptions {
            l0_compaction_threshold: 2,
            l0_slowdown_writes_trigger: 2,
            l0_stop_writes_trigger: 3,
            ..EngineOptions::default()
        };

        let engine = StorageEngine::open(engine_dir.path(), opts).unwrap();
        let sst = open_empty_index_sstable(sst_dir.path());

        // Inject 3 L0 files (reaches stop trigger)
        {
            let mut ssts = engine.inner.sstables.write().unwrap();
            ssts.insert(0, vec![sst.clone(), sst.clone(), sst.clone()]);
        }

        std::thread::scope(|s| {
            let engine = &engine;
            let writer = s.spawn(move || {
                // This will block
                let _ = engine.put(DbKey::Int(1), DbValue::Int(10));
            });

            // Wait a bit and verify writer is blocked/stalled
            std::thread::sleep(std::time::Duration::from_millis(50));
            assert!(!writer.is_finished());

            // Shutdown the engine, which must unblock the stalled writer
            engine.shutdown();

            // The writer should now finish
            std::thread::sleep(std::time::Duration::from_millis(50));
            assert!(writer.is_finished());
            writer.join().unwrap();
        });
    }

    #[test]
    fn test_snapshot_mem_range_multiple_memtables() {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = StorageEngine::open(dir.path(), no_autoflush_options()).unwrap();

        // 1. Write initial values (will end up in the oldest immutable memtable)
        engine.put(DbKey::Int(1), DbValue::Int(10)).unwrap();
        engine.put(DbKey::Int(2), DbValue::Int(20)).unwrap();
        engine.put(DbKey::Int(3), DbValue::Int(30)).unwrap();

        // Seal via the real engine path — rotates the WAL and assigns proper ids.
        let _imm1 = engine
            .inner
            .seal_active()
            .unwrap()
            .expect("active was non-empty");

        // 2. Write second round of values (will end up in a newer immutable memtable)
        engine.put(DbKey::Int(2), DbValue::Int(22)).unwrap(); // update key 2
        engine.delete(DbKey::Int(3)).unwrap(); // delete key 3 (tombstone)
        engine.put(DbKey::Int(4), DbValue::Int(40)).unwrap(); // new key 4

        // Seal again.
        let _imm2 = engine
            .inner
            .seal_active()
            .unwrap()
            .expect("active was non-empty");

        // 3. Write final values (will end up in the active memtable)
        engine.put(DbKey::Int(1), DbValue::Int(11)).unwrap(); // update key 1
        engine.put(DbKey::Int(5), DbValue::Int(50)).unwrap(); // new key 5

        let start = DbKey::Int(0);
        let end = DbKey::Int(10);
        let merged = engine.inner.snapshot_mem_range(&start, &end);

        // Expected sorted merged results:
        // Key 1: newest is 11 (active memtable)
        // Key 2: newest is 22 (newer immutable memtable)
        // Key 3: newest is None/Tombstone (newer immutable memtable)
        // Key 4: newest is 40 (newer immutable memtable)
        // Key 5: newest is 50 (active memtable)
        let expected = vec![
            (DbKey::Int(1), Some(DbValue::Int(11))),
            (DbKey::Int(2), Some(DbValue::Int(22))),
            (DbKey::Int(3), None),
            (DbKey::Int(4), Some(DbValue::Int(40))),
            (DbKey::Int(5), Some(DbValue::Int(50))),
        ];
        assert_eq!(merged, expected);
    }
}
