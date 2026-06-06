use dtdb_storage::sstable::SstableWriter;
use dtdb_storage::wal::{Wal, WalEntry};
use dtdb_storage::{
    CoalesceKey, CompressionType, DbKey, DbValue, EngineOptions, Executor, InlineExecutor,
    PassthroughValueRewriter, PeriodicHandle, Priority, Result, StorageEngine, ValueRewriter,
};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tempfile::TempDir;

// Helper to create keys and values
fn k_int(val: i64) -> DbKey {
    DbKey::Int(val)
}

fn v_str(val: &str) -> DbValue {
    DbValue::string(val)
}

fn get_test_options() -> EngineOptions {
    EngineOptions {
        compression: CompressionType::Uncompressed,
        memtable_size_limit: 1024,
        block_size_limit: 64,
        wal_size_limit: 1024 * 1024,
        l0_compaction_threshold: 2,
        sstable_target_size: 1024,
        base_level_size_limit: 10 * 1024,
        level_size_multiplier: 10,
        max_level: 7,
        block_cache_capacity: 1000,
        wal_sync_interval_ms: None,
        fsync_method: dtdb_storage::FsyncMethod::default(),
        // One immutable slot keeps these recovery tests' segment counts
        // deterministic; they exercise recovery, not write backpressure.
        max_write_buffer_number: 2,
        l0_slowdown_writes_trigger: 8,
        l0_stop_writes_trigger: 16,
        l0_slowdown_max_sleep_ms: 16,
    }
}

#[test]
fn test_wal_tolerant_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("active.wal");

    // 1. Write some valid entries to WAL
    {
        let mut wal = Wal::open(&wal_path, None, dtdb_storage::FsyncMethod::default()).unwrap();
        wal.append_put(&k_int(1), &v_str("apple")).unwrap();
        wal.append_put(&k_int(2), &v_str("banana")).unwrap();
    }

    // 2. Corrupt/truncate the end of the WAL file
    {
        let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
        // Write a truncated length prefix (only 2 bytes instead of 4)
        file.write_all(&[42u8, 0u8]).unwrap();
        file.sync_all().unwrap();
    }

    // 3. Recover from the corrupted WAL.
    // It should cleanly recover the 2 valid entries and ignore the trailing corruption.
    let entries = Wal::recover(&wal_path).unwrap();
    assert_eq!(entries.len(), 2);

    match &entries[0] {
        WalEntry::Put { key, value } => {
            assert_eq!(key, &k_int(1));
            assert_eq!(value, &v_str("apple"));
        }
        _ => panic!("Expected Put entry"),
    }

    match &entries[1] {
        WalEntry::Put { key, value } => {
            assert_eq!(key, &k_int(2));
            assert_eq!(value, &v_str("banana"));
        }
        _ => panic!("Expected Put entry"),
    }
}

#[test]
fn test_atomic_sstable_write() {
    let temp_dir = TempDir::new().unwrap();
    let sst_path = temp_dir.path().join("L0_00001.sst");
    let temp_sst_path = temp_dir.path().join("L0_00001.sst.tmp");

    // 1. Start writing but crash (drop without finish)
    {
        let mut writer = SstableWriter::create(
            &sst_path,
            100,
            CompressionType::Uncompressed,
            1,
            vec![],
            dtdb_storage::FsyncMethod::Fsync,
        )
        .unwrap();
        writer.append(&k_int(1), Some(&v_str("apple"))).unwrap();
        // Dropped here, simulated crash
    }

    // Temp file should exist, but final file should not
    assert!(temp_sst_path.exists());
    assert!(!sst_path.exists());

    // Clean up temp file
    let _ = fs::remove_file(&temp_sst_path);

    // 2. Write and successfully finish
    {
        let mut writer = SstableWriter::create(
            &sst_path,
            100,
            CompressionType::Uncompressed,
            1,
            vec![],
            dtdb_storage::FsyncMethod::Fsync,
        )
        .unwrap();
        writer.append(&k_int(1), Some(&v_str("apple"))).unwrap();
        writer.finish().unwrap();
    }

    // Temp file should be renamed/gone, and final file should exist
    assert!(!temp_sst_path.exists());
    assert!(sst_path.exists());
}

#[test]
fn test_compaction_crash_garbage_collection() {
    let temp_dir = TempDir::new().unwrap();
    let options = get_test_options();

    // 1. Setup engine and write data to create active SSTables
    {
        let engine = StorageEngine::open(temp_dir.path(), options).unwrap();
        engine.put(k_int(1), v_str("apple")).unwrap();
        engine.flush_memtable().unwrap();
        engine.put(k_int(2), v_str("banana")).unwrap();
        engine.flush_memtable().unwrap();
        // Dropping the engine quiesces any background compaction the second
        // flush kicked off (l0_compaction_threshold = 2), so it can't still be
        // deleting files when we reopen below.
    }

    // Verify sstable files exist on disk
    let files_before = fs::read_dir(temp_dir.path())
        .unwrap()
        .map(|r| r.unwrap().path())
        .collect::<Vec<_>>();
    assert!(
        files_before
            .iter()
            .any(|p| p.file_name().unwrap() == "manifest" && p.is_dir())
    );

    // 2. Simulate a compaction crash by creating a dummy garbage SSTable on disk
    // which is not registered in the manifest
    let garbage_sst_path = temp_dir.path().join("L0_99999.sst");
    fs::write(
        &garbage_sst_path,
        b"garbage data from interrupted compaction",
    )
    .unwrap();
    assert!(garbage_sst_path.exists());

    // 3. Reopen the StorageEngine. It should scan the directory, detect
    // the unregistered SSTable, and delete it automatically.
    {
        let _engine = StorageEngine::open(temp_dir.path(), options).unwrap();

        // The garbage SSTable should have been deleted!
        assert!(
            !garbage_sst_path.exists(),
            "Garbage SSTable file was not cleaned up!"
        );
    }
}

// ---------------------------------------------------------------------------
// Engine shutdown / quiescence
// ---------------------------------------------------------------------------

/// Sorted list of `*.sst` file names in `dir` (the engine's on-disk SSTable
/// set), so a test can assert the set is unchanged without depending on fsync
/// timing.
fn list_sst_files(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.ends_with(".sst"))
        .collect();
    names.sort();
    names
}

/// An [`Executor`] that *stores* submitted one-shot tasks instead of running
/// them, so a test can deterministically control when (and whether) background
/// compaction executes — no threads, no timing. Periodic work is delegated to
/// an [`InlineExecutor`] (unused here since these tests disable WAL syncing).
struct ManualExecutor {
    tasks: Mutex<Vec<Box<dyn FnOnce() + Send + 'static>>>,
    inline: InlineExecutor,
}

impl ManualExecutor {
    fn new() -> Self {
        ManualExecutor {
            tasks: Mutex::new(Vec::new()),
            inline: InlineExecutor,
        }
    }

    fn pending(&self) -> usize {
        self.tasks.lock().unwrap().len()
    }

    /// Run every stored task, draining the queue.
    fn run_all(&self) {
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        for task in tasks {
            task();
        }
    }

    /// Run (and remove) the task at `index`, leaving the rest queued. Lets a test
    /// complete a *newer* flush while an older one stays pending — the
    /// out-of-order completion concurrent flush allows. Indexing by position is
    /// stable here because flush tasks are submitted in seal order and each
    /// flush's follow-up compaction task is appended at the end.
    fn run_at(&self, index: usize) {
        let task = {
            let mut tasks = self.tasks.lock().unwrap();
            assert!(index < tasks.len(), "no task at index {index}");
            tasks.remove(index)
        };
        task();
    }
}

impl Executor for ManualExecutor {
    fn submit(
        &self,
        _priority: Priority,
        _key: Option<CoalesceKey>,
        task: Box<dyn FnOnce() + Send + 'static>,
    ) {
        self.tasks.lock().unwrap().push(task);
    }

    fn submit_periodic(
        &self,
        every: Duration,
        priority: Priority,
        task: Box<dyn Fn() + Send + Sync + 'static>,
    ) -> PeriodicHandle {
        self.inline.submit_periodic(every, priority, task)
    }
}

/// After `shutdown()`, a background compaction that was already submitted (but
/// had not yet begun) must not run against the engine — otherwise it could
/// delete manifest-registered SSTable files out from under a concurrent reopen,
/// surfacing as a spurious ENOENT. Fully deterministic: the `ManualExecutor`
/// only runs the queued compaction *after* we shut down.
#[test]
fn shutdown_prevents_queued_compaction_from_touching_state() {
    let temp_dir = TempDir::new().unwrap();
    let options = get_test_options(); // l0_compaction_threshold = 2, no WAL sync

    let exec = Arc::new(ManualExecutor::new());
    let engine = StorageEngine::open_with_executor(
        temp_dir.path(),
        options,
        exec.clone(),
        Arc::new(PassthroughValueRewriter),
    )
    .unwrap();

    // Two flushes reach l0_compaction_threshold, so a background compaction is
    // submitted — but the ManualExecutor only stores it; nothing runs yet.
    engine.put(k_int(1), v_str("apple")).unwrap();
    engine.flush_memtable().unwrap();
    engine.put(k_int(2), v_str("banana")).unwrap();
    engine.flush_memtable().unwrap();

    assert!(
        exec.pending() >= 1,
        "expected a background compaction to be queued after crossing the L0 threshold"
    );

    // Snapshot the SSTable set while compaction has not run.
    let ssts_before = list_sst_files(temp_dir.path());
    assert!(!ssts_before.is_empty());

    // Quiesce, then release the queued compaction. It must observe the shutdown
    // flag and return without creating or deleting any SSTable.
    engine.shutdown();
    exec.run_all();

    let ssts_after = list_sst_files(temp_dir.path());
    assert_eq!(
        ssts_before, ssts_after,
        "a compaction submitted before shutdown must not mutate state afterward"
    );

    // The directory is consistent: reopening sees every registered file.
    drop(engine);
    let reopened = StorageEngine::open(temp_dir.path(), options).unwrap();
    assert_eq!(reopened.get(&k_int(1)).unwrap(), Some(v_str("apple")));
    assert_eq!(reopened.get(&k_int(2)).unwrap(), Some(v_str("banana")));
}

/// A [`ValueRewriter`] that blocks the first value it rewrites until released,
/// letting a test hold a compaction *in flight* at a deterministic point (no
/// fsync/sleep timing). Compaction only invokes the rewriter on a layout
/// mismatch, so the test arranges one.
struct BlockingRewriter {
    started: Mutex<Option<mpsc::Sender<()>>>,
    release: Arc<(Mutex<bool>, Condvar)>,
    fired: AtomicBool,
}

impl ValueRewriter for BlockingRewriter {
    fn rewrite(&self, _src: &[u8], _dst: &[u8], value: &DbValue) -> Result<DbValue> {
        if !self.fired.swap(true, Ordering::SeqCst) {
            // Signal that a compaction is now in flight, then block.
            if let Some(tx) = self.started.lock().unwrap().take() {
                let _ = tx.send(());
            }
            let (lock, cv) = &*self.release;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = cv.wait(released).unwrap();
            }
        }
        Ok(value.clone())
    }
}

/// `shutdown()` must *wait* for an in-flight compaction to finish — not return
/// while a worker is still mutating engine state. We pin a compaction inside the
/// rewriter, call `shutdown()` on another thread, and prove it cannot return
/// until we release the compaction. Deterministic: while the compaction is
/// parked the in-flight count is non-zero, so `quiesce` provably blocks.
#[test]
fn shutdown_waits_for_in_flight_compaction() {
    let temp_dir = TempDir::new().unwrap();
    let options = get_test_options(); // l0_compaction_threshold = 2

    let (started_tx, started_rx) = mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let rewriter = Arc::new(BlockingRewriter {
        started: Mutex::new(Some(started_tx)),
        release: release.clone(),
        fired: AtomicBool::new(false),
    });

    // InlineExecutor runs each submitted compaction on its own thread, so the
    // compaction runs concurrently with our shutdown call.
    let engine = Arc::new(
        StorageEngine::open_with_executor(
            temp_dir.path(),
            options,
            Arc::new(InlineExecutor),
            rewriter,
        )
        .unwrap(),
    );

    // First SSTable is written with the default (empty) layout.
    engine.put(k_int(1), v_str("apple")).unwrap();
    engine.flush_memtable().unwrap();

    // Switch the target layout so compacting the first SSTable forces a rewrite
    // (and thus blocks inside BlockingRewriter).
    engine.set_target_layout(vec![1]);

    // Second flush crosses the L0 threshold and triggers background compaction.
    engine.put(k_int(2), v_str("banana")).unwrap();
    engine.flush_memtable().unwrap();

    // Wait until a compaction is parked in the rewriter (in flight).
    started_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("a background compaction should have started rewriting");

    // Call shutdown() on another thread; it must block while compaction runs.
    let returned = Arc::new(AtomicBool::new(false));
    let join = {
        let engine = engine.clone();
        let returned = returned.clone();
        std::thread::spawn(move || {
            engine.shutdown();
            returned.store(true, Ordering::SeqCst);
        })
    };

    // The compaction is still parked (in-flight count > 0), so shutdown cannot
    // have returned yet. This holds regardless of thread scheduling.
    assert!(
        !returned.load(Ordering::SeqCst),
        "shutdown() returned while a compaction was still in flight"
    );

    // Release the compaction; shutdown must now complete.
    {
        let (lock, cv) = &*release;
        *lock.lock().unwrap() = true;
        cv.notify_all();
    }
    join.join().unwrap();
    assert!(returned.load(Ordering::SeqCst));

    // The compaction ran to completion and the engine is consistent on reopen.
    drop(engine);
    let reopened = StorageEngine::open(temp_dir.path(), options).unwrap();
    assert_eq!(reopened.get(&k_int(1)).unwrap(), Some(v_str("apple")));
    assert_eq!(reopened.get(&k_int(2)).unwrap(), Some(v_str("banana")));
}

fn wal_segment_path(dir: &std::path::Path, id: u64) -> std::path::PathBuf {
    dir.join(format!("wal_{id:020}.wal"))
}

fn list_wal_segments(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut segs: Vec<std::path::PathBuf> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.extension().is_some_and(|ext| ext == "wal")
                && p.file_stem()
                    .and_then(|s| s.to_str())
                    .is_some_and(|s| s.starts_with("wal_"))
        })
        .collect();
    segs.sort();
    segs
}

// Several WAL segments left on disk (as a multi-memtable engine or a crash
// mid-rotation would leave them) must all be replayed oldest-first on open, so
// the newest write of each key wins. The replayed segments are then retired and
// a single fresh active segment is opened past every id seen.
#[test]
fn test_recovery_replays_multiple_wal_segments_in_order() {
    let temp_dir = TempDir::new().unwrap();
    let options = get_test_options();

    // Older segment (lower id): establishes k1, k2.
    {
        let mut wal = Wal::open(
            wal_segment_path(temp_dir.path(), 5),
            None,
            options.fsync_method,
        )
        .unwrap();
        wal.append_put(&k_int(1), &v_str("old")).unwrap();
        wal.append_put(&k_int(2), &v_str("keep")).unwrap();
    }
    // Newer segment (higher id): overrides k1, adds k4.
    {
        let mut wal = Wal::open(
            wal_segment_path(temp_dir.path(), 9),
            None,
            options.fsync_method,
        )
        .unwrap();
        wal.append_put(&k_int(1), &v_str("new")).unwrap();
        wal.append_put(&k_int(4), &v_str("four")).unwrap();
    }

    {
        let engine = StorageEngine::open(temp_dir.path(), options).unwrap();
        // Newer segment wins for the overlapping key.
        assert_eq!(engine.get(&k_int(1)).unwrap(), Some(v_str("new")));
        assert_eq!(engine.get(&k_int(2)).unwrap(), Some(v_str("keep")));
        assert_eq!(engine.get(&k_int(4)).unwrap(), Some(v_str("four")));
        assert_eq!(engine.get(&k_int(3)).unwrap(), None);
    }

    // The two replayed segments are gone; exactly one fresh active segment
    // remains, numbered past the highest id seen (9 -> 10).
    let segments = list_wal_segments(temp_dir.path());
    assert_eq!(segments.len(), 1, "expected one fresh active segment");
    assert_eq!(segments[0], wal_segment_path(temp_dir.path(), 10));
    assert!(
        Wal::recover(&segments[0]).unwrap().is_empty(),
        "fresh active segment should hold no records"
    );
}

// A pre-segmentation `active.wal` must still be recovered (replayed before any
// numbered segment) so existing directories migrate transparently.
#[test]
fn test_recovery_migrates_legacy_active_wal() {
    let temp_dir = TempDir::new().unwrap();
    let options = get_test_options();

    {
        let mut wal = Wal::open(
            temp_dir.path().join("active.wal"),
            None,
            options.fsync_method,
        )
        .unwrap();
        wal.append_put(&k_int(1), &v_str("legacy")).unwrap();
    }

    {
        let engine = StorageEngine::open(temp_dir.path(), options).unwrap();
        assert_eq!(engine.get(&k_int(1)).unwrap(), Some(v_str("legacy")));
    }

    // Legacy file retired; a numbered active segment now exists.
    assert!(!temp_dir.path().join("active.wal").exists());
    assert_eq!(list_wal_segments(temp_dir.path()).len(), 1);
}

/// Concurrent flush can write a newer memtable's SSTable before an older one's.
/// WAL retirement, however, is strictly oldest-first, so a crash after such an
/// out-of-order flush leaves *every* not-yet-retired segment on disk — a
/// contiguous newest-suffix. Recovery replays them oldest-first, so the newest
/// value of each key wins.
///
/// Were retirement done per-flush (retiring the newer segment as soon as it
/// flushed), the older segment would survive alone and recovery would replay its
/// stale value into the highest-id SSTable, shadowing the newer flushed value.
#[test]
fn out_of_order_flush_then_crash_recovers_newest_values() {
    let temp_dir = TempDir::new().unwrap();
    // Tiny memtable so every write seals; room for several immutables.
    let options = EngineOptions {
        memtable_size_limit: 1,
        max_write_buffer_number: 6,
        ..get_test_options()
    };

    {
        let exec = Arc::new(ManualExecutor::new());
        let engine = StorageEngine::open_with_executor(
            temp_dir.path(),
            options,
            exec.clone(),
            Arc::new(PassthroughValueRewriter),
        )
        .unwrap();

        // Three seals: imm0 = {1: old}, imm1 = {2: keep}, imm2 = {1: new}.
        engine.put(k_int(1), v_str("v1-old")).unwrap();
        engine.put(k_int(2), v_str("keep")).unwrap();
        engine.put(k_int(1), v_str("v1-new")).unwrap();
        assert_eq!(list_wal_segments(temp_dir.path()).len(), 4); // 3 sealed + 1 active

        // Complete the two newer flushes out of order, leaving the oldest
        // (imm0, holding the stale value of key 1) un-flushed. Flush tasks are
        // queued in seal order [imm0, imm1, imm2]; run imm2 then imm1.
        exec.run_at(2); // imm2's SSTable
        exec.run_at(1); // imm1's SSTable

        // Both newer SSTables are durable, but oldest-first retirement is blocked
        // behind imm0, so no WAL segment has been retired yet.
        assert_eq!(list_sst_files(temp_dir.path()).len(), 2);
        assert_eq!(
            list_wal_segments(temp_dir.path()).len(),
            4,
            "no segment may retire while the oldest flush is pending"
        );

        // Crash: drop the engine and executor without running imm0's flush. The
        // queued task is dropped un-run; all four segments remain on disk.
        drop(engine);
        drop(exec);
    }

    // Recover. Replaying all surviving segments oldest-first yields the newest
    // value of key 1 (v1-new), not the stale v1-old that imm0's segment holds.
    let reopened = StorageEngine::open(temp_dir.path(), options).unwrap();
    assert_eq!(reopened.get(&k_int(1)).unwrap(), Some(v_str("v1-new")));
    assert_eq!(reopened.get(&k_int(2)).unwrap(), Some(v_str("keep")));
}
