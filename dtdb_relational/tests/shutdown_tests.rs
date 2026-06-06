//! Tests for [`Database::shutdown`] quiescing background compaction so a
//! dropped database/table cannot have files resurrected under its directory by
//! a late compaction racing the removal.

use dtdb_relational::{Column, DataType, Database, DatabaseOptions, Schema};
use dtdb_storage::{
    CoalesceKey, CompressionType, DbKey, DbValue, Executor, FsyncMethod, InlineExecutor,
    PeriodicHandle, Priority,
};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;

/// An [`Executor`] that *stores* submitted one-shot tasks (e.g. background
/// compaction) instead of running them, so a test deterministically controls
/// when — and whether — they execute. Periodic work (the engines' background
/// WAL sync) is delegated to an [`InlineExecutor`].
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

    fn run_all(&self) {
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        for task in tasks {
            task();
        }
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

/// An [`Executor`] that *captures* periodic tasks (returning a real, cancelable
/// handle obtained from an [`InlineExecutor`] with an effectively-infinite
/// interval, so the inline scheduler never fires them) and lets a test fire each
/// tick by hand. One-shot submits (compaction) are simply stored.
struct PeriodicCapture {
    inline: InlineExecutor,
    periodics: Mutex<Vec<Arc<dyn Fn() + Send + Sync + 'static>>>,
}

impl PeriodicCapture {
    fn new() -> Self {
        PeriodicCapture {
            inline: InlineExecutor,
            periodics: Mutex::new(Vec::new()),
        }
    }

    /// Invoke every captured periodic tick once, simulating the scheduler firing
    /// them.
    fn fire_all_periodics(&self) {
        let tasks = self.periodics.lock().unwrap().clone();
        for task in tasks {
            task();
        }
    }
}

impl Executor for PeriodicCapture {
    fn submit(
        &self,
        _priority: Priority,
        _key: Option<CoalesceKey>,
        _task: Box<dyn FnOnce() + Send + 'static>,
    ) {
        // One-shot work (e.g. compaction) is irrelevant to this test; drop it.
    }

    fn submit_periodic(
        &self,
        _every: Duration,
        priority: Priority,
        task: Box<dyn Fn() + Send + Sync + 'static>,
    ) -> PeriodicHandle {
        let task: Arc<dyn Fn() + Send + Sync + 'static> = Arc::from(task);
        self.periodics.lock().unwrap().push(task.clone());
        // Hand back a real handle, but on an interval long enough that the inline
        // scheduler never fires it during the test — we drive ticks manually.
        self.inline.submit_periodic(
            Duration::from_secs(3600),
            priority,
            Box::new(move || task()),
        )
    }
}

fn single_int_pk_schema() -> Schema {
    Schema::new(vec![
        Column {
            id: 0,
            name: "id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_nullable: false,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            id: 0,
            name: "name".to_string(),
            data_type: DataType::String,
            is_primary_key: false,
            is_nullable: true,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
    ])
}

fn test_options() -> DatabaseOptions {
    options_with_flush_interval(None)
}

fn options_with_flush_interval(flush_interval_ms: Option<u64>) -> DatabaseOptions {
    DatabaseOptions {
        compression: CompressionType::Uncompressed,
        memtable_size_limit: 1024,
        block_size_limit: 64,
        wal_size_limit: 1024 * 1024,
        flush_interval_ms,
        l0_compaction_threshold: Some(2),
        sstable_target_size: Some(1024),
        base_level_size_limit: Some(10 * 1024),
        level_size_multiplier: Some(10),
        max_level: Some(7),
        block_cache_capacity: Some(0),
        analyze_frequency_ms: None,
        wal_sync_interval_ms: None,
        memory_budget: None,
        fsync_method: FsyncMethod::default(),
        l0_slowdown_writes_trigger: None,
        l0_stop_writes_trigger: None,
        l0_slowdown_max_sleep_ms: None,
    }
}

/// Recursively collect the sorted names of every `*.sst` file under `dir`.
fn sst_files(dir: &Path) -> Vec<String> {
    let mut out = Vec::new();
    collect_sst(dir, &mut out);
    out.sort();
    out
}

fn collect_sst(dir: &Path, out: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_sst(&path, out);
        } else if path.extension().is_some_and(|e| e == "sst") {
            out.push(path.file_name().unwrap().to_string_lossy().into_owned());
        }
    }
}

/// After `Database::shutdown()`, a background compaction that was submitted
/// before shutdown must not mutate on-disk state — otherwise it could write or
/// delete SSTable files under a database directory that is being removed,
/// leaving orphans behind.
///
/// Fully deterministic: a `ManualExecutor` holds the queued compaction and only
/// runs it *after* we shut down, so there is no reliance on fsync/scheduler
/// timing.
#[test]
fn shutdown_quiesces_engines_so_queued_compaction_is_noop() {
    let temp_dir = TempDir::new().unwrap();
    let exec = Arc::new(ManualExecutor::new());
    let db =
        Database::open_with_options_and_executor(temp_dir.path(), test_options(), exec.clone())
            .unwrap();

    db.create_table("t", single_int_pk_schema()).unwrap();
    let table = db.get_table("t").unwrap();
    // The single (default) locality-group engine backing the table.
    let engine = table.engines.values().next().unwrap().clone();

    // Two flushes cross l0_compaction_threshold (2), so the engine submits a
    // background compaction — which the ManualExecutor only stores.
    engine.put(DbKey::Int(1), DbValue::string("apple")).unwrap();
    engine.flush_memtable().unwrap();
    engine
        .put(DbKey::Int(2), DbValue::string("banana"))
        .unwrap();
    engine.flush_memtable().unwrap();

    assert!(
        exec.pending() >= 1,
        "expected a background compaction to be queued after crossing the L0 threshold"
    );

    // Snapshot the SSTable set while the compaction has not run.
    let before = sst_files(temp_dir.path());
    assert!(
        before.len() >= 2,
        "expected at least two L0 SSTables, got {before:?}"
    );

    // Quiesce the database, then release the queued compaction. Because every
    // engine is now shutting down, the compaction must observe the flag and
    // return without merging/creating/deleting any SSTable.
    db.shutdown();
    exec.run_all();

    let after = sst_files(temp_dir.path());
    assert_eq!(
        before, after,
        "a compaction submitted before Database::shutdown() must not mutate state afterward"
    );
}

/// `drop_table` must quiesce the table's engines and remove the directory
/// cleanly even though a background compaction was queued, leaving no SSTable
/// files behind under the database directory.
#[test]
fn drop_table_quiesces_engine_and_leaves_no_orphan_files() {
    let temp_dir = TempDir::new().unwrap();
    let exec = Arc::new(ManualExecutor::new());
    let db =
        Database::open_with_options_and_executor(temp_dir.path(), test_options(), exec.clone())
            .unwrap();

    db.create_table("t", single_int_pk_schema()).unwrap();
    let table = db.get_table("t").unwrap();
    let engine = table.engines.values().next().unwrap().clone();

    engine.put(DbKey::Int(1), DbValue::string("apple")).unwrap();
    engine.flush_memtable().unwrap();
    engine
        .put(DbKey::Int(2), DbValue::string("banana"))
        .unwrap();
    engine.flush_memtable().unwrap();
    assert!(exec.pending() >= 1);

    // Drop the table: it shuts the engine down before renaming/removing the
    // directory. The `engine`/`table` clones we hold keep the engine alive, but
    // shutdown has already flipped its quiescing flag.
    drop(table);
    db.drop_table("t").unwrap();

    // Releasing the queued compaction now is a no-op (the engine is shut down),
    // so nothing is written back under the removed table path.
    exec.run_all();

    assert!(
        !temp_dir.path().join("t").exists(),
        "table directory should have been removed"
    );
    assert!(
        sst_files(temp_dir.path()).is_empty(),
        "no SSTable files should remain after dropping the table"
    );
}

/// `Database::shutdown()` must *drain* the database's own periodic tasks, not
/// merely cancel their schedule. After shutdown, a periodic flush tick that
/// fires must do nothing — otherwise it would persist an SSTable under a
/// database directory that is being removed.
///
/// Deterministic: `PeriodicCapture` lets the test fire the flush tick by hand
/// instead of waiting on a scheduler.
#[test]
fn shutdown_drains_periodic_flush_so_late_tick_is_noop() {
    let temp_dir = TempDir::new().unwrap();
    let exec = Arc::new(PeriodicCapture::new());
    let db = Arc::new(
        Database::open_with_options_and_executor(
            temp_dir.path(),
            options_with_flush_interval(Some(50)),
            exec.clone(),
        )
        .unwrap(),
    );

    db.create_table("t", single_int_pk_schema()).unwrap();
    // Register the periodic flush schedule (captured by PeriodicCapture).
    db.start_background_flush_if_needed(&db);

    let table = db.get_table("t").unwrap();
    let engine = table.engines.values().next().unwrap().clone();

    // Put a row into the memtable (not yet flushed to an SSTable).
    engine.put(DbKey::Int(1), DbValue::string("apple")).unwrap();
    assert!(sst_files(temp_dir.path()).is_empty());

    // Sanity: firing the flush tick *before* shutdown does real work — it
    // persists the memtable as an SSTable. This proves the captured tick is live.
    exec.fire_all_periodics();
    let after_first_tick = sst_files(temp_dir.path());
    assert_eq!(
        after_first_tick.len(),
        1,
        "a flush tick before shutdown should persist one SSTable, got {after_first_tick:?}"
    );

    // Stage more unflushed data, then quiesce the database.
    engine
        .put(DbKey::Int(2), DbValue::string("banana"))
        .unwrap();
    db.shutdown();

    // Firing the flush tick now must be a no-op: the database is quiescing, so
    // no new SSTable is written under the (about-to-be-removed) directory.
    exec.fire_all_periodics();
    assert_eq!(
        sst_files(temp_dir.path()),
        after_first_tick,
        "a periodic flush tick after shutdown must not persist anything"
    );
}

/// The periodic ANALYZE task must be drained on shutdown just like the flush
/// task: a tick firing afterward must not run (it would otherwise recompute and
/// persist statistics for a database being torn down).
///
/// Self-evident and deterministic via the analyzed `row_count`: a tick fired
/// *before* shutdown recomputes it (proving the captured tick is live), while a
/// tick fired *after* shutdown leaves a table's baseline statistics untouched.
#[test]
fn shutdown_drains_periodic_analyze_so_late_tick_is_noop() {
    let temp_dir = TempDir::new().unwrap();
    let exec = Arc::new(PeriodicCapture::new());
    let mut options = test_options();
    options.analyze_frequency_ms = Some(50);
    let db = Arc::new(
        Database::open_with_options_and_executor(temp_dir.path(), options, exec.clone()).unwrap(),
    );

    // Table "t" with flushed data, so a real ANALYZE moves its row_count off the
    // (zero) baseline that `create_table` seeds.
    db.create_table("t", single_int_pk_schema()).unwrap();
    let t_engine = db
        .get_table("t")
        .unwrap()
        .engines
        .values()
        .next()
        .unwrap()
        .clone();
    for i in 0..3 {
        t_engine.put(DbKey::Int(i), DbValue::string("x")).unwrap();
    }
    t_engine.flush_memtable().unwrap();

    // Register the periodic ANALYZE schedule (captured by PeriodicCapture).
    db.start_background_analyze_if_needed(&db);

    let t_row_count = |db: &Database| db.get_table_statistics("t").map(|s| s.row_count);
    let t_before = t_row_count(&db);

    // Firing the tick *before* shutdown runs ANALYZE, recomputing statistics —
    // proving the captured tick is live.
    exec.fire_all_periodics();
    assert_ne!(
        t_row_count(&db),
        t_before,
        "an ANALYZE tick before shutdown should recompute statistics (row_count)"
    );

    // Add a second table with data, capture its baseline, then quiesce.
    db.create_table("u", single_int_pk_schema()).unwrap();
    let u_engine = db
        .get_table("u")
        .unwrap()
        .engines
        .values()
        .next()
        .unwrap()
        .clone();
    for i in 0..3 {
        u_engine.put(DbKey::Int(i), DbValue::string("x")).unwrap();
    }
    u_engine.flush_memtable().unwrap();
    let u_baseline = db.get_table_statistics("u").map(|s| s.row_count);
    db.shutdown();

    // Firing the tick now must be a no-op: "u" keeps its baseline statistics
    // because the ANALYZE tick observes the shutdown flag.
    exec.fire_all_periodics();
    assert_eq!(
        db.get_table_statistics("u").map(|s| s.row_count),
        u_baseline,
        "a periodic ANALYZE tick after shutdown must not recompute statistics"
    );
}
