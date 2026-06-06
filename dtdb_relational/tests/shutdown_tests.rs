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
    DatabaseOptions {
        compression: CompressionType::Uncompressed,
        memtable_size_limit: 1024,
        block_size_limit: 64,
        wal_size_limit: 1024 * 1024,
        flush_interval_ms: None,
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
    let db = Database::open_with_options_and_executor(
        temp_dir.path(),
        test_options(),
        exec.clone(),
    )
    .unwrap();

    db.create_table("t", single_int_pk_schema()).unwrap();
    let table = db.get_table("t").unwrap();
    // The single (default) locality-group engine backing the table.
    let engine = table.engines.values().next().unwrap().clone();

    // Two flushes cross l0_compaction_threshold (2), so the engine submits a
    // background compaction — which the ManualExecutor only stores.
    engine.put(DbKey::Int(1), DbValue::string("apple")).unwrap();
    engine.flush_memtable().unwrap();
    engine.put(DbKey::Int(2), DbValue::string("banana")).unwrap();
    engine.flush_memtable().unwrap();

    assert!(
        exec.pending() >= 1,
        "expected a background compaction to be queued after crossing the L0 threshold"
    );

    // Snapshot the SSTable set while the compaction has not run.
    let before = sst_files(temp_dir.path());
    assert!(before.len() >= 2, "expected at least two L0 SSTables, got {before:?}");

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
    let db = Database::open_with_options_and_executor(
        temp_dir.path(),
        test_options(),
        exec.clone(),
    )
    .unwrap();

    db.create_table("t", single_int_pk_schema()).unwrap();
    let table = db.get_table("t").unwrap();
    let engine = table.engines.values().next().unwrap().clone();

    engine.put(DbKey::Int(1), DbValue::string("apple")).unwrap();
    engine.flush_memtable().unwrap();
    engine.put(DbKey::Int(2), DbValue::string("banana")).unwrap();
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
