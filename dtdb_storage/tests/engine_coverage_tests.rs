//! Tests covering engine paths that the existing suites leave untouched:
//! `write_batch`, point/multi reads served from Level 1+ SSTables (the
//! binary-search branch), the value-rewrite-on-layout-change path, and the
//! background WAL-sync schedule.

use dtdb_storage::{
    CompressionType, DbKey, DbValue, EngineOptions, Executor, Result, StorageEngine, ValueRewriter,
    WalEntry, default_executor,
};
use tempfile::TempDir;

use std::sync::Arc;

fn k_int(val: i64) -> DbKey {
    DbKey::Int(val)
}

fn v_int(val: i64) -> DbValue {
    DbValue::Int(val)
}

/// Base options with a large memtable so writes stay resident until an explicit
/// flush, unless overridden by the caller.
fn base_options() -> EngineOptions {
    EngineOptions {
        compression: CompressionType::Lz4,
        memtable_size_limit: 1024 * 1024,
        block_size_limit: 4096,
        wal_size_limit: 32 * 1024 * 1024,
        l0_compaction_threshold: 4,
        sstable_target_size: 2 * 1024 * 1024,
        base_level_size_limit: 10 * 1024 * 1024,
        level_size_multiplier: 10,
        max_level: 7,
        block_cache_capacity: 1000,
        wal_sync_interval_ms: None,
        ..Default::default()
    }
}

#[test]
fn test_write_batch_applies_puts_and_deletes() {
    let temp_dir = TempDir::new().unwrap();
    let engine = StorageEngine::open(temp_dir.path(), base_options()).unwrap();

    // Seed a key that the batch will later delete.
    engine.put(k_int(2), v_int(20)).unwrap();

    // A batch mixing Put, Delete, and a nested Batch (which write_batch ignores
    // for memtable application but still durably logs).
    let entries = vec![
        WalEntry::Put {
            key: k_int(1),
            value: v_int(100),
        },
        WalEntry::Delete { key: k_int(2) },
        WalEntry::Batch(vec![WalEntry::Put {
            key: k_int(3),
            value: v_int(300),
        }]),
        WalEntry::Put {
            key: k_int(4),
            value: v_int(400),
        },
    ];
    engine.write_batch(entries).unwrap();

    assert_eq!(engine.get(&k_int(1)).unwrap(), Some(v_int(100)));
    assert_eq!(engine.get(&k_int(2)).unwrap(), None); // deleted by the batch
    // The nested Batch variant is a no-op for the memtable apply loop.
    assert_eq!(engine.get(&k_int(3)).unwrap(), None);
    assert_eq!(engine.get(&k_int(4)).unwrap(), Some(v_int(400)));
}

#[test]
fn test_write_batch_empty_is_noop() {
    let temp_dir = TempDir::new().unwrap();
    let engine = StorageEngine::open(temp_dir.path(), base_options()).unwrap();
    engine.put(k_int(1), v_int(1)).unwrap();

    engine.write_batch(vec![]).unwrap();

    assert_eq!(engine.get(&k_int(1)).unwrap(), Some(v_int(1)));
    let stats = engine.get_statistics().unwrap();
    assert_eq!(stats.memtable_entries, 1);
}

#[test]
fn test_write_batch_triggers_flush_when_memtable_full() {
    let temp_dir = TempDir::new().unwrap();
    let options = EngineOptions {
        memtable_size_limit: 5, // tiny: any batch overflows and flushes
        ..base_options()
    };
    let engine = StorageEngine::open(temp_dir.path(), options).unwrap();

    engine
        .write_batch(vec![
            WalEntry::Put {
                key: k_int(1),
                value: v_int(1),
            },
            WalEntry::Put {
                key: k_int(2),
                value: v_int(2),
            },
        ])
        .unwrap();

    // The batch overflowed the memtable, so it is sealed and flushed in the
    // background. The data stays readable the whole time (served from the
    // immutable memtable until the flush lands it in an SSTable).
    assert_eq!(engine.get(&k_int(1)).unwrap(), Some(v_int(1)));
    assert_eq!(engine.get(&k_int(2)).unwrap(), Some(v_int(2)));

    // The background flush eventually produces an L0 SSTable.
    let flushed = wait_until(std::time::Duration::from_secs(5), || {
        engine.get_statistics().unwrap().num_sstables >= 1
    });
    assert!(flushed, "expected a background flush to produce an SSTable");
    assert_eq!(engine.get(&k_int(1)).unwrap(), Some(v_int(1)));
    assert_eq!(engine.get(&k_int(2)).unwrap(), Some(v_int(2)));
}

/// Polls `cond` until it returns true or `timeout` elapses. Returns whether the
/// condition was met. Used to await background work (e.g. an async flush) without
/// a fixed sleep.
fn wait_until(timeout: std::time::Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    cond()
}

/// Builds an engine whose data is spread across L1 with several
/// non-overlapping files, exercising the binary-search read path that the
/// existing multi_get test (single L0 flush) never reaches.
fn engine_with_l1_files(db_path: &std::path::Path) -> StorageEngine {
    let options = EngineOptions {
        compression: CompressionType::Uncompressed,
        memtable_size_limit: 1000,
        l0_compaction_threshold: 10,
        sstable_target_size: 1, // force one file per compacted key
        max_level: 4,
        block_cache_capacity: 0,
        ..base_options()
    };
    let engine = StorageEngine::open(db_path, options).unwrap();

    // Each (flush, compact) pair lands a single key as its own L1 file, leaving
    // L1 = [10, 20, 30, 40] as non-overlapping sorted files.
    for key in [10i64, 20, 30, 40] {
        engine.put(k_int(key), v_int(key)).unwrap();
        engine.flush_memtable().unwrap();
        engine.compact().unwrap();
    }
    engine
}

#[test]
fn test_multi_get_binary_search_across_l1_files() {
    let temp_dir = TempDir::new().unwrap();
    let engine = engine_with_l1_files(temp_dir.path());

    // Add a fresh key in the memtable to also exercise the memtable branch.
    engine.put(k_int(99), v_int(99)).unwrap();

    let keys = vec![
        k_int(10), // first L1 file
        k_int(30), // middle L1 file (binary search must land here)
        k_int(40), // last L1 file
        k_int(99), // memtable
        k_int(25), // gap between files -> miss
        k_int(50), // past the last file -> miss
    ];
    let results = engine.multi_get(&keys).unwrap();

    assert_eq!(results[0], Some(v_int(10)));
    assert_eq!(results[1], Some(v_int(30)));
    assert_eq!(results[2], Some(v_int(40)));
    assert_eq!(results[3], Some(v_int(99)));
    assert_eq!(results[4], None);
    assert_eq!(results[5], None);
}

#[test]
fn test_get_point_lookup_across_l1_files() {
    let temp_dir = TempDir::new().unwrap();
    let engine = engine_with_l1_files(temp_dir.path());

    // Each of these resolves through the L1 binary-search branch of `get`.
    assert_eq!(engine.get(&k_int(10)).unwrap(), Some(v_int(10)));
    assert_eq!(engine.get(&k_int(20)).unwrap(), Some(v_int(20)));
    assert_eq!(engine.get(&k_int(40)).unwrap(), Some(v_int(40)));
    // Keys that fall between/after the files miss cleanly.
    assert_eq!(engine.get(&k_int(15)).unwrap(), None);
    assert_eq!(engine.get(&k_int(100)).unwrap(), None);
}

#[test]
fn test_filtered_scan_across_l1_files() {
    let temp_dir = TempDir::new().unwrap();
    let engine = engine_with_l1_files(temp_dir.path());

    // Spans several non-overlapping L1 files; the scan must merge them.
    let all = engine
        .filtered_scan(&k_int(0), &k_int(50), |_, _| true)
        .unwrap();
    assert_eq!(
        all,
        vec![
            (k_int(10), v_int(10)),
            (k_int(20), v_int(20)),
            (k_int(30), v_int(30)),
            (k_int(40), v_int(40)),
        ]
    );

    // A filter that only keeps the even-tens still works across files.
    let filtered = engine
        .filtered_scan(
            &k_int(0),
            &k_int(50),
            |_, v| matches!(v, DbValue::Int(n) if n % 20 == 0),
        )
        .unwrap();
    assert_eq!(
        filtered,
        vec![(k_int(20), v_int(20)), (k_int(40), v_int(40))]
    );
}

#[test]
fn test_write_batch_recovers_from_wal_after_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // `write_batch` logs the whole batch as a single WAL `Batch` record. Write
    // one, then drop the engine WITHOUT flushing so the record survives only in
    // the WAL.
    {
        let engine = StorageEngine::open(&db_path, base_options()).unwrap();
        engine
            .write_batch(vec![
                WalEntry::Put {
                    key: k_int(1),
                    value: v_int(10),
                },
                WalEntry::Put {
                    key: k_int(2),
                    value: v_int(20),
                },
                WalEntry::Delete { key: k_int(1) },
            ])
            .unwrap();
    }

    // Reopening replays the WAL `Batch` record (exercising the recursive Batch
    // arm of recovery) and rebuilds the same state.
    let engine = StorageEngine::open(&db_path, base_options()).unwrap();
    assert_eq!(engine.get(&k_int(1)).unwrap(), None); // deleted within the batch
    assert_eq!(engine.get(&k_int(2)).unwrap(), Some(v_int(20)));
}

#[test]
fn test_multi_get_returns_none_for_on_disk_tombstone() {
    let temp_dir = TempDir::new().unwrap();
    let engine = StorageEngine::open(temp_dir.path(), base_options()).unwrap();

    // Put then delete a key, and flush so the tombstone lives in an L0 SSTable
    // (not the memtable). A second live key shares the flush.
    engine.put(k_int(1), v_int(100)).unwrap();
    engine.put(k_int(2), v_int(200)).unwrap();
    engine.delete(k_int(1)).unwrap();
    engine.flush_memtable().unwrap();

    // multi_get must surface the on-disk tombstone as `None`, distinct from the
    // "key never existed" miss for key 3.
    let results = engine.multi_get(&[k_int(1), k_int(2), k_int(3)]).unwrap();
    assert_eq!(results[0], None); // tombstone in L0 SSTable
    assert_eq!(results[1], Some(v_int(200)));
    assert_eq!(results[2], None); // absent
}

/// Rewriter that bumps integers by 1000 whenever the source and destination
/// layouts differ, so a layout change is observable through reads.
#[derive(Debug)]
struct BumpingRewriter;

impl ValueRewriter for BumpingRewriter {
    fn rewrite(&self, src_layout: &[u8], dst_layout: &[u8], value: &DbValue) -> Result<DbValue> {
        assert_ne!(src_layout, dst_layout, "rewrite called for equal layouts");
        match value {
            DbValue::Int(n) => Ok(DbValue::Int(n + 1000)),
            other => Ok(other.clone()),
        }
    }
}

fn open_with_rewriter(db_path: &std::path::Path) -> StorageEngine {
    let options = EngineOptions {
        compression: CompressionType::Uncompressed,
        memtable_size_limit: 1000,
        l0_compaction_threshold: 10,
        max_level: 4,
        block_cache_capacity: 0,
        ..base_options()
    };
    let executor: Arc<dyn Executor> = default_executor();
    StorageEngine::open_with_executor(db_path, options, executor, Arc::new(BumpingRewriter))
        .unwrap()
}

#[test]
fn test_value_rewrite_on_layout_change_for_get_and_scan() {
    let temp_dir = TempDir::new().unwrap();
    let engine = open_with_rewriter(temp_dir.path());

    // Data is flushed under the default (empty) layout.
    engine.put(k_int(1), v_int(1)).unwrap();
    engine.put(k_int(2), v_int(2)).unwrap();
    engine.put(k_int(3), v_int(3)).unwrap();
    engine.flush_memtable().unwrap();

    // No layout change yet: reads come back verbatim.
    assert_eq!(engine.get(&k_int(1)).unwrap(), Some(v_int(1)));

    // Now declare a new target layout. SSTables on disk still carry the old
    // (empty) layout, so reads must route through the rewriter.
    engine.set_target_layout(vec![1, 2, 3]);

    // Point read from L0 goes through the rewrite branch.
    assert_eq!(engine.get(&k_int(1)).unwrap(), Some(v_int(1001)));
    // multi_get from L0 goes through its rewrite branch.
    let got = engine.multi_get(&[k_int(2), k_int(3)]).unwrap();
    assert_eq!(got, vec![Some(v_int(1002)), Some(v_int(1003))]);
    // filtered_scan from L0 goes through its rewrite branch.
    let scanned = engine
        .filtered_scan(&k_int(1), &k_int(3), |_, _| true)
        .unwrap();
    assert_eq!(
        scanned,
        vec![
            (k_int(1), v_int(1001)),
            (k_int(2), v_int(1002)),
            (k_int(3), v_int(1003)),
        ]
    );
    // scan_iter from L0 goes through its rewrite branch.
    let mut iter = engine.scan_iter(&k_int(1), &k_int(3)).unwrap();
    let mut via_iter = Vec::new();
    while let Some((k, v)) = iter.next().unwrap() {
        via_iter.push((k, v));
    }
    assert_eq!(
        via_iter,
        vec![
            (k_int(1), v_int(1001)),
            (k_int(2), v_int(1002)),
            (k_int(3), v_int(1003)),
        ]
    );
}

#[test]
fn test_value_rewrite_persisted_during_compaction() {
    let temp_dir = TempDir::new().unwrap();
    let engine = open_with_rewriter(temp_dir.path());

    engine.put(k_int(5), v_int(5)).unwrap();
    engine.flush_memtable().unwrap(); // L0 file written with empty layout

    // Change the target layout, then compact. Compaction rewrites values from
    // the source layout into the new target layout and writes them out, so the
    // bump is baked into the resulting SSTable.
    engine.set_target_layout(vec![9, 9]);
    engine.compact().unwrap();

    // The compacted file now carries layout [9,9] == target, so a plain read
    // should NOT bump again — proving the rewrite happened once, at compaction.
    assert_eq!(engine.get(&k_int(5)).unwrap(), Some(v_int(1005)));
}

#[test]
fn test_background_wal_sync_keeps_data_durable() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    {
        let options = EngineOptions {
            memtable_size_limit: 1024 * 1024, // keep entries in WAL, no flush
            wal_sync_interval_ms: Some(5),    // enable the background sync schedule
            ..base_options()
        };
        let engine = StorageEngine::open(&db_path, options).unwrap();
        engine.put(k_int(1), v_int(1)).unwrap();
        engine.put(k_int(2), v_int(2)).unwrap();

        // Give the periodic sync task a few cycles to run.
        std::thread::sleep(std::time::Duration::from_millis(50));

        // An explicit sync_wal must also succeed on a background-synced engine.
        engine.sync_wal().unwrap();
    } // drop cancels the schedule

    // Reopen: the WAL must replay the un-flushed entries.
    let engine = StorageEngine::open(&db_path, base_options()).unwrap();
    assert_eq!(engine.get(&k_int(1)).unwrap(), Some(v_int(1)));
    assert_eq!(engine.get(&k_int(2)).unwrap(), Some(v_int(2)));
}
