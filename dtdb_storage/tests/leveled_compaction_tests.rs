use dtdb_storage::{CompressionType, DbKey, DbValue, EngineOptions, StorageEngine};
use std::fs;
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

// Helper to create keys and values easily
fn k_int(val: i64) -> DbKey {
    DbKey::Int(val)
}

fn v_int(val: i64) -> DbValue {
    DbValue::Int(val)
}

fn v_str(val: &str) -> DbValue {
    DbValue::string(val)
}

/// Polls `cond` until it returns true or `timeout` elapses; returns whether it
/// was met. Used to await background work (flushes and compaction now run
/// asynchronously) without a fixed sleep.
fn wait_until(timeout: std::time::Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if cond() {
            return true;
        }
        thread::sleep(std::time::Duration::from_millis(5));
    }
    cond()
}

// Helper to count SSTable files in a directory that belong to a specific level
fn count_sst_files_at_level(dir: &std::path::Path, target_level: usize) -> usize {
    let mut count = 0;
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "sst")
                && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                && stem.starts_with('L')
            {
                let parts: Vec<&str> = stem[1..].split('_').collect();
                if parts.len() == 2
                    && let Ok(level) = parts[0].parse::<usize>()
                    && level == target_level
                {
                    count += 1;
                }
            }
        }
    }
    count
}

#[test]
fn test_l0_to_l1_auto_compaction() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // Set compaction threshold to 2 files in L0
    let options = EngineOptions {
        compression: CompressionType::Uncompressed,
        memtable_size_limit: 5, // very small memtable threshold to force flushes
        block_size_limit: 4096,
        wal_size_limit: 32 * 1024 * 1024,
        l0_compaction_threshold: 2,
        sstable_target_size: 2 * 1024 * 1024,
        base_level_size_limit: 10 * 1024 * 1024,
        level_size_multiplier: 10,
        max_level: 7,
        block_cache_capacity: 1000,
        wal_sync_interval_ms: None,
        ..Default::default()
    };
    let engine = StorageEngine::open(&db_path, options).unwrap();

    // 1. Write first key -> triggers the 1st L0 flush, which runs in the
    // background. Below the threshold (2), so no compaction follows and L0
    // settles at one file.
    engine.put(k_int(1), v_str("val1")).unwrap();
    assert!(
        wait_until(std::time::Duration::from_secs(2), || {
            count_sst_files_at_level(&db_path, 0) == 1
        }),
        "first background flush did not produce an L0 file"
    );
    assert_eq!(count_sst_files_at_level(&db_path, 1), 0);

    // 2. Write second key -> triggers 2nd L0 flush. Since threshold = 2,
    // it triggers L0 -> L1 compaction automatically!
    engine.put(k_int(2), v_str("val2")).unwrap();

    // Auto compaction runs asynchronously. Let's poll for up to 1 second for it to complete.
    let mut success = false;
    for _ in 0..100 {
        if count_sst_files_at_level(&db_path, 0) == 0 && count_sst_files_at_level(&db_path, 1) == 1
        {
            success = true;
            break;
        }
        thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(success, "Compaction L0 -> L1 did not complete in time");

    // Verify consistency
    assert_eq!(engine.get(&k_int(1)).unwrap(), Some(v_str("val1")));
    assert_eq!(engine.get(&k_int(2)).unwrap(), Some(v_str("val2")));
}

#[test]
fn test_sstable_splitting_by_target_size() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // Set sstable target size very small (e.g. 50 bytes)
    let options = EngineOptions {
        compression: CompressionType::Uncompressed,
        memtable_size_limit: 50,
        block_size_limit: 4096,
        wal_size_limit: 32 * 1024 * 1024,
        l0_compaction_threshold: 10, // high threshold to avoid auto compaction during writes
        sstable_target_size: 20,     // very small to force splitting during compaction
        base_level_size_limit: 10 * 1024 * 1024,
        level_size_multiplier: 10,
        max_level: 7,
        block_cache_capacity: 1000,
        wal_sync_interval_ms: None,
        ..Default::default()
    };
    let engine = StorageEngine::open(&db_path, options).unwrap();

    // Write multiple keys to generate L0 files
    engine.put(k_int(1), v_str("value1_large_data")).unwrap();
    engine.flush_memtable().unwrap();
    engine.put(k_int(2), v_str("value2_large_data")).unwrap();
    engine.flush_memtable().unwrap();
    engine.put(k_int(3), v_str("value3_large_data")).unwrap();
    engine.flush_memtable().unwrap();

    let l0_count = count_sst_files_at_level(&db_path, 0);
    assert!(l0_count > 1);

    // Run manual compaction
    engine.compact().unwrap();

    // Level 0 should be empty
    assert_eq!(count_sst_files_at_level(&db_path, 0), 0);
    // Level 1 should have split the merged data into multiple files (>= 2 files)
    let l1_count = count_sst_files_at_level(&db_path, 1);
    assert!(l1_count >= 2);

    // Verify all keys are still fully queryable
    assert_eq!(
        engine.get(&k_int(1)).unwrap(),
        Some(v_str("value1_large_data"))
    );
    assert_eq!(
        engine.get(&k_int(2)).unwrap(),
        Some(v_str("value2_large_data"))
    );
    assert_eq!(
        engine.get(&k_int(3)).unwrap(),
        Some(v_str("value3_large_data"))
    );
}

#[test]
fn test_tombstone_purging() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // Set options
    let options = EngineOptions {
        compression: CompressionType::Uncompressed,
        memtable_size_limit: 5,
        block_size_limit: 4096,
        wal_size_limit: 32 * 1024 * 1024,
        l0_compaction_threshold: 10,
        sstable_target_size: 2 * 1024 * 1024,
        base_level_size_limit: 10 * 1024 * 1024,
        level_size_multiplier: 10,
        max_level: 2, // max level is 2
        block_cache_capacity: 1000,
        wal_sync_interval_ms: None,
        ..Default::default()
    };
    let engine = StorageEngine::open(&db_path, options).unwrap();

    // Write a key and flush it to L0, then compact it to L1
    engine.put(k_int(1), v_str("val1")).unwrap();
    engine.compact().unwrap();
    assert_eq!(count_sst_files_at_level(&db_path, 0), 0);
    assert_eq!(count_sst_files_at_level(&db_path, 1), 1);

    // Write a delete tombstone for the key, which is sealed and flushed to L0 in
    // the background.
    engine.delete(k_int(1)).unwrap();
    assert!(
        wait_until(std::time::Duration::from_secs(2), || {
            count_sst_files_at_level(&db_path, 0) == 1
        }),
        "tombstone background flush did not produce an L0 file"
    );

    // Run compaction again. Since target level is Level 1, and there are no files in level > 1 (max level is 2, level 2 is empty),
    // the tombstone can be purged!
    engine.compact().unwrap();

    // Both levels should be checked. Let's verify value is None
    assert_eq!(engine.get(&k_int(1)).unwrap(), None);
}

#[test]
fn test_deep_stress_and_consistency() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let options = EngineOptions {
        compression: CompressionType::Uncompressed,
        memtable_size_limit: 50,
        block_size_limit: 4096,
        wal_size_limit: 1024,
        l0_compaction_threshold: 3,
        sstable_target_size: 100,
        base_level_size_limit: 1000,
        level_size_multiplier: 2,
        max_level: 4,
        block_cache_capacity: 1000,
        wal_sync_interval_ms: None,
        ..Default::default()
    };

    let engine = Arc::new(StorageEngine::open(&db_path, options).unwrap());

    // We will do concurrent read/write and compaction operations
    let writer_engine = engine.clone();

    let writer_handle = thread::spawn(move || {
        for i in 0..100 {
            // Write key
            writer_engine.put(k_int(i % 10), v_int(i)).unwrap();
            // Delete key sometimes
            if i % 7 == 0 {
                writer_engine.delete(k_int(i % 10)).unwrap();
            }
            thread::sleep(std::time::Duration::from_millis(1));
        }
    });

    let compactor_engine = engine.clone();
    let compactor_handle = thread::spawn(move || {
        for _ in 0..20 {
            let _ = compactor_engine.compact();
            thread::sleep(std::time::Duration::from_millis(5));
        }
    });

    writer_handle.join().unwrap();
    compactor_handle.join().unwrap();

    // Let's do a final major compaction to settle all levels
    engine.compact().unwrap();

    // Scan everything and make sure there are no out-of-order reads or crashes
    let scan_res = engine
        .filtered_scan(&k_int(0), &k_int(9), |_, _| true)
        .unwrap();
    for (k, v) in scan_res {
        // Assert the key variant and value type are correct
        match (k, v) {
            (DbKey::Int(key_val), DbValue::Int(_)) => {
                assert!((0..=9).contains(&key_val));
            }
            other => panic!("Unexpected scan result: {:?}", other),
        }
    }
}

/// Catch-all for the concurrent-flush machinery: many writer threads on disjoint
/// key ranges (so the expected final state is deterministic) plus a compactor,
/// with a tiny memtable and `max_write_buffer_number > 2` to force many flushes
/// running in parallel on the real thread pool. Every key must read back with its
/// exact value, both live and after a reopen (which exercises recovery of any
/// WAL segments still un-retired at shutdown).
#[test]
fn concurrent_writers_and_flushes_stay_consistent() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let options = EngineOptions {
        compression: CompressionType::Uncompressed,
        memtable_size_limit: 64, // tiny: most writes seal a memtable
        block_size_limit: 4096,
        wal_size_limit: 32 * 1024 * 1024,
        l0_compaction_threshold: 4,
        sstable_target_size: 2 * 1024 * 1024,
        base_level_size_limit: 16 * 1024,
        level_size_multiplier: 4,
        max_level: 7,
        block_cache_capacity: 1000,
        wal_sync_interval_ms: None,
        max_write_buffer_number: 4, // up to 3 immutables flushing at once
        ..Default::default()
    };

    const THREADS: i64 = 4;
    const PER_THREAD: i64 = 250;
    // Disjoint, well-separated key ranges per thread keep the final value of
    // every key deterministic regardless of interleaving.
    let key = |t: i64, i: i64| k_int(t * 1_000_000 + i);

    {
        let engine = Arc::new(StorageEngine::open(&db_path, options).unwrap());

        thread::scope(|s| {
            for t in 0..THREADS {
                let engine = &engine;
                s.spawn(move || {
                    for i in 0..PER_THREAD {
                        engine.put(key(t, i), v_int(t * 1_000_000 + i)).unwrap();
                    }
                });
            }
            // A compactor running alongside the writers, exercising flush/compact
            // overlap.
            let engine = &engine;
            s.spawn(move || {
                for _ in 0..30 {
                    engine.compact().unwrap();
                    thread::sleep(std::time::Duration::from_millis(2));
                }
            });
        });

        // Settle everything, then verify every key is present with its value.
        engine.compact().unwrap();
        for t in 0..THREADS {
            for i in 0..PER_THREAD {
                assert_eq!(
                    engine.get(&key(t, i)).unwrap(),
                    Some(v_int(t * 1_000_000 + i)),
                    "live read mismatch for thread {t} index {i}"
                );
            }
        }
    }

    // Reopen: any immutables still queued at shutdown live only in un-retired WAL
    // segments, so a correct read here proves recovery handles them.
    let reopened = StorageEngine::open(&db_path, options).unwrap();
    for t in 0..THREADS {
        for i in 0..PER_THREAD {
            assert_eq!(
                reopened.get(&key(t, i)).unwrap(),
                Some(v_int(t * 1_000_000 + i)),
                "post-reopen read mismatch for thread {t} index {i}"
            );
        }
    }
}
