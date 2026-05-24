use std::fs;
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;
use dtdb_storage::{DbKey, DbValue, StorageEngine, CompressionType, EngineOptions};

// Helper to create keys and values easily
fn k_int(val: i64) -> DbKey {
    DbKey::Int(val)
}

fn v_int(val: i64) -> DbValue {
    DbValue::Int(val)
}

fn v_str(val: &str) -> DbValue {
    DbValue::String(val.to_string())
}

// Helper to count SSTable files in a directory that belong to a specific level
fn count_sst_files_at_level(dir: &std::path::Path, target_level: usize) -> usize {
    let mut count = 0;
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "sst") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if stem.starts_with('L') {
                        let parts: Vec<&str> = stem[1..].split('_').collect();
                        if parts.len() == 2 {
                            if let Ok(level) = parts[0].parse::<usize>() {
                                if level == target_level {
                                    count += 1;
                                }
                            }
                        }
                    }
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
    };
    let engine = StorageEngine::open(&db_path, options).unwrap();

    // 1. Write first key -> triggers 1st L0 flush (L0 file count = 1)
    engine.put(k_int(1), v_str("val1")).unwrap();
    assert_eq!(count_sst_files_at_level(&db_path, 0), 1);
    assert_eq!(count_sst_files_at_level(&db_path, 1), 0);

    // 2. Write second key -> triggers 2nd L0 flush. Since threshold = 2,
    // it triggers L0 -> L1 compaction automatically!
    engine.put(k_int(2), v_str("val2")).unwrap();
    
    // Auto compaction runs asynchronously. Let's poll for up to 1 second for it to complete.
    let mut success = false;
    for _ in 0..100 {
        if count_sst_files_at_level(&db_path, 0) == 0 && count_sst_files_at_level(&db_path, 1) == 1 {
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
    assert_eq!(engine.get(&k_int(1)).unwrap(), Some(v_str("value1_large_data")));
    assert_eq!(engine.get(&k_int(2)).unwrap(), Some(v_str("value2_large_data")));
    assert_eq!(engine.get(&k_int(3)).unwrap(), Some(v_str("value3_large_data")));
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
    };
    let engine = StorageEngine::open(&db_path, options).unwrap();

    // Write a key and flush it to L0, then compact it to L1
    engine.put(k_int(1), v_str("val1")).unwrap();
    engine.compact().unwrap();
    assert_eq!(count_sst_files_at_level(&db_path, 0), 0);
    assert_eq!(count_sst_files_at_level(&db_path, 1), 1);

    // Write a delete tombstone for the key, which flushes to L0
    engine.delete(k_int(1)).unwrap();
    assert_eq!(count_sst_files_at_level(&db_path, 0), 1);

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
    let scan_res = engine.filtered_scan(&k_int(0), &k_int(9), |_, _| true).unwrap();
    for (k, v) in scan_res {
        // Assert the key variant and value type are correct
        match (k, v) {
            (DbKey::Int(key_val), DbValue::Int(_)) => {
                assert!(key_val >= 0 && key_val <= 9);
            }
            other => panic!("Unexpected scan result: {:?}", other),
        }
    }
}
