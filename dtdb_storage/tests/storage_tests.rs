use dtdb_storage::memtable::MemTable;
use dtdb_storage::sstable::{SstableReader, SstableWriter};
use dtdb_storage::wal::Wal;
use dtdb_storage::{CompressionType, DbKey, DbValue, EngineOptions, StorageEngine, WalEntry};
use std::fs;
use tempfile::TempDir;

// Helper to create keys and values easily
fn k_int(val: i64) -> DbKey {
    DbKey::Int(val)
}

fn k_str(val: &str) -> DbKey {
    DbKey::String(val.to_string())
}

fn v_int(val: i64) -> DbValue {
    DbValue::Int(val)
}

fn v_str(val: &str) -> DbValue {
    DbValue::String(val.to_string())
}

#[test]
fn test_memtable_basic() {
    let mem = MemTable::new();
    assert_eq!(mem.get(&k_int(1)), None);

    mem.put(k_int(1), v_int(100));
    mem.put(k_int(2), v_str("hello"));

    assert_eq!(mem.get(&k_int(1)), Some(Some(v_int(100))));
    assert_eq!(mem.get(&k_int(2)), Some(Some(v_str("hello"))));

    // Delete
    mem.delete(k_int(1));
    assert_eq!(mem.get(&k_int(1)), Some(None)); // Tombstone

    // Scan
    let results = mem.scan(&k_int(1), &k_int(3), |_, _| true);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], (k_int(2), v_str("hello")));
}

#[test]
fn test_wal_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("test.wal");

    // Write entries to WAL
    {
        let mut wal = Wal::open(&wal_path, None).unwrap();
        wal.append_put(&k_int(1), &v_int(100)).unwrap();
        wal.append_put(&k_str("a"), &v_str("apple")).unwrap();
        wal.append_delete(&k_int(1)).unwrap();
    }

    // Recover from WAL
    let entries = Wal::recover(&wal_path).unwrap();
    assert_eq!(entries.len(), 3);

    // Replay into a MemTable
    let mem = MemTable::new();
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
    for entry in entries {
        apply_entry(&mem, entry);
    }

    assert_eq!(mem.get(&k_int(1)), Some(None)); // Tombstone
    assert_eq!(mem.get(&k_str("a")), Some(Some(v_str("apple"))));
}

#[test]
fn test_sstable_write_read() {
    let temp_dir = TempDir::new().unwrap();
    let sst_path = temp_dir.path().join("00001.sst");

    // Write to SSTable with small block size (e.g. 50 bytes) to force block splitting
    {
        let mut writer = SstableWriter::create(&sst_path, 50, CompressionType::Lz4).unwrap();
        writer.append(k_int(1), Some(v_int(10))).unwrap();
        writer.append(k_int(2), Some(v_int(20))).unwrap();
        writer.append(k_int(3), None).unwrap(); // Tombstone
        writer.append(k_int(4), Some(v_int(40))).unwrap();
        writer.finish().unwrap();
    }

    // Read and verify
    let reader = SstableReader::open(&sst_path, 1, 0, None).unwrap();
    assert_eq!(reader.get(&k_int(1)).unwrap(), Some(Some(v_int(10))));
    assert_eq!(reader.get(&k_int(2)).unwrap(), Some(Some(v_int(20))));
    assert_eq!(reader.get(&k_int(3)).unwrap(), Some(None)); // Tombstone
    assert_eq!(reader.get(&k_int(4)).unwrap(), Some(Some(v_int(40))));
    assert_eq!(reader.get(&k_int(5)).unwrap(), None); // Missing key

    // Scan
    let scan_res = reader.scan(&k_int(2), &k_int(4), |_, _| true).unwrap();
    assert_eq!(scan_res.len(), 2);
    assert_eq!(scan_res[0], (k_int(2), v_int(20)));
    assert_eq!(scan_res[1], (k_int(4), v_int(40))); // Tombstone for k=3 should be skipped in normal scan

    // Scan Raw (includes tombstones)
    let raw_scan_res = reader.scan_raw(&k_int(2), &k_int(4)).unwrap();
    assert_eq!(raw_scan_res.len(), 3);
    assert_eq!(raw_scan_res[0], (k_int(2), Some(v_int(20))));
    assert_eq!(raw_scan_res[1], (k_int(3), None));
    assert_eq!(raw_scan_res[2], (k_int(4), Some(v_int(40))));
}

#[test]
fn test_engine_crud() {
    let temp_dir = TempDir::new().unwrap();
    // Use very small memtable threshold (60 bytes) to force automatic flushes
    let options = EngineOptions {
        compression: CompressionType::Lz4,
        memtable_size_limit: 60,
        block_size_limit: 4096,
        wal_size_limit: 32 * 1024 * 1024,
        l0_compaction_threshold: 4,
        sstable_target_size: 2 * 1024 * 1024,
        base_level_size_limit: 10 * 1024 * 1024,
        level_size_multiplier: 10,
        max_level: 7,
        block_cache_capacity: 1000,
        wal_sync_interval_ms: None,
    };
    let engine = StorageEngine::open(temp_dir.path(), options).unwrap();

    engine.put(k_str("a"), v_str("val_a")).unwrap();
    engine.put(k_str("b"), v_str("val_b")).unwrap();

    assert_eq!(engine.get(&k_str("a")).unwrap(), Some(v_str("val_a")));
    assert_eq!(engine.get(&k_str("b")).unwrap(), Some(v_str("val_b")));

    // Put a third item to exceed 60 bytes limit, which should trigger automatic flush.
    engine.put(k_str("c"), v_str("val_c")).unwrap();

    // The data should still be queryable
    assert_eq!(engine.get(&k_str("a")).unwrap(), Some(v_str("val_a")));
    assert_eq!(engine.get(&k_str("b")).unwrap(), Some(v_str("val_b")));
    assert_eq!(engine.get(&k_str("c")).unwrap(), Some(v_str("val_c")));

    // Delete a key
    engine.delete(k_str("b")).unwrap();
    assert_eq!(engine.get(&k_str("b")).unwrap(), None);

    // Range scan matching key prefix
    let scan_res = engine
        .filtered_scan(&k_str("a"), &k_str("c"), |_, _| true)
        .unwrap();
    assert_eq!(scan_res.len(), 2);
    assert_eq!(scan_res[0], (k_str("a"), v_str("val_a")));
    assert_eq!(scan_res[1], (k_str("c"), v_str("val_c")));
}

#[test]
fn test_engine_crash_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // 1. Open engine, write some keys, and drop it.
    // Use a large memtable limit so entries stay in memtable/WAL and are not flushed.
    {
        let options = EngineOptions {
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
        };
        let engine = StorageEngine::open(&db_path, options).unwrap();
        engine.put(k_int(1), v_str("one")).unwrap();
        engine.put(k_int(2), v_str("two")).unwrap();
        engine.delete(k_int(1)).unwrap();
        // Dropping engine closes files but WAL remains on disk.
    }

    // 2. Re-open the engine and verify it recovered from WAL.
    {
        let options = EngineOptions {
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
        };
        let engine = StorageEngine::open(&db_path, options).unwrap();
        assert_eq!(engine.get(&k_int(1)).unwrap(), None); // Deleted
        assert_eq!(engine.get(&k_int(2)).unwrap(), Some(v_str("two"))); // Retained

        // The WAL should have been cleared/flushed into a .sst file during recovery
        let wal_path = db_path.join("active.wal");
        assert!(!wal_path.exists() || fs::metadata(wal_path).unwrap().len() == 0);

        // Verify there is an sst file
        let mut sst_count = 0;
        for entry in fs::read_dir(&db_path).unwrap() {
            let entry = entry.unwrap();
            if entry.path().extension().is_some_and(|ext| ext == "sst") {
                sst_count += 1;
            }
        }
        assert_eq!(sst_count, 1);
    }
}

#[test]
fn test_engine_compaction() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // 1. Write overlapping values, forcing multiple flushes (small memtable threshold)
    {
        let options = EngineOptions {
            compression: CompressionType::Lz4,
            memtable_size_limit: 5,
            block_size_limit: 4096,
            wal_size_limit: 32 * 1024 * 1024,
            l0_compaction_threshold: 10,
            sstable_target_size: 2 * 1024 * 1024,
            base_level_size_limit: 10 * 1024 * 1024,
            level_size_multiplier: 10,
            max_level: 7,
            block_cache_capacity: 1000,
            wal_sync_interval_ms: None,
        };
        let engine = StorageEngine::open(&db_path, options).unwrap();
        engine.put(k_int(1), v_str("v1_old")).unwrap(); // Flushes
        engine.put(k_int(1), v_str("v1_new")).unwrap(); // Flushes
        engine.put(k_int(2), v_str("v2")).unwrap(); // Flushes
        engine.delete(k_int(2)).unwrap(); // Flushes

        // Verify we have multiple SST files on disk
        let mut sst_count = 0;
        for entry in fs::read_dir(&db_path).unwrap() {
            let entry = entry.unwrap();
            if entry.path().extension().is_some_and(|ext| ext == "sst") {
                sst_count += 1;
            }
        }
        assert!(sst_count > 1);

        // Run Compaction
        engine.compact().unwrap();

        // 2. Verify all data was merged into a single SSTable
        let mut sst_count = 0;
        let mut sst_path = None;
        for entry in fs::read_dir(&db_path).unwrap() {
            let entry = entry.unwrap();
            if entry.path().extension().is_some_and(|ext| ext == "sst") {
                sst_count += 1;
                sst_path = Some(entry.path());
            }
        }
        assert_eq!(sst_count, 1);
        assert_eq!(
            sst_path.unwrap().file_name().unwrap().to_str().unwrap(),
            "L1_00005.sst"
        );

        // Verify queries are correct
        assert_eq!(engine.get(&k_int(1)).unwrap(), Some(v_str("v1_new")));
        assert_eq!(engine.get(&k_int(2)).unwrap(), None); // Tombstone should have been discarded entirely
    }
}

#[test]
fn test_engine_statistics() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let options = EngineOptions {
        compression: CompressionType::Uncompressed,
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
    };
    let engine = StorageEngine::open(&db_path, options).unwrap();

    // 1. Initially stats should be empty
    let stats = engine.get_statistics().unwrap();
    assert_eq!(stats.num_sstables, 0);
    assert_eq!(stats.sstable_entries, 0);
    assert_eq!(stats.memtable_entries, 0);

    // 2. Put some keys in memtable
    engine.put(k_int(1), v_str("one")).unwrap();
    engine.put(k_int(2), v_str("two")).unwrap();
    engine.delete(k_int(1)).unwrap(); // inserts a tombstone

    let stats = engine.get_statistics().unwrap();
    assert_eq!(stats.num_sstables, 0);
    assert_eq!(stats.sstable_entries, 0);
    assert_eq!(stats.memtable_entries, 2); // k=1 (tombstone) + k=2 (value)
    assert_eq!(stats.memtable_tombstones, 1);
    assert!(stats.memtable_uncompressed_bytes > 0);

    // 3. Flush memtable to disk
    engine.flush_memtable().unwrap();

    let stats = engine.get_statistics().unwrap();
    assert_eq!(stats.num_sstables, 1);
    assert_eq!(stats.sstable_entries, 2); // k=1 (tombstone) + k=2 (value)
    assert_eq!(stats.sstable_tombstones, 1);
    assert!(stats.sstable_uncompressed_bytes > 0);
    assert_eq!(stats.memtable_entries, 0);
    assert_eq!(stats.memtable_tombstones, 0);

    // 4. Compact the level - since k=1 was deleted, compaction should clean up tombstones and old versions.
    engine.compact().unwrap();

    let stats = engine.get_statistics().unwrap();
    // After compaction, only k=2 is left. k=1 and its tombstone are completely discarded because there are no levels below.
    assert_eq!(stats.num_sstables, 1);
    assert_eq!(stats.sstable_entries, 1);
    assert_eq!(stats.sstable_tombstones, 0);
}

#[test]
fn test_memtable_size_tracking() {
    let mem = MemTable::new();
    assert_eq!(mem.byte_size(), 0);

    mem.put(k_int(1), v_int(100)); // key=8 bytes, value=8 bytes. Total = 16 bytes
    assert_eq!(mem.byte_size(), 16);

    // Overwrite existing key
    mem.put(k_int(1), v_str("hello")); // key=8 bytes, value="hello"=5 bytes. Total = 13 bytes
    assert_eq!(mem.byte_size(), 13);

    // Add another key
    mem.put(k_str("a"), v_int(42)); // key="a"=1 byte, value=8 bytes. Total = 9 bytes. Running total = 22 bytes
    assert_eq!(mem.byte_size(), 22);

    // Delete a key
    mem.delete(k_int(1)); // key=8 bytes, value=None(tombstone)=1 byte. Total = 9 bytes. Running total = 9 + 9 = 18 bytes
    assert_eq!(mem.byte_size(), 18);

    // Clear
    mem.clear();
    assert_eq!(mem.byte_size(), 0);
}
