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
        let mut writer = SstableWriter::create(&sst_path, 50, CompressionType::Lz4, 4).unwrap();
        writer.append(&k_int(1), Some(&v_int(10))).unwrap();
        writer.append(&k_int(2), Some(&v_int(20))).unwrap();
        writer.append(&k_int(3), None).unwrap(); // Tombstone
        writer.append(&k_int(4), Some(&v_int(40))).unwrap();
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

#[test]
fn test_engine_multi_get() {
    let temp_dir = TempDir::new().unwrap();
    let options = EngineOptions {
        compression: CompressionType::Lz4,
        memtable_size_limit: 1000,
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

    // Insert keys: some in memtable, some in SSTables
    engine.put(k_int(1), v_int(100)).unwrap();
    engine.put(k_int(2), v_int(200)).unwrap();
    engine.flush_memtable().unwrap(); // Flushes k=1, k=2 to SSTable

    engine.put(k_int(3), v_int(300)).unwrap();
    engine.put(k_int(4), v_int(400)).unwrap();
    engine.delete(k_int(2)).unwrap(); // Tombstone in memtable

    // multi_get queries
    let keys = vec![k_int(1), k_int(2), k_int(3), k_int(4), k_int(5)];
    let results = engine.multi_get(&keys).unwrap();

    assert_eq!(results[0], Some(v_int(100))); // From SSTable
    assert_eq!(results[1], None); // Deleted (tombstone in memtable)
    assert_eq!(results[2], Some(v_int(300))); // From memtable
    assert_eq!(results[3], Some(v_int(400))); // From memtable
    assert_eq!(results[4], None); // Non-existent key
}

#[test]
fn test_engine_compaction_round_robin() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // 1. Create three files in Level 1 under large limit
    {
        let options = EngineOptions {
            compression: CompressionType::Uncompressed,
            memtable_size_limit: 1000,
            block_size_limit: 4096,
            wal_size_limit: 32 * 1024 * 1024,
            l0_compaction_threshold: 10,
            sstable_target_size: 1, // Split into individual files
            base_level_size_limit: 10 * 1024 * 1024, // 10MB
            level_size_multiplier: 10,
            max_level: 4,
            block_cache_capacity: 0,
            wal_sync_interval_ms: None,
        };
        let engine = StorageEngine::open(&db_path, options).unwrap();

        // Write key 10
        engine.put(k_int(10), v_int(10)).unwrap();
        engine.flush_memtable().unwrap();
        engine.compact().unwrap(); // L0 -> L1 (File 1: key 10)

        // Write key 20
        engine.put(k_int(20), v_int(20)).unwrap();
        engine.flush_memtable().unwrap();
        engine.compact().unwrap(); // L0 -> L1 (File 2: key 20)

        // Write key 30
        engine.put(k_int(30), v_int(30)).unwrap();
        engine.flush_memtable().unwrap();
        engine.compact().unwrap(); // L0 -> L1 (File 3: key 30)
    }

    // 2. Reopen the engine with a tiny base_level_size_limit to trigger L1 -> L2 compaction
    let mut l2_keys = Vec::new();
    {
        // Delete options.bin so we can change base_level_size_limit
        std::fs::remove_file(db_path.join("options.bin")).unwrap();

        let options = EngineOptions {
            compression: CompressionType::Uncompressed,
            memtable_size_limit: 1000,
            block_size_limit: 4096,
            wal_size_limit: 32 * 1024 * 1024,
            l0_compaction_threshold: 10,
            sstable_target_size: 1,
            base_level_size_limit: 400, // Limit of 400 bytes triggers compaction only when >= 3 files are present
            level_size_multiplier: 1000, // Large multiplier to prevent L2 -> L3 cascading
            max_level: 4,
            block_cache_capacity: 0,
            wal_sync_interval_ms: None,
        };
        let engine = StorageEngine::open(&db_path, options).unwrap();

        // This compact run will trigger L1 -> L2 compaction because L1 size > limit (1)
        // It should pick list[0], which is key 10, and compact it to Level 2.
        engine.compact().unwrap();

        // Write a new file with key 5 (sorted before key 20 and 30)
        engine.put(k_int(5), v_int(5)).unwrap();
        engine.flush_memtable().unwrap();
        engine.compact().unwrap(); // L0 -> L1 (key 5). This also triggers L1 -> L2 compaction again.

        // If round-robin works:
        // - L1 -> L2 compaction should pick key 20 (since last_compacted was key 10, and 20 > 10).
        // - Level 2 should now contain key 10 and key 20.
        // - Level 1 should contain key 5 and key 30.
        // If it was NOT round-robin (always picking list[0]), it would pick key 5 (since 5 < 20).

        // Let's inspect L2 files on disk
        for entry in std::fs::read_dir(&db_path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "sst") {
                let filename = path.file_name().unwrap().to_str().unwrap();
                if filename.starts_with("L2_") {
                    // Open the L2 sstable reader
                    let reader = SstableReader::open(&path, 0, 2, None).unwrap();
                    if let Some(fk) = reader.first_key() {
                        l2_keys.push(fk.clone());
                    }
                }
            }
        }
    }

    l2_keys.sort();
    assert_eq!(l2_keys, vec![k_int(10), k_int(20)]);
}

#[test]
fn test_engine_crash_recovery_multiple_restarts() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();
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

    // 1. Open engine, write some keys, and drop it (leaving WAL).
    {
        let engine = StorageEngine::open(&db_path, options).unwrap();
        engine.put(k_int(42), v_str("forty-two")).unwrap();
        engine.put(k_int(43), v_str("forty-three")).unwrap();
    }

    // 2. Re-open engine (WAL replay creates L0 SSTable, deletes WAL). Drop it immediately.
    {
        let engine = StorageEngine::open(&db_path, options).unwrap();
        assert_eq!(engine.get(&k_int(42)).unwrap(), Some(v_str("forty-two")));
    }

    // 3. Re-open engine again. Verify that the recovered data is still there!
    {
        let engine = StorageEngine::open(&db_path, options).unwrap();
        assert_eq!(engine.get(&k_int(42)).unwrap(), Some(v_str("forty-two")));
        assert_eq!(engine.get(&k_int(43)).unwrap(), Some(v_str("forty-three")));
    }
}

#[test]
fn test_scan_iter_lower_bound() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let options = EngineOptions {
        compression: CompressionType::Lz4,
        memtable_size_limit: 1000,
        block_size_limit: 4096,
        wal_size_limit: 32 * 1024 * 1024,
        l0_compaction_threshold: 4,
        sstable_target_size: 2 * 1024 * 1024,
        base_level_size_limit: 10 * 1024 * 1024,
        level_size_multiplier: 10,
        max_level: 7,
        block_cache_capacity: 0,
        wal_sync_interval_ms: None,
    };
    let engine = StorageEngine::open(&db_path, options).unwrap();

    // Write keys: 10, 20, 30, 40 to L0 SSTable
    engine.put(k_int(10), v_int(10)).unwrap();
    engine.put(k_int(20), v_int(20)).unwrap();
    engine.put(k_int(30), v_int(30)).unwrap();
    engine.put(k_int(40), v_int(40)).unwrap();
    engine.flush_memtable().unwrap();

    // scan_iter from 25 to 35. Should only yield key 30.
    let mut iter = engine.scan_iter(&k_int(25), &k_int(35)).unwrap();
    let mut results = Vec::new();
    while let Some(entry) = iter.next().unwrap() {
        results.push(entry);
    }
    assert_eq!(results, vec![(k_int(30), v_int(30))]);
}

#[test]
fn test_scan_iter_stops_loading_blocks_past_end() {
    // Regression: scanning a tiny window of a large SSTable used to drain every
    // remaining block of every above-range source. The iterator must drop the
    // source once it observes a key past `end`, and SstableBlockIterator must
    // refuse to load further blocks whose first_key exceeds `end`.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let options = EngineOptions {
        compression: CompressionType::Lz4,
        memtable_size_limit: 10 * 1024 * 1024,
        // Tiny blocks force many small blocks per SSTable so the bug, if present,
        // shows up as many physical block reads.
        block_size_limit: 64,
        wal_size_limit: 32 * 1024 * 1024,
        l0_compaction_threshold: 16,
        sstable_target_size: 8 * 1024 * 1024,
        base_level_size_limit: 10 * 1024 * 1024,
        level_size_multiplier: 10,
        max_level: 7,
        block_cache_capacity: 0,
        wal_sync_interval_ms: None,
    };
    let engine = StorageEngine::open(&db_path, options).unwrap();

    let n: i64 = 2000;
    for i in 0..n {
        engine.put(k_int(i), v_int(i)).unwrap();
    }
    engine.flush_memtable().unwrap();

    dtdb_storage::reset_physical_blocks_read();

    // Scan a window of only 5 keys near the start of the (2000-key) file.
    let mut iter = engine.scan_iter(&k_int(0), &k_int(4)).unwrap();
    let mut results = Vec::new();
    while let Some(entry) = iter.next().unwrap() {
        results.push(entry);
    }
    assert_eq!(results.len(), 5);

    let blocks_read = dtdb_storage::PHYSICAL_BLOCKS_READ.load(std::sync::atomic::Ordering::SeqCst);
    // With ~64-byte blocks the file has hundreds of blocks; a correctly bounded
    // iterator only needs a handful of them.
    assert!(
        blocks_read < 20,
        "scan_iter read {blocks_read} blocks for a 5-key window; expected early termination"
    );
}
