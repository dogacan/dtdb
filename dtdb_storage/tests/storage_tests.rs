use dtdb_storage::memtable::MemTable;
use dtdb_storage::sstable::{SstableReader, SstableWriter};
use dtdb_storage::wal::Wal;
use dtdb_storage::{
    CompressionType, DbKey, DbValue, EngineOptions, FsyncMethod, StorageEngine, WalEntry,
};
use std::fs;
use tempfile::TempDir;

// Helper to create keys and values easily
fn k_int(val: i64) -> DbKey {
    DbKey::Int(val)
}

fn k_str(val: &str) -> DbKey {
    DbKey::string(val)
}

fn v_int(val: i64) -> DbValue {
    DbValue::Int(val)
}

fn v_str(val: &str) -> DbValue {
    DbValue::string(val)
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
        let mut wal = Wal::open(&wal_path, None, FsyncMethod::default()).unwrap();
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
        let mut writer =
            SstableWriter::create(&sst_path, 50, CompressionType::Lz4, 4, vec![]).unwrap();
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
        ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
        };
        let engine = StorageEngine::open(&db_path, options).unwrap();
        assert_eq!(engine.get(&k_int(1)).unwrap(), None); // Deleted
        assert_eq!(engine.get(&k_int(2)).unwrap(), Some(v_str("two"))); // Retained

        // The WAL should have been cleared/flushed into a .sst file during
        // recovery: it holds no pending records (an empty log is still a few
        // header bytes on disk, so check the records, not the byte length).
        let wal_path = db_path.join("active.wal");
        let pending = dtdb_storage::wal::Wal::recover(&wal_path).unwrap();
        assert!(
            pending.is_empty(),
            "WAL should hold no records after recovery flushed it"
        );

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
            ..Default::default()
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
        ..Default::default()
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
fn test_memtable_size_tracking_grow_and_typed_values() {
    let mem = MemTable::new();
    assert!(mem.is_empty());
    assert_eq!(mem.len(), 0);

    // Overwrite with a LARGER value: the delta is positive, exercising the
    // grow branch of `put` (the existing test only shrinks).
    mem.put(k_int(1), v_str("ab")); // key=8 + value=2 = 10
    assert_eq!(mem.byte_size(), 10);
    mem.put(k_int(1), v_str("abcdef")); // value grows 2 -> 6, delta +4
    assert_eq!(mem.byte_size(), 14);

    // Delete a key whose old value is a 1-byte value: tombstone overhead (1)
    // minus old size (1) is a non-negative delta, exercising delete's add arm.
    mem.put(k_str("b"), DbValue::Bool(true)); // key=1 + value=1 = 2; total 16
    assert_eq!(mem.byte_size(), 16);
    mem.delete(k_str("b")); // 1 - 1 = 0 delta
    assert_eq!(mem.byte_size(), 16);

    assert_eq!(mem.len(), 2);
    assert!(!mem.is_empty());
}

#[test]
fn test_memtable_byte_size_all_value_types() {
    use chrono::{NaiveDate, NaiveTime};
    use rust_decimal::Decimal;

    // Each variant's accounted byte size, isolated in its own memtable so the
    // running total equals key (8) + the value's contribution.
    let cases: Vec<(DbValue, usize)> = vec![
        (DbValue::Float(1.5), 8),
        (
            DbValue::Date(NaiveDate::from_ymd_opt(2026, 6, 4).unwrap()),
            4,
        ),
        (
            DbValue::Time(NaiveTime::from_hms_opt(12, 30, 0).unwrap()),
            8,
        ),
        (
            DbValue::Timestamp(
                NaiveDate::from_ymd_opt(2026, 6, 4)
                    .unwrap()
                    .and_hms_opt(1, 2, 3)
                    .unwrap(),
            ),
            8,
        ),
        (DbValue::Decimal(Decimal::new(12345, 2)), 16),
        (DbValue::Null, 1),
    ];

    for (value, expected_value_bytes) in cases {
        let mem = MemTable::new();
        mem.put(k_int(1), value.clone());
        assert_eq!(
            mem.byte_size(),
            8 + expected_value_bytes,
            "unexpected byte size for {value:?}"
        );
    }
}

#[test]
fn test_memtable_default_matches_new() {
    let mem = MemTable::default();
    assert!(mem.is_empty());
    mem.put(k_int(1), v_int(1));
    assert_eq!(mem.get(&k_int(1)), Some(Some(v_int(1))));
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
        ..Default::default()
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
            ..Default::default()
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

    // Measure the on-disk size of the three single-entry L1 files so the
    // compaction threshold is derived from the actual serialized size rather
    // than a magic constant tied to one serialization format (the absolute
    // sizes differ between e.g. bincode and postcard).
    let l1_total_size: u64 = std::fs::read_dir(&db_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension().is_some_and(|ext| ext == "sst")
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("L1_"))
        })
        .map(|p| std::fs::metadata(&p).unwrap().len())
        .sum();
    // All three files have the same size, so a limit one byte below their
    // total triggers L1 -> L2 compaction only when all three are present: any
    // two of them stay at or below the limit.
    let three_file_limit = (l1_total_size - 1) as usize;

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
            base_level_size_limit: three_file_limit,
            level_size_multiplier: 1000, // Large multiplier to prevent L2 -> L3 cascading
            max_level: 4,
            block_cache_capacity: 0,
            wal_sync_interval_ms: None,
            ..Default::default()
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
        ..Default::default()
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
        ..Default::default()
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
fn test_sstable_block_checksum_detects_bitrot() {
    use std::io::{Seek, SeekFrom, Write};
    let temp_dir = TempDir::new().unwrap();
    let sst_path = temp_dir.path().join("00001.sst");

    {
        let mut writer =
            SstableWriter::create(&sst_path, 64, CompressionType::Lz4, 4, vec![]).unwrap();
        for i in 0..16 {
            writer.append(&k_int(i), Some(&v_int(i * 10))).unwrap();
        }
        writer.finish().unwrap();
    }

    // Sanity: clean read works.
    {
        let reader = SstableReader::open(&sst_path, 1, 0, None).unwrap();
        assert_eq!(reader.get(&k_int(0)).unwrap(), Some(Some(v_int(0))));
    }

    // Flip a byte inside the first data block (offset 0).
    {
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&sst_path)
            .unwrap();
        f.seek(SeekFrom::Start(8)).unwrap();
        let mut b = [0u8; 1];
        std::io::Read::read_exact(&mut f, &mut b).unwrap();
        b[0] ^= 0xFF;
        f.seek(SeekFrom::Start(8)).unwrap();
        f.write_all(&b).unwrap();
        f.sync_all().unwrap();
    }

    // Opening no longer reads any data block (last_key comes from the index
    // stats), so the checksum is verified on the lookup that reads the damaged
    // block. What's not acceptable is silently returning wrong data.
    let result =
        SstableReader::open(&sst_path, 1, 0, None).and_then(|r| r.get(&k_int(0)).map(|_| ()));
    let err = result.expect_err("corrupted block must be detected");
    let msg = format!("{err}");
    assert!(
        msg.contains("checksum") || msg.contains("Corruption") || msg.contains("decompress"),
        "expected corruption-flavored error, got: {msg}"
    );
}

#[test]
fn test_sstable_index_checksum_detects_corruption() {
    use std::io::{Seek, SeekFrom, Write};
    let temp_dir = TempDir::new().unwrap();
    let sst_path = temp_dir.path().join("00001.sst");

    {
        let mut writer =
            SstableWriter::create(&sst_path, 64, CompressionType::Lz4, 4, vec![]).unwrap();
        for i in 0..16 {
            writer.append(&k_int(i), Some(&v_int(i * 10))).unwrap();
        }
        writer.finish().unwrap();
    }

    // Sanity: a clean file opens fine.
    SstableReader::open(&sst_path, 1, 0, None).unwrap();

    // The footer's first 8 bytes (28 bytes from EOF) hold the index offset.
    // Flip a byte inside the index block so its checksum no longer matches.
    let data = std::fs::read(&sst_path).unwrap();
    let footer_start = data.len() - 28;
    let index_offset =
        u64::from_le_bytes(data[footer_start..footer_start + 8].try_into().unwrap());
    {
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&sst_path)
            .unwrap();
        f.seek(SeekFrom::Start(index_offset)).unwrap();
        let mut b = [0u8; 1];
        std::io::Read::read_exact(&mut f, &mut b).unwrap();
        b[0] ^= 0xFF;
        f.seek(SeekFrom::Start(index_offset)).unwrap();
        f.write_all(&b).unwrap();
        f.sync_all().unwrap();
    }

    // Open must fail at the checksum check, before deserializing the index.
    let err = SstableReader::open(&sst_path, 1, 0, None)
        .err()
        .expect("corrupt index block must be detected");
    let msg = format!("{err}");
    assert!(
        msg.contains("checksum") || msg.contains("Corruption"),
        "expected corruption-flavored error, got: {msg}"
    );
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
        ..Default::default()
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

    let blocks_read = dtdb_storage::get_physical_blocks_read();
    // With ~64-byte blocks the file has hundreds of blocks; a correctly bounded
    // iterator only needs a handful of them.
    assert!(
        blocks_read < 20,
        "scan_iter read {blocks_read} blocks for a 5-key window; expected early termination"
    );
}

/// Options with a large memtable so manually-flushed SSTables and live
/// memtable entries coexist for merge tests, and no block cache so reads are
/// deterministic.
fn merge_test_options() -> EngineOptions {
    EngineOptions {
        compression: CompressionType::Lz4,
        memtable_size_limit: 8 * 1024 * 1024,
        block_size_limit: 4096,
        wal_size_limit: 32 * 1024 * 1024,
        l0_compaction_threshold: 100,
        sstable_target_size: 2 * 1024 * 1024,
        base_level_size_limit: 10 * 1024 * 1024,
        level_size_multiplier: 10,
        max_level: 7,
        block_cache_capacity: 0,
        wal_sync_interval_ms: None,
        ..Default::default()
    }
}

#[test]
fn test_scan_iter_merges_memtable_over_sstable_with_tombstones() {
    let temp_dir = TempDir::new().unwrap();
    let engine = StorageEngine::open(temp_dir.path(), merge_test_options()).unwrap();

    // Persist a baseline into an L0 SSTable.
    for i in [10, 20, 30, 40, 50] {
        engine.put(k_int(i), v_int(i)).unwrap();
    }
    engine.flush_memtable().unwrap();

    // Live memtable mutations layered on top of the SSTable:
    //   - 20 is overwritten (memtable must win),
    //   - 40 is deleted (tombstone must hide the SSTable value),
    //   - 25 is brand new and interleaves between SSTable keys.
    engine.put(k_int(20), v_int(200)).unwrap();
    engine.delete(k_int(40)).unwrap();
    engine.put(k_int(25), v_int(25)).unwrap();

    let mut iter = engine.scan_iter(&k_int(0), &k_int(100)).unwrap();
    let mut results = Vec::new();
    while let Some(entry) = iter.next().unwrap() {
        results.push(entry);
    }

    assert_eq!(
        results,
        vec![
            (k_int(10), v_int(10)),
            (k_int(20), v_int(200)), // memtable shadows SSTable
            (k_int(25), v_int(25)),  // memtable-only key
            (k_int(30), v_int(30)),
            // 40 deleted — absent
            (k_int(50), v_int(50)),
        ]
    );
}

#[test]
fn test_scan_iter_newer_l0_file_shadows_older_for_same_key() {
    // Two L0 SSTables both contain key 20. During a scan their iterators sit in
    // the merge heap with equal keys, so the heap's priority tie-break must
    // surface the newer file's value (and suppress the older duplicate).
    let temp_dir = TempDir::new().unwrap();
    let engine = StorageEngine::open(temp_dir.path(), merge_test_options()).unwrap();

    // Older L0 file: keys 10, 20(old), 30.
    engine.put(k_int(10), v_int(10)).unwrap();
    engine.put(k_int(20), v_int(200)).unwrap();
    engine.put(k_int(30), v_int(30)).unwrap();
    engine.flush_memtable().unwrap();

    // Newer L0 file: re-writes 20 and deletes 30.
    engine.put(k_int(20), v_int(999)).unwrap();
    engine.delete(k_int(30)).unwrap();
    engine.flush_memtable().unwrap();

    let mut iter = engine.scan_iter(&k_int(0), &k_int(100)).unwrap();
    let mut results = Vec::new();
    while let Some(entry) = iter.next().unwrap() {
        results.push(entry);
    }

    assert_eq!(
        results,
        vec![
            (k_int(10), v_int(10)),
            (k_int(20), v_int(999)), // newer L0 file wins the tie
                                     // 30 deleted by the newer file's tombstone
        ]
    );
}

#[test]
fn test_filtered_scan_applies_filter_and_honors_tombstones() {
    let temp_dir = TempDir::new().unwrap();
    let engine = StorageEngine::open(temp_dir.path(), merge_test_options()).unwrap();

    for i in 1..=5 {
        engine.put(k_int(i), v_int(i)).unwrap();
    }
    engine.flush_memtable().unwrap();

    engine.put(k_int(3), v_int(30)).unwrap(); // shadow with an even value
    engine.delete(k_int(5)).unwrap(); // tombstone
    engine.put(k_int(6), v_int(60)).unwrap(); // memtable-only

    // Keep only even values. Exercises the filter's true and false arms across
    // both the memtable and the SSTable, plus tombstone shadowing.
    let results = engine
        .filtered_scan(&k_int(0), &k_int(10), |_, v| match v {
            DbValue::Int(n) => n % 2 == 0,
            _ => false,
        })
        .unwrap();

    assert_eq!(
        results,
        vec![
            (k_int(2), v_int(2)),
            (k_int(3), v_int(30)), // memtable shadow, passes even filter
            (k_int(4), v_int(4)),
            (k_int(6), v_int(60)),
        ]
    );
}

#[test]
fn test_scan_and_filtered_scan_across_compacted_levels() {
    // After compaction the data lives in level >= 1, exercising the level-1+
    // branches of both scan_iter and filtered_scan.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let options = EngineOptions {
        compression: CompressionType::Lz4,
        memtable_size_limit: 5, // tiny: every put flushes
        block_size_limit: 4096,
        wal_size_limit: 32 * 1024 * 1024,
        l0_compaction_threshold: 10,
        sstable_target_size: 2 * 1024 * 1024,
        base_level_size_limit: 10 * 1024 * 1024,
        level_size_multiplier: 10,
        max_level: 7,
        block_cache_capacity: 0,
        wal_sync_interval_ms: None,
        ..Default::default()
    };
    let engine = StorageEngine::open(&db_path, options).unwrap();

    for i in 0..20 {
        engine.put(k_int(i), v_int(i * 10)).unwrap();
    }
    // Push the L0 files down into level 1.
    engine.compact().unwrap();

    // No L0 *.sst should remain; everything is in L1+.
    let l0_remaining = fs::read_dir(&db_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.starts_with("L0_") && n.ends_with(".sst"))
        })
        .count();
    assert_eq!(l0_remaining, 0, "compaction should have drained L0");

    // scan_iter over the compacted levels.
    let mut iter = engine.scan_iter(&k_int(5), &k_int(9)).unwrap();
    let mut scanned = Vec::new();
    while let Some(entry) = iter.next().unwrap() {
        scanned.push(entry);
    }
    assert_eq!(
        scanned,
        vec![
            (k_int(5), v_int(50)),
            (k_int(6), v_int(60)),
            (k_int(7), v_int(70)),
            (k_int(8), v_int(80)),
            (k_int(9), v_int(90)),
        ]
    );

    // filtered_scan over the same compacted levels.
    let filtered = engine
        .filtered_scan(&k_int(0), &k_int(19), |_, v| match v {
            DbValue::Int(n) => *n >= 150,
            _ => false,
        })
        .unwrap();
    let expected: Vec<_> = (15..20).map(|i| (k_int(i), v_int(i * 10))).collect();
    assert_eq!(filtered, expected);
}

#[test]
fn test_open_reconstructs_missing_manifest_and_cleans_tmp_files() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    {
        let engine = StorageEngine::open(&db_path, merge_test_options()).unwrap();
        for i in 0..5 {
            engine.put(k_int(i), v_int(i)).unwrap();
        }
        engine.flush_memtable().unwrap();
    }

    // Simulate a lost manifest plus a leftover temp file from a crash
    // mid-write. Reopen must rebuild the manifest from the on-disk L*.sst files
    // and sweep away the orphaned .tmp.
    fs::remove_dir_all(db_path.join("manifest")).unwrap();
    let stray_tmp = db_path.join("L9_99999.sst.tmp");
    fs::write(&stray_tmp, b"garbage").unwrap();

    let engine = StorageEngine::open(&db_path, merge_test_options()).unwrap();
    for i in 0..5 {
        assert_eq!(engine.get(&k_int(i)).unwrap(), Some(v_int(i)));
    }
    assert!(
        !stray_tmp.exists(),
        "leftover .tmp file should be cleaned up"
    );
    assert!(
        db_path.join("manifest").join("CURRENT").exists(),
        "manifest should have been rebuilt"
    );
}

#[test]
fn test_scan_iter_stops_at_memtable_key_past_end() {
    // A purely-in-memtable scan whose `end` falls before the last key must stop
    // as soon as it reaches a memtable key past `end`, rather than walking the
    // rest of the memtable.
    let temp_dir = TempDir::new().unwrap();
    let engine = StorageEngine::open(temp_dir.path(), merge_test_options()).unwrap();

    engine.put(k_int(10), v_int(10)).unwrap();
    engine.put(k_int(20), v_int(20)).unwrap();
    engine.put(k_int(30), v_int(30)).unwrap();

    let mut iter = engine.scan_iter(&k_int(0), &k_int(15)).unwrap();
    let mut results = Vec::new();
    while let Some(entry) = iter.next().unwrap() {
        results.push(entry);
    }
    assert_eq!(results, vec![(k_int(10), v_int(10))]);
}

/// Builds many single-key, non-overlapping L1 SSTables (sstable_target_size = 1
/// makes compaction emit one file per entry), then exercises the binary-search
/// pruning in `scan_iter` / `filtered_scan` over a sub-range: leading files
/// below `start` must be skipped and trailing files past `end` must not be
/// opened.
fn many_l1_files_options() -> EngineOptions {
    EngineOptions {
        compression: CompressionType::Uncompressed,
        memtable_size_limit: 8 * 1024 * 1024,
        block_size_limit: 4096,
        wal_size_limit: 32 * 1024 * 1024,
        l0_compaction_threshold: 100,
        sstable_target_size: 1, // one entry per output SSTable
        base_level_size_limit: 10 * 1024 * 1024,
        level_size_multiplier: 10,
        max_level: 7,
        block_cache_capacity: 0,
        wal_sync_interval_ms: None,
        ..Default::default()
    }
}

#[test]
fn test_range_scan_prunes_nonoverlapping_l1_files() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let engine = StorageEngine::open(&db_path, many_l1_files_options()).unwrap();

    // Keys 0,10,20,...,90 → after compaction, ten one-key L1 files.
    for i in 0..10 {
        engine.put(k_int(i * 10), v_int(i * 10)).unwrap();
    }
    engine.compact().unwrap();

    // Confirm we really produced multiple L1 files (so partition_point has work
    // to do rather than trivially returning 0).
    let l1_files = fs::read_dir(&db_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.starts_with("L1_") && n.ends_with(".sst"))
        })
        .count();
    assert!(l1_files >= 5, "expected several L1 files, got {l1_files}");

    // scan_iter over [25, 65]: skips files for 0/10/20 (below start) and must
    // break before opening the file for 70 (past end).
    let mut iter = engine.scan_iter(&k_int(25), &k_int(65)).unwrap();
    let mut scanned = Vec::new();
    while let Some(entry) = iter.next().unwrap() {
        scanned.push(entry);
    }
    assert_eq!(
        scanned,
        vec![
            (k_int(30), v_int(30)),
            (k_int(40), v_int(40)),
            (k_int(50), v_int(50)),
            (k_int(60), v_int(60)),
        ]
    );

    // filtered_scan over the same window exercises its own pruning loop.
    let filtered = engine
        .filtered_scan(&k_int(25), &k_int(65), |_, _| true)
        .unwrap();
    assert_eq!(
        filtered,
        vec![
            (k_int(30), v_int(30)),
            (k_int(40), v_int(40)),
            (k_int(50), v_int(50)),
            (k_int(60), v_int(60)),
        ]
    );

    // A live memtable value for a key already in L1 must shadow the L1 copy,
    // exercising the duplicate-skip path of the merge.
    engine.put(k_int(40), v_int(4000)).unwrap();
    let mut iter = engine.scan_iter(&k_int(35), &k_int(45)).unwrap();
    let mut shadowed = Vec::new();
    while let Some(entry) = iter.next().unwrap() {
        shadowed.push(entry);
    }
    assert_eq!(shadowed, vec![(k_int(40), v_int(4000))]);
}

#[test]
fn test_compaction_at_max_level_drops_tombstones() {
    // When the compaction target IS the max level there is no level below, so a
    // tombstone is dropped unconditionally (the `target_level == max_level`
    // branch and its empty lower-level snapshot).
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let options = EngineOptions {
        compression: CompressionType::Uncompressed,
        memtable_size_limit: 8 * 1024 * 1024,
        block_size_limit: 4096,
        wal_size_limit: 32 * 1024 * 1024,
        l0_compaction_threshold: 100,
        sstable_target_size: 2 * 1024 * 1024,
        base_level_size_limit: 10 * 1024 * 1024,
        level_size_multiplier: 10,
        max_level: 1, // L0 compacts straight into the max level (L1)
        block_cache_capacity: 0,
        wal_sync_interval_ms: None,
        ..Default::default()
    };
    let engine = StorageEngine::open(&db_path, options).unwrap();

    // One SSTable carrying a value, a second carrying its tombstone.
    engine.put(k_int(1), v_int(100)).unwrap();
    engine.flush_memtable().unwrap();
    engine.delete(k_int(1)).unwrap();
    engine.flush_memtable().unwrap();

    engine.compact().unwrap();

    assert_eq!(engine.get(&k_int(1)).unwrap(), None);
    // Both the value and the tombstone are gone — nothing survives at max level.
    let stats = engine.get_statistics().unwrap();
    assert_eq!(stats.sstable_entries, 0);
    assert_eq!(stats.sstable_tombstones, 0);
}

#[test]
fn test_sstable_write_read_various_types() {
    let temp_dir = TempDir::new().unwrap();
    let sst_path = temp_dir.path().join("types.sst");

    let date = chrono::NaiveDate::from_ymd_opt(2026, 6, 2).unwrap();
    let time = chrono::NaiveTime::from_hms_opt(12, 34, 56).unwrap();
    let ts = date.and_hms_opt(12, 34, 56).unwrap();
    let dec = rust_decimal::Decimal::new(12345, 2);

    {
        let mut writer =
            SstableWriter::create(&sst_path, 1024, CompressionType::Uncompressed, 5, vec![])
                .unwrap();
        writer
            .append(&k_int(1), Some(&DbValue::Bool(true)))
            .unwrap();
        writer
            .append(&k_int(2), Some(&DbValue::Date(date)))
            .unwrap();
        writer
            .append(&k_int(3), Some(&DbValue::Time(time)))
            .unwrap();
        writer
            .append(&k_int(4), Some(&DbValue::Timestamp(ts)))
            .unwrap();
        writer
            .append(&k_int(5), Some(&DbValue::Decimal(dec)))
            .unwrap();
        writer.finish().unwrap();
    }

    let reader = SstableReader::open(&sst_path, 1, 0, None).unwrap();
    assert_eq!(
        reader.get(&k_int(1)).unwrap(),
        Some(Some(DbValue::Bool(true)))
    );
    assert_eq!(
        reader.get(&k_int(2)).unwrap(),
        Some(Some(DbValue::Date(date)))
    );
    assert_eq!(
        reader.get(&k_int(3)).unwrap(),
        Some(Some(DbValue::Time(time)))
    );
    assert_eq!(
        reader.get(&k_int(4)).unwrap(),
        Some(Some(DbValue::Timestamp(ts)))
    );
    assert_eq!(
        reader.get(&k_int(5)).unwrap(),
        Some(Some(DbValue::Decimal(dec)))
    );
}

#[test]
fn test_sstable_corruption_handling() {
    use dtdb_storage::sstable::{IndexBlock, IndexEntry, StatsBlock};
    use std::io::Write;

    let temp_dir = TempDir::new().unwrap();

    // 1. File size too small to contain footer.
    {
        let sst_path = temp_dir.path().join("too_small.sst");
        std::fs::write(&sst_path, b"too short").unwrap();
        let res = SstableReader::open(&sst_path, 1, 0, None);
        assert!(res.is_err());
        let err_msg = res.err().unwrap().to_string();
        assert!(
            err_msg.contains("too small to contain footer"),
            "expected 'too small to contain footer', got: {err_msg}"
        );
    }

    // 2. Invalid SSTable magic number.
    {
        let sst_path = temp_dir.path().join("invalid_magic.sst");
        let mut data = vec![0u8; 28];
        data[20..28].copy_from_slice(b"BADMAGIC");
        std::fs::write(&sst_path, &data).unwrap();
        let res = SstableReader::open(&sst_path, 1, 0, None);
        assert!(res.is_err());
        let err_msg = res.err().unwrap().to_string();
        assert!(
            err_msg.contains("Invalid SSTable magic number"),
            "expected 'Invalid SSTable magic number', got: {err_msg}"
        );
    }

    // 3. SSTable footer points outside file.
    {
        let sst_path = temp_dir.path().join("outside_file.sst");
        let mut data = vec![0u8; 28];
        data[0..8].copy_from_slice(&100u64.to_le_bytes());
        data[8..16].copy_from_slice(&10u64.to_le_bytes());
        // bytes [16..20] are the index checksum; the footer-bounds check fires
        // before it is consulted, so its value is irrelevant here.
        data[20..28].copy_from_slice(b"DTDB_SST");
        std::fs::write(&sst_path, &data).unwrap();
        let res = SstableReader::open(&sst_path, 1, 0, None);
        assert!(res.is_err());
        let err_msg = res.err().unwrap().to_string();
        assert!(
            err_msg.contains("points outside file"),
            "expected 'points outside file', got: {err_msg}"
        );
    }

    // 4. SSTable index is non-empty but stats.max_key is missing.
    {
        let sst_path = temp_dir.path().join("missing_max_key.sst");
        let mut file = std::fs::File::create(&sst_path).unwrap();
        file.write_all(&[0u8; 10]).unwrap();
        let index_offset = 10u64;

        let index_block = IndexBlock {
            entries: vec![IndexEntry {
                first_key: DbKey::Int(1),
                offset: 0,
                size: 10,
                checksum: 0,
            }],
            compression: CompressionType::Uncompressed,
            stats: StatsBlock {
                num_entries: 1,
                num_tombstones: 0,
                total_uncompressed_bytes: 10,
                min_key: None,
                max_key: None, // Missing!
            },
            bloom_filter: None,
            layout: vec![],
        };
        let index_bytes = postcard::to_allocvec(&index_block).unwrap();
        file.write_all(&index_bytes).unwrap();
        let index_len = index_bytes.len() as u64;
        let index_checksum = xxhash_rust::xxh32::xxh32(&index_bytes, 0);

        file.write_all(&index_offset.to_le_bytes()).unwrap();
        file.write_all(&index_len.to_le_bytes()).unwrap();
        file.write_all(&index_checksum.to_le_bytes()).unwrap();
        file.write_all(b"DTDB_SST").unwrap();
        file.sync_all().unwrap();
        drop(file);

        let res = SstableReader::open(&sst_path, 1, 0, None);
        assert!(res.is_err());
        let err_msg = res.err().unwrap().to_string();
        assert!(
            err_msg.contains("stats.max_key is missing"),
            "expected 'stats.max_key is missing', got: {err_msg}"
        );
    }

    // 5. Block offset/size out of file boundaries.
    {
        let sst_path = temp_dir.path().join("block_outside_file.sst");
        let mut file = std::fs::File::create(&sst_path).unwrap();
        file.write_all(&[0u8; 10]).unwrap();
        let index_offset = 10u64;

        let index_block = IndexBlock {
            entries: vec![IndexEntry {
                first_key: DbKey::Int(1),
                offset: 0,
                size: 100, // Outside file boundaries!
                checksum: 0,
            }],
            compression: CompressionType::Uncompressed,
            stats: StatsBlock {
                num_entries: 1,
                num_tombstones: 0,
                total_uncompressed_bytes: 10,
                min_key: Some(DbKey::Int(1)),
                max_key: Some(DbKey::Int(1)),
            },
            bloom_filter: None,
            layout: vec![],
        };
        let index_bytes = postcard::to_allocvec(&index_block).unwrap();
        file.write_all(&index_bytes).unwrap();
        let index_len = index_bytes.len() as u64;
        let index_checksum = xxhash_rust::xxh32::xxh32(&index_bytes, 0);

        file.write_all(&index_offset.to_le_bytes()).unwrap();
        file.write_all(&index_len.to_le_bytes()).unwrap();
        file.write_all(&index_checksum.to_le_bytes()).unwrap();
        file.write_all(b"DTDB_SST").unwrap();
        file.sync_all().unwrap();
        drop(file);

        let reader = SstableReader::open(&sst_path, 1, 0, None).unwrap();
        let res = reader.get(&DbKey::Int(1));
        assert!(res.is_err());
        let err_msg = res.err().unwrap().to_string();
        assert!(
            err_msg.contains("block extent outside file"),
            "expected 'block extent outside file', got: {err_msg}"
        );
    }
}

#[test]
fn test_compaction_preserves_all_value_types() {
    // Compaction recomputes each entry's byte size (to honor
    // `sstable_target_size`) with a per-variant match. Feed it one value of
    // every type so every arm of that match is exercised, and confirm the
    // values round-trip through the compacted SSTable unchanged.
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let options = EngineOptions {
        compression: CompressionType::Uncompressed,
        memtable_size_limit: 1024 * 1024,
        block_size_limit: 4096,
        wal_size_limit: 32 * 1024 * 1024,
        l0_compaction_threshold: 10,
        sstable_target_size: 2 * 1024 * 1024,
        base_level_size_limit: 10 * 1024 * 1024,
        level_size_multiplier: 10,
        max_level: 7,
        block_cache_capacity: 0,
        wal_sync_interval_ms: None,
        ..Default::default()
    };
    let engine = StorageEngine::open(&db_path, options).unwrap();

    let date = chrono::NaiveDate::from_ymd_opt(2026, 6, 4).unwrap();
    let time = chrono::NaiveTime::from_hms_opt(1, 2, 3).unwrap();
    let ts = date.and_hms_opt(1, 2, 3).unwrap();
    let dec = rust_decimal::Decimal::new(150, 2);

    let entries: Vec<(DbKey, DbValue)> = vec![
        (k_int(1), v_int(10)),
        (k_int(2), DbValue::Float(1.5)),
        (k_int(3), v_str("text")),
        (k_int(4), DbValue::bytes(vec![1, 2, 3, 4])),
        (k_int(5), DbValue::Bool(true)),
        (k_int(6), DbValue::Null),
        (k_int(7), DbValue::Date(date)),
        (k_int(8), DbValue::Time(time)),
        (k_int(9), DbValue::Timestamp(ts)),
        (k_int(10), DbValue::Decimal(dec)),
    ];
    for (k, v) in &entries {
        engine.put(k.clone(), v.clone()).unwrap();
    }
    engine.flush_memtable().unwrap();

    // Drop the L0 file into L1 via the compaction merge path.
    engine.compact().unwrap();

    for (k, v) in &entries {
        assert_eq!(
            engine.get(k).unwrap(),
            Some(v.clone()),
            "mismatch for {k:?}"
        );
    }
}

#[test]
fn test_sstable_empty_reads_return_nothing() {
    let temp_dir = TempDir::new().unwrap();
    let sst_path = temp_dir.path().join("empty.sst");

    // Finish a writer without appending anything: the index ends up empty.
    {
        let writer =
            SstableWriter::create(&sst_path, 4096, CompressionType::Lz4, 0, vec![]).unwrap();
        writer.finish().unwrap();
    }

    let reader = SstableReader::open(&sst_path, 1, 0, None).unwrap();
    // An empty SSTable reports no first key.
    assert_eq!(reader.first_key(), None);
    // Point lookups and scans over an empty index short-circuit to "nothing".
    assert_eq!(reader.get(&k_int(1)).unwrap(), None);
    assert!(
        reader
            .scan(&k_int(0), &k_int(100), |_, _| true)
            .unwrap()
            .is_empty()
    );
    assert!(reader.scan_raw(&k_int(0), &k_int(100)).unwrap().is_empty());
}

#[test]
fn test_sstable_get_key_below_first_block_misses() {
    let temp_dir = TempDir::new().unwrap();
    let sst_path = temp_dir.path().join("below.sst");

    {
        let mut writer =
            SstableWriter::create(&sst_path, 4096, CompressionType::Uncompressed, 3, vec![])
                .unwrap();
        writer.append(&k_int(10), Some(&v_int(100))).unwrap();
        writer.append(&k_int(20), Some(&v_int(200))).unwrap();
        writer.append(&k_int(30), Some(&v_int(300))).unwrap();
        writer.finish().unwrap();
    }

    let reader = SstableReader::open(&sst_path, 1, 0, None).unwrap();
    // A key smaller than the first block's first key takes the `idx == 0` miss
    // branch of the index binary search.
    assert_eq!(reader.get(&k_int(5)).unwrap(), None);
    assert_eq!(reader.get(&k_int(20)).unwrap(), Some(Some(v_int(200))));
}

#[test]
fn test_sstable_create_path_without_extension() {
    let temp_dir = TempDir::new().unwrap();
    // No file extension: the writer must still derive a temp path (".tmp")
    // and rename it into place on finish.
    let sst_path = temp_dir.path().join("no_extension");

    {
        let mut writer =
            SstableWriter::create(&sst_path, 4096, CompressionType::Lz4, 1, vec![]).unwrap();
        writer.append(&k_int(1), Some(&v_int(42))).unwrap();
        writer.finish().unwrap();
    }

    assert!(sst_path.exists(), "final file should exist after finish");
    let reader = SstableReader::open(&sst_path, 1, 0, None).unwrap();
    assert_eq!(reader.get(&k_int(1)).unwrap(), Some(Some(v_int(42))));
}

#[test]
fn test_sstable_scan_stops_before_blocks_past_end() {
    let temp_dir = TempDir::new().unwrap();
    let sst_path = temp_dir.path().join("multi_block.sst");

    // Tiny block size forces one entry per block, so an `end` in an early
    // block leaves later blocks whose first_key exceeds `end` — exercising the
    // early `break` in the scan loop.
    {
        let mut writer =
            SstableWriter::create(&sst_path, 1, CompressionType::Uncompressed, 5, vec![]).unwrap();
        for i in 1..=5 {
            writer.append(&k_int(i * 10), Some(&v_int(i * 10))).unwrap();
        }
        writer.finish().unwrap();
    }

    let reader = SstableReader::open(&sst_path, 1, 0, None).unwrap();
    let res = reader.scan(&k_int(10), &k_int(25), |_, _| true).unwrap();
    assert_eq!(res, vec![(k_int(10), v_int(10)), (k_int(20), v_int(20))]);

    let raw = reader.scan_raw(&k_int(10), &k_int(25)).unwrap();
    assert_eq!(
        raw,
        vec![(k_int(10), Some(v_int(10))), (k_int(20), Some(v_int(20)))]
    );
}
