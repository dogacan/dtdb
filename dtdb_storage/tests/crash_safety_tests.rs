use dtdb_storage::sstable::SstableWriter;
use dtdb_storage::wal::{Wal, WalEntry};
use dtdb_storage::{CompressionType, DbKey, DbValue, EngineOptions, StorageEngine};
use std::fs::{self, OpenOptions};
use std::io::Write;
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
        let mut writer =
            SstableWriter::create(&sst_path, 100, CompressionType::Uncompressed, 1, vec![])
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
        let mut writer =
            SstableWriter::create(&sst_path, 100, CompressionType::Uncompressed, 1, vec![])
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
