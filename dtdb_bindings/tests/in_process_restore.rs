use dtdb_bindings::new_in_process_client;
use dtdb_relational::{Database, DatabaseOptions};
use dtdb_storage::CompressionType;
use std::sync::Arc;

/// Regression test: constructing the in-process client must succeed when an
/// existing database directory has `flush_interval_ms` configured. Restore
/// path spawns a periodic-flush Tokio task; if construction is not entered
/// into the Runtime context, `tokio::spawn` panics with
/// "there is no reactor running".
#[test]
fn new_in_process_client_restores_db_with_flush_interval() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data_dir = tmp.path();
    let db_dir = data_dir.join("flushy");

    {
        let options = DatabaseOptions {
            compression: CompressionType::Lz4,
            memtable_size_limit: 1024 * 1024,
            block_size_limit: 4096,
            wal_size_limit: 32 * 1024 * 1024,
            flush_interval_ms: Some(50),
            l0_compaction_threshold: None,
            sstable_target_size: None,
            base_level_size_limit: None,
            level_size_multiplier: None,
            max_level: None,
            block_cache_capacity: Some(1000),
            analyze_frequency_ms: None,
            wal_sync_interval_ms: None,
            memory_budget: None,
            fsync_method: dtdb_storage::FsyncMethod::default(),
            l0_slowdown_writes_trigger: None,
            l0_stop_writes_trigger: None,
            l0_slowdown_max_sleep_ms: None,
        };
        let _db = Arc::new(
            Database::open_with_options_and_executor(
                &db_dir,
                options,
                dtdb_storage::default_executor(),
            )
            .expect("create db"),
        );
    }

    let client = new_in_process_client(data_dir.to_str().unwrap())
        .expect("client construction should not panic when restoring a DB with flush_interval_ms");
    drop(client);
}
