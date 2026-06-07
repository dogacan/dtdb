//! Single-threaded steady-state write target for sampling profilers.
//!
//! Mirrors the `put_random_heavy` workload (200B values, 256 KiB memtable so
//! flushes/compactions are exercised) but runs one thread in a tight loop long
//! enough for a sampler to attribute the per-put hot path. Run:
//!
//!   cargo build -p dtdb_benchmark --example profile_write --profile profiling
//!   samply record ./target/profiling/examples/profile_write

use dtdb_storage::{CompressionType, DbKey, DbValue, EngineOptions, StorageEngine};
use tempfile::TempDir;

const VALUE_SIZE: usize = 200;
const DISTINCT_KEYS: usize = 10_000;
const TOTAL_PUTS: usize = 2_000_000;

fn make_key(i: usize) -> DbKey {
    DbKey::string(format!("key_{i:08}"))
}

fn make_value(i: usize) -> DbValue {
    DbValue::bytes(vec![(i % 256) as u8; VALUE_SIZE])
}

fn shuffled_indices(n: usize) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..n).collect();
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    for i in (1..n).rev() {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let j = (state >> 33) as usize % (i + 1);
        idx.swap(i, j);
    }
    idx
}

fn heavy_options() -> EngineOptions {
    EngineOptions {
        compression: CompressionType::Lz4,
        memtable_size_limit: 256 * 1024,
        block_size_limit: 4 * 1024,
        wal_size_limit: 256 * 1024,
        l0_compaction_threshold: 4,
        sstable_target_size: 256 * 1024,
        base_level_size_limit: 512 * 1024,
        level_size_multiplier: 4,
        max_level: 7,
        block_cache_capacity: 1000,
        wal_sync_interval_ms: Some(1_000),
        ..Default::default()
    }
}

fn main() {
    let keys = shuffled_indices(DISTINCT_KEYS);
    let tmp = TempDir::new().unwrap();
    let engine = StorageEngine::open(tmp.path(), heavy_options()).unwrap();

    let start = std::time::Instant::now();
    let mut n = 0usize;
    while n < TOTAL_PUTS {
        for &i in &keys {
            engine.put(make_key(i), make_value(i)).unwrap();
            n += 1;
            if n >= TOTAL_PUTS {
                break;
            }
        }
    }
    let elapsed = start.elapsed();
    eprintln!(
        "{TOTAL_PUTS} puts in {:.2}s = {:.3} us/put",
        elapsed.as_secs_f64(),
        elapsed.as_secs_f64() * 1e6 / TOTAL_PUTS as f64,
    );
}
