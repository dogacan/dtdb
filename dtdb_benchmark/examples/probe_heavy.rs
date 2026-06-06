//! Probe for the `put_random_heavy` regression vs RocksDB.
//!
//! Decomposes where dtdb_storage spends wall-clock time during the heavy
//! random-write workload (10k * 200B values, tiny 256KB memtable/SSTable so
//! flushes + compactions are forced). Run:
//!
//!   cargo run -p dtdb_benchmark --example probe_heavy --release
//!
//! It runs the same workload under three configurations to attribute cost,
//! comparing 1 thread vs 4 threads to isolate lock contention.

use std::time::Instant;

use dtdb_storage::{CompressionType, DbKey, DbValue, EngineOptions, StorageEngine};
use tempfile::TempDir;

const VALUE_SIZE: usize = 200;
const HEAVY_WRITE_OPS: usize = 10_000;

// A put slower than this almost certainly tripped an inline memtable flush;
// steady-state puts are sub-microsecond.
const FLUSH_STALL_THRESHOLD_NS: u64 = 50_000; // 50 µs

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

struct Run {
    label: &'static str,
    threads: usize,
    total_ms: f64,
    avg_put_us: f64,
    /// Per-put timing breakdown (only meaningful when flushes happen).
    flush_count: usize,
    flush_total_ms: f64,
    flush_max_ms: f64,
    puts_total_ms: f64,
    /// Live on-disk state after the run.
    num_sstables: u64,
}

fn run(label: &'static str, opts: EngineOptions, keys: &[usize], threads: usize) -> Run {
    let tmp = TempDir::new().unwrap();
    let engine = StorageEngine::open(tmp.path(), opts).unwrap();

    let start = Instant::now();
    let durations = if threads == 1 {
        let mut durs = Vec::with_capacity(keys.len());
        for &i in keys {
            let t0 = Instant::now();
            engine.put(make_key(i), make_value(i)).unwrap();
            durs.push(t0.elapsed().as_nanos() as u64);
        }
        durs
    } else {
        let keys_per_thread = keys.len() / threads;
        let mut thread_durations = vec![Vec::new(); threads];
        std::thread::scope(|s| {
            for (t, durs) in thread_durations.iter_mut().enumerate() {
                let engine = &engine;
                let keys = keys;
                s.spawn(move || {
                    let start_idx = t * keys_per_thread;
                    let end_idx = if t == threads - 1 {
                        keys.len()
                    } else {
                        (t + 1) * keys_per_thread
                    };
                    durs.reserve(end_idx - start_idx);
                    for i in start_idx..end_idx {
                        let val_idx = keys[i];
                        let t0 = Instant::now();
                        engine.put(make_key(val_idx), make_value(val_idx)).unwrap();
                        durs.push(t0.elapsed().as_nanos() as u64);
                    }
                });
            }
        });
        let mut durs = Vec::with_capacity(keys.len());
        for td in thread_durations {
            durs.extend(td);
        }
        durs
    };
    let total_ms = start.elapsed().as_secs_f64() * 1e3;

    let total_put_ns: u64 = durations.iter().sum();
    let avg_put_us = (total_put_ns as f64 / keys.len() as f64) / 1000.0;

    let mut flush_count = 0;
    let mut flush_total_ns = 0u64;
    let mut flush_max_ns = 0u64;
    let mut puts_total_ns = 0u64;
    for &d in &durations {
        if d >= FLUSH_STALL_THRESHOLD_NS {
            flush_count += 1;
            flush_total_ns += d;
            flush_max_ns = flush_max_ns.max(d);
        } else {
            puts_total_ns += d;
        }
    }

    let stats = engine.get_statistics().unwrap();

    Run {
        label,
        threads,
        total_ms,
        avg_put_us,
        flush_count,
        flush_total_ms: flush_total_ns as f64 / 1e6,
        flush_max_ms: flush_max_ns as f64 / 1e6,
        puts_total_ms: puts_total_ns as f64 / 1e6,
        num_sstables: stats.num_sstables as u64,
    }
}

fn main() {
    let keys = shuffled_indices(HEAVY_WRITE_OPS);

    // base: no flush, no compaction (2 GiB limits keep everything in memtable).
    let mut base = heavy_options();
    base.memtable_size_limit = 2 * 1024 * 1024 * 1024;
    base.wal_size_limit = 2 * 1024 * 1024 * 1024;

    // flush, but never compact: L0 threshold unreachable + huge level limits.
    let mut flush_only = heavy_options();
    flush_only.l0_compaction_threshold = usize::MAX - 10;
    flush_only.l0_slowdown_writes_trigger = usize::MAX - 5;
    flush_only.l0_stop_writes_trigger = usize::MAX;
    flush_only.base_level_size_limit = 64 * 1024 * 1024 * 1024;

    let runs = [
        run("base (no flush/compact)", base, &keys, 1),
        run("base (no flush/compact)", base, &keys, 4),
        run("flush, no compaction", flush_only, &keys, 1),
        run("flush, no compaction", flush_only, &keys, 4),
        run("full heavy (flush+compact)", heavy_options(), &keys, 1),
        run("full heavy (flush+compact)", heavy_options(), &keys, 4),
    ];

    println!(
        "\n{} ops, {}-byte values, 256 KiB memtable/SSTable\n",
        HEAVY_WRITE_OPS, VALUE_SIZE
    );
    println!(
        "{:<30} {:>8} {:>10} {:>11} {:>8} {:>11} {:>10} {:>11} {:>8}",
        "config",
        "threads",
        "total ms",
        "avg put us",
        "flushes",
        "flush ms",
        "max ms",
        "puts ms",
        "ssts"
    );
    println!("{}", "-".repeat(112));
    for r in &runs {
        println!(
            "{:<30} {:>8} {:>10.2} {:>11.2} {:>8} {:>11.2} {:>10.3} {:>11.2} {:>8}",
            r.label,
            r.threads,
            r.total_ms,
            r.avg_put_us,
            r.flush_count,
            r.flush_total_ms,
            r.flush_max_ms,
            r.puts_total_ms,
            r.num_sstables,
        );
    }
}
