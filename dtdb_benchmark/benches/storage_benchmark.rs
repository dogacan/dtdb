//! Storage-engine micro-benchmarks for `dtdb_storage`.
//!
//! These measure the cost of the core key-value operations — `put`,
//! `write_batch`, `delete`, `get`, `multi_get`, and range/full scans — directly
//! against `StorageEngine`. They complement the SQL-level DML benchmarks in
//! `dml_benchmark.rs` (which exercise the whole parser -> relational -> storage
//! stack) and the cache/compression tuning matrix in
//! `dtdb_storage/benches/storage_benchmark.rs` (which sweeps cache sizes for
//! reads only). The focus here is per-operation throughput at a single,
//! representative engine configuration.

use criterion::{BatchSize, Criterion, Throughput, black_box, criterion_group, criterion_main};
use dtdb_storage::{CompressionType, DbKey, DbValue, EngineOptions, StorageEngine, WalEntry};
use tempfile::TempDir;

/// Size of each stored value, in bytes — large enough that the dataset spans
/// many SSTable blocks once flushed.
const VALUE_SIZE: usize = 200;

/// Number of rows in the pre-populated dataset shared by the read/scan benches.
const DATASET_KEYS: usize = 10_000;

/// Number of operations performed per measured iteration of the write benches.
const WRITE_OPS: usize = 1_000;

fn make_key(i: usize) -> DbKey {
    // Zero-padded so lexicographic string order matches numeric order, which
    // keeps range bounds intuitive.
    DbKey::String(format!("key_{i:08}"))
}

fn make_value(i: usize) -> DbValue {
    DbValue::Bytes(vec![(i % 256) as u8; VALUE_SIZE])
}

/// Engine options tuned for benchmarking.
///
/// The key choice is `wal_sync_interval_ms = Some(1000)`: it lets a background
/// thread fsync the WAL about once a second instead of fsyncing on every write.
/// The write benches therefore measure the engine's own write path (WAL
/// serialize + buffered append + sorted-memtable insert) rather than the host's
/// fsync latency, which is hardware-bound and would otherwise dominate the
/// numbers and add large run-to-run variance. To benchmark *durable* (synced)
/// write throughput instead, set this to `None`.
///
/// The memtable limit is generous so a single measured batch of `WRITE_OPS`
/// writes never triggers a mid-iteration flush, and the block cache is large
/// enough to hold the whole read dataset so reads measure the lookup +
/// deserialize path without disk-read noise.
fn bench_options() -> EngineOptions {
    EngineOptions {
        compression: CompressionType::Lz4,
        memtable_size_limit: 16 * 1024 * 1024,
        block_size_limit: 4 * 1024,
        wal_size_limit: 128 * 1024 * 1024,
        l0_compaction_threshold: 4,
        sstable_target_size: 4 * 1024 * 1024,
        base_level_size_limit: 16 * 1024 * 1024,
        level_size_multiplier: 10,
        max_level: 7,
        block_cache_capacity: 2_000,
        wal_sync_interval_ms: Some(1_000),
    }
}

/// A deterministic shuffle of `0..n` (Fisher–Yates driven by an LCG), so the
/// "random" write order is reproducible across runs without pulling in `rand`.
fn shuffled_indices(n: usize) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..n).collect();
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    for i in (1..n).rev() {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        // Use the high bits of the LCG state, which mix better than the low ones.
        let j = (state >> 33) as usize % (i + 1);
        idx.swap(i, j);
    }
    idx
}

/// Opens a fresh engine in a throwaway temp dir. The `TempDir` is returned so
/// the caller keeps it alive for as long as the engine is used.
fn fresh_engine() -> (TempDir, StorageEngine) {
    let temp_dir = TempDir::new().unwrap();
    let engine = StorageEngine::open(temp_dir.path(), bench_options()).unwrap();
    (temp_dir, engine)
}

/// Opens an engine pre-loaded with `DATASET_KEYS` rows, then flushes and
/// compacts so the data lives in on-disk SSTables — the realistic steady state
/// for read/scan benchmarks rather than an entirely in-memory memtable.
fn populated_engine() -> (TempDir, StorageEngine) {
    let (temp_dir, engine) = fresh_engine();
    for i in 0..DATASET_KEYS {
        engine.put(make_key(i), make_value(i)).unwrap();
    }
    engine.flush_memtable().unwrap();
    engine.compact().unwrap();
    (temp_dir, engine)
}

/// `put` / `write_batch` throughput. Each iteration runs against a fresh engine
/// (built in the unmeasured `iter_batched` setup) so the dataset never grows
/// across iterations and timings stay comparable.
fn bench_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage/write");
    group.throughput(Throughput::Elements(WRITE_OPS as u64));

    // Sequential keys: best case for the BTreeMap-backed memtable — every
    // insert lands at the growing right edge of the tree.
    group.bench_function("put_sequential", |b| {
        b.iter_batched(
            fresh_engine,
            |(_tmp, engine)| {
                for i in 0..WRITE_OPS {
                    engine.put(make_key(i), make_value(i)).unwrap();
                }
                black_box(&engine);
            },
            BatchSize::SmallInput,
        );
    });

    // Random key order: inserts land all over the memtable's key range,
    // defeating the sequential fast path above.
    let random_keys = shuffled_indices(WRITE_OPS);
    group.bench_function("put_random", |b| {
        b.iter_batched(
            fresh_engine,
            |(_tmp, engine)| {
                for &i in &random_keys {
                    engine.put(make_key(i), make_value(i)).unwrap();
                }
                black_box(&engine);
            },
            BatchSize::SmallInput,
        );
    });

    // One atomic batch vs. `WRITE_OPS` individual puts: isolates the per-call
    // WAL-append and lock overhead that `write_batch` amortizes. The entries
    // are built in the (unmeasured) setup so only the apply is timed.
    group.bench_function("write_batch", |b| {
        b.iter_batched(
            || {
                let (tmp, engine) = fresh_engine();
                let entries: Vec<WalEntry> = (0..WRITE_OPS)
                    .map(|i| WalEntry::Put {
                        key: make_key(i),
                        value: make_value(i),
                    })
                    .collect();
                (tmp, engine, entries)
            },
            |(_tmp, engine, entries)| {
                engine.write_batch(entries).unwrap();
                black_box(&engine);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// `delete` throughput. Setup populates a fresh engine with `WRITE_OPS` keys
/// (so the deletes target real rows); the measured routine appends a tombstone
/// for each. No compaction in setup — a delete writes a tombstone to the
/// memtable regardless of where the live value resides, so its cost is
/// independent of compaction state.
fn bench_deletes(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage/delete");
    group.throughput(Throughput::Elements(WRITE_OPS as u64));

    group.bench_function("delete_existing", |b| {
        b.iter_batched(
            || {
                let (tmp, engine) = fresh_engine();
                for i in 0..WRITE_OPS {
                    engine.put(make_key(i), make_value(i)).unwrap();
                }
                (tmp, engine)
            },
            |(_tmp, engine)| {
                for i in 0..WRITE_OPS {
                    engine.delete(make_key(i)).unwrap();
                }
                black_box(&engine);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Point-lookup throughput (`get` / `multi_get`) against a shared, compacted
/// dataset. Reported per key looked up.
fn bench_reads(c: &mut Criterion) {
    let (_tmp, engine) = populated_engine();

    // 100 existing keys strided across the whole keyspace, so lookups touch
    // many different SSTable blocks instead of one hot block.
    let hit_keys: Vec<DbKey> = (0..100)
        .map(|i| make_key((i * 97) % DATASET_KEYS))
        .collect();

    // 80% hits / 20% misses (every 5th key was never inserted), a realistic
    // point-read mix where the bloom filter should reject the misses cheaply.
    let mixed_keys: Vec<DbKey> = (0..100)
        .map(|i| {
            if i % 5 == 0 {
                make_key(DATASET_KEYS + i)
            } else {
                make_key((i * 97) % DATASET_KEYS)
            }
        })
        .collect();

    let mut group = c.benchmark_group("storage/read");
    group.throughput(Throughput::Elements(100));

    group.bench_function("get_point_hit", |b| {
        b.iter(|| {
            for key in &hit_keys {
                black_box(engine.get(key).unwrap());
            }
        });
    });

    group.bench_function("get_point_mixed", |b| {
        b.iter(|| {
            for key in &mixed_keys {
                black_box(engine.get(key).unwrap());
            }
        });
    });

    // The same 100 keys fetched in a single call.
    group.bench_function("multi_get", |b| {
        b.iter(|| {
            black_box(engine.multi_get(&hit_keys).unwrap());
        });
    });

    group.finish();
}

/// Scan throughput against the shared, compacted dataset.
fn bench_scans(c: &mut Criterion) {
    let (_tmp, engine) = populated_engine();

    // A 200-key window. Range bounds are inclusive on both ends, so
    // [1000, 1199] covers exactly 200 rows.
    let range_start = make_key(1_000);
    let range_end = make_key(1_199);

    let mut group = c.benchmark_group("storage/scan");

    // Full materialization of the window (accept-all predicate).
    group.throughput(Throughput::Elements(200));
    group.bench_function("range_filtered", |b| {
        b.iter(|| {
            let rows = engine
                .filtered_scan(&range_start, &range_end, |_, _| true)
                .unwrap();
            black_box(rows);
        });
    });

    // Same window iterated, but the predicate keeps only keys ending in '0'
    // (~10%): same scan cost, far fewer pairs cloned into the result.
    group.bench_function("range_selective", |b| {
        b.iter(|| {
            let rows = engine
                .filtered_scan(&range_start, &range_end, |k, _| match k {
                    DbKey::String(s) => s.as_bytes().last() == Some(&b'0'),
                    _ => false,
                })
                .unwrap();
            black_box(rows);
        });
    });

    // Full-keyspace streaming scan through the merge iterator (memtable + every
    // SSTable level). Reported per row yielded.
    let full_start = make_key(0);
    let full_end = make_key(DATASET_KEYS - 1);
    group.throughput(Throughput::Elements(DATASET_KEYS as u64));
    group.bench_function("full_scan_iter", |b| {
        b.iter(|| {
            let mut iter = engine.scan_iter(&full_start, &full_end).unwrap();
            let mut count = 0usize;
            while let Some((k, v)) = iter.next().unwrap() {
                black_box(&k);
                black_box(&v);
                count += 1;
            }
            black_box(count);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_writes,
    bench_deletes,
    bench_reads,
    bench_scans
);
criterion_main!(benches);
