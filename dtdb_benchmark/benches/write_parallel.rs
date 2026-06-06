use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use dtdb_storage::{CompressionType, DbKey, DbValue, EngineOptions, StorageEngine};
use tempfile::TempDir;

const VALUE_SIZE: usize = 200;
const TOTAL_OPS: usize = 10_000;

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

fn bench_options() -> EngineOptions {
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

fn bench_parallel_puts(c: &mut Criterion) {
    let mut g = c.benchmark_group("write_parallel");
    g.throughput(Throughput::Elements(TOTAL_OPS as u64));
    let random_keys = shuffled_indices(TOTAL_OPS);

    for threads in [1, 2, 4, 8] {
        g.bench_function(format!("threads_{threads}"), |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let engine = StorageEngine::open(tmp.path(), bench_options()).unwrap();
                    (tmp, engine)
                },
                |(_tmp, engine)| {
                    std::thread::scope(|s| {
                        let chunk_size = TOTAL_OPS / threads;
                        for t in 0..threads {
                            let engine = &engine;
                            let random_keys = &random_keys;
                            s.spawn(move || {
                                let start = t * chunk_size;
                                let end = if t == threads - 1 {
                                    TOTAL_OPS
                                } else {
                                    (t + 1) * chunk_size
                                };
                                for &idx in &random_keys[start..end] {
                                    engine.put(make_key(idx), make_value(idx)).unwrap();
                                }
                            });
                        }
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }
    g.finish();
}

criterion_group!(benches, bench_parallel_puts);
criterion_main!(benches);
