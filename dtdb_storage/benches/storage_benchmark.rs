use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use dtdb_storage::{CompressionType, DbKey, DbValue, EngineOptions, StorageEngine};
use tempfile::TempDir;

fn make_key(i: usize) -> DbKey {
    DbKey::String(format!("key_{:06}", i))
}

fn make_value(i: usize) -> DbValue {
    // ~200 bytes value
    DbValue::Bytes(vec![(i % 256) as u8; 200])
}

fn create_options(cache_capacity: usize, compression: CompressionType) -> EngineOptions {
    EngineOptions {
        compression,
        memtable_size_limit: 1024 * 1024, // 1MB memtable
        block_size_limit: 2048, // Small block size to create many blocks (~10 key-values per block)
        wal_size_limit: 32 * 1024 * 1024,
        l0_compaction_threshold: 4,
        sstable_target_size: 2 * 1024 * 1024,
        base_level_size_limit: 10 * 1024 * 1024,
        level_size_multiplier: 10,
        max_level: 7,
        block_cache_capacity: cache_capacity,
        wal_sync_interval_ms: None,
    }
}

fn benchmark_storage(c: &mut Criterion) {
    let num_keys = 2000;

    // Pre-generate point lookup keys:
    // 80% existing keys, 20% non-existing keys
    let mut lookup_keys = Vec::with_capacity(100);
    for i in 0..80 {
        let key_idx = (i * 17) % num_keys;
        lookup_keys.push(make_key(key_idx));
    }
    for i in 0..20 {
        lookup_keys.push(make_key(num_keys + i));
    }

    // Pre-generate scan ranges (scans of around 20 keys)
    let mut scan_ranges = Vec::with_capacity(5);
    for i in 0..5 {
        let start_idx = (i * 300) % (num_keys - 30);
        scan_ranges.push((make_key(start_idx), make_key(start_idx + 20)));
    }

    // --- POINT LOOKUP BENCHMARK ---
    {
        let mut group = c.benchmark_group("Point Lookups");

        // Define configurations to test: (Cache Capacity, Compression, Label)
        let configs = vec![
            (0, CompressionType::Lz4, "No Cache (LZ4)"),
            (5, CompressionType::Lz4, "Tiny Cache (LZ4)"),
            (1000, CompressionType::Lz4, "Large Cache (LZ4)"),
            (
                1000,
                CompressionType::Uncompressed,
                "Large Cache (Uncompressed)",
            ),
        ];

        for (cache_cap, comp_type, label) in configs {
            // Create a unique temporary directory for this configuration
            let temp_dir = TempDir::new().unwrap();
            let options = create_options(cache_cap, comp_type);

            // Populate the database
            let engine = StorageEngine::open(temp_dir.path(), options).unwrap();
            for i in 0..num_keys {
                engine.put(make_key(i), make_value(i)).unwrap();
            }
            engine.compact().unwrap();

            // Run the benchmark
            group.bench_with_input(BenchmarkId::from_parameter(label), &engine, |b, engine| {
                b.iter(|| {
                    for key in &lookup_keys {
                        let val = engine.get(key).unwrap();
                        black_box(val);
                    }
                });
            });
        }
        group.finish();
    }

    // --- RANGE SCAN BENCHMARK ---
    {
        let mut group = c.benchmark_group("Range Scans");

        let configs = vec![
            (0, CompressionType::Lz4, "No Cache (LZ4)"),
            (1000, CompressionType::Lz4, "Large Cache (LZ4)"),
        ];

        for (cache_cap, comp_type, label) in configs {
            let temp_dir = TempDir::new().unwrap();
            let options = create_options(cache_cap, comp_type);

            let engine = StorageEngine::open(temp_dir.path(), options).unwrap();
            for i in 0..num_keys {
                engine.put(make_key(i), make_value(i)).unwrap();
            }
            engine.compact().unwrap();

            group.bench_with_input(BenchmarkId::from_parameter(label), &engine, |b, engine| {
                b.iter(|| {
                    for range in &scan_ranges {
                        let scan_res = engine
                            .filtered_scan(&range.0, &range.1, |_, _| true)
                            .unwrap();
                        black_box(scan_res);
                    }
                });
            });
        }
        group.finish();
    }
}

criterion_group!(benches, benchmark_storage);
criterion_main!(benches);
