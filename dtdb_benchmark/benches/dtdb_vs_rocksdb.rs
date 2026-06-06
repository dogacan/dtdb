//! Comparative benchmarks: dtdb_storage vs. RocksDB.
//!
//! This suite runs the *same* logical key-value workload against both the
//! `dtdb_storage` StorageEngine and `rocksdb::DB` to track their relative
//! performance at the KV layer.
//!
//! Run with:
//!   cargo bench -p dtdb_benchmark --bench dtdb_vs_rocksdb --features compare-rocksdb

fn main() {
    #[cfg(feature = "compare-rocksdb")]
    imp::run();
    #[cfg(not(feature = "compare-rocksdb"))]
    println!(
        "dtdb_vs_rocksdb is gated behind the `compare-rocksdb` feature.\n\
         Run: cargo bench -p dtdb_benchmark --bench dtdb_vs_rocksdb --features compare-rocksdb"
    );
}

#[cfg(feature = "compare-rocksdb")]
mod imp {
    use criterion::{BatchSize, Criterion, Throughput, black_box};
    use dtdb_storage::{CompressionType, DbKey, DbValue, EngineOptions, StorageEngine, WalEntry};
    use tempfile::TempDir;

    const VALUE_SIZE: usize = 200;
    const DATASET_KEYS: usize = 10_000;
    const WRITE_OPS: usize = 1_000;

    // Heavy benchmark parameters to trigger flushes/compactions
    const HEAVY_WRITE_OPS: usize = 10_000; // Total 2MB of payload data

    fn make_key(i: usize) -> DbKey {
        DbKey::string(format!("key_{i:08}"))
    }

    fn make_value(i: usize) -> DbValue {
        DbValue::bytes(vec![(i % 256) as u8; VALUE_SIZE])
    }

    fn serialize_key(key: &DbKey) -> Vec<u8> {
        postcard::to_allocvec(key).unwrap()
    }

    fn serialize_value(val: &DbValue) -> Vec<u8> {
        postcard::to_allocvec(val).unwrap()
    }

    fn deserialize_value(bytes: &[u8]) -> DbValue {
        postcard::from_bytes(bytes).unwrap()
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
            ..Default::default()
        }
    }

    fn rocksdb_options() -> (rocksdb::Options, rocksdb::Cache) {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_write_buffer_size(16 * 1024 * 1024);
        opts.set_level_zero_file_num_compaction_trigger(4);
        opts.set_target_file_size_base(4 * 1024 * 1024);
        opts.set_max_bytes_for_level_base(16 * 1024 * 1024);
        opts.set_max_write_buffer_number(2);
        opts.set_level_compaction_dynamic_level_bytes(false);

        let mut block_opts = rocksdb::BlockBasedOptions::default();
        block_opts.set_block_size(4 * 1024);
        let cache = rocksdb::Cache::new_lru_cache(8 * 1024 * 1024);
        block_opts.set_block_cache(&cache);
        opts.set_block_based_table_factory(&block_opts);

        (opts, cache)
    }

    fn heavy_options() -> EngineOptions {
        EngineOptions {
            compression: CompressionType::Lz4,
            memtable_size_limit: 256 * 1024, // 256 KB memtable
            block_size_limit: 4 * 1024,
            wal_size_limit: 256 * 1024, // 256 KB WAL
            l0_compaction_threshold: 4,
            sstable_target_size: 256 * 1024,   // 256 KB SSTable
            base_level_size_limit: 512 * 1024, // 512 KB L1 size limit
            level_size_multiplier: 4,
            max_level: 7,
            block_cache_capacity: 1000,
            wal_sync_interval_ms: Some(1_000), // background WAL sync
            ..Default::default()
        }
    }

    fn rocksdb_heavy_options() -> (rocksdb::Options, rocksdb::Cache) {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_write_buffer_size(256 * 1024); // 256 KB memtable
        opts.set_level_zero_file_num_compaction_trigger(4);
        opts.set_target_file_size_base(256 * 1024); // 256 KB SSTable
        opts.set_max_bytes_for_level_base(512 * 1024); // 512 KB L1 limit
        opts.set_max_bytes_for_level_multiplier(4.0);
        opts.set_max_write_buffer_number(2);
        opts.set_level_compaction_dynamic_level_bytes(false);

        let mut block_opts = rocksdb::BlockBasedOptions::default();
        block_opts.set_block_size(4 * 1024);
        let cache = rocksdb::Cache::new_lru_cache(4 * 1024 * 1024); // 4MB cache
        block_opts.set_block_cache(&cache);
        opts.set_block_based_table_factory(&block_opts);

        (opts, cache)
    }

    fn dtdb_populated() -> (TempDir, StorageEngine) {
        let tmp = TempDir::new().unwrap();
        let engine = StorageEngine::open(tmp.path(), bench_options()).unwrap();
        for i in 0..DATASET_KEYS {
            engine.put(make_key(i), make_value(i)).unwrap();
        }
        engine.flush_memtable().unwrap();
        engine.compact().unwrap();
        (tmp, engine)
    }

    struct RocksDbPopulated {
        _tmp: TempDir,
        db: rocksdb::DB,
        _opts: rocksdb::Options,
        _cache: rocksdb::Cache,
    }

    fn rocksdb_populated() -> RocksDbPopulated {
        let tmp = TempDir::new().unwrap();
        let (opts, cache) = rocksdb_options();
        let db = rocksdb::DB::open(&opts, tmp.path()).unwrap();
        for i in 0..DATASET_KEYS {
            let k = serialize_key(&make_key(i));
            let v = serialize_value(&make_value(i));
            db.put(k, v).unwrap();
        }
        db.flush().unwrap();
        db.compact_range(None::<&[u8]>, None::<&[u8]>);
        RocksDbPopulated {
            _tmp: tmp,
            db,
            _opts: opts,
            _cache: cache,
        }
    }

    fn bench_put_sequential(c: &mut Criterion) {
        let mut g = c.benchmark_group("dtdb_vs_rocksdb/put_sequential");
        g.throughput(Throughput::Elements(WRITE_OPS as u64));

        g.bench_function("dtdb_storage", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let engine = StorageEngine::open(tmp.path(), bench_options()).unwrap();
                    (tmp, engine)
                },
                |(_tmp, engine)| {
                    for i in 0..WRITE_OPS {
                        engine.put(make_key(i), make_value(i)).unwrap();
                    }
                },
                BatchSize::SmallInput,
            );
        });

        g.bench_function("rocksdb", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let (opts, cache) = rocksdb_options();
                    let db = rocksdb::DB::open(&opts, tmp.path()).unwrap();
                    (tmp, db, opts, cache)
                },
                |(_tmp, db, _opts, _cache)| {
                    for i in 0..WRITE_OPS {
                        let k = serialize_key(&make_key(i));
                        let v = serialize_value(&make_value(i));
                        db.put(k, v).unwrap();
                    }
                },
                BatchSize::SmallInput,
            );
        });
        g.finish();
    }

    fn bench_put_random(c: &mut Criterion) {
        let mut g = c.benchmark_group("dtdb_vs_rocksdb/put_random");
        g.throughput(Throughput::Elements(WRITE_OPS as u64));
        let random_keys = shuffled_indices(WRITE_OPS);

        g.bench_function("dtdb_storage", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let engine = StorageEngine::open(tmp.path(), bench_options()).unwrap();
                    (tmp, engine)
                },
                |(_tmp, engine)| {
                    for &i in &random_keys {
                        engine.put(make_key(i), make_value(i)).unwrap();
                    }
                },
                BatchSize::SmallInput,
            );
        });

        g.bench_function("rocksdb", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let (opts, cache) = rocksdb_options();
                    let db = rocksdb::DB::open(&opts, tmp.path()).unwrap();
                    (tmp, db, opts, cache)
                },
                |(_tmp, db, _opts, _cache)| {
                    for &i in &random_keys {
                        let k = serialize_key(&make_key(i));
                        let v = serialize_value(&make_value(i));
                        db.put(k, v).unwrap();
                    }
                },
                BatchSize::SmallInput,
            );
        });
        g.finish();
    }

    fn bench_put_random_heavy(c: &mut Criterion) {
        let mut g = c.benchmark_group("dtdb_vs_rocksdb/put_random_heavy");
        g.throughput(Throughput::Elements(HEAVY_WRITE_OPS as u64));
        let random_keys = shuffled_indices(HEAVY_WRITE_OPS);

        g.bench_function("dtdb_storage", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let engine = StorageEngine::open(tmp.path(), heavy_options()).unwrap();
                    (tmp, engine)
                },
                |(_tmp, engine)| {
                    for &i in &random_keys {
                        engine.put(make_key(i), make_value(i)).unwrap();
                    }
                },
                BatchSize::SmallInput,
            );
        });

        g.bench_function("rocksdb", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let (opts, cache) = rocksdb_heavy_options();
                    let db = rocksdb::DB::open(&opts, tmp.path()).unwrap();
                    (tmp, db, opts, cache)
                },
                |(_tmp, db, _opts, _cache)| {
                    for &i in &random_keys {
                        let k = serialize_key(&make_key(i));
                        let v = serialize_value(&make_value(i));
                        db.put(k, v).unwrap();
                    }
                },
                BatchSize::SmallInput,
            );
        });
        g.finish();
    }

    fn bench_put_random_heavy_concurrent(c: &mut Criterion) {
        let mut g = c.benchmark_group("dtdb_vs_rocksdb/put_random_heavy_concurrent");
        g.throughput(Throughput::Elements(HEAVY_WRITE_OPS as u64));
        let random_keys = shuffled_indices(HEAVY_WRITE_OPS);
        const THREADS: usize = 4;

        g.bench_function("dtdb_storage", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let engine = StorageEngine::open(tmp.path(), heavy_options()).unwrap();
                    (tmp, engine)
                },
                |(_tmp, engine)| {
                    std::thread::scope(|s| {
                        let chunk_size = HEAVY_WRITE_OPS / THREADS;
                        for t in 0..THREADS {
                            let engine = &engine;
                            let random_keys = &random_keys;
                            s.spawn(move || {
                                let start = t * chunk_size;
                                let end = if t == THREADS - 1 {
                                    HEAVY_WRITE_OPS
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

        g.bench_function("rocksdb", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let (opts, cache) = rocksdb_heavy_options();
                    let db = rocksdb::DB::open(&opts, tmp.path()).unwrap();
                    (tmp, db, opts, cache)
                },
                |(_tmp, db, _opts, _cache)| {
                    std::thread::scope(|s| {
                        let chunk_size = HEAVY_WRITE_OPS / THREADS;
                        for t in 0..THREADS {
                            let db = &db;
                            let random_keys = &random_keys;
                            s.spawn(move || {
                                let start = t * chunk_size;
                                let end = if t == THREADS - 1 {
                                    HEAVY_WRITE_OPS
                                } else {
                                    (t + 1) * chunk_size
                                };
                                for &idx in &random_keys[start..end] {
                                    let k = serialize_key(&make_key(idx));
                                    let v = serialize_value(&make_value(idx));
                                    db.put(k, v).unwrap();
                                }
                            });
                        }
                    });
                },
                BatchSize::SmallInput,
            );
        });
        g.finish();
    }

    fn bench_write_batch(c: &mut Criterion) {
        let mut g = c.benchmark_group("dtdb_vs_rocksdb/write_batch");
        g.throughput(Throughput::Elements(WRITE_OPS as u64));

        g.bench_function("dtdb_storage", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let engine = StorageEngine::open(tmp.path(), bench_options()).unwrap();
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
                },
                BatchSize::SmallInput,
            );
        });

        g.bench_function("rocksdb", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let (opts, cache) = rocksdb_options();
                    let db = rocksdb::DB::open(&opts, tmp.path()).unwrap();
                    // Build the typed key/value pairs untimed, matching dtdb's
                    // setup that pre-builds Vec<WalEntry>.
                    let kvs: Vec<(DbKey, DbValue)> = (0..WRITE_OPS)
                        .map(|i| (make_key(i), make_value(i)))
                        .collect();
                    (tmp, db, kvs, opts, cache)
                },
                |(_tmp, db, kvs, _opts, _cache)| {
                    // Serialize inside the timed region so the cost is counted on
                    // both sides — dtdb's write_batch serializes WalEntry internally.
                    let mut batch = rocksdb::WriteBatch::default();
                    for (k, v) in &kvs {
                        batch.put(serialize_key(k), serialize_value(v));
                    }
                    db.write(batch).unwrap();
                },
                BatchSize::SmallInput,
            );
        });
        g.finish();
    }

    fn bench_delete(c: &mut Criterion) {
        let mut g = c.benchmark_group("dtdb_vs_rocksdb/delete_existing");
        g.throughput(Throughput::Elements(WRITE_OPS as u64));

        g.bench_function("dtdb_storage", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let engine = StorageEngine::open(tmp.path(), bench_options()).unwrap();
                    for i in 0..WRITE_OPS {
                        engine.put(make_key(i), make_value(i)).unwrap();
                    }
                    (tmp, engine)
                },
                |(_tmp, engine)| {
                    for i in 0..WRITE_OPS {
                        engine.delete(make_key(i)).unwrap();
                    }
                },
                BatchSize::SmallInput,
            );
        });

        g.bench_function("rocksdb", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let (opts, cache) = rocksdb_options();
                    let db = rocksdb::DB::open(&opts, tmp.path()).unwrap();
                    for i in 0..WRITE_OPS {
                        let k = serialize_key(&make_key(i));
                        let v = serialize_value(&make_value(i));
                        db.put(k, v).unwrap();
                    }
                    (tmp, db, opts, cache)
                },
                |(_tmp, db, _opts, _cache)| {
                    for i in 0..WRITE_OPS {
                        let k = serialize_key(&make_key(i));
                        db.delete(k).unwrap();
                    }
                },
                BatchSize::SmallInput,
            );
        });
        g.finish();
    }

    fn bench_reads(c: &mut Criterion) {
        let (_dtdb_tmp, dtdb_engine) = dtdb_populated();
        let rocksdb_populated = rocksdb_populated();

        let hit_keys: Vec<DbKey> = (0..100)
            .map(|i| make_key((i * 97) % DATASET_KEYS))
            .collect();

        let mixed_keys: Vec<DbKey> = (0..100)
            .map(|i| {
                if i % 5 == 0 {
                    make_key(DATASET_KEYS + i)
                } else {
                    make_key((i * 97) % DATASET_KEYS)
                }
            })
            .collect();

        let rocksdb_hit_keys: Vec<Vec<u8>> = hit_keys.iter().map(serialize_key).collect();
        let rocksdb_mixed_keys: Vec<Vec<u8>> = mixed_keys.iter().map(serialize_key).collect();

        let mut g = c.benchmark_group("dtdb_vs_rocksdb/get_point_hit");
        g.throughput(Throughput::Elements(100));

        g.bench_function("dtdb_storage", |b| {
            b.iter(|| {
                for key in &hit_keys {
                    black_box(dtdb_engine.get(key).unwrap());
                }
            });
        });

        g.bench_function("rocksdb", |b| {
            b.iter(|| {
                for key in &rocksdb_hit_keys {
                    let val_bytes = rocksdb_populated.db.get(key).unwrap();
                    if let Some(bytes) = val_bytes {
                        black_box(deserialize_value(&bytes));
                    }
                }
            });
        });

        g.finish();

        let mut g = c.benchmark_group("dtdb_vs_rocksdb/get_point_mixed");
        g.throughput(Throughput::Elements(100));

        g.bench_function("dtdb_storage", |b| {
            b.iter(|| {
                for key in &mixed_keys {
                    black_box(dtdb_engine.get(key).unwrap());
                }
            });
        });

        g.bench_function("rocksdb", |b| {
            b.iter(|| {
                for key in &rocksdb_mixed_keys {
                    let val_bytes = rocksdb_populated.db.get(key).unwrap();
                    if let Some(bytes) = val_bytes {
                        black_box(deserialize_value(&bytes));
                    }
                }
            });
        });

        g.finish();
    }

    fn bench_scans(c: &mut Criterion) {
        let (_dtdb_tmp, dtdb_engine) = dtdb_populated();
        let rocksdb_populated = rocksdb_populated();

        let mut g = c.benchmark_group("dtdb_vs_rocksdb/scan_full");
        g.throughput(Throughput::Elements(DATASET_KEYS as u64));

        let full_start = make_key(0);
        let full_end = make_key(DATASET_KEYS - 1);

        g.bench_function("dtdb_storage", |b| {
            b.iter(|| {
                let mut iter = dtdb_engine.scan_iter(&full_start, &full_end).unwrap();
                let mut count = 0usize;
                while let Some((k, v)) = iter.next().unwrap() {
                    black_box(k);
                    black_box(v);
                    count += 1;
                }
                assert_eq!(count, DATASET_KEYS);
            });
        });

        let start_bytes = serialize_key(&full_start);
        let end_bytes = serialize_key(&full_end);

        g.bench_function("rocksdb", |b| {
            b.iter(|| {
                let iter = rocksdb_populated.db.iterator(rocksdb::IteratorMode::From(
                    &start_bytes,
                    rocksdb::Direction::Forward,
                ));
                let mut count = 0usize;
                for item in iter {
                    let (k, v) = item.unwrap();
                    if k.as_ref() > &end_bytes {
                        break;
                    }
                    let key: DbKey = postcard::from_bytes(&k).unwrap();
                    let val: DbValue = postcard::from_bytes(&v).unwrap();
                    black_box(key);
                    black_box(val);
                    count += 1;
                }
                assert_eq!(count, DATASET_KEYS);
            });
        });

        g.finish();
    }

    fn bench_mixed_write_heavy(c: &mut Criterion) {
        let mut g = c.benchmark_group("dtdb_vs_rocksdb/mixed_write_heavy");
        g.throughput(Throughput::Elements(HEAVY_WRITE_OPS as u64));
        let random_keys = shuffled_indices(HEAVY_WRITE_OPS);
        const WRITERS: usize = 4;

        g.bench_function("dtdb_storage", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let engine = StorageEngine::open(tmp.path(), heavy_options()).unwrap();
                    (tmp, engine)
                },
                |(_tmp, engine)| {
                    std::thread::scope(|s| {
                        let chunk_size = HEAVY_WRITE_OPS / WRITERS;
                        // Spawn writers
                        for t in 0..WRITERS {
                            let engine = &engine;
                            let random_keys = &random_keys;
                            s.spawn(move || {
                                let start = t * chunk_size;
                                let end = if t == WRITERS - 1 {
                                    HEAVY_WRITE_OPS
                                } else {
                                    (t + 1) * chunk_size
                                };
                                for &idx in &random_keys[start..end] {
                                    engine.put(make_key(idx), make_value(idx)).unwrap();
                                }
                            });
                        }
                        // Spawn 1 reader
                        let engine = &engine;
                        let random_keys = &random_keys;
                        s.spawn(move || {
                            for i in 0..chunk_size {
                                let idx = random_keys[i % HEAVY_WRITE_OPS];
                                let _ = engine.get(&make_key(idx)).unwrap();
                            }
                        });
                    });
                },
                BatchSize::SmallInput,
            );
        });

        g.bench_function("rocksdb", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let (opts, cache) = rocksdb_heavy_options();
                    let db = rocksdb::DB::open(&opts, tmp.path()).unwrap();
                    (tmp, db, opts, cache)
                },
                |(_tmp, db, _opts, _cache)| {
                    std::thread::scope(|s| {
                        let chunk_size = HEAVY_WRITE_OPS / WRITERS;
                        // Spawn writers
                        for t in 0..WRITERS {
                            let db = &db;
                            let random_keys = &random_keys;
                            s.spawn(move || {
                                let start = t * chunk_size;
                                let end = if t == WRITERS - 1 {
                                    HEAVY_WRITE_OPS
                                } else {
                                    (t + 1) * chunk_size
                                };
                                for &idx in &random_keys[start..end] {
                                    let k = serialize_key(&make_key(idx));
                                    let v = serialize_value(&make_value(idx));
                                    db.put(k, v).unwrap();
                                }
                            });
                        }
                        // Spawn 1 reader
                        let db = &db;
                        let random_keys = &random_keys;
                        s.spawn(move || {
                            for i in 0..chunk_size {
                                let idx = random_keys[i % HEAVY_WRITE_OPS];
                                let k = serialize_key(&make_key(idx));
                                let _ = db.get(k).unwrap();
                            }
                        });
                    });
                },
                BatchSize::SmallInput,
            );
        });
        g.finish();
    }

    fn bench_mixed_read_heavy(c: &mut Criterion) {
        let mut g = c.benchmark_group("dtdb_vs_rocksdb/mixed_read_heavy");
        g.throughput(Throughput::Elements(HEAVY_WRITE_OPS as u64));
        let random_keys = shuffled_indices(HEAVY_WRITE_OPS);
        const READERS: usize = 4;

        g.bench_function("dtdb_storage", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let engine = StorageEngine::open(tmp.path(), heavy_options()).unwrap();
                    // Pre-populate so that reads actually find keys
                    for i in 0..HEAVY_WRITE_OPS {
                        engine.put(make_key(i), make_value(i)).unwrap();
                    }
                    (tmp, engine)
                },
                |(_tmp, engine)| {
                    std::thread::scope(|s| {
                        let chunk_size = HEAVY_WRITE_OPS / READERS;
                        // Spawn readers
                        for t in 0..READERS {
                            let engine = &engine;
                            let random_keys = &random_keys;
                            s.spawn(move || {
                                let start = t * chunk_size;
                                let end = if t == READERS - 1 {
                                    HEAVY_WRITE_OPS
                                } else {
                                    (t + 1) * chunk_size
                                };
                                for &idx in &random_keys[start..end] {
                                    let _ = engine.get(&make_key(idx)).unwrap();
                                }
                            });
                        }
                        // Spawn 1 writer
                        let engine = &engine;
                        let random_keys = &random_keys;
                        s.spawn(move || {
                            for i in 0..chunk_size {
                                let idx = random_keys[i % HEAVY_WRITE_OPS];
                                engine.put(make_key(idx), make_value(idx + 1000)).unwrap();
                            }
                        });
                    });
                },
                BatchSize::SmallInput,
            );
        });

        g.bench_function("rocksdb", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let (opts, cache) = rocksdb_heavy_options();
                    let db = rocksdb::DB::open(&opts, tmp.path()).unwrap();
                    // Pre-populate
                    for i in 0..HEAVY_WRITE_OPS {
                        let k = serialize_key(&make_key(i));
                        let v = serialize_value(&make_value(i));
                        db.put(k, v).unwrap();
                    }
                    (tmp, db, opts, cache)
                },
                |(_tmp, db, _opts, _cache)| {
                    std::thread::scope(|s| {
                        let chunk_size = HEAVY_WRITE_OPS / READERS;
                        // Spawn readers
                        for t in 0..READERS {
                            let db = &db;
                            let random_keys = &random_keys;
                            s.spawn(move || {
                                let start = t * chunk_size;
                                let end = if t == READERS - 1 {
                                    HEAVY_WRITE_OPS
                                } else {
                                    (t + 1) * chunk_size
                                };
                                for &idx in &random_keys[start..end] {
                                    let k = serialize_key(&make_key(idx));
                                    let _ = db.get(k).unwrap();
                                }
                            });
                        }
                        // Spawn 1 writer
                        let db = &db;
                        let random_keys = &random_keys;
                        s.spawn(move || {
                            for i in 0..chunk_size {
                                let idx = random_keys[i % HEAVY_WRITE_OPS];
                                let k = serialize_key(&make_key(idx));
                                let v = serialize_value(&make_value(idx + 1000));
                                db.put(k, v).unwrap();
                            }
                        });
                    });
                },
                BatchSize::SmallInput,
            );
        });
        g.finish();
    }

    pub fn run() {
        let mut c = Criterion::default().configure_from_args();
        bench_put_sequential(&mut c);
        bench_put_random(&mut c);
        bench_put_random_heavy(&mut c);
        bench_put_random_heavy_concurrent(&mut c);
        bench_mixed_write_heavy(&mut c);
        bench_mixed_read_heavy(&mut c);
        bench_write_batch(&mut c);
        bench_delete(&mut c);
        bench_reads(&mut c);
        bench_scans(&mut c);
        c.final_summary();
    }
}
