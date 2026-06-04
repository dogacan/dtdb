use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use dtdb_relational::{Database, Transaction};
use dtdb_sql::SqlEngine;
use std::sync::Arc;
use tempfile::TempDir;

const N: usize = 500;

fn make_c_val(i: usize) -> String {
    format!("val_{:06}", i)
}

fn setup_unindexed_table() -> (TempDir, Arc<Database>, SqlEngine) {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    let engine = SqlEngine::new(db.clone());

    let tx = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t1 (a INT PRIMARY KEY, b INT, c STRING)", &tx)
        .unwrap();
    tx.commit().unwrap();

    (temp_dir, db, engine)
}

fn setup_indexed_table() -> (TempDir, Arc<Database>, SqlEngine) {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    let engine = SqlEngine::new(db.clone());

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t2 (a INT PRIMARY KEY, b INT, c STRING)", &tx1)
        .unwrap();
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    engine.execute("CREATE INDEX t2_b ON t2(b)", &tx2).unwrap();
    tx2.commit().unwrap();

    (temp_dir, db, engine)
}

fn populate_table(db: &Arc<Database>, engine: &SqlEngine, table_name: &str, size: usize) {
    let tx = Transaction::new(3, db.clone());
    for i in 1..=size {
        let sql = format!(
            "INSERT INTO {} (a, b, c) VALUES ({}, {}, '{}')",
            table_name,
            i,
            i,
            make_c_val(i)
        );
        engine.execute(&sql, &tx).unwrap();
    }
    tx.commit().unwrap();
}

fn setup_fulltext_table(with_index: bool) -> (TempDir, Arc<Database>, SqlEngine) {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    let engine = SqlEngine::new(db.clone());

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE fts_table (id INT PRIMARY KEY, content STRING)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();

    if with_index {
        let tx2 = Transaction::new(2, db.clone());
        engine
            .execute(
                "CREATE FULLTEXT INDEX idx_content ON fts_table(content)",
                &tx2,
            )
            .unwrap();
        tx2.commit().unwrap();
    }

    (temp_dir, db, engine)
}

fn populate_fts_table(db: &Arc<Database>, engine: &SqlEngine, size: usize) {
    let tx = Transaction::new(3, db.clone());
    let words = [
        "quick",
        "brown",
        "fox",
        "jumps",
        "over",
        "the",
        "lazy",
        "dog",
        "database",
        "engine",
        "rust",
        "relational",
        "compaction",
        "transaction",
        "isolation",
        "concurrency",
    ];
    for i in 1..=size {
        let w1 = words[i % words.len()];
        let w2 = words[(i + 3) % words.len()];
        let w3 = words[(i + 7) % words.len()];
        let sentence = format!("{} {} {}", w1, w2, w3);
        let sql = format!(
            "INSERT INTO fts_table (id, content) VALUES ({}, '{}')",
            i, sentence
        );
        engine.execute(&sql, &tx).unwrap();
    }
    tx.commit().unwrap();
}

fn benchmark_dml(c: &mut Criterion) {
    // --- INSERT BENCHMARKS ---
    let mut group = c.benchmark_group("INSERT Benchmarks");

    group.bench_function("Insert (No Index)", |b| {
        b.iter_batched(
            setup_unindexed_table,
            |(_temp_dir, db, engine)| {
                let tx = Transaction::new(4, db.clone());
                for i in 1..=N {
                    let sql = format!(
                        "INSERT INTO t1 (a, b, c) VALUES ({}, {}, '{}')",
                        i,
                        i,
                        make_c_val(i)
                    );
                    engine.execute(&sql, &tx).unwrap();
                }
                tx.commit().unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("Insert Ordered (Indexed)", |b| {
        b.iter_batched(
            setup_indexed_table,
            |(_temp_dir, db, engine)| {
                let tx = Transaction::new(4, db.clone());
                for i in 1..=N {
                    let sql = format!(
                        "INSERT INTO t2 (a, b, c) VALUES ({}, {}, '{}')",
                        i,
                        i,
                        make_c_val(i)
                    );
                    engine.execute(&sql, &tx).unwrap();
                }
                tx.commit().unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    // Generate shuffled/unordered keys to test random index insertion
    let mut shuffled_keys: Vec<usize> = (1..=N).collect();
    let mut state = 42u64;
    for i in (1..N).rev() {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let j = (state as usize) % (i + 1);
        shuffled_keys.swap(i, j);
    }

    group.bench_function("Insert Unordered (Indexed)", |b| {
        b.iter_batched(
            setup_indexed_table,
            |(_temp_dir, db, engine)| {
                let tx = Transaction::new(4, db.clone());
                for &key in &shuffled_keys {
                    let sql = format!(
                        "INSERT INTO t2 (a, b, c) VALUES ({}, {}, '{}')",
                        key,
                        key,
                        make_c_val(key)
                    );
                    engine.execute(&sql, &tx).unwrap();
                }
                tx.commit().unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();

    // --- SELECT BENCHMARKS ---
    let mut group = c.benchmark_group("SELECT Benchmarks");

    // Setup shared pre-populated databases for read-only benchmarks
    let (_temp_unindexed, db_unindexed, engine_unindexed) = setup_unindexed_table();
    populate_table(&db_unindexed, &engine_unindexed, "t1", N);

    let (_temp_indexed, db_indexed, engine_indexed) = setup_indexed_table();
    populate_table(&db_indexed, &engine_indexed, "t2", N);

    group.bench_function("Select Range (No Index)", |b| {
        let tx = Transaction::new(100, db_unindexed.clone());
        b.iter(|| {
            // Select range from unindexed table
            let sql = "SELECT COUNT(*), AVG(b), SUM(b) FROM t1 WHERE b BETWEEN 100 AND 400";
            let res = engine_unindexed.execute(sql, &tx).unwrap();
            criterion::black_box(res);
        });
    });

    group.bench_function("Select Range (Indexed)", |b| {
        let tx = Transaction::new(100, db_indexed.clone());
        b.iter(|| {
            // Select range from indexed table using PK
            let sql = "SELECT COUNT(*), AVG(a), SUM(a) FROM t2 WHERE a BETWEEN 100 AND 400";
            let res = engine_indexed.execute(sql, &tx).unwrap();
            criterion::black_box(res);
        });
    });

    group.finish();

    // --- UPDATE BENCHMARKS ---
    let mut group = c.benchmark_group("UPDATE Benchmarks");

    group.bench_function("Update Point (Indexed)", |b| {
        b.iter_batched(
            || {
                let (temp_dir, db, engine) = setup_indexed_table();
                populate_table(&db, &engine, "t2", N);
                (temp_dir, db, engine)
            },
            |(_temp_dir, db, engine)| {
                let tx = Transaction::new(5, db.clone());
                // Update 50 individual records by PK
                for i in 1..=50 {
                    let key = (i * 7) % N + 1;
                    let sql = format!("UPDATE t2 SET b = b + 1 WHERE a = {}", key);
                    engine.execute(&sql, &tx).unwrap();
                }
                tx.commit().unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("Update Batch (Indexed)", |b| {
        b.iter_batched(
            || {
                let (temp_dir, db, engine) = setup_indexed_table();
                populate_table(&db, &engine, "t2", N);
                (temp_dir, db, engine)
            },
            |(_temp_dir, db, engine)| {
                let tx = Transaction::new(5, db.clone());
                engine.execute("UPDATE t2 SET b = b * 2", &tx).unwrap();
                tx.commit().unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();

    // --- DELETE BENCHMARKS ---
    let mut group = c.benchmark_group("DELETE Benchmarks");

    group.bench_function("Delete Point (Indexed)", |b| {
        b.iter_batched(
            || {
                let (temp_dir, db, engine) = setup_indexed_table();
                populate_table(&db, &engine, "t2", N);
                (temp_dir, db, engine)
            },
            |(_temp_dir, db, engine)| {
                let tx = Transaction::new(6, db.clone());
                // Delete 50 individual records by PK
                for i in 1..=50 {
                    let key = (i * 7) % N + 1;
                    let sql = format!("DELETE FROM t2 WHERE a = {}", key);
                    engine.execute(&sql, &tx).unwrap();
                }
                tx.commit().unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("Delete Batch (Indexed)", |b| {
        b.iter_batched(
            || {
                let (temp_dir, db, engine) = setup_indexed_table();
                populate_table(&db, &engine, "t2", N);
                (temp_dir, db, engine)
            },
            |(_temp_dir, db, engine)| {
                let tx = Transaction::new(6, db.clone());
                engine
                    .execute("DELETE FROM t2 WHERE a BETWEEN 100 AND 300", &tx)
                    .unwrap();
                tx.commit().unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();

    // --- FULLTEXT SEARCH BENCHMARKS ---
    let mut group = c.benchmark_group("FULLTEXT Benchmarks");

    let (_temp_no_idx, db_no_idx, engine_no_idx) = setup_fulltext_table(false);
    populate_fts_table(&db_no_idx, &engine_no_idx, N);

    let (_temp_idx, db_idx, engine_idx) = setup_fulltext_table(true);
    populate_fts_table(&db_idx, &engine_idx, N);

    // 1. Single token query
    group.bench_function("Token Search (No Index)", |b| {
        let tx = Transaction::new(100, db_no_idx.clone());
        b.iter(|| {
            let sql = "SELECT id FROM fts_table WHERE MATCH(content) AGAINST('database')";
            let res = engine_no_idx.execute(sql, &tx).unwrap();
            criterion::black_box(res);
        });
    });

    group.bench_function("Token Search (Indexed)", |b| {
        let tx = Transaction::new(100, db_idx.clone());
        b.iter(|| {
            let sql = "SELECT id FROM fts_table WHERE MATCH(content) AGAINST('database')";
            let res = engine_idx.execute(sql, &tx).unwrap();
            criterion::black_box(res);
        });
    });

    // 2. Boolean query (AND)
    group.bench_function("Boolean Search (No Index)", |b| {
        let tx = Transaction::new(100, db_no_idx.clone());
        b.iter(|| {
            let sql = "SELECT id FROM fts_table WHERE MATCH(content) AGAINST('rust AND database')";
            let res = engine_no_idx.execute(sql, &tx).unwrap();
            criterion::black_box(res);
        });
    });

    group.bench_function("Boolean Search (Indexed)", |b| {
        let tx = Transaction::new(100, db_idx.clone());
        b.iter(|| {
            let sql = "SELECT id FROM fts_table WHERE MATCH(content) AGAINST('rust AND database')";
            let res = engine_idx.execute(sql, &tx).unwrap();
            criterion::black_box(res);
        });
    });

    // 3. Phrase query
    group.bench_function("Phrase Search (No Index)", |b| {
        let tx = Transaction::new(100, db_no_idx.clone());
        b.iter(|| {
            let sql =
                "SELECT id FROM fts_table WHERE MATCH(content) AGAINST('\"rust transaction\"')";
            let res = engine_no_idx.execute(sql, &tx).unwrap();
            criterion::black_box(res);
        });
    });

    group.bench_function("Phrase Search (Indexed)", |b| {
        let tx = Transaction::new(100, db_idx.clone());
        b.iter(|| {
            let sql =
                "SELECT id FROM fts_table WHERE MATCH(content) AGAINST('\"rust transaction\"')";
            let res = engine_idx.execute(sql, &tx).unwrap();
            criterion::black_box(res);
        });
    });

    group.finish();
}

fn load_shakespeare_data() -> Vec<(String, i32, String)> {
    let paths = [
        "text_data.txt",
        "../text_data.txt",
        "dtdb_benchmark/text_data.txt",
    ];
    let mut content = None;
    for p in &paths {
        if let Ok(c) = std::fs::read_to_string(p) {
            content = Some(c);
            break;
        }
    }
    let content = content.expect("Could not find text_data.txt in any of the search paths");
    let mut records = Vec::new();
    for line in content.lines() {
        if line.is_empty() {
            continue;
        }
        if let Some(first_comma) = line.find(',') {
            let speaker = &line[..first_comma];
            let rest = &line[first_comma + 1..];
            if let Some(second_comma) = rest.find(',') {
                let id_str = &rest[..second_comma];
                let mut text = &rest[second_comma + 1..];
                if let Ok(id) = id_str.parse::<i32>() {
                    if text.starts_with('"') && text.ends_with('"') && text.len() >= 2 {
                        text = &text[1..text.len() - 1];
                    }
                    let clean_body = text.replace(['\'', '"'], "");
                    let clean_speaker = speaker.replace(['\'', '"'], "");
                    records.push((clean_speaker, id, clean_body));
                }
            }
        }
    }
    records
}

fn setup_shakespeare_unindexed(
    data: &[(String, i32, String)],
) -> (TempDir, Arc<Database>, SqlEngine) {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    let engine = SqlEngine::new(db.clone());

    let tx1 = Transaction::new(1, db.clone());
    engine
        .execute(
            "CREATE TABLE shakespeare (id INT PRIMARY KEY, speaker STRING, body STRING)",
            &tx1,
        )
        .unwrap();
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    for (i, (speaker, _id, body)) in data.iter().enumerate() {
        let sql = format!(
            "INSERT INTO shakespeare (id, speaker, body) VALUES ({}, '{}', '{}')",
            i, speaker, body
        );
        engine.execute(&sql, &tx2).unwrap();
    }
    tx2.commit().unwrap();

    (temp_dir, db, engine)
}

fn setup_shakespeare_indexed(
    data: &[(String, i32, String)],
) -> (TempDir, Arc<Database>, SqlEngine) {
    let (temp_dir, db, engine) = setup_shakespeare_unindexed(data);

    let tx = Transaction::new(10, db.clone());
    engine
        .execute(
            "CREATE FULLTEXT INDEX shake_body_fts ON shakespeare (body) USING trigram",
            &tx,
        )
        .unwrap();
    tx.commit().unwrap();

    db.get_table("shakespeare")
        .unwrap()
        .flush_memtable()
        .unwrap();

    let tx_analyze = Transaction::new(11, db.clone());
    engine
        .execute("ANALYZE TABLE shakespeare", &tx_analyze)
        .unwrap();
    tx_analyze.commit().unwrap();

    (temp_dir, db, engine)
}

fn setup_shakespeare_btree(data: &[(String, i32, String)]) -> (TempDir, Arc<Database>, SqlEngine) {
    let (temp_dir, db, engine) = setup_shakespeare_unindexed(data);

    let tx = Transaction::new(10, db.clone());
    engine
        .execute("CREATE INDEX shake_body_btree ON shakespeare (body)", &tx)
        .unwrap();
    tx.commit().unwrap();

    (temp_dir, db, engine)
}

fn benchmark_like_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("LIKE Benchmarks");

    let data = load_shakespeare_data();

    let (_temp_unindexed, db_unindexed, engine_unindexed) = setup_shakespeare_unindexed(&data);
    let (_temp_indexed, db_indexed, engine_indexed) = setup_shakespeare_indexed(&data);
    let (_temp_btree, db_btree, engine_btree) = setup_shakespeare_btree(&data);

    // 1. Infix LIKE without index (full sequential scan)
    group.bench_function("Infix LIKE (No Index)", |b| {
        let tx = Transaction::new(100, db_unindexed.clone());
        b.iter(|| {
            let sql = "SELECT COUNT(*) FROM shakespeare WHERE body LIKE '%speak%'";
            let res = engine_unindexed.execute(sql, &tx).unwrap();
            criterion::black_box(res);
        });
    });

    // 2. Infix LIKE with trigram fulltext index + ANALYZE (accelerated)
    group.bench_function("Infix LIKE (Trigram Index + ANALYZE)", |b| {
        let tx = Transaction::new(100, db_indexed.clone());
        b.iter(|| {
            let sql = "SELECT COUNT(*) FROM shakespeare WHERE body LIKE '%speak%'";
            let res = engine_indexed.execute(sql, &tx).unwrap();
            criterion::black_box(res);
        });
    });

    // 3. Prefix LIKE without index (full sequential scan)
    group.bench_function("Prefix LIKE (No Index)", |b| {
        let tx = Transaction::new(100, db_unindexed.clone());
        b.iter(|| {
            let sql = "SELECT COUNT(*) FROM shakespeare WHERE body LIKE 'Before%'";
            let res = engine_unindexed.execute(sql, &tx).unwrap();
            criterion::black_box(res);
        });
    });

    // 4. Prefix LIKE with B-tree index (range scan)
    group.bench_function("Prefix LIKE (B-tree Index)", |b| {
        let tx = Transaction::new(100, db_btree.clone());
        b.iter(|| {
            let sql = "SELECT COUNT(*) FROM shakespeare WHERE body LIKE 'Before%'";
            let res = engine_btree.execute(sql, &tx).unwrap();
            criterion::black_box(res);
        });
    });

    group.finish();
}

criterion_group!(benches, benchmark_dml, benchmark_like_queries);
criterion_main!(benches);
