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
}

criterion_group!(benches, benchmark_dml);
criterion_main!(benches);
