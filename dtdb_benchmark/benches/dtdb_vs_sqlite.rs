//! Comparative DML benchmarks: dtdb vs. SQLite.
//!
//! This suite runs the *same* logical workload against both engines and reports
//! them side by side, so the relative cost of core DML operations can be
//! tracked. It is loosely modeled on SQLite's classic "Database Speed
//! Comparison" / `speedtest1` test cases, restricted to the DML core (INSERT /
//! SELECT / UPDATE / DELETE) that both engines support, plus indexed LIKE
//! queries (trigram fulltext for infix, B-tree range for prefix). Schema
//! creation (DDL) is performed on both but is *not* timed; index DDL is
//! necessarily engine-specific (e.g. dtdb's trigram fulltext index vs. SQLite's
//! FTS5 virtual table), tuned so each side serves the LIKE from its index.
//!
//! Both engines run in-process (dtdb via [`SqlEngine`], SQLite via `rusqlite`),
//! so there is no IPC/network skew. For an apples-to-apples, power-loss-durable
//! comparison both are configured to fsync at every commit: SQLite uses WAL +
//! `synchronous=FULL`, and dtdb fsyncs one transaction-log record per commit by
//! default. Most workloads generate data host-side and emit it as literal SQL,
//! staying within the dialect subset common to both engines; the LIKE benches
//! instead load the bundled `text_data.txt` (Tiny Shakespeare) and bind it via
//! parameters. Matching LIKE semantics across engines matters there: dtdb's LIKE
//! is case-sensitive, so the SQLite side forces the same via `case_sensitive 1`
//! (FTS5) and `PRAGMA case_sensitive_like = ON` (B-tree).
//!
//! This bench is only built with `--features compare-sqlite` (which pulls in a
//! bundled SQLite, requiring a C compiler). Without it, the binary is a stub.
//!
//! Run with:
//!   cargo bench -p dtdb_benchmark --bench dtdb_vs_sqlite --features compare-sqlite

fn main() {
    #[cfg(feature = "compare-sqlite")]
    imp::run();
    #[cfg(not(feature = "compare-sqlite"))]
    println!(
        "dtdb_vs_sqlite is gated behind the `compare-sqlite` feature.\n\
         Run: cargo bench -p dtdb_benchmark --bench dtdb_vs_sqlite --features compare-sqlite"
    );
}

#[cfg(feature = "compare-sqlite")]
#[allow(clippy::result_large_err)]
mod imp {
    use criterion::{BatchSize, Criterion, Throughput};
    use dtdb_api::in_process::InProcessClient;
    use tempfile::TempDir;

    /// Rows inserted per iteration of the INSERT benches.
    const WRITE_ROWS: usize = 1_000;
    /// Rows pre-loaded for the UPDATE/DELETE benches (fresh per iteration).
    const MUTATE_ROWS: usize = 1_000;
    /// Rows pre-loaded once for the read benches.
    const READ_ROWS: usize = 5_000;
    /// Number of point operations (point SELECT / point UPDATE).
    const POINTS: usize = 200;

    // ----- shared workload (identical SQL for both engines) -------------------

    /// Point lookup / update keys, strided across the dataset of size `n`.
    fn point_ids(n: usize) -> Vec<usize> {
        (0..POINTS).map(|j| (j * 97) % n).collect()
    }

    fn populate_dtdb(client: &InProcessClient, n: usize) {
        use dtdb_api::sql_query;
        client
            .run_in_transaction("bench", |tx| {
                for i in 0..n {
                    let results = tx.execute_query(
                        sql_query!("INSERT INTO bench (id, k, v) VALUES (@id, @k, @v)")
                            .bind("id", i as i64)
                            .bind("k", ((i * 31) % 100_000) as i64)
                            .bind("v", format!("val_{i:08}")),
                    )?;
                    for r in results {
                        r?;
                    }
                }
                Ok(())
            })
            .unwrap();
    }

    fn populate_sqlite(conn: &mut rusqlite::Connection, n: usize) {
        let txn = conn.transaction().unwrap();
        {
            let mut stmt = txn
                .prepare("INSERT INTO bench (id, k, v) VALUES (?1, ?2, ?3)")
                .unwrap();
            for i in 0..n {
                stmt.execute(rusqlite::params![
                    i as i64,
                    ((i * 31) % 100_000) as i64,
                    format!("val_{i:08}")
                ])
                .unwrap();
            }
        }
        txn.commit().unwrap();
    }

    // ----- the benches --------------------------------------------------------

    /// 1000 INSERTs, each in its own auto-committed transaction (one fsync each).
    fn bench_insert_autocommit(c: &mut Criterion) {
        use dtdb_api::sql_query;
        use dtdb_storage::CompressionType;

        let mut g = c.benchmark_group("dml/insert_autocommit");
        g.throughput(Throughput::Elements(WRITE_ROWS as u64));

        g.bench_function("dtdb_client", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let client = InProcessClient::open(tmp.path()).unwrap();
                    client.create_db("bench", CompressionType::Lz4).unwrap();
                    let results = client
                        .execute_query(
                            "bench",
                            sql_query!("CREATE TABLE bench (id INT PRIMARY KEY, k INT, v STRING)"),
                        )
                        .unwrap();
                    for r in results {
                        r.unwrap();
                    }
                    (tmp, client)
                },
                |(tmp, client)| {
                    for i in 0..WRITE_ROWS {
                        let results = client
                            .execute_query(
                                "bench",
                                sql_query!("INSERT INTO bench (id, k, v) VALUES (@id, @k, @v)")
                                    .bind("id", i as i64)
                                    .bind("k", ((i * 31) % 100_000) as i64)
                                    .bind("v", format!("val_{i:08}")),
                            )
                            .unwrap();
                        for r in results {
                            r.unwrap();
                        }
                    }
                    drop(tmp);
                },
                BatchSize::SmallInput,
            );
        });

        g.bench_function("sqlite_prepared", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let conn = rusqlite::Connection::open(tmp.path().join("bench.sqlite")).unwrap();
                    conn.execute_batch(
                        "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; \
                         CREATE TABLE bench (id INTEGER PRIMARY KEY, k INTEGER, v TEXT);",
                    )
                    .unwrap();
                    (tmp, conn)
                },
                |(tmp, conn)| {
                    let mut stmt = conn
                        .prepare("INSERT INTO bench (id, k, v) VALUES (?1, ?2, ?3)")
                        .unwrap();
                    for i in 0..WRITE_ROWS {
                        stmt.execute(rusqlite::params![
                            i as i64,
                            ((i * 31) % 100_000) as i64,
                            format!("val_{i:08}")
                        ])
                        .unwrap();
                    }
                    drop(tmp);
                },
                BatchSize::SmallInput,
            );
        });

        g.finish();
    }

    /// Full-scan aggregate filtered on the unindexed `k` column.
    fn bench_select_scan(c: &mut Criterion) {
        use dtdb_api::sql_query;
        use dtdb_storage::CompressionType;

        let mut g = c.benchmark_group("dml/select_scan_aggregate");
        g.throughput(Throughput::Elements(READ_ROWS as u64));

        // Setup dtdb once
        let dtdb_tmp = TempDir::new().unwrap();
        let dtdb_client = InProcessClient::open(dtdb_tmp.path()).unwrap();
        dtdb_client
            .create_db("bench", CompressionType::Lz4)
            .unwrap();
        let results = dtdb_client
            .execute_query(
                "bench",
                sql_query!("CREATE TABLE bench (id INT PRIMARY KEY, k INT, v STRING)"),
            )
            .unwrap();
        for r in results {
            r.unwrap();
        }
        populate_dtdb(&dtdb_client, READ_ROWS);

        g.bench_function("dtdb_client", |b| {
            b.iter(|| {
                let results = dtdb_client
                    .execute_query(
                        "bench",
                        sql_query!(
                            "SELECT COUNT(*), SUM(k) FROM bench WHERE k BETWEEN @lo AND @hi"
                        )
                        .bind("lo", 0i64)
                        .bind("hi", 50000i64),
                    )
                    .unwrap();
                for r in results {
                    let _resp = r.unwrap();
                }
            });
        });

        // Setup sqlite once
        let sqlite_tmp = TempDir::new().unwrap();
        let mut sqlite_conn =
            rusqlite::Connection::open(sqlite_tmp.path().join("bench.sqlite")).unwrap();
        sqlite_conn
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; \
                 CREATE TABLE bench (id INTEGER PRIMARY KEY, k INTEGER, v TEXT);",
            )
            .unwrap();
        populate_sqlite(&mut sqlite_conn, READ_ROWS);

        g.bench_function("sqlite_prepared", |b| {
            let mut stmt = sqlite_conn
                .prepare("SELECT COUNT(*), SUM(k) FROM bench WHERE k BETWEEN ?1 AND ?2")
                .unwrap();
            b.iter(|| {
                let mut rows = stmt.query(rusqlite::params![0i64, 50000i64]).unwrap();
                while rows.next().unwrap().is_some() {}
            });
        });

        g.finish();
    }

    /// Full scan filtered on the unindexed `k` column that projects the heap-
    /// allocated `v STRING` column. Unlike `select_scan_aggregate` (which only
    /// touches the `Int` column `k`), this materializes and clones a string
    /// payload per scanned row, exercising the `Arc<str>` snapshot/clone path.
    fn bench_select_scan_text(c: &mut Criterion) {
        use dtdb_api::sql_query;
        use dtdb_storage::CompressionType;

        let mut g = c.benchmark_group("dml/select_scan_text");
        g.throughput(Throughput::Elements(READ_ROWS as u64));

        // Setup dtdb once
        let dtdb_tmp = TempDir::new().unwrap();
        let dtdb_client = InProcessClient::open(dtdb_tmp.path()).unwrap();
        dtdb_client
            .create_db("bench", CompressionType::Lz4)
            .unwrap();
        let results = dtdb_client
            .execute_query(
                "bench",
                sql_query!("CREATE TABLE bench (id INT PRIMARY KEY, k INT, v STRING)"),
            )
            .unwrap();
        for r in results {
            r.unwrap();
        }
        populate_dtdb(&dtdb_client, READ_ROWS);

        g.bench_function("dtdb_client", |b| {
            b.iter(|| {
                let results = dtdb_client
                    .execute_query(
                        "bench",
                        sql_query!("SELECT v FROM bench WHERE k BETWEEN @lo AND @hi")
                            .bind("lo", 0i64)
                            .bind("hi", 50000i64),
                    )
                    .unwrap();
                for r in results {
                    let _resp = r.unwrap();
                }
            });
        });

        // Setup sqlite once
        let sqlite_tmp = TempDir::new().unwrap();
        let mut sqlite_conn =
            rusqlite::Connection::open(sqlite_tmp.path().join("bench.sqlite")).unwrap();
        sqlite_conn
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; \
                 CREATE TABLE bench (id INTEGER PRIMARY KEY, k INTEGER, v TEXT);",
            )
            .unwrap();
        populate_sqlite(&mut sqlite_conn, READ_ROWS);

        g.bench_function("sqlite_prepared", |b| {
            let mut stmt = sqlite_conn
                .prepare("SELECT v FROM bench WHERE k BETWEEN ?1 AND ?2")
                .unwrap();
            b.iter(|| {
                let mut rows = stmt.query(rusqlite::params![0i64, 50000i64]).unwrap();
                while rows.next().unwrap().is_some() {}
            });
        });

        g.finish();
    }

    /// 200 point lookups by primary key.
    fn bench_select_point(c: &mut Criterion) {
        use dtdb_api::sql_query;
        use dtdb_storage::CompressionType;

        let mut g = c.benchmark_group("dml/select_point_pk");
        g.throughput(Throughput::Elements(POINTS as u64));

        let ids = point_ids(READ_ROWS);

        // Setup dtdb once
        let dtdb_tmp = TempDir::new().unwrap();
        let dtdb_client = InProcessClient::open(dtdb_tmp.path()).unwrap();
        dtdb_client
            .create_db("bench", CompressionType::Lz4)
            .unwrap();
        let results = dtdb_client
            .execute_query(
                "bench",
                sql_query!("CREATE TABLE bench (id INT PRIMARY KEY, k INT, v STRING)"),
            )
            .unwrap();
        for r in results {
            r.unwrap();
        }
        populate_dtdb(&dtdb_client, READ_ROWS);

        g.bench_function("dtdb_client", |b| {
            b.iter(|| {
                for &id in &ids {
                    let results = dtdb_client
                        .execute_query(
                            "bench",
                            sql_query!("SELECT k, v FROM bench WHERE id = @id")
                                .bind("id", id as i64),
                        )
                        .unwrap();
                    for r in results {
                        let _resp = r.unwrap();
                    }
                }
            });
        });

        // Setup sqlite once
        let sqlite_tmp = TempDir::new().unwrap();
        let mut sqlite_conn =
            rusqlite::Connection::open(sqlite_tmp.path().join("bench.sqlite")).unwrap();
        sqlite_conn
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; \
                 CREATE TABLE bench (id INTEGER PRIMARY KEY, k INTEGER, v TEXT);",
            )
            .unwrap();
        populate_sqlite(&mut sqlite_conn, READ_ROWS);

        g.bench_function("sqlite_prepared", |b| {
            let mut stmt = sqlite_conn
                .prepare("SELECT k, v FROM bench WHERE id = ?1")
                .unwrap();
            b.iter(|| {
                for &id in &ids {
                    let mut rows = stmt.query(rusqlite::params![id as i64]).unwrap();
                    while rows.next().unwrap().is_some() {}
                }
            });
        });

        g.finish();
    }

    /// 200 point UPDATEs by primary key, each auto-committed.
    fn bench_update_point(c: &mut Criterion) {
        use dtdb_api::sql_query;
        use dtdb_storage::CompressionType;

        let mut g = c.benchmark_group("dml/update_point_pk");
        g.throughput(Throughput::Elements(POINTS as u64));

        let ids = point_ids(MUTATE_ROWS);

        g.bench_function("dtdb_client", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let client = InProcessClient::open(tmp.path()).unwrap();
                    client.create_db("bench", CompressionType::Lz4).unwrap();
                    let results = client
                        .execute_query(
                            "bench",
                            sql_query!("CREATE TABLE bench (id INT PRIMARY KEY, k INT, v STRING)"),
                        )
                        .unwrap();
                    for r in results {
                        r.unwrap();
                    }
                    populate_dtdb(&client, MUTATE_ROWS);
                    (tmp, client)
                },
                |(tmp, client)| {
                    for &id in &ids {
                        let results = client
                            .execute_query(
                                "bench",
                                sql_query!("UPDATE bench SET k = k + 1 WHERE id = @id")
                                    .bind("id", id as i64),
                            )
                            .unwrap();
                        for r in results {
                            r.unwrap();
                        }
                    }
                    drop(tmp);
                },
                BatchSize::SmallInput,
            );
        });

        g.bench_function("sqlite_prepared", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let mut conn =
                        rusqlite::Connection::open(tmp.path().join("bench.sqlite")).unwrap();
                    conn.execute_batch(
                        "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; \
                         CREATE TABLE bench (id INTEGER PRIMARY KEY, k INTEGER, v TEXT);",
                    )
                    .unwrap();
                    populate_sqlite(&mut conn, MUTATE_ROWS);
                    (tmp, conn)
                },
                |(tmp, conn)| {
                    let mut stmt = conn
                        .prepare("UPDATE bench SET k = k + 1 WHERE id = ?1")
                        .unwrap();
                    for &id in &ids {
                        stmt.execute(rusqlite::params![id as i64]).unwrap();
                    }
                    drop(tmp);
                },
                BatchSize::SmallInput,
            );
        });

        g.finish();
    }

    /// Range DELETE over a contiguous primary-key band (~half the table).
    fn bench_delete_range(c: &mut Criterion) {
        use dtdb_api::sql_query;
        use dtdb_storage::CompressionType;

        let mut g = c.benchmark_group("dml/delete_range");
        let lo = MUTATE_ROWS / 4;
        let hi = 3 * MUTATE_ROWS / 4;
        g.throughput(Throughput::Elements((hi - lo + 1) as u64));

        g.bench_function("dtdb_client", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let client = InProcessClient::open(tmp.path()).unwrap();
                    client.create_db("bench", CompressionType::Lz4).unwrap();
                    let results = client
                        .execute_query(
                            "bench",
                            sql_query!("CREATE TABLE bench (id INT PRIMARY KEY, k INT, v STRING)"),
                        )
                        .unwrap();
                    for r in results {
                        r.unwrap();
                    }
                    populate_dtdb(&client, MUTATE_ROWS);
                    (tmp, client)
                },
                |(tmp, client)| {
                    let results = client
                        .execute_query(
                            "bench",
                            sql_query!("DELETE FROM bench WHERE id BETWEEN @lo AND @hi")
                                .bind("lo", lo as i64)
                                .bind("hi", hi as i64),
                        )
                        .unwrap();
                    for r in results {
                        r.unwrap();
                    }
                    drop(tmp);
                },
                BatchSize::SmallInput,
            );
        });

        g.bench_function("sqlite_prepared", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let mut conn =
                        rusqlite::Connection::open(tmp.path().join("bench.sqlite")).unwrap();
                    conn.execute_batch(
                        "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; \
                         CREATE TABLE bench (id INTEGER PRIMARY KEY, k INTEGER, v TEXT);",
                    )
                    .unwrap();
                    populate_sqlite(&mut conn, MUTATE_ROWS);
                    (tmp, conn)
                },
                |(tmp, conn)| {
                    let mut stmt = conn
                        .prepare("DELETE FROM bench WHERE id BETWEEN ?1 AND ?2")
                        .unwrap();
                    stmt.execute(rusqlite::params![lo as i64, hi as i64])
                        .unwrap();
                    drop(tmp);
                },
                BatchSize::SmallInput,
            );
        });

        g.finish();
    }

    /// 1000 INSERTs in one transaction, driven through the realistic *client*
    /// interface on both sides: dtdb via the in-process `InProcessClient` +
    /// `run_in_transaction` + the parameterized `sql_query!` macro (so the
    /// engine's parse cache sees one template and parses once), and SQLite via a
    /// reused prepared statement.
    fn bench_insert_txn_client(c: &mut Criterion) {
        use dtdb_api::sql_query;
        use dtdb_storage::CompressionType;

        let mut g = c.benchmark_group("dml/insert_txn_client");
        g.throughput(Throughput::Elements(WRITE_ROWS as u64));

        g.bench_function("dtdb_client", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let client = InProcessClient::open(tmp.path()).unwrap();
                    client.create_db("bench", CompressionType::Lz4).unwrap();
                    let results = client
                        .execute_query(
                            "bench",
                            sql_query!("CREATE TABLE bench (id INT PRIMARY KEY, k INT, v STRING)"),
                        )
                        .unwrap();
                    for r in results {
                        r.unwrap();
                    }
                    (tmp, client)
                },
                |(tmp, client)| {
                    client
                        .run_in_transaction("bench", |tx| {
                            for i in 0..WRITE_ROWS {
                                let results = tx.execute_query(
                                    sql_query!("INSERT INTO bench (id, k, v) VALUES (@id, @k, @v)")
                                        .bind("id", i as i64)
                                        .bind("k", ((i * 31) % 100_000) as i64)
                                        .bind("v", format!("val_{i:08}")),
                                )?;
                                for r in results {
                                    r?;
                                }
                            }
                            Ok(())
                        })
                        .unwrap();
                    drop(tmp);
                },
                BatchSize::SmallInput,
            );
        });

        g.bench_function("sqlite_prepared", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let conn = rusqlite::Connection::open(tmp.path().join("bench.sqlite")).unwrap();
                    conn.execute_batch(
                        "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; \
                         CREATE TABLE bench (id INTEGER PRIMARY KEY, k INTEGER, v TEXT);",
                    )
                    .unwrap();
                    (tmp, conn)
                },
                |(tmp, mut conn)| {
                    let txn = conn.transaction().unwrap();
                    {
                        let mut stmt = txn
                            .prepare("INSERT INTO bench (id, k, v) VALUES (?1, ?2, ?3)")
                            .unwrap();
                        for i in 0..WRITE_ROWS {
                            stmt.execute(rusqlite::params![
                                i as i64,
                                ((i * 31) % 100_000) as i64,
                                format!("val_{i:08}")
                            ])
                            .unwrap();
                        }
                    }
                    txn.commit().unwrap();
                    drop(tmp);
                },
                BatchSize::SmallInput,
            );
        });

        g.finish();
    }

    /// Concurrent transactions inserting disjoint keys.
    fn bench_concurrent_inserts(c: &mut Criterion) {
        use dtdb_api::sql_query;
        use dtdb_storage::CompressionType;

        let mut g = c.benchmark_group("dml/concurrent_inserts");
        let num_threads = 4;
        let txns_per_thread = 100;
        g.throughput(Throughput::Elements((num_threads * txns_per_thread) as u64));

        g.bench_function("dtdb_client", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let client = InProcessClient::open(tmp.path()).unwrap();
                    client.create_db("bench", CompressionType::Lz4).unwrap();
                    let results = client
                        .execute_query(
                            "bench",
                            sql_query!("CREATE TABLE bench (id INT PRIMARY KEY, k INT, v STRING)"),
                        )
                        .unwrap();
                    for r in results {
                        r.unwrap();
                    }
                    (tmp, client)
                },
                |(tmp, client)| {
                    std::thread::scope(|s| {
                        for thread_idx in 0..num_threads {
                            let client_clone = client.clone();
                            s.spawn(move || {
                                for i in 0..txns_per_thread {
                                    let key = thread_idx * txns_per_thread + i;
                                    let results = client_clone
                                        .execute_query(
                                            "bench",
                                            sql_query!(
                                                "INSERT INTO bench (id, k, v) VALUES (@id, @k, @v)"
                                            )
                                            .bind("id", key as i64)
                                            .bind("k", ((key * 31) % 100_000) as i64)
                                            .bind("v", format!("val_{key:08}")),
                                        )
                                        .unwrap();
                                    for r in results {
                                        r.unwrap();
                                    }
                                }
                            });
                        }
                    });
                    drop(tmp);
                },
                BatchSize::SmallInput,
            );
        });

        g.bench_function("sqlite_prepared", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let conn = rusqlite::Connection::open(tmp.path().join("bench.sqlite")).unwrap();
                    conn.execute_batch(
                        "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; \
                         CREATE TABLE bench (id INTEGER PRIMARY KEY, k INTEGER, v TEXT);",
                    )
                    .unwrap();
                    (tmp, conn)
                },
                |(tmp, _conn)| {
                    let db_path = tmp.path().join("bench.sqlite");
                    std::thread::scope(|s| {
                        for thread_idx in 0..num_threads {
                            let db_path = db_path.clone();
                            s.spawn(move || {
                                let conn = rusqlite::Connection::open(db_path).unwrap();
                                conn.busy_timeout(std::time::Duration::from_secs(30))
                                    .unwrap();
                                conn.execute_batch(
                                    "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;",
                                )
                                .unwrap();
                                let mut stmt = conn
                                    .prepare("INSERT INTO bench (id, k, v) VALUES (?1, ?2, ?3)")
                                    .unwrap();
                                for i in 0..txns_per_thread {
                                    let key = thread_idx * txns_per_thread + i;
                                    stmt.execute(rusqlite::params![
                                        key as i64,
                                        ((key * 31) % 100_000) as i64,
                                        format!("val_{key:08}")
                                    ])
                                    .unwrap();
                                }
                            });
                        }
                    });
                    drop(tmp);
                },
                BatchSize::SmallInput,
            );
        });

        g.finish();
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

    fn bench_infix_like(c: &mut Criterion) {
        use dtdb_api::sql_query;
        use dtdb_storage::CompressionType;

        let mut g = c.benchmark_group("dml/infix_like");

        let data = load_shakespeare_data();

        // Setup dtdb once
        let dtdb_tmp = TempDir::new().unwrap();
        let dtdb_client = InProcessClient::open(dtdb_tmp.path()).unwrap();
        dtdb_client
            .create_db("bench", CompressionType::Lz4)
            .unwrap();

        let results = dtdb_client
            .execute_query(
                "bench",
                sql_query!(
                    "CREATE TABLE bench (id INT PRIMARY KEY, line_id INT, speaker STRING, body STRING)"
                ),
            )
            .unwrap();
        for r in results {
            r.unwrap();
        }

        // Create fulltext index on dtdb
        let results = dtdb_client
            .execute_query(
                "bench",
                sql_query!("CREATE FULLTEXT INDEX shake_body_fts ON bench (body) USING trigram"),
            )
            .unwrap();
        for r in results {
            r.unwrap();
        }

        // Populate dtdb
        dtdb_client
            .run_in_transaction("bench", |tx| {
                for (i, (speaker, line_id, body)) in data.iter().enumerate() {
                    let results = tx.execute_query(
                        sql_query!(
                            "INSERT INTO bench (id, line_id, speaker, body) \
                             VALUES (@id, @line_id, @speaker, @body)"
                        )
                        .bind("id", i as i64)
                        .bind("line_id", *line_id as i64)
                        .bind("speaker", speaker.clone())
                        .bind("body", body.clone()),
                    )?;
                    for r in results {
                        r?;
                    }
                }
                Ok(())
            })
            .unwrap();

        // Flush and Analyze dtdb
        dtdb_client.flush_db("bench").unwrap();
        let results = dtdb_client
            .execute_query("bench", sql_query!("ANALYZE TABLE bench"))
            .unwrap();
        for r in results {
            r.unwrap();
        }

        g.bench_function("dtdb_trigram_fts", |b| {
            b.iter(|| {
                let results = dtdb_client
                    .execute_query(
                        "bench",
                        sql_query!("SELECT COUNT(*) FROM bench WHERE body LIKE @pattern")
                            .bind("pattern", "%speak%".to_string()),
                    )
                    .unwrap();
                for r in results {
                    let _resp = r.unwrap();
                }
            });
        });

        // Setup SQLite once (with FTS5 trigram virtual table)
        let sqlite_tmp = TempDir::new().unwrap();
        let mut sqlite_conn =
            rusqlite::Connection::open(sqlite_tmp.path().join("bench.sqlite")).unwrap();
        sqlite_conn
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; \
                 CREATE VIRTUAL TABLE bench USING \
                 fts5(line_id UNINDEXED, speaker, body, tokenize='trigram case_sensitive 1');",
            )
            .unwrap();

        // Populate SQLite
        let txn = sqlite_conn.transaction().unwrap();
        {
            let mut stmt = txn
                .prepare(
                    "INSERT INTO bench (rowid, line_id, speaker, body) VALUES (?1, ?2, ?3, ?4)",
                )
                .unwrap();
            for (i, (speaker, line_id, body)) in data.iter().enumerate() {
                stmt.execute(rusqlite::params![i as i64, line_id, speaker, body])
                    .unwrap();
            }
        }
        txn.commit().unwrap();

        g.bench_function("sqlite_fts5_trigram", |b| {
            let mut stmt = sqlite_conn
                .prepare("SELECT COUNT(*) FROM bench WHERE body LIKE ?1")
                .unwrap();
            b.iter(|| {
                let mut rows = stmt.query(rusqlite::params!["%speak%"]).unwrap();
                while rows.next().unwrap().is_some() {}
            });
        });

        g.finish();
    }

    fn bench_prefix_like(c: &mut Criterion) {
        use dtdb_api::sql_query;
        use dtdb_storage::CompressionType;

        let mut g = c.benchmark_group("dml/prefix_like");

        let data = load_shakespeare_data();

        // Setup dtdb once
        let dtdb_tmp = TempDir::new().unwrap();
        let dtdb_client = InProcessClient::open(dtdb_tmp.path()).unwrap();
        dtdb_client
            .create_db("bench", CompressionType::Lz4)
            .unwrap();

        let results = dtdb_client
            .execute_query(
                "bench",
                sql_query!(
                    "CREATE TABLE bench (id INT PRIMARY KEY, line_id INT, speaker STRING, body STRING)"
                ),
            )
            .unwrap();
        for r in results {
            r.unwrap();
        }

        // Create B-tree index on body column for dtdb
        let results = dtdb_client
            .execute_query(
                "bench",
                sql_query!("CREATE INDEX shake_body_btree ON bench (body)"),
            )
            .unwrap();
        for r in results {
            r.unwrap();
        }

        // Populate dtdb
        dtdb_client
            .run_in_transaction("bench", |tx| {
                for (i, (speaker, line_id, body)) in data.iter().enumerate() {
                    let results = tx.execute_query(
                        sql_query!(
                            "INSERT INTO bench (id, line_id, speaker, body) \
                             VALUES (@id, @line_id, @speaker, @body)"
                        )
                        .bind("id", i as i64)
                        .bind("line_id", *line_id as i64)
                        .bind("speaker", speaker.clone())
                        .bind("body", body.clone()),
                    )?;
                    for r in results {
                        r?;
                    }
                }
                Ok(())
            })
            .unwrap();

        // Flush and Analyze dtdb so the range scan runs against on-disk SSTables
        // with up-to-date statistics, matching the infix bench and SQLite's
        // committed file.
        dtdb_client.flush_db("bench").unwrap();
        let results = dtdb_client
            .execute_query("bench", sql_query!("ANALYZE TABLE bench"))
            .unwrap();
        for r in results {
            r.unwrap();
        }

        g.bench_function("dtdb_btree_range", |b| {
            b.iter(|| {
                let results = dtdb_client
                    .execute_query(
                        "bench",
                        sql_query!("SELECT COUNT(*) FROM bench WHERE body LIKE @pattern")
                            .bind("pattern", "Before%".to_string()),
                    )
                    .unwrap();
                for r in results {
                    let _resp = r.unwrap();
                }
            });
        });

        // Setup SQLite once (with PRAGMA case_sensitive_like = ON and B-tree index)
        let sqlite_tmp = TempDir::new().unwrap();
        let mut sqlite_conn =
            rusqlite::Connection::open(sqlite_tmp.path().join("bench.sqlite")).unwrap();
        sqlite_conn
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; \
                 PRAGMA case_sensitive_like = ON; \
                 CREATE TABLE bench (id INTEGER PRIMARY KEY, line_id INTEGER, speaker TEXT, body TEXT); \
                 CREATE INDEX shake_body_btree ON bench (body);",
            )
            .unwrap();

        // Populate SQLite
        let txn = sqlite_conn.transaction().unwrap();
        {
            let mut stmt = txn
                .prepare("INSERT INTO bench (id, line_id, speaker, body) VALUES (?1, ?2, ?3, ?4)")
                .unwrap();
            for (i, (speaker, line_id, body)) in data.iter().enumerate() {
                stmt.execute(rusqlite::params![i as i64, line_id, speaker, body])
                    .unwrap();
            }
        }
        txn.commit().unwrap();

        g.bench_function("sqlite_btree_range", |b| {
            let mut stmt = sqlite_conn
                .prepare("SELECT COUNT(*) FROM bench WHERE body LIKE ?1")
                .unwrap();
            b.iter(|| {
                let mut rows = stmt.query(rusqlite::params!["Before%"]).unwrap();
                while rows.next().unwrap().is_some() {}
            });
        });

        g.finish();
    }

    pub fn run() {
        let mut c = Criterion::default().configure_from_args();
        bench_insert_autocommit(&mut c);
        bench_insert_txn_client(&mut c);
        bench_select_scan(&mut c);
        bench_select_scan_text(&mut c);
        bench_select_point(&mut c);
        bench_update_point(&mut c);
        bench_delete_range(&mut c);
        bench_concurrent_inserts(&mut c);
        bench_infix_like(&mut c);
        bench_prefix_like(&mut c);
        c.final_summary();
    }
}
