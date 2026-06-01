//! Comparative DML benchmarks: dtdb vs. SQLite.
//!
//! This suite runs the *same* logical workload against both engines and reports
//! them side by side, so the relative cost of core DML operations can be
//! tracked. It is loosely modeled on SQLite's classic "Database Speed
//! Comparison" / `speedtest1` test cases, restricted to the DML core (INSERT /
//! SELECT / UPDATE / DELETE) that both engines support. Schema creation (DDL)
//! is performed identically on both but is *not* timed.
//!
//! Both engines run in-process (dtdb via [`SqlEngine`], SQLite via `rusqlite`),
//! so there is no IPC/network skew. For an apples-to-apples, power-loss-durable
//! comparison both are configured to fsync at every commit: SQLite uses WAL +
//! `synchronous=FULL`, and dtdb fsyncs one transaction-log record per commit by
//! default. Data is generated host-side and emitted as literal SQL, so every
//! statement stays within the dialect subset common to both engines.
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
        c.final_summary();
    }
}
