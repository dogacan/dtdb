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
mod imp {
    use criterion::measurement::WallTime;
    use criterion::{BatchSize, BenchmarkGroup, Criterion, Throughput, black_box};
    use dtdb_relational::{Database, Transaction};
    use dtdb_sql::{ExecutionResult, SqlEngine};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
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

    /// `INSERT` for row `i`. `k` is a non-monotonic integer (so range filters on
    /// it are not trivially served by the primary key) and `v` is a short text.
    fn insert_sql(i: usize) -> String {
        format!(
            "INSERT INTO bench (id, k, v) VALUES ({i}, {}, 'val_{i:08}')",
            (i * 31) % 100_000
        )
    }

    /// Point lookup / update keys, strided across the dataset of size `n`.
    fn point_ids(n: usize) -> Vec<usize> {
        (0..POINTS).map(|j| (j * 97) % n).collect()
    }

    // ----- engine abstraction -------------------------------------------------

    /// A SQL engine under test. All methods take `&self`; mutation happens
    /// through interior mutability so the benches can share one handle.
    trait SqlTarget {
        /// Creates the standard `bench(id, k, v)` table (DDL, untimed).
        fn create_bench_table(&self);
        /// Runs a single statement in its own (auto-committed) transaction.
        fn exec(&self, sql: &str);
        /// Runs `stmts` atomically as one transaction (one commit).
        fn exec_txn(&self, stmts: &[String]);
        /// Runs a query and returns the number of rows produced.
        fn query_rows(&self, sql: &str) -> usize;

        /// Inserts `n` rows in a single transaction (untimed setup helper).
        fn populate(&self, n: usize) {
            let stmts: Vec<String> = (0..n).map(insert_sql).collect();
            self.exec_txn(&stmts);
        }

        /// Spawns concurrent threads, each executing write transactions.
        fn run_concurrent_inserts(&self, num_threads: usize, txns_per_thread: usize);
    }

    // ----- dtdb target --------------------------------------------------------

    struct DtdbTarget {
        _tmp: TempDir,
        db: Arc<Database>,
        engine: SqlEngine,
        next_tx: AtomicU64,
    }

    impl DtdbTarget {
        fn new() -> Self {
            let tmp = TempDir::new().unwrap();
            let db = Arc::new(Database::open(tmp.path()).unwrap());
            let engine = SqlEngine::new(db.clone());
            Self {
                _tmp: tmp,
                db,
                engine,
                next_tx: AtomicU64::new(1),
            }
        }

        fn begin(&self) -> Transaction {
            Transaction::new(
                self.next_tx.fetch_add(1, Ordering::Relaxed),
                self.db.clone(),
            )
        }
    }

    impl SqlTarget for DtdbTarget {
        fn create_bench_table(&self) {
            let tx = self.begin();
            self.engine
                .execute(
                    "CREATE TABLE bench (id INT PRIMARY KEY, k INT, v STRING)",
                    &tx,
                )
                .unwrap();
            tx.commit().unwrap();
        }

        fn exec(&self, sql: &str) {
            let tx = self.begin();
            self.engine.execute(sql, &tx).unwrap();
            tx.commit().unwrap();
        }

        fn exec_txn(&self, stmts: &[String]) {
            let tx = self.begin();
            for s in stmts {
                self.engine.execute(s, &tx).unwrap();
            }
            tx.commit().unwrap();
        }

        fn query_rows(&self, sql: &str) -> usize {
            let tx = self.begin();
            let res = self.engine.execute(sql, &tx).unwrap();
            tx.commit().unwrap();
            match res {
                ExecutionResult::Select { rows, .. } => rows.len(),
                ExecutionResult::Insert { count }
                | ExecutionResult::Update { count }
                | ExecutionResult::Delete { count } => count,
                _ => 0,
            }
        }

        fn run_concurrent_inserts(&self, num_threads: usize, txns_per_thread: usize) {
            std::thread::scope(|s| {
                for thread_idx in 0..num_threads {
                    s.spawn(move || {
                        let engine = SqlEngine::new(self.db.clone());
                        for i in 0..txns_per_thread {
                            let key = thread_idx * txns_per_thread + i;
                            let sql = format!(
                                "INSERT INTO bench (id, k, v) VALUES ({key}, {}, 'val_{key:08}')",
                                (key * 31) % 100_000
                            );
                            let tx = Transaction::new(
                                self.next_tx
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                                self.db.clone(),
                            );
                            engine.execute(&sql, &tx).unwrap();
                            tx.commit().unwrap();
                        }
                    });
                }
            });
        }
    }

    // ----- SQLite target ------------------------------------------------------

    struct SqliteTarget {
        _tmp: TempDir,
        conn: rusqlite::Connection,
    }

    impl SqliteTarget {
        fn new() -> Self {
            let tmp = TempDir::new().unwrap();
            let conn = rusqlite::Connection::open(tmp.path().join("bench.sqlite")).unwrap();
            // Match dtdb's per-commit, power-loss durability.
            conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
                .unwrap();
            Self { _tmp: tmp, conn }
        }
    }

    impl SqlTarget for SqliteTarget {
        fn create_bench_table(&self) {
            self.conn
                .execute_batch("CREATE TABLE bench (id INTEGER PRIMARY KEY, k INTEGER, v TEXT);")
                .unwrap();
        }

        fn exec(&self, sql: &str) {
            self.conn.execute(sql, []).unwrap();
        }

        fn exec_txn(&self, stmts: &[String]) {
            // `unchecked_transaction` takes `&self`, letting the trait stay
            // `&self`-only; it begins/commits a real deferred transaction.
            let tx = self.conn.unchecked_transaction().unwrap();
            for s in stmts {
                tx.execute(s, []).unwrap();
            }
            tx.commit().unwrap();
        }

        fn query_rows(&self, sql: &str) -> usize {
            let mut stmt = self.conn.prepare(sql).unwrap();
            let mut rows = stmt.query([]).unwrap();
            let mut n = 0;
            while rows.next().unwrap().is_some() {
                n += 1;
            }
            n
        }

        fn run_concurrent_inserts(&self, num_threads: usize, txns_per_thread: usize) {
            let db_path = self._tmp.path().join("bench.sqlite");
            std::thread::scope(|s| {
                for thread_idx in 0..num_threads {
                    let db_path = db_path.clone();
                    s.spawn(move || {
                        let conn = rusqlite::Connection::open(db_path).unwrap();
                        conn.busy_timeout(std::time::Duration::from_secs(30))
                            .unwrap();
                        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
                            .unwrap();
                        for i in 0..txns_per_thread {
                            let key = thread_idx * txns_per_thread + i;
                            let sql = format!(
                                "INSERT INTO bench (id, k, v) VALUES ({key}, {}, 'val_{key:08}')",
                                (key * 31) % 100_000
                            );
                            conn.execute(&sql, []).unwrap();
                        }
                    });
                }
            });
        }
    }

    // ----- bench helpers ------------------------------------------------------

    fn make_dtdb() -> Box<dyn SqlTarget> {
        Box::new(DtdbTarget::new())
    }
    fn make_sqlite() -> Box<dyn SqlTarget> {
        Box::new(SqliteTarget::new())
    }
    type TargetFactory = fn() -> Box<dyn SqlTarget>;
    const TARGETS: &[(&str, TargetFactory)] = &[("dtdb", make_dtdb), ("sqlite", make_sqlite)];

    /// A write bench: fresh engine per iteration (built + prepared in the
    /// untimed setup), then `routine` is timed against it.
    fn write_bench(
        group: &mut BenchmarkGroup<WallTime>,
        prepare: impl Fn(&dyn SqlTarget),
        routine: impl Fn(&dyn SqlTarget),
    ) {
        for (name, make) in TARGETS {
            group.bench_function(*name, |b| {
                b.iter_batched(
                    || {
                        let t = make();
                        t.create_bench_table();
                        prepare(t.as_ref());
                        t
                    },
                    |t| routine(t.as_ref()),
                    BatchSize::SmallInput,
                );
            });
        }
    }

    /// A read bench: each engine is built and populated once, then `routine` is
    /// timed repeatedly against the shared handle.
    fn read_bench(group: &mut BenchmarkGroup<WallTime>, routine: impl Fn(&dyn SqlTarget)) {
        for (name, make) in TARGETS {
            let target = make();
            target.create_bench_table();
            target.populate(READ_ROWS);
            group.bench_function(*name, |b| b.iter(|| routine(target.as_ref())));
        }
    }

    // ----- the benches --------------------------------------------------------

    /// 1000 INSERTs, each in its own auto-committed transaction (one fsync each).
    fn bench_insert_autocommit(c: &mut Criterion) {
        let mut g = c.benchmark_group("dml/insert_autocommit");
        g.throughput(Throughput::Elements(WRITE_ROWS as u64));
        write_bench(
            &mut g,
            |_t| {},
            |t| {
                for i in 0..WRITE_ROWS {
                    t.exec(&insert_sql(i));
                }
            },
        );
        g.finish();
    }

    /// 1000 INSERTs batched into a single transaction (one commit total).
    fn bench_insert_txn(c: &mut Criterion) {
        let mut g = c.benchmark_group("dml/insert_txn");
        g.throughput(Throughput::Elements(WRITE_ROWS as u64));
        let inserts: Vec<String> = (0..WRITE_ROWS).map(insert_sql).collect();
        write_bench(&mut g, |_t| {}, |t| t.exec_txn(&inserts));
        g.finish();
    }

    /// Full-scan aggregate filtered on the unindexed `k` column.
    fn bench_select_scan(c: &mut Criterion) {
        let mut g = c.benchmark_group("dml/select_scan_aggregate");
        g.throughput(Throughput::Elements(READ_ROWS as u64));
        let sql = "SELECT COUNT(*), SUM(k) FROM bench WHERE k BETWEEN 0 AND 50000";
        read_bench(&mut g, |t| {
            black_box(t.query_rows(sql));
        });
        g.finish();
    }

    /// 200 point lookups by primary key.
    fn bench_select_point(c: &mut Criterion) {
        let mut g = c.benchmark_group("dml/select_point_pk");
        g.throughput(Throughput::Elements(POINTS as u64));
        let ids = point_ids(READ_ROWS);
        read_bench(&mut g, |t| {
            for &id in &ids {
                black_box(t.query_rows(&format!("SELECT k, v FROM bench WHERE id = {id}")));
            }
        });
        g.finish();
    }

    /// 200 point UPDATEs by primary key, each auto-committed.
    fn bench_update_point(c: &mut Criterion) {
        let mut g = c.benchmark_group("dml/update_point_pk");
        g.throughput(Throughput::Elements(POINTS as u64));
        let ids = point_ids(MUTATE_ROWS);
        write_bench(
            &mut g,
            |t| t.populate(MUTATE_ROWS),
            |t| {
                for &id in &ids {
                    t.exec(&format!("UPDATE bench SET k = k + 1 WHERE id = {id}"));
                }
            },
        );
        g.finish();
    }

    /// Range DELETE over a contiguous primary-key band (~half the table).
    fn bench_delete_range(c: &mut Criterion) {
        let mut g = c.benchmark_group("dml/delete_range");
        let lo = MUTATE_ROWS / 4;
        let hi = 3 * MUTATE_ROWS / 4;
        g.throughput(Throughput::Elements((hi - lo + 1) as u64));
        write_bench(
            &mut g,
            |t| t.populate(MUTATE_ROWS),
            move |t| t.exec(&format!("DELETE FROM bench WHERE id BETWEEN {lo} AND {hi}")),
        );
        g.finish();
    }

    /// 1000 INSERTs in one transaction, driven through the realistic *client*
    /// interface on both sides: dtdb via the in-process `DuctTapeDbClient` +
    /// `run_in_transaction` + the parameterized `sql_query!` macro (so the
    /// engine's parse cache sees one template and parses once), and SQLite via a
    /// reused prepared statement. This is the apples-to-apples "parameterized
    /// bulk insert" pattern a real application would use, in contrast to
    /// `insert_txn` above which re-parses a distinct literal string per row.
    fn bench_insert_txn_client(c: &mut Criterion) {
        use dtdb_api::client::DuctTapeDbClient;
        use dtdb_api::sql_query;
        use dtdb_storage::CompressionType;
        use futures_util::StreamExt;

        let mut g = c.benchmark_group("dml/insert_txn_client");
        g.throughput(Throughput::Elements(WRITE_ROWS as u64));

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();

        g.bench_function("dtdb_client", |b| {
            b.iter_batched(
                || {
                    let tmp = TempDir::new().unwrap();
                    let mut client = DuctTapeDbClient::in_process(tmp.path()).unwrap();
                    rt.block_on(async {
                        client
                            .create_db("bench", CompressionType::Lz4)
                            .await
                            .unwrap();
                        let mut stream = client
                            .execute_query(
                                "bench",
                                sql_query!(
                                    "CREATE TABLE bench (id INT PRIMARY KEY, k INT, v STRING)"
                                ),
                            )
                            .await
                            .unwrap();
                        while let Some(r) = stream.next().await {
                            r.unwrap();
                        }
                    });
                    (tmp, client)
                },
                |(tmp, mut client)| {
                    rt.block_on(async {
                        client
                            .run_in_transaction("bench", |tx| async move {
                                for i in 0..WRITE_ROWS {
                                    tx.execute_query(
                                        sql_query!(
                                            "INSERT INTO bench (id, k, v) VALUES (@id, @k, @v)"
                                        )
                                        .bind("id", i as i64)
                                        .bind("k", ((i * 31) % 100_000) as i64)
                                        .bind("v", format!("val_{i:08}")),
                                    )
                                    .await?;
                                }
                                Ok(())
                            })
                            .await
                            .unwrap();
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
        let mut g = c.benchmark_group("dml/concurrent_inserts");
        let num_threads = 4;
        let txns_per_thread = 100;
        g.throughput(Throughput::Elements((num_threads * txns_per_thread) as u64));
        write_bench(
            &mut g,
            |_t| {},
            move |t| t.run_concurrent_inserts(num_threads, txns_per_thread),
        );
        g.finish();
    }

    pub fn run() {
        let mut c = Criterion::default().configure_from_args();
        bench_insert_autocommit(&mut c);
        bench_insert_txn(&mut c);
        bench_insert_txn_client(&mut c);
        bench_select_scan(&mut c);
        bench_select_point(&mut c);
        bench_update_point(&mut c);
        bench_delete_range(&mut c);
        bench_concurrent_inserts(&mut c);
        c.final_summary();
    }
}
