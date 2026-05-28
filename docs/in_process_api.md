# In-Process API

DuctTapeDB exposes two ways to talk to a database from the same process: a high-level **client façade** in `dtdb_api` that mirrors the gRPC API but executes locally, and the lower-level **Rust crates** (`dtdb_relational`, `dtdb_sql`) which the client is built on top of and which you can also call directly when you need fine-grained control.

This document describes both. For the C++ / Swift FFI wrapper see [bindings.md](bindings.md); for the SQL dialect see [sql_support.md](sql_support.md); for tuning the storage engine and transaction semantics see [configuration.md](configuration.md).

---

## 1. When to use which API

| Layer | Crate | Use when |
| --- | --- | --- |
| Client façade | `dtdb_api::DuctTapeDbClient` | You want one API that works for both embedded and remote (gRPC) execution. The constructor decides the mode. |
| Direct relational + SQL | `dtdb_relational::{Database, Transaction}` + `dtdb_sql::SqlEngine` | You're building purely in-process and want to avoid the `async`/`tonic::Status` wrapping the client adds. |
| Direct key-value | `dtdb_storage::StorageEngine` | You don't need a relational schema — you just want the LSM-tree. |

The client façade is the recommended entry point for application code. Reach for the lower layers only when you need behaviour the client doesn't expose (e.g. a custom `ThreadSpawner`, raw `DbKey`/`DbValue` access, or holding a `Transaction` across many statements without the closure shape).

---

## 2. Quick start: `DuctTapeDbClient::in_process`

```rust
use dtdb_api::{DuctTapeDbClient, sql_query};
use dtdb_storage::CompressionType;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Open (or create) a database root directory on disk.
    let mut client = DuctTapeDbClient::in_process("/tmp/dtdb-data")?;

    // 2. Create a logical database inside that root. Each database lives in
    //    its own subdirectory and has independent tables, statistics, and
    //    transaction state.
    client.create_db("app", CompressionType::Lz4).await?;

    // 3. Execute single statements with execute_query. The auto-commit
    //    transaction is created and committed for you.
    let mut stream = client
        .execute_query("app", sql_query!("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"))
        .await?;
    while stream.message().await?.is_some() {}

    // 4. Run multi-statement transactions with run_in_transaction. The
    //    closure receives a TransactionClient; the transaction is committed
    //    on Ok(_) and rolled back on Err(_).
    client.run_in_transaction("app", |tx| async move {
        tx.execute_query(sql_query!("INSERT INTO users (id, name) VALUES (1, 'alice')")).await?;
        tx.execute_query(sql_query!("INSERT INTO users (id, name) VALUES (2, 'bob')")).await?;
        Ok::<_, tonic::Status>(())
    }).await?;

    Ok(())
}
```

The `sql_query!` macro accepts only string literals at compile time — this prevents accidental string interpolation in place of real parameter binding. To bind parameters use `.bind(name, value)` on the returned query.

### `execute_query` vs `run_in_transaction`

- `execute_query(db_name, query)` runs a single statement in its own auto-committed transaction at the default isolation level. Use it for one-off reads, DDL, or single-row writes.
- `run_in_transaction(db_name, |tx| async move { ... })` opens a single transaction, hands you a `TransactionClient`, and commits on `Ok(_)` / rolls back on `Err(_)`. Use it whenever you need read-your-own-writes across statements, multi-statement atomicity, or a non-default isolation level (via `run_in_transaction_with_options`).

In in-process mode both methods route directly into the local `SqlEngine` — there is no serialization, no network hop, no Tokio runtime context switch. The `async` is preserved purely so the same call sites work unchanged when switched to `DuctTapeDbClient::connect(...)` against a remote server.

### Cloning the client

`DuctTapeDbClient` is cheap to clone — the in-process variant is backed by an `Arc<DuctTapeDbServiceImpl>` and the remote variant by a tonic channel. Hand clones out to your handlers; there is no connection pool to manage.

---

## 3. Direct relational + SQL usage

For tight in-process loops or when you need direct access to `Transaction` (for example, to keep one open across an async boundary that doesn't fit the closure shape), skip the client and use `dtdb_relational` + `dtdb_sql` directly. There's no `async` and no `tonic::Status` in this path.

```rust
use std::sync::Arc;
use dtdb_relational::{Database, Transaction};
use dtdb_sql::{ExecutionResult, SqlEngine};

let db = Arc::new(Database::open("/tmp/dtdb-app")?);
let engine = SqlEngine::new(db.clone());

// DDL: one transaction per statement.
{
    let tx = Transaction::new(1, db.clone());
    engine.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)", &tx)?;
    tx.commit()?;
}

// Multi-statement transaction.
let tx = Transaction::new(2, db.clone());
engine.execute("INSERT INTO users (id, name) VALUES (1, 'alice')", &tx)?;
engine.execute("INSERT INTO users (id, name) VALUES (2, 'bob')", &tx)?;

match engine.execute("SELECT id, name FROM users", &tx)? {
    ExecutionResult::Select { rows, schema } => {
        for row in rows {
            println!("{:?}", row);
        }
        let _ = schema;
    }
    other => panic!("unexpected: {other:?}"),
}

tx.commit()?;
```

A few practical notes:

- `Transaction::new(tx_id, db)` chooses the default `IsolationLevel::SnapshotIsolation`. Use `Transaction::new_with_isolation(tx_id, db, level)` for anything else — see [configuration.md](configuration.md#transaction-isolation-levels).
- `tx_id` is yours to allocate. The simplest scheme is a monotonic counter. The relational layer uses it for OCC bookkeeping; it does not need to match anything on disk.
- `tx.commit()` consumes `tx`. Dropping a `Transaction` without committing is equivalent to `rollback()` — the buffered writes are discarded.
- A single `Database` instance is the **only** allowed handle to a given data directory in a given process. Opening it twice in parallel will race on the on-disk catalog. Wrap it in an `Arc` and share.

### Bypassing SQL: row-level reads and writes

`Transaction` exposes typed key/value methods that skip the SQL pipeline entirely. Useful for hot paths where you know the schema and primary key shape:

```rust
use dtdb_storage::{DbKey, DbValue};
use dtdb_relational::Row;

let tx = Transaction::new(3, db.clone());
tx.put(
    "users",
    DbKey::Int(3),
    Row::new(vec![DbValue::Int(3), DbValue::String("carol".into())]),
)?;
let row = tx.get("users", &DbKey::Int(3))?;
tx.commit()?;
```

These methods enforce schema and primary-key validation, participate in the transaction's read-set and write-buffer just like SQL statements, and respect the configured isolation level.

### Range scans

`Transaction::scan_iter` returns a streaming iterator over a `[start, end]` range that merges the buffered writes with the on-disk state:

```rust
let mut iter = tx.scan_iter("users", &DbKey::Int(0), &DbKey::Int(1000), None)?;
while let Some(row) = iter.next()? {
    // ...
}
```

The iterator publishes every key it reads into the transaction's read-set immediately (not at drop), so committing while the iterator is still alive correctly detects concurrent modifications.

---

## 4. Concurrency model

A single `Database` is `Send + Sync` once wrapped in `Arc`. You can:

- open many `Transaction`s concurrently from many threads
- run `SqlEngine::execute` from any thread that holds the `Arc<Database>`
- mix direct `tx.put` / `tx.get` calls with SQL statements in the same transaction

What you can **not** do:

- share a single `Transaction` across threads in a way that overlaps with `commit()` — `commit` consumes `self`
- open the same data directory from two processes (no file locking is performed; the on-disk catalog will diverge)
- run DDL (`CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`) concurrently with a transaction that touches the same table without expecting the OCC validation to abort one of them

DDL acquires the catalog write lock and waits for active readers on the target table to drain (configurable via the busy-wait poll interval — see source). Transactions that have already cached the table handle keep working; new lookups block.

---

## 5. Error handling

Each layer has its own error type:

- `dtdb_storage::StorageError` — I/O, bincode failures, WAL corruption.
- `dtdb_relational::RelationalError` — schema violations, transaction conflicts, table-not-found. Wraps `StorageError`.
- `dtdb_sql` — currently returns `Result<_, String>`. (Tracked for replacement with a typed error.)
- `dtdb_api` — surfaces `tonic::Status` so the same call sites work for in-process and remote modes. In-process errors are mapped to `Status::aborted` (OCC conflict), `Status::not_found` (database absent), and `Status::internal` (everything else).

The `RelationalError::TransactionConflict` variant is the one you'll see most often in code that mixes concurrent transactions; treat it as a retry signal. The `DuctTapeDbClient` does not retry on your behalf.
