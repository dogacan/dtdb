# In-Process API

DuctTapeDB can run **embedded** — inside your own process, with no server, no network, and no serialization between your code and the storage engine. The entry point is `dtdb_api::in_process::InProcessClient`: a small, synchronous handle that takes the same `SqlQuery` objects and returns the same row types as the remote (gRPC) client, but executes everything directly against the local SQL → relational → storage stack.

This document covers the embedded client end to end: opening a database, running statements and transactions, reading results, the concurrency model, and DuctTapeDB's full-text search with custom Rust tokenizers. For the gRPC/network client see [§9](#9-the-remote-counterpart); for the C++/Swift FFI wrapper see [bindings.md](bindings.md); for the SQL dialect see [sql_support.md](sql_support.md); for storage and transaction tuning knobs see [configuration.md](configuration.md).

---

## 1. Choosing a client

There are exactly two supported ways to talk to a database, and both live in `dtdb_api`:

| Client | Type | Shape | Use when |
| --- | --- | --- | --- |
| Embedded | `dtdb_api::in_process::InProcessClient` | **Synchronous** | The database lives in your process. No runtime, no network — calls go straight into the local engine. |
| Remote | `dtdb_api::client::RemoteClient` | **Async** (`tonic`) | You're talking to a `dtdb` server over gRPC. |

Both expose the same conceptual surface — `create_db`, `execute_query`, `run_in_transaction` — and accept the same `SqlQuery` values, so application code ports between them with mostly mechanical changes (add/remove `.await`, swap the constructor). See [§9](#9-the-remote-counterpart).

> **A note on the lower layers.** `InProcessClient` is built on `dtdb_relational` (`Database`, `Transaction`) and `dtdb_sql` (`SqlEngine`). Those crates are internal building blocks, **not** a supported public API: you'd have to allocate transaction IDs by hand, guarantee a single `Database` handle per data directory yourself, and manage commit/rollback manually with no `tonic::Status` mapping. The client exists precisely to remove those footguns. Reach past it only if you are extending DuctTapeDB itself.

---

## 2. Quick start: `InProcessClient`

```rust,no_run
use dtdb_api::{in_process::InProcessClient, sql_query};
use dtdb_storage::CompressionType;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Open (or create) a database root directory on disk. Existing
    //    databases under this root are discovered and reopened automatically.
    let client = InProcessClient::open("/tmp/dtdb-data")?;

    // 2. Create a logical database inside that root. Each database lives in
    //    its own subdirectory with independent tables, statistics, and
    //    transaction state. Creating one that already exists is a no-op
    //    (the response reports success = false), not an error.
    client.create_db("app", CompressionType::Lz4)?;

    // 3. Run single statements with execute_query. DDL and writes are
    //    auto-committed in their own transaction before the call returns.
    client.execute_query("app", sql_query!(
        "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"
    ))?;

    // 4. Group multiple statements into one atomic transaction with
    //    run_in_transaction. The closure runs against a transaction client;
    //    the transaction commits on Ok(_) and rolls back on Err(_).
    client.run_in_transaction("app", |tx| {
        tx.execute_query(sql_query!("INSERT INTO users (id, name) VALUES (1, 'alice')"))?;
        tx.execute_query(sql_query!("INSERT INTO users (id, name) VALUES (2, 'bob')"))?;
        Ok(())
    })?;

    // 5. Read results back. The query result is an iterator of rows.
    let result = client.execute_query("app", sql_query!(
        "SELECT id, name FROM users ORDER BY id"
    ))?;
    for row in result {
        let row = row?;
        // get_by_index returns Option<&DbValue> — the value type yielded in a row.
        println!("{:?} {:?}", row.get_by_index(0), row.get_by_index(1));
    }

    Ok(())
}
```

Two things to notice up front:

- **No `async`.** `InProcessClient` is fully synchronous. There is no Tokio runtime, no `.await`, and no network hop — `execute_query` calls straight into the local `SqlEngine`.
- **You never allocate transaction IDs or manage `Database` handles.** The client owns a catalog that maps each database name to its single `Database`/`SqlEngine` pair and hands out transaction IDs from an internal counter.

### `sql_query!` and parameter binding

The `sql_query!` macro accepts **only string literals** — it fails to compile on a runtime `String`. This is deliberate: it forces you to use real parameter binding instead of formatting values into SQL text.

```rust,no_run
# use dtdb_api::sql_query;
let q = sql_query!("SELECT * FROM users WHERE name = @name AND id > @min")
    .bind("name", "alice")
    .bind("min", 10i64);
```

`@name` placeholders are filled from the values you `.bind(...)`; binding handles quoting and type coercion so user input can't break out of its slot. If you must build a query from a non-literal string, construct it explicitly with `SqlQuery::new(text)` and accept that you own the escaping.

---

## 3. `execute_query` vs `run_in_transaction`

These are the two ways to run statements. The difference is transaction scope.

**`execute_query(db_name, query)`** runs a single statement in its own auto-committed transaction at the default isolation level. Use it for one-off reads, DDL, or single-statement writes.

- For DDL and writes (`CREATE TABLE`, `INSERT`, `UPDATE`, `DELETE`, …) the statement executes and **commits before the call returns**; the returned result yields no rows (see [§4](#4-reading-results) for the `info_message()` summary).
- For `SELECT`, rows are produced lazily as you iterate, and the read transaction commits when the iterator is fully drained. If you drop the iterator early it rolls back instead — harmless for a read, but it means a partially-consumed `SELECT` does not finalize its read-set.

**`run_in_transaction(db_name, |tx| { ... })`** opens one transaction, hands your closure an `InProcessTransactionClient`, and **commits on `Ok(_)` / rolls back on `Err(_)`**. Use it whenever you need:

- multi-statement atomicity,
- read-your-own-writes across statements, or
- a non-default isolation level (via `run_in_transaction_with_options`, see [§5](#5-transactions-isolation-and-conflicts)).

```rust,no_run
# use dtdb_api::{in_process::InProcessClient, sql_query};
# use dtdb_storage::DbValue;
# fn main() -> Result<(), Box<dyn std::error::Error>> {
# let client = InProcessClient::open("/tmp/dtdb-doctest")?;
let names: Vec<String> = client.run_in_transaction("app", |tx| {
    tx.execute_query(sql_query!("UPDATE users SET name = 'ALICE' WHERE id = 1"))?;

    let mut out = Vec::new();
    let result = tx.execute_query(sql_query!("SELECT name FROM users ORDER BY id"))?;
    for row in result {
        // DbValue::String wraps an Arc<str>; to_string() gives an owned String.
        if let Some(DbValue::String(s)) = row?.get_by_index(0) {
            out.push(s.to_string());
        }
    }
    Ok(out) // committed here
})?;
# Ok(())
# }
```

The closure returns any `Result<T, Status>`; the `T` is handed back to you on commit. Returning `Err(_)` (including propagating a `?` from a failed statement) rolls the whole transaction back.

> **DDL is rejected inside `run_in_transaction`.** `CREATE TABLE` / `DROP TABLE` / index DDL must run as standalone `execute_query` calls — attempting them inside a transaction closure returns `Status::invalid_argument`. Run your schema changes first, then open transactions against the resulting tables.

---

## 4. Reading results

`execute_query` (and the transaction client's `execute_query`) returns an `InProcessQueryResult`, which is an `Iterator<Item = Result<Row, Status>>` plus two helpers:

```rust,no_run
# use dtdb_api::{in_process::InProcessClient, sql_query};
# fn main() -> Result<(), Box<dyn std::error::Error>> {
# let client = InProcessClient::open("/tmp/dtdb-doctest")?;
let result = client.execute_query("app", sql_query!("SELECT id, name FROM users"))?;

// schema() describes the result columns (None for non-SELECT statements).
// Iterating consumes `result`, so clone the schema up front if you want to
// resolve column names while reading rows.
let schema = result.schema().cloned();
if let Some(schema) = &schema {
    let cols: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    println!("columns: {cols:?}"); // ["id", "name"]
}

for row in result {
    let row = row?;                                // each item is a Result<Row, Status>
    let id = row.get_by_index(0);                  // Option<&DbValue>, positional
    let name = schema.as_ref()
        .and_then(|s| row.get_by_name(s, "name")); // Option<&DbValue>, by column name
    println!("{id:?} {name:?}");
}
# Ok(())
# }
```

- **`row.get_by_index(i)`** → `Option<&DbValue>` by column position.
- **`row.get_by_name(schema, "col")`** → `Option<&DbValue>`, resolving the name through the result schema.
- Values are `dtdb_storage::DbValue` (`Int`, `Float`, `String`, `Bytes`, `Bool`, `Null`, and the date/time/decimal variants) — match on the variant you expect.

For DDL and writes, `schema()` is `None` and the iterator is empty; use **`info_message()`** for a human-readable summary instead:

```rust,no_run
# use dtdb_api::{in_process::InProcessClient, sql_query};
# fn main() -> Result<(), Box<dyn std::error::Error>> {
# let client = InProcessClient::open("/tmp/dtdb-doctest")?;
let result = client.execute_query("app", sql_query!(
    "INSERT INTO users (id, name) VALUES (3, 'carol')"
))?;
println!("{}", result.info_message().unwrap()); // "Inserted 1 row(s)."
# Ok(())
# }
```

`info_message()` returns things like `"Inserted 3 row(s)."`, `"Table created successfully."`, or `"Updated 1 row(s)."`, and `None` for `SELECT`.

---

## 5. Transactions, isolation, and conflicts

`run_in_transaction` uses the default isolation level (`SnapshotIsolation`). To pick another, use `run_in_transaction_with_options`:

```rust,no_run
# use dtdb_api::{in_process::InProcessClient, sql_query};
# fn main() -> Result<(), Box<dyn std::error::Error>> {
# let client = InProcessClient::open("/tmp/dtdb-doctest")?;
use dtdb_api::client::{IsolationLevel, TransactionOptions};

let opts = TransactionOptions {
    isolation_level: IsolationLevel::RepeatableRead,
};

client.run_in_transaction_with_options("app", opts, |tx| {
    tx.execute_query(sql_query!("INSERT INTO users (id, name) VALUES (4, 'dave')"))?;
    Ok(())
})?;
# Ok(())
# }
```

The four levels (`ReadUncommitted`, `ReadCommitted`, `RepeatableRead`, `SnapshotIsolation`) and their conflict-detection semantics are documented in [configuration.md → Transaction isolation levels](configuration.md#3-transaction-isolation-levels).

DuctTapeDB uses optimistic concurrency control: conflicts are detected **at commit time**, not by blocking. When a concurrent transaction has modified data your transaction read (or, under `SnapshotIsolation`, inserted into a range you scanned), the commit aborts and surfaces as `Status::aborted`. Treat that as a **retry signal** — the client does not retry on your behalf:

```rust,no_run
# use dtdb_api::{in_process::InProcessClient, sql_query};
# fn main() -> Result<(), Box<dyn std::error::Error>> {
# let client = InProcessClient::open("/tmp/dtdb-doctest")?;
let result = loop {
    match client.run_in_transaction("app", |tx| {
        tx.execute_query(sql_query!("UPDATE users SET name = 'ALICE' WHERE id = 1"))?;
        Ok(())
    }) {
        Err(e) if e.code() == tonic::Code::Aborted => continue, // conflict: retry
        other => break other, // success, or a non-retryable error
    }
};
result?;
# Ok(())
# }
```

---

## 6. Cloning and the concurrency model

`InProcessClient` is `Clone` and `Send + Sync` — it's a thin handle over an `Arc`'d catalog. Clone it freely and hand clones to your worker threads; there is no connection pool to manage and clones share the same underlying databases.

```rust,no_run
# use dtdb_api::{in_process::InProcessClient, sql_query};
# use dtdb_storage::CompressionType;
# fn main() -> Result<(), Box<dyn std::error::Error>> {
let client = InProcessClient::open("/tmp/dtdb-data")?;
client.create_db("app", CompressionType::Lz4)?;

let mut handles = Vec::new();
for i in 0i64..4 {
    let client = client.clone();
    handles.push(std::thread::spawn(move || {
        client.run_in_transaction("app", |tx| {
            tx.execute_query(
                sql_query!("INSERT INTO users (id, name) VALUES (@id, @name)")
                    .bind("id", 100 + i)
                    .bind("name", format!("worker-{i}")),
            )?;
            Ok(())
        })
    }));
}
for h in handles { h.join().unwrap()?; }
# Ok(())
# }
```

What holds and what doesn't:

- **Many concurrent transactions are fine.** Each `execute_query` / `run_in_transaction` call gets its own transaction ID from the catalog's counter and its own snapshot. Overlapping writers are reconciled by OCC at commit (`Status::aborted` on conflict — retry).
- **One process per data directory.** The catalog assumes it is the only handle to its directory. DuctTapeDB performs no cross-process file locking; opening the same `data_dir` from a second process (or a second `InProcessClient` in the same process) will diverge the on-disk catalog. Open it once and share clones.
- **DDL contends with active work on the same table.** `CREATE INDEX` / `DROP TABLE` and friends take the catalog/table write lock; transactions already holding a table handle keep running, while new lookups wait for the DDL to finish.

---

## 7. Error handling

Every `InProcessClient` method (except `open`, which returns `Result<_, String>`) returns `Result<_, tonic::Status>` — the same error type the remote client uses, so error-handling code ports unchanged. The mapping:

| `Status` code | Cause |
| --- | --- |
| `not_found` | The named database doesn't exist in this catalog. |
| `invalid_argument` | A SQL parse/type error, or attempting DDL inside `run_in_transaction`. |
| `aborted` | An OCC conflict (or a commit failure). **Retry-able.** |
| `internal` | Lower-level storage/relational failures. |

Inspect `status.code()` to branch (e.g. retry on `Code::Aborted`, surface `Code::InvalidArgument` to the caller) and `status.message()` for the human-readable detail. Under the hood these wrap the lower-layer error types (`dtdb_storage::StorageError`, `dtdb_relational::RelationalError`); the client flattens them into `Status` so you have one error type to handle.

---

## 8. Full-text search with custom tokenizers

DuctTapeDB ships a full-text search engine built on an inverted index, and — distinctively — lets you plug in **your own tokenizer written in Rust**. A tokenizer decides how text is split into searchable terms, and the *same* tokenizer is applied to both the indexed column values and the query string, so indexing and querying always agree on what a "term" is.

### The `Tokenizer` trait

A tokenizer is any type implementing `dtdb_relational::Tokenizer`. The trait has a single method:

```rust,no_run
use dtdb_relational::Tokenizer;

/// Splits a comma-delimited column (e.g. a `tags` field) into normalized terms.
struct CommaTokenizer;

impl Tokenizer for CommaTokenizer {
    fn tokenize(&self, text: &str) -> Vec<String> {
        text.split(',')
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .collect()
    }
}
```

Because `tokenize` is plain Rust, the tokenizer can do whatever you need: character n-grams for substring/fuzzy matching, language-specific stemming, stripping diacritics, splitting `CamelCase`, and so on. For example, a character-trigram tokenizer that enables substring-style matching:

```rust,no_run
# use dtdb_relational::Tokenizer;
struct Trigram;

impl Tokenizer for Trigram {
    fn tokenize(&self, text: &str) -> Vec<String> {
        let chars: Vec<char> = text.to_lowercase().chars().collect();
        chars.windows(3).map(|w| w.iter().collect()).collect()
    }
}
```

The built-in tokenizer is named **`simple`**: it splits on whitespace, lowercases each term, and drops empty tokens. It's registered automatically and is the default when a `FULLTEXT` index doesn't name a tokenizer.

### Registering a tokenizer

Tokenizers live in a **process-global registry**, keyed by name. Register yours once at startup, before any DDL or query that references it:

```rust,no_run
# use dtdb_relational::Tokenizer;
# struct CommaTokenizer;
# impl Tokenizer for CommaTokenizer { fn tokenize(&self, text: &str) -> Vec<String> { text.split(',').map(|s| s.trim().to_lowercase()).filter(|s| !s.is_empty()).collect() } }
# struct Trigram;
# impl Tokenizer for Trigram { fn tokenize(&self, text: &str) -> Vec<String> { text.to_lowercase().chars().collect::<Vec<_>>().windows(3).map(|w| w.iter().collect()).collect() } }
use std::sync::Arc;
use dtdb_relational::register_global_tokenizer;

register_global_tokenizer("comma", Arc::new(CommaTokenizer));
register_global_tokenizer("trigram", Arc::new(Trigram));
```

`Database::register_tokenizer(name, tokenizer)` is a convenience wrapper that delegates to the same global registry — it has no per-database scope and exists only for discoverability.

> **The registry is not persisted, but the *name* is.** A `FULLTEXT` index records its tokenizer's name in the table's `schema.bin`. The tokenizer implementation itself is your Rust code, so it is **not** stored on disk. That means: re-register every custom tokenizer (under the same name) on each process start. A database that references a tokenizer name nothing has registered will fail when that index is built or queried.

### Creating a full-text index and querying it

Declare a `FULLTEXT` index with `CREATE FULLTEXT INDEX`, optionally naming a tokenizer with `USING`; query it with `MATCH(col) AGAINST('...')`:

```rust,no_run
# use dtdb_api::{in_process::InProcessClient, sql_query};
# fn main() -> Result<(), Box<dyn std::error::Error>> {
# let client = InProcessClient::open("/tmp/dtdb-doctest")?;
// DDL must be a standalone statement (see §3).
client.execute_query("app", sql_query!(
    "CREATE FULLTEXT INDEX idx_tags ON items (tags) USING comma"
))?;

let result = client.execute_query("app", sql_query!(
    "SELECT id FROM items WHERE MATCH(tags) AGAINST('rust AND database')"
))?;
# Ok(())
# }
```

Omit `USING <tokenizer>` to fall back to the built-in `simple` tokenizer. The optimizer uses the index automatically when one exists on the matched column (visible as `PhysicalFullTextScan` in `EXPLAIN`); without an index, `MATCH … AGAINST` still works by falling back to a sequential scan that evaluates the query against each row.

The query string inside `AGAINST(...)` is its own small boolean language:

- **Bare terms are AND-ed by default.** `rust database` matches rows containing both terms — the same as `rust AND database`.
- **Explicit boolean operators.** `AND`, `OR` (case-insensitive), and parentheses for grouping: `(rust OR c++) AND database`. `AND` binds tighter than `OR`.
- **Phrase queries.** `"new york"` matches rows where those terms appear consecutively. An empty or whitespace-only phrase (`""`) is rejected at parse time.
- Every term and phrase is run through the index's tokenizer before matching, so your custom normalization applies uniformly to queries too.

The DDL and `MATCH … AGAINST` grammar are also covered, from the SQL-dialect side, in [sql_support.md](sql_support.md#create-fulltext-index).

---

## 9. The remote counterpart

The embedded client has an async twin, `dtdb_api::client::RemoteClient`, that speaks gRPC to a `dtdb` server. It mirrors `InProcessClient`'s surface so code ports with mechanical changes:

```rust,no_run
use dtdb_api::{client::RemoteClient, sql_query};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = RemoteClient::connect("http://127.0.0.1:50051".to_string()).await?;

    client.run_in_transaction("app", |tx| async move {
        tx.execute_query(sql_query!("INSERT INTO users (id, name) VALUES (1, 'alice')")).await?;
        Ok(())
    }).await?;

    Ok(())
}
```

The differences from the embedded client are exactly what you'd expect from a network client: every method is `async` (needs `.await`), the methods take `&mut self`, you `connect(addr)` instead of `open(dir)`, the `run_in_transaction` closure is an `async move` block, and `execute_query` returns a response **stream** rather than a synchronous row iterator. The query input (`SqlQuery` / `sql_query!`), the `tonic::Status` error type, and `TransactionOptions` are identical. For running and securing the server itself, and for the FFI bindings layered on top of these clients, see [bindings.md](bindings.md).
