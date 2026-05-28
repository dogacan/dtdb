# Configuration Reference

This document covers configuration knobs that aren't part of the SQL dialect itself: database-wide storage options, per-locality-group overrides, transaction isolation levels, the background analyze loop, tokenizer registration, and a few smaller features that exist but weren't documented elsewhere.

For the SQL dialect see [sql_support.md](sql_support.md); for the in-process API see [in_process_api.md](in_process_api.md); for the C++/Swift FFI see [bindings.md](bindings.md).

---

## 1. `DatabaseOptions`

`DatabaseOptions` is the per-database configuration struct passed to `Database::open_with_options` (or `DuctTapeDbClient::create_db_with_options` at the gRPC layer). It is persisted at `<data_dir>/<db>/db_options.bin` on first creation and reloaded on subsequent opens — changing field values in code only affects newly-created databases, **not** existing ones.

```rust
pub struct DatabaseOptions {
    pub compression: CompressionType,
    pub memtable_size_limit: usize,
    pub block_size_limit: usize,
    pub wal_size_limit: usize,
    pub flush_interval_ms: Option<u64>,
    pub l0_compaction_threshold: Option<usize>,
    pub sstable_target_size: Option<usize>,
    pub base_level_size_limit: Option<usize>,
    pub level_size_multiplier: Option<usize>,
    pub max_level: Option<usize>,
    pub block_cache_capacity: Option<usize>,
    pub analyze_frequency_ms: Option<u64>,
    pub wal_sync_interval_ms: Option<u64>,
    pub sort_memory_budget: Option<usize>,
}
```

### Storage engine knobs

| Field | Default | Meaning |
| --- | --- | --- |
| `compression` | `CompressionType::Lz4` | Per-block compression for SSTables. `Lz4` or `Uncompressed`. |
| `memtable_size_limit` | `1 MiB` | Trigger threshold for flushing the in-memory memtable to an L0 SSTable. Larger values mean fewer flushes but more memory and longer recovery replay. |
| `block_size_limit` | `4 KiB` | Target on-disk SSTable block size before compression. Smaller blocks give better point-read latency, larger blocks compress better and give better scan throughput. |
| `wal_size_limit` | `32 MiB` | Maximum WAL size before the engine rolls into a fresh WAL file. |
| `flush_interval_ms` | `None` (disabled) | If set, a background thread also flushes the memtable on this wall-clock interval, even if `memtable_size_limit` hasn't been hit. Useful for low-write workloads that want bounded data-loss windows. |
| `l0_compaction_threshold` | `4` | Number of L0 SSTables that triggers a compaction to L1. |
| `sstable_target_size` | `2 MiB` | Target size for individual SSTables produced by compaction. |
| `base_level_size_limit` | `10 MiB` | Size budget for the smallest non-L0 level (L1). Higher levels grow by `level_size_multiplier`. |
| `level_size_multiplier` | `10` | Ratio between consecutive levels. With the defaults: L1 = 10 MiB, L2 = 100 MiB, L3 = 1 GiB, … |
| `max_level` | `7` | Hard cap on compaction depth. Beyond this, SSTables stay in `max_level`. |
| `block_cache_capacity` | `1000` blocks | Process-wide LRU block cache shared across tables in this database. Each entry holds one decompressed block (≈ `block_size_limit` bytes). |
| `wal_sync_interval_ms` | `None` (sync on every commit) | If set, the WAL fsync is batched to this interval instead of being driven by every transaction commit. Trades durability (up to `interval_ms` of work can be lost on crash) for throughput. |

### Transaction layer knobs

| Field | Default | Meaning |
| --- | --- | --- |
| `analyze_frequency_ms` | `None` (disabled) | If set, a background thread refreshes table-level statistics (row counts, distinct-value estimates) at this interval. The cost-based optimizer uses these stats to plan scan vs. index choices. The collector starts lazily on the first transaction the database opens, not at `Database::open` time. |

### SQL engine knobs

| Field | Default | Meaning |
| --- | --- | --- |
| `sort_memory_budget` | `None` (in-memory, no spill) | Reserved for the future external-sort path. Currently the planner sorts in memory and may OOM on large inputs — set this once spill-to-disk lands. |

### Constructing a non-default `DatabaseOptions`

```rust
use dtdb_relational::{Database, DatabaseOptions};
use dtdb_storage::CompressionType;

let options = DatabaseOptions {
    compression: CompressionType::Lz4,
    memtable_size_limit: 8 * 1024 * 1024,   // 8 MiB memtable
    block_size_limit: 16 * 1024,            // 16 KiB blocks
    wal_size_limit: 64 * 1024 * 1024,
    flush_interval_ms: None,
    l0_compaction_threshold: Some(4),
    sstable_target_size: Some(4 * 1024 * 1024),
    base_level_size_limit: Some(40 * 1024 * 1024),
    level_size_multiplier: Some(10),
    max_level: Some(6),
    block_cache_capacity: Some(2048),
    analyze_frequency_ms: Some(60_000),     // refresh stats every 60s
    wal_sync_interval_ms: None,
    sort_memory_budget: None,
};

let db = Database::open_with_options("/tmp/dtdb-app", options)?;
```

There is no `Default` impl today; provide every field explicitly. The values shown above are reasonable starting points for a workload with rows in the low-kilobyte range — adjust based on profiling.

---

## 2. Per-locality-group overrides

Locality groups partition a table's columns into separate physical storage engines (one LSM-tree per group). This is configured at `CREATE TABLE` time via `WITH (locality_groups = '...')`. Each named group can also override most of the `DatabaseOptions` storage knobs for itself, using a parenthesized list after the group's column list:

```sql
CREATE TABLE events (
    id INT PRIMARY KEY,
    ts INT,
    payload TEXT,
    blob BYTEA
) WITH (locality_groups = '
    hot:ts(memtable_size_limit=4194304, block_size_limit=16384, compression=lz4);
    cold:payload,blob(memtable_size_limit=33554432, compression=lz4, sstable_target_size=8388608)
');
```

Recognized per-group options (all optional; absent options fall back to the database-level value):

- `compression` — `lz4` or `uncompressed`
- `memtable_size_limit`
- `block_size_limit`
- `wal_size_limit`
- `l0_compaction_threshold`
- `sstable_target_size`
- `base_level_size_limit`
- `level_size_multiplier`
- `max_level`
- `block_cache_capacity`
- `wal_sync_interval_ms`

Column names listed before the `(...)` form the group; columns not mentioned in any group go into a default `default` group with database-level options. If no `locality_groups` clause is given at all, the table uses a single un-named storage engine at the table root — no `default/` subdirectory — and looks like a plain LSM-tree from the outside.

### When to use locality groups

- **Wide rows with a hot subset**: put the frequently-read columns in their own group with a small `block_size_limit` and a large `block_cache_capacity`. Reads of those columns skip the cold blob bytes entirely.
- **Different compression sweet spots**: store small numeric columns uncompressed and bulk text/blob columns LZ4-compressed.
- **Different write rates**: a high-churn column in its own group flushes and compacts independently of the rest of the table.

`SELECT` queries prune locality groups automatically — only the groups whose columns are referenced get touched. `UPDATE` currently reads all groups regardless (tracked for improvement).

---

## 3. Transaction isolation levels

`Transaction::new(tx_id, db)` opens a transaction at the default isolation, `IsolationLevel::SnapshotIsolation`. Use `Transaction::new_with_isolation(tx_id, db, level)` (or `TransactionOptions { isolation_level: ... }` through the client) to pick a different level. Four levels are implemented:

| Level | Read behavior | OCC validation on commit | Notes |
| --- | --- | --- | --- |
| `ReadUncommitted` | Reads see the current committed state plus any uncommitted writes from this same transaction; no read-set is tracked. | Validates only write-write conflicts. | Lowest overhead; used internally by the background statistics collector. Don't use this for application logic that cares about consistency. |
| `ReadCommitted` | Reads see only data committed at the moment of each read, plus this transaction's own writes. | Validates only write-write conflicts. | Cheapest level that gives "no dirty reads." Phantom reads and non-repeatable reads are both allowed. |
| `RepeatableRead` | Every key this transaction reads is added to a read-set. Re-reading the same key returns the same value for the lifetime of the transaction. | On commit, the read-set is checked against the commit log: any concurrent commit that modified a key we read causes our commit to abort with `RelationalError::TransactionConflict`. | Catches read-write conflicts but not phantom inserts (a concurrent INSERT into a range we scanned isn't detected). |
| `SnapshotIsolation` (default) | Same as `RepeatableRead`, plus every range scan records its `[start, end]` bounds. | Adds a phantom-conflict check: any concurrent commit whose new/modified keys fell inside any range we scanned causes our commit to abort. | Default. The strongest level the engine supports today. Equivalent to PostgreSQL's `REPEATABLE READ`, not its `SERIALIZABLE`. |

The OCC bookkeeping is shared between all levels: each commit appends a record of its mutated keys to a bounded commit-history log; validations scan that log forward from the transaction's start version. Long-running transactions can therefore see growing validation cost; treat aborts as a normal outcome and have a retry loop ready.

### Picking a level

- Default to `SnapshotIsolation` for application code.
- Drop to `RepeatableRead` when you scan large ranges and don't care about phantoms but want to keep the read-set check.
- Drop further to `ReadCommitted` for batch-style processes where each statement is independently consistent.
- Use `ReadUncommitted` only for diagnostics or background bookkeeping that explicitly wants to see in-flight state without contributing to the OCC log itself.

---

## 4. Custom tokenizers (full-text search)

`MATCH(column) AGAINST('query')` uses a registered tokenizer to split both indexed text and query strings into terms. The built-in tokenizer is `simple` (Unicode word-boundary split, lowercased, no stemming). You can register your own:

```rust
use std::sync::Arc;
use dtdb_relational::{register_global_tokenizer, Tokenizer};

struct LowercaseTrigram;
impl Tokenizer for LowercaseTrigram {
    fn name(&self) -> &str { "lc_trigram" }
    fn tokenize(&self, input: &str) -> Vec<String> {
        let chars: Vec<char> = input.to_lowercase().chars().collect();
        chars.windows(3).map(|w| w.iter().collect()).collect()
    }
}

register_global_tokenizer("lc_trigram", Arc::new(LowercaseTrigram));
```

Tokenizers live in a process-global registry; register them once at startup, before any DDL that references them. To use the registered tokenizer for a FULLTEXT index:

```sql
CREATE INDEX users_name_fts ON users(name) USING FULLTEXT WITH (tokenizer = 'lc_trigram');
```

The tokenizer name is stored in `schema.bin`, so a database that references a custom tokenizer will fail to open if that tokenizer isn't registered first. Re-register the same tokenizer (under the same name) on every process start.

`Database::register_tokenizer(name, tokenizer)` is a convenience wrapper that delegates to `register_global_tokenizer` — it has no per-database scope and is provided only for discoverability.

### Query syntax

Inside `AGAINST(...)` the query string supports:

- Bare words: `cat dog` matches rows containing both tokens (`AND` is the default).
- Explicit boolean operators: `cat AND dog`, `cat OR dog`, parentheses for grouping.
- Phrase queries: `"new york"` matches rows where those two tokens appear consecutively. Empty phrases (`""` or a phrase that tokenizes to nothing) are rejected at parse time.
- Single-character SQL `LIKE` wildcard `_` is **not** an FTS feature; that lives in `LIKE`, not `MATCH … AGAINST`.

---

## 5. SQL-side niceties that aren't in the dialect doc

A few small features the SQL doc doesn't call out explicitly:

- **`AUTOINCREMENT` keyword.** In addition to `AUTO_INCREMENT` (MySQL style) and the `SERIAL` data type (Postgres style), the single-word SQLite spelling `AUTOINCREMENT` is accepted in column definitions.
- **`LIKE` single-character wildcard.** `_` matches exactly one character, complementing `%` which matches zero or more. As of recent revisions, `LIKE` is implemented via a cached compiled regex (RE2-style NFA), so adversarial patterns like `'%a%a%a%a%b'` run in linear time.
- **Implicit cross join.** `SELECT * FROM a JOIN b` with no `ON` clause is accepted and treated as a Cartesian product. You can also write `FROM a, b` for the same thing. Prefer an explicit `CROSS JOIN` for clarity.
- **Three-valued logic.** `AND`, `OR`, and `NOT` follow standard SQL `NULL` semantics: `NULL AND FALSE` is `FALSE`, `NULL AND TRUE` is `NULL`, `NULL OR TRUE` is `TRUE`, `NOT NULL` is `NULL`. Comparison operators (`=`, `<`, etc.) all propagate `NULL`.
- **Ambiguous column references.** After a `JOIN`, an unqualified reference (`name`) that matches more than one column in the combined schema is now a hard error (`Ambiguous column reference '...': matches [...]`). Qualify with the table or alias name (`a.name`).
- **`@param` placeholders.** SQL submitted through `dtdb_api::sql_query!` may contain `@name` placeholders; bind values with `.bind("name", value)`. Quoting and type coercion happen on the client side before the SQL reaches the engine. (A real prepared-statement protocol is on the roadmap; today the macro re-parses each call.)

---

## 6. On-disk layout

For reference, a database directory looks like:

```
<data_dir>/
  <db_name>/
    db_options.bin             # bincode-serialized DatabaseOptions, persisted on first open
    transactions.log           # global write-ahead transaction log (prepare/commit records)
    <table_name>/
      schema.bin               # bincode-serialized Schema
      statistics.bin           # bincode-serialized TableStatistics
      MANIFEST                 # storage-engine LSM manifest (current SSTables per level)
      wal_*.log                # one or more WAL segments
      *.sst                    # SSTable files
      default/                 # only present when locality groups are used; mirrors the
                               # above layout per group
      index_<idx_name>/        # one full storage engine per secondary / FULLTEXT index
```

`db_options.bin` is the source of truth for storage knobs after creation; if you ever need to migrate to different settings, the supported path is a logical export/import, not editing this file.
