# ADR 0004: Reference-counted payloads for owned LSM scan-iterator snapshots

- **Status:** Accepted
- **Date:** 2026-06-01
- **Deciders:** dtdb maintainers

## Context

DuctTapeDB's storage engine (`dtdb_storage`) implements range scanning over its
active `MemTable` and its `SSTables`. The current design uses an **owned
iterator**: on construction, `StorageEngine::scan_iter` takes a read lock on the
`MemTable`, runs a range query over the internal `BTreeMap` (`scan_range_raw`),
clones every key and value in the range into an owned `Vec<(DbKey,
Option<DbValue>)>`, and releases the lock immediately. The `ScanIterator` then
merges that snapshot with the streaming SSTable block iterators during
Volcano-style pull execution, cloning again per row to track the `last_key`
dedup cursor and to yield owned `(DbKey, DbValue)` pairs.

This isolates the iterator's lifetime from the locks of the underlying
structures, but it copies heap buffers twice on the hot path:

1. **Up-front**, when the memtable range is materialized into the snapshot `Vec`.
2. **Per row**, when keys and deserialized values are cloned during traversal.

Both copies deep-clone the heap payloads behind `DbKey`/`DbValue` (the `String`,
`Vec<u8>`, and `Vec<DbKey>` arms). We want to cut this cost, especially for
memory-resident datasets where every read is served from the `MemTable` without
touching disk.

There are two **orthogonal** design questions here, and it is worth separating
them because the original framing conflated them:

- **Axis 1 — Lifetime: owned snapshot vs. borrowed references (`&'a`).** Does the
  iterator own its data or borrow it from the underlying structures?
- **Axis 2 — Payload cost: deep-copy vs. shared (`Arc`) payloads.** *Given* an
  owned snapshot, are the heap buffers deep-copied or shared by reference count?

The current design is "owned + deep-copy." This ADR changes only Axis 2.

---

## Decision

We keep the **owned iterator** (Axis 1 unchanged) and switch its payloads to
**reference-counted sharing** (Axis 2).

Concretely, we refactor the heap-allocated arms of `DbKey` and `DbValue` to be
wrapped in reference-counted smart pointers — `Arc<str>` for strings, `Arc<[u8]>`
for byte buffers, and `Arc<Vec<DbKey>>` for composite keys. Snapshotting a range,
and the per-row clones during traversal, then become `Arc` pointer clones (an
atomic refcount bump) instead of deep buffer copies.

### Axis 1: why we keep an owned snapshot and reject borrowing (`&'a`)

A zero-copy borrowed iterator (`&'a DbKey`, `&'a DbValue`) was considered and
rejected for two architectural reasons:

1. **Concurrency loss & deadlock risk.** The `MemTable` is protected by an
   `RwLock`. Borrowed references would require holding the `RwLockReadGuard` live
   inside the `ScanIterator` for the entire pull-based scan, blocking any
   concurrent writer trying to commit to the `MemTable`. Worse, a multi-statement
   transaction that holds a read cursor and then issues a write would deadlock the
   thread against its own read guard.
2. **Lifetime contamination of the Volcano engine.** A lifetime parameter `'a` on
   `ScanIterator<'a>` propagates into `TableScanIterator<'a>`,
   `TransactionScanIterator<'a>`, the adapter iterators, and ultimately the
   physical operators (`PhysicalSeqScan`, the `PhysicalOperator` trait object,
   etc.), forcing `Box<dyn PhysicalOperator + 'a>` throughout the query engine.
   This adds substantial borrow-checker friction and complexity across the whole
   relational layer.

Note that both reasons argue against borrowing **independently of Axis 2** — we
would keep an owned snapshot even if we left payloads deep-copied. The `Arc`
decision below is a separate, additive optimization.

### Axis 2: why `Arc` payloads instead of deep copies

Sharing the underlying buffers by reference count turns each key/value clone —
both in the up-front snapshot and per row — from an allocate-and-`memcpy` into an
atomic increment of a shared refcount. Snapshot isolation is preserved: an
outstanding iterator holds its own `Arc` to the buffer it captured, so a writer
that later overwrites the memtable entry does not mutate the bytes the iterator
sees.

### Scope: what this does *not* change

This is reference-counting of **payloads**, not of the snapshot **structure**. The
memtable range is still eagerly materialized into a `Vec` on construction
(`scan_range_raw`), so a scan still:

- allocates an O(range) snapshot `Vec`, and
- performs O(range) `Arc` clones before yielding the first row.

`Arc` removes the deep buffer copies but not the up-front materialization; this
is not lazy streaming over the memtable. A truly streaming, O(1)-snapshot design
is discussed under Rejected/Deferred alternatives.

---

## Implementation notes and prerequisites

These are concrete consequences of wrapping `DbKey`/`DbValue` payloads in `Arc`
and must be handled as part of the change:

- **serde `rc` feature is required (enabled).** `Arc<str>` and `Arc<[u8]>` do
  **not** implement `Serialize`/`Deserialize` unless serde's `rc` feature is
  enabled, and `DbKey`/`DbValue` both derive serde and are written to disk via
  bincode. The workspace `serde` dependency now enables `["derive", "rc"]`; this
  was a hard prerequisite, not a detail.
- **No on-disk format change / no migration.** With the `rc` feature, `Arc<str>`
  serializes byte-for-byte identically to `str` (and `Arc<[u8]>` to `[u8]`), so
  existing SSTables, WAL frames, and the manifest remain readable. There is no
  format break.
- **Sharing is in-memory only; deserialization does not dedup.** serde's `rc`
  support gives every deserialized `Arc` its own allocation. The refcount sharing
  is a runtime property of the live memtable/snapshot path; it is not reconstructed
  when reading SSTable blocks back from disk. This is fine for the targeted hot
  path but should not be mistaken for on-disk deduplication.
- **Ordering and equality semantics are preserved.** `Arc<T>: Ord`/`PartialEq`
  compare the pointee, not the pointer, so the derived `Ord` on `DbKey`, the
  `BTreeMap` ordering, and `DbValue`'s hand-written `PartialEq`/`Hash` are
  unaffected. (Worth verifying in tests, since the entire store relies on key
  ordering not silently becoming pointer comparison.)
- **Construction-site blast radius.** `DbKey`/`DbValue` are foundational types used
  across the whole workspace (storage, relational, sql, api, bindings, benches).
  Pattern matches keep working through `Deref` (e.g. `DbValue::String(s)` still
  binds an `&str`), but every *construction* site changes. The implementation adds
  `DbKey::string`/`composite` and `DbValue::string`/`bytes` constructors (accepting
  `impl Into<Arc<…>>`) to keep call sites readable. A handful of sites needed real
  thought rather than mechanical edits: in-place mutation of a now-`Arc` composite
  key (`parts.pop()`), `&str` comparisons/`.as_str()` on `Arc<str>`, and the
  FFI/gRPC boundaries that must still hand out owned `String`/`Vec<u8>`.

---

## Consequences

### Positive
* **Cheaper clones on the hot path, confirmed end-to-end.** Measured with the
  `dtdb_vs_sqlite` comparison bench, run back-to-back on the same machine against
  the pre-change commit, using the *unchanged* SQLite cases as a control to rule
  out machine drift (the control moved only ±4% between runs):

  | scan (5k rows, served from memtable) | before | after | change |
  |---|---|---|---|
  | `select_scan_text` — projects the `v STRING` column | 1.19 ms | 0.97 ms | **~18% faster** |
  | `select_scan_aggregate` — integer-only control | 0.98 ms | 0.82 ms | **~17% faster** |

  (An initial run was discarded after the SQLite control "regressed" 162%,
  revealing the machine — not the code — had slowed; the numbers above are from a
  clean back-to-back run.)

* **Smaller value enums — a second, broader win.** Wrapping the payloads shrank
  both `DbKey` and `DbValue` from **32 to 24 bytes**: `String`/`Vec` carry a
  capacity word, whereas `Arc<str>`/`Arc<[u8]>` are 16-byte fat pointers and
  `Arc<Vec<DbKey>>` is an 8-byte thin pointer. Every move of these enums through
  the Volcano pipeline (rows, `BinaryHeap` entries, yielded pairs) is therefore
  cheaper, which is why even the integer-only aggregate scan — which never
  materializes a heap payload — improved by a similar margin. This effect was not
  anticipated in the original framing, which attributed the win solely to avoided
  string copies.

* **Deadlock-free concurrency.** The memtable read lock is still released
  immediately after the snapshot is built, so readers never block writers and the
  read-cursor-plus-write transaction deadlock cannot occur. Regression-tested by
  `scan_iterator_does_not_block_concurrent_writers`.
* **No lifetime pollution.** The query execution pipeline stays `'static`; no
  lifetime parameter leaks into the physical operators.

### Negative
* **Atomic refcount overhead.** `Arc` clone/drop is cheap single-threaded, but
  cross-thread churn bounces the refcount cache line. This is negligible compared
  to heap allocation, but it is not free under heavily concurrent scans.
* **Up-front materialization remains.** As noted under Scope, large memtable
  ranges are still buffered into a `Vec` before the first row is yielded; this ADR
  does not make memtable scans lazy.
* **Wide construction-site refactor.** See the blast-radius note above, plus the
  serde `rc` prerequisite.

---

## Rejected and deferred alternatives

* **Zero-copy borrowing (`&'a DbKey`, `&'a DbValue`).** Rejected on Axis 1:
  holding the `RwLockReadGuard` for the scan duration blocks writers and
  self-deadlocks read-then-write transactions, and the `'a` lifetime contaminates
  the entire Volcano operator tree (see Decision → Axis 1).

* **Structural reference-counted snapshot (deferred).** Make the active memtable an
  `Arc<BTreeMap<...>>` (or a persistent/immutable map such as `im::OrdMap`). A scan
  would clone a single `Arc` to the frozen structure and iterate it lazily under no
  lock — giving an O(1) snapshot, no up-front `Vec`, *and* no lifetime pollution.
  This is the design that most literally matches "reference-counted snapshotting,"
  and it solves the up-front-materialization limitation that the chosen design
  leaves open. We are not adopting it now because it pushes copy-on-write cost onto
  the write path (every mutation either clones the structure or requires a
  persistent data structure), which is a larger change with its own throughput
  trade-offs. It remains the natural next step if up-front snapshot cost shows up
  in profiles for large ranges. The `Arc`-payload work here is complementary and
  not wasted if we later pursue this.

* **Coarse-grained table locks.** Holding a table lock for the whole scan would
  avoid both lifetimes and the deadlock, but would serialize readers against
  writers and degrade concurrent transaction throughput to near zero. Rejected.

---

## Verification

* **Benchmarks.** End-to-end results are in Consequences → Positive, backed by the
  `select_scan_text` case added to `dtdb_vs_sqlite` (which projects the heap-
  allocated string column the original int-only scan never touched). A
  `scan_iter`-level micro-benchmark in `storage_benchmark.rs` additionally tracks
  per-row traversal cost on the exact path this ADR optimizes — the prototype
  figure of ~4.1× was raw iteration in isolation, whereas the ~18% end-to-end
  number reflects traversal being only one component of full query execution.
* **Tests.** New tests lock in the invariants this change rests on: `Arc` buffer
  sharing via `ptr_eq` (guards against a future deep-copy regression), by-value
  ordering/equality, byte-identical serde format vs. `String`/`Vec<u8>`, bincode
  round-trips per variant, scan snapshot isolation under concurrent mutation, and
  the no-read-lock-held guarantee.

## Follow-ups

* **Structural snapshot** remains the natural next step if the up-front O(range)
  `Vec` materialization shows up in profiles for large ranges (see Rejected and
  deferred alternatives → structural reference-counted snapshot).
