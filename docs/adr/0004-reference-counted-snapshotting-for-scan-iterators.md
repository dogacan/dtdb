# ADR 0004: Reference-counted snapshotting for LSM scan iterators to avoid lifetime pollution and deadlocks

- **Status:** Proposed
- **Date:** 2026-06-01
- **Deciders:** dtdb maintainers

## Context

DuctTapeDB's storage engine (`dtdb_storage`) implements range scanning over its active `MemTable` and `SSTables`. In the initial design, range scanning is implemented using an **Option (A): Owned Iterator** pattern. 
Upon creating a `ScanIterator`, the storage engine:
1. Locks the `MemTable` via a read lock.
2. Performs a range query on the internal `BTreeMap`, cloning all keys and values in the range up front, collecting them into an owned `Vec<(DbKey, Option<DbValue>)>`.
3. Releases the lock immediately.
4. Performs additional copies during traversal inside `ScanIterator::next` (e.g., cloning keys to update the `last_key` deduplication tracking and cloning keys/values to yield owned types `(DbKey, DbValue)`).

While this design completely isolates the lifetime of the iterator from the locks of the underlying structures, it introduces a significant performance overhead (~50ns per row during in-memory scans), primarily due to:
* Upfront memory allocation and copying of every key-value pair in the scanned range.
* Continuous per-row cloning of keys and deserialized values during Volcano-style pull execution.

We want to optimize this hot path, especially for memory-resident datasets where all reads are served directly from the `MemTable` without hitting disk.

---

## Decision

Instead of pursuing a zero-copy borrowed iterator design that returns references (`&'a DbKey`, `&'a DbValue`), we will adopt a **reference-counted snapshotting design (Option 2)**. 

We will refactor the heap-allocated payloads of `DbKey` and `DbValue` (such as strings and byte vectors) to be wrapped in reference-counted smart pointers (e.g., `Arc<str>`, `Arc<[u8]>`, or `Arc<Vec<DbKey>>`). When constructing the `ScanIterator`, snapshotting the range will involve cloning the `Arc`s rather than deep-copying the underlying buffers. 

### Why we considered and rejected zero-copy borrowing (`&'a DbKey`)

We explicitly considered propagating lifetimes (`'a`) to make the scan iterator zero-copy, but rejected it due to two major architectural constraints:

1. **Concurrency Loss & Deadlock Risk**: 
   Because the `MemTable` is protected by an `RwLock`, returning borrowed references would require holding the `RwLockReadGuard` active inside the `ScanIterator` for the entire duration of the query execution. Since Volcano query plans consume rows pull-style, this read lock would remain open for a long time, blocking any concurrent writers trying to commit to the `MemTable`. Furthermore, in a multi-statement transaction, executing a read statement that holds a cursor while initiating a write would immediately **deadlock** the thread.
   
2. **Lifetime Contamination of the Volcano Engine**: 
   Propagating a lifetime parameter `'a` from the storage layer forces `ScanIterator<'a>` to pollute `TableScanIterator<'a>`, `TransactionScanIterator<'a>`, the adapter iterators, and eventually the physical query operators (`PhysicalSeqScan`, `PhysicalOperator` trait object, etc.). Resolving this requires parameterized Volcano operator trees (`Box<dyn PhysicalOperator + 'a>`), adding massive compiler friction, borrow-checker complications, and code complexity across the entire query engine.

---

## Consequences

### Positive
* **Performance Gain**: Benchmarks show that cloning `Arc` pointers instead of allocating and copying raw byte vectors provides a **~4.16x speedup** on row iteration (reducing scan traversal overhead from ~24.4 ns/row to ~5.9 ns/row).
* **Deadlock-Free Concurrency**: The read lock on the `MemTable` continues to be released immediately after iterator construction, ensuring readers do not block writers.
* **No Lifetime Pollution**: The query execution pipeline remains completely `'static`, avoiding lifetime parameters across the relational database and physical operators.

### Negative
* **Wrapper Overhead**: Wrapping fields in `Arc` adds minor pointer indirection and atomic ref-counting overhead during construction/clones, though this is negligible compared to heap allocations.
* **Refactoring Surface**: Requires minor updates to relational row serialization and key creation functions to construct and wrap values in `Arc`s.

---

## Rejected alternatives

* **Option 1: Zero-copy borrowing (`&'a DbKey`, `&'a DbValue`)**: Rejected due to lifetime propagation complexity and concurrency deadlocks (as detailed in Context).
* **Coarse-grained Table Locks**: Locking the table for the entire duration of a scan would avoid code lifetimes but degrade transaction throughput to near-zero for concurrent workloads.
