# ADR 0002: Separating In-Process (Synchronous) and Remote (Asynchronous gRPC) Client APIs

- **Status:** Proposed
- **Date:** 2026-05-31
- **Deciders:** dtdb maintainers

## Context

Today, [DuctTapeDbClient](file:///Users/dogacan/projects/dtdb/dtdb_api/src/client.rs) acts as a unified client façade that handles both in-process (embedded) and remote (gRPC) execution modes. To enable these modes to share code and call-sites, the entire API was designed as asynchronous:

- Methods like `execute_query` return an async gRPC stream.
- `run_in_transaction` takes an async closure that returns a `Future`.
- All calls execute within a Tokio runtime context.

Profiling of the in-process path (specifically using the `select_point_pk` benchmark) revealed that this unified async design imposes massive performance overhead. Out of a total `23.39 µs` execution time per query, the core engine execution takes only `3.74 µs` (16%). The remaining `19.48 µs` (83.2%) is spent on client-side and server-side wrapper overhead:

1. **Tokio Runtime Transitions & Thread Hops:** Entering and exiting the Tokio runtime (`rt.block_on`) and worker thread pool scheduling context switches (`spawn_blocking`) to keep async tasks alive.
2. **Asynchronous Streaming Channels:** Dynamically allocating and passing row packets across `tokio::sync::mpsc` channels between the local executor and the client façade.
3. **Protobuf Translation & Serialization:** Translating strongly typed relational values (`DbValue`, `Row`) into protobuf messages (`ParamValue`, `Row` strings) and back.

While remote network execution inherently requires async channels and serialization, in-process execution does not. Bypassing these layers entirely for embedded usage would restore pure engine performance.

## Decision

Formally split the client façade in [dtdb_api](file:///Users/dogacan/projects/dtdb/dtdb_api) into two separate, specialized client implementations:

1. **`InProcessClient` (Synchronous, Embedded)**
2. **`RemoteClient` (Asynchronous, gRPC)**

### 1. `InProcessClient` API Design

The embedded client will run entirely synchronously on the caller's thread, avoiding any async framework or network structures.

- **Synchronous Execution:** All client methods (`create_db`, `execute_query`, `run_in_transaction`) are synchronous (`fn` instead of `async fn`).
- **Thread Safety (`&self`):** Unlike gRPC clients which require mutability (`&mut self`) to handle channel sends, the in-process client operates directly on thread-safe database handles. Its methods take `&self`, meaning it can be wrapped in an `Arc` and shared concurrently across threads without lock contention.
- **Synchronous Transaction API:** `run_in_transaction` accepts a standard synchronous closure:
  ```rust
  client.run_in_transaction("db", |tx| {
      tx.execute_query(sql_query!("..."))?;
      Ok(())
  })?;
  ```
- **Memory-Efficient Streaming Iterator:** To prevent materializing massive tables in memory, `execute_query` for `SELECT` statements returns a streaming `Iterator<Item = Result<Row, Status>>` (wrapped in `InProcessQueryResult`).
- **Automatic Transaction Lifetime:** For auto-committed single statements, the streaming iterator manages the transaction:
  - On exhaustion (`next() -> None`), the iterator automatically commits the transaction.
  - On error or early drop (e.g., exiting a loop early via `break`), the iterator's `Drop` implementation rolls back the transaction.
- **Native Relational Values:** The iterator yields native relational `Row` and `DbValue` types directly, giving the caller type-safe access without string/protobuf formatting overhead.

### 2. `RemoteClient` API Design

The remote client maintains the current asynchronous API, communicating over network gRPC streams using Tokio and tonic.

---

## Consequences

### Positive

- **Performance Restoration:** Eliminates ~83% of client wrapper overhead for in-process query execution, enabling maximum throughput for embedded workloads.
- **Simplified Embedded Usage:** Callers using the database in-process no longer need a Tokio runtime context, resolving runtime transitions (`block_on` panics) and simplifying library integration.
- **Native Data Access:** In-process callers gain direct, type-safe access to native Rust `DbValue` fields without parsing stringified column values.
- **Strict Auto-Commit Safety:** Safe transaction disposal is guaranteed via the `Drop` implementation on the streaming row iterator.

### Negative / Costs

- **API Duplication:** Callers will no longer have a single unified client struct. Switching from an embedded database to a remote gRPC service will require adapting call sites to include `.await` and changing synchronous closures to `async move` futures. (The relational model and SQL dialect, however, remain identical).

---

## Rejected Alternatives

### Option 1: Add synchronous direct methods to the unified client

We considered adding sync methods (like `execute_query_sync`) alongside the existing async methods on `DuctTapeDbClient`.

- **Why rejected:** This keeps the unused async/gRPC dependencies and state, making the client structure bloated, and forces the client to carry `&mut self` on sync methods or use internal mutability.

### Option 3: Optimize the async in-process path

We considered keeping the async path but optimizing it by bypassing protobuf translation and tuning the channels.

- **Why rejected:** This does not solve the main bottleneck of Tokio runtime entry and thread context hops (`spawn_blocking`), which dominate the in-process latency.
