# ADR 0002: Separating In-Process (Synchronous) and Remote (Asynchronous gRPC) Client APIs

- **Status:** Accepted - implemented
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

Formally split the client façade in [dtdb_api](file:///Users/dogacan/projects/dtdb/dtdb_api) into two separate, specialized client implementations layered over a single shared execution core:

1. **`InProcessClient` (Synchronous, Embedded)**
2. **`RemoteClient` (Asynchronous, gRPC)**

### Shared Execution Core (anti-drift foundation)

The primary risk of splitting the client is that the two implementations drift apart in SQL semantics, transaction behavior, or error mapping. We avoid this by **not** writing two parallel implementations. Instead, both clients sit on top of one synchronous execution kernel.

This kernel already exists implicitly: both the current in-process path and the server RPC funnel through the same calls — `sql_engine.execute_with_params(...)` followed by `tx.commit()` / `tx.rollback()` ([client.rs:479](file:///Users/dogacan/projects/dtdb/dtdb_api/src/client.rs), [server.rs:427](file:///Users/dogacan/projects/dtdb/dtdb_api/src/server.rs)) — and only diverge afterward, when the native `ExecutionResult` is converted to protobuf via `execution_result_to_responses` ([server.rs:444](file:///Users/dogacan/projects/dtdb/dtdb_api/src/server.rs)). We make this layering explicit:

1. **One sync execution core** — engine call plus transaction lifecycle — that yields native `ExecutionResult` / `Row` / `DbValue`. This is the single source of truth for query semantics.
2. **`InProcessClient`** — a thin synchronous wrapper that returns native rows directly, with no protobuf or async machinery.
3. **`RemoteClient` (server side)** — an async adapter that calls the *same* core (via `spawn_blocking`) and applies `execution_result_to_responses` to serialize.

**Invariant (to be upheld going forward):** The only permitted differences between the two client APIs are (a) `async` / `.await`, (b) `&mut self` vs `&self`, and (c) native-relational vs protobuf row types. SQL semantics, transaction behavior, and error mapping must never differ — because they all originate from the shared core. Any change that would violate this is a signal to push logic down into the core rather than into one client.

> A parameterized conformance test suite that runs identical SQL against both clients and asserts identical results is the enforcement mechanism for this invariant. It is tracked as a fast follow-up to this change rather than a prerequisite.

### 1. `InProcessClient` API Design

The embedded client will run entirely synchronously on the caller's thread, avoiding any async framework or network structures.

- **Synchronous Execution:** All client methods (`create_db`, `execute_query`, `run_in_transaction`) are synchronous (`fn` instead of `async fn`).
- **Thread Safety (`&self`):** Unlike gRPC clients which require mutability (`&mut self`) to handle channel sends, the in-process client operates directly on thread-safe database handles. Its methods take `&self`, meaning it can be wrapped in an `Arc` and shared concurrently across threads. (Concurrency is then bounded by whatever synchronization the engine itself uses internally, not by the client wrapper.)
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
  - This applies to a *single auto-committed statement*, where rollback-on-early-exit is a no-op for the common `SELECT` case (rolling back a read changes nothing observable) and is all-or-nothing for a buffered write. It is not a substitute for `run_in_transaction`, which is the path for explicit multi-statement atomicity.
- **Native Relational Values:** The iterator yields native relational `Row` and `DbValue` types directly, giving the caller type-safe access without string/protobuf formatting overhead.
- **Error Type:** The in-process client continues to surface errors as `tonic::Status`, matching the remote client. Strictly, `Status` is a gRPC type and an embedded, Tokio-free client has no inherent need for it; introducing a transport-neutral error type would be cleaner. We deliberately keep `Status` for now because **error parity between the two clients directly serves the anti-drift goal** — the same error variants surface in both. Decoupling the error type is left as possible future work, not part of this split.
- **Ownership:** Because execution is synchronous and the closure cannot clone-and-escape a transaction handle across an `.await`, the in-process transaction path can hold the `Transaction` directly (by value / `&mut`) instead of the current `Arc<Transaction>` + `Arc::try_unwrap` dance ([client.rs:317-334](file:///Users/dogacan/projects/dtdb/dtdb_api/src/client.rs)). This removes the "Failed to acquire exclusive transaction ownership" runtime failure mode entirely.

### 2. `RemoteClient` API Design

The remote client maintains the current asynchronous API, communicating over network gRPC streams using Tokio and tonic. There is no behavioral change from today's async path; it is renamed from the unified façade and re-pointed at the shared execution core on the server side. It retains `&mut self` and `async fn` signatures.

---

## Consequences

### Positive

- **Performance Restoration:** Removes the Tokio, channel, and protobuf layers that account for ~83% (19.48 µs of 23.39 µs) of per-query latency on the `select_point_pk` benchmark today. The native iterator, transaction setup, and `Row`/`DbValue` construction are not free, so the realistic target is "approaching the 3.74 µs engine floor" rather than exact elimination; the `select_point_pk` number serves as the before/after regression baseline.
- **Simplified Embedded Usage:** Callers using the database in-process no longer need a Tokio runtime context, resolving runtime transitions (`block_on` panics) and simplifying library integration.
- **Native Data Access:** In-process callers gain direct, type-safe access to native Rust `DbValue` fields without parsing stringified column values.
- **Strict Auto-Commit Safety:** Safe transaction disposal is guaranteed via the `Drop` implementation on the streaming row iterator.
- **Simpler Transaction Ownership:** Dropping the async closure requirement lets the in-process path own the `Transaction` directly, removing the `Arc::try_unwrap` commit dance and its associated runtime failure mode.
- **Bounded Drift by Construction:** Because both clients are thin adapters over one shared execution core, divergence is structurally constrained to transport concerns (sync/async, native/proto) — not query or transaction semantics.

### Negative / Costs

- **API Duplication:** Callers will no longer have a single unified client struct. Switching from an embedded database to a remote gRPC service requires more than adding `.await`: call sites must adopt `&mut self` (the remote client requires mutability), wrap synchronous closures as `async move` futures, and adapt to protobuf row types instead of native `Row`/`DbValue`. (The relational model and SQL dialect, however, remain identical, and the shared core keeps semantics and error variants aligned.)
- **Two Surfaces to Maintain:** Adding or changing a client method now means touching both clients. The conformance test suite (fast follow-up) is what keeps this honest.

---

## Rejected Alternatives

The chosen approach above is **Option 2: two specialized clients over a shared execution core**. The alternatives considered were:

### Option 1: Add synchronous direct methods to the unified client

We considered adding sync methods (like `execute_query_sync`) alongside the existing async methods on `DuctTapeDbClient`.

- **Why rejected:** This keeps the unused async/gRPC dependencies and state, making the client structure bloated, and forces the client to carry `&mut self` on sync methods or use internal mutability.

### Option 3: Optimize the async in-process path

We considered keeping the async path but optimizing it by bypassing protobuf translation and tuning the channels.

- **Why rejected:** This does not solve the main bottleneck of Tokio runtime entry and thread context hops (`spawn_blocking`), which dominate the in-process latency.
