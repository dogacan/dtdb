# DuctTapeDB Project Plan

DuctTapeDB (`dtdb`) is a from-scratch, educational relational database written in Rust. It is designed to teach database design principles, emphasizing clean abstractions and highly readable code over raw performance.

---

## 1. Core Architecture

The database is built using a strict bottom-up structure. Each layer communicates only with the layer directly below it via function calls. Lower layers never initiate communication or reference higher layers.

```
┌────────────────────────────────────────────────────────┐
│                        Layer 4                         │
│                      RPC Server                        │
└───────────────────────────┬────────────────────────────┘
                            │ (gRPC / tarpc Calls)
                            ▼
┌────────────────────────────────────────────────────────┐
│                        Layer 3                         │
│         SQL Parser, Planner, Optimizer & Executor       │
└───────────────────────────┬────────────────────────────┘
                            │ (Volcano Physical Operators)
                            ▼
┌────────────────────────────────────────────────────────┐
│                        Layer 2                         │
│    Relational Mapping & Buffered Transaction Manager    │
└───────────────────────────┬────────────────────────────┘
                            │ (Primary-Key-based KV Ops)
                            ▼
┌────────────────────────────────────────────────────────┐
│                        Layer 1                         │
│                LSM-Tree Storage Engine                 │
└────────────────────────────────────────────────────────┘
```

### Layer 1: LSM-Tree Storage Engine
Layer 1 is a Log-Structured Merge (LSM) tree storage engine handling strongly-typed key-value storage.
- **Memtable**: Backed by a standard Rust `BTreeMap` protected by an `RwLock` for simplicity and thread safety.
- **Write-Ahead Log (WAL)**: Append-only binary log recording mutations (`Put` / `Delete`) for crash recovery.
- **SSTables (Sorted String Tables)**: Once the memtable fills up, it is written to disk as a sorted SSTable. To allow binary search without loading the entire file, SSTables contain an index block at the tail.
- **Compaction**: A simplified compaction strategy that merges SSTables and in-memory data, removing deleted keys and keeping files sorted.
- **Data Compression**: Optional block-level compression via the `lz4-flex` crate.
- **Operations**:
  - `Get(key) -> Option<Value>`
  - `Put(key, value)`
  - `Delete(key)`
  - `FilteredScan(key1, key2, filter_closure) -> Iterator`
  - `Compact()`

### Layer 2: Relational Mapping & Transactions
Layer 2 maps relational schemas and tables to Layer 1 storage tables.
- **Mapping Model (Row-Store)**: To ensure simplicity and robust transactional atomicity, each SQL table maps to exactly one Layer 1 table.
  - The Layer 1 **key** is the Primary Key.
  - The Layer 1 **value** is a serialized tuple/map of column values (e.g., Row).
- **Transactions (ACID)**:
  - **Isolation**: Serialized execution (single global transaction lock) simplifies concurrency control.
  - **Atomicity & Durability**: Implemented using a **Buffered Write** approach. When a transaction starts:
    1. Writes are buffered locally in memory (`TransactionContext`).
    2. Reads search the local buffer first, falling back to Layer 1 (Read-Your-Own-Writes).
    3. On `ROLLBACK`, the local buffer is discarded.
    4. On `COMMIT`, the buffered mutations are written to Layer 1 as a single atomic batch.
- **Operations**:
  - `Get(key, columns) -> Row`
  - `Put(key, column_value_map)`
  - `Delete(key)`
  - `FilteredScan(key1, key2, filter)`

### Layer 3: SQL Parsing, Planning, Optimization & Execution
Layer 3 handles SQL queries, parses them, optimizes execution, and runs physical plans.
- **Parser**: Offloaded to the external `sqlparser` crate, which converts SQL query strings into an Abstract Syntax Tree (AST).
- **Logical Planner**: Translates the SQL AST into a custom relational algebra tree (`LogicalPlan` enum) consisting of operations like `Scan`, `Filter`, `Join`, `Project`, and `Limit`.
- **Optimizer**: Applies rewriting rules to the logical plan (e.g., filter pushdowns, converting scans with primary keys to key-lookup range scans).
- **Executor (Volcano Iterator Model)**: Compiles the optimized logical plan into a tree of physical operators implementing a `next()` interface for streaming rows:
  ```rust
  pub trait PhysicalOperator {
      fn next(&mut self) -> Result<Option<Row>, DbError>;
  }
  ```

### Layer 4: RPC Server
Layer 4 is the client-facing boundary.
- Exposes a query execution endpoint over gRPC (via `tonic` / `prost`) or `tarpc`.
- Accepts SQL query strings and transaction commands, processes them sequentially, and returns tabular results.

---

## 2. Dependencies

To balance the "build-from-scratch" educational value with realistic engineering scopes, we restrict external crates to:
1. **`sqlparser`**: SQL parsing AST generation.
2. **`lz4-flex`**: Pure Rust LZ4 compression for SSTables.
3. **`tonic`** + **`prost`** (or **`tarpc`**): RPC server and Protobuf serialization.
4. **`serde`** + **`bincode`**: Internal serialization of rows and metadata.

---

## 3. Project Workspace Layout

```text
dtdb/
├── Cargo.toml            # Workspace configuration
├── docs/
│   └── project_plan.md   # This document
├── dtdb_storage/         # Layer 1: LSM storage engine
│   ├── Cargo.toml
│   └── src/
│       ├── memtable.rs
│       ├── wal.rs
│       ├── sstable.rs
│       └── lib.rs
├── dtdb_relational/      # Layer 2: Relational tables & Transaction manager
│   ├── Cargo.toml
│   └── src/
│       ├── transaction.rs
│       ├── schema.rs
│       └── lib.rs
├── dtdb_sql/             # Layer 3: Planner, Optimizer & Volcano executor
│   ├── Cargo.toml
│   └── src/
│       ├── parser.rs
│       ├── planner.rs
│       ├── optimizer.rs
│       ├── executor/     # Volcano physical operators
│       └── lib.rs
└── dtdb_server/          # Layer 4: RPC Server
    ├── Cargo.toml
    └── src/
        └── main.rs
```

---

## 4. Implementation Milestones

### Milestone 1: Layer 1 Core Storage
- [ ] Set up Cargo workspace.
- [ ] Implement Memtable (`BTreeMap` + `RwLock`).
- [ ] Implement WAL writer/reader for durability.
- [ ] Implement SSTable file format (with trailing index block).
- [ ] Implement crash recovery (WAL replay) and basic `Compact()`.

### Milestone 2: Layer 2 Relational Mapping
- [ ] Implement Row serialization and Schema definitions.
- [ ] Set up mapping of SQL tables to Layer 1 LSM tables.
- [ ] Implement table CRUD logic.

### Milestone 3: Transaction Management
- [ ] Design the transactional client context / buffer.
- [ ] Implement local buffering for writes.
- [ ] Implement transaction `COMMIT` and `ROLLBACK` logic.

### Milestone 4: Layer 3 Logical & Physical Planning
- [ ] Integrate the `sqlparser` crate.
- [ ] Build the Logical Planner translating AST into custom relational algebra.
- [ ] Build basic Volcano physical operators (`SeqScan`, `Filter`, `Project`, `Limit`).
- [ ] Connect SQL execution queries directly to the relational mapping engine.

### Milestone 5: Optimizer & Advanced Execution
- [ ] Implement filter pushdowns and primary-key index scan optimizations.
- [ ] Implement `HashJoin` physical operator.
- [ ] Implement `HashAggregate` physical operator for `GROUP BY` and aggregations (`COUNT`, `SUM`, etc.).

### Milestone 6: Layer 4 RPC Interface
- [ ] Implement the RPC server endpoint accepting SQL queries.
- [ ] Build a simple CLI client tool to query and manage the database.
- [ ] Write integration test suites validating end-to-end ACID transactions.
