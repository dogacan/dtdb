# Guide for AI Agents 🤖

Welcome! If you are an AI assistant contributing to **DuctTapeDB**, please follow these rules, guidelines, and constraints to ensure high-quality contributions and maintain codebase consistency.

---

## 🚦 Verification Checklist (Must Pass Before Commit)

Before proposing or committing any changes, run the appropriate verification checks:

1. **Format and Lints (Always Run)**:
   Ensure the code compiles without warnings and passes all Clippy guidelines:

   ```bash
   cargo fmt --check
   cargo clippy --all-targets -- -D warnings
   ```

2. **Unit & Integration Tests (Always Run)**:
   Ensure all tests compile and pass:

   ```bash
   RUSTFLAGS="-D warnings" cargo test
   ```

3. **ThreadSanitizer (TSAN) Checks (Run only when changes have possible thread safety/concurrency effects)**:
   Since DuctTapeDB has multi-threaded operations in its storage engine (e.g., background compaction spawner), if your work has possible concurrency errors or thread-safety implications, verify that no thread/concurrency issues are introduced:

   ```bash
   ./scripts/run_tsan.sh
   ```

---

## 🧪 Fuzz / Property Testing

The `dtdb_fuzz` crate (`dtdb_fuzz/`) hosts four `#[ignore]`d targets that
exercise the durability and concurrency surfaces with random inputs:

| Target | What it shakes out |
| --- | --- |
| `wal_recovery` (bolero) | `Wal::recover` panics, non-determinism, truncation safety |
| `sstable_reader` (bolero) | SSTable parsing under bit-flip / truncation / footer corruption |
| `concurrent_txns` (proptest + real threads) | OCC panics, deadlocks, read-your-writes |
| `sql_op_sequence` (proptest) | End-to-end durability across `flush_db` + reopen |

Targets are `#[ignore]`d so `cargo test` skips them. Run locally with:

```bash
./scripts/run_fuzz.sh                       # default PR-time budget
BOLERO_FUZZ_ITERATIONS=50000 PROPTEST_CASES=4096 ./scripts/run_fuzz.sh  # long run
```

CI runs this script in a dedicated `fuzz` job (see `.github/workflows/rust.yml`):
short budget on every push / PR, large budget on the nightly cron.

When adding a new target: drop a new `fuzz_targets/foo.rs` file, add a matching
`[[test]]` block to `dtdb_fuzz/Cargo.toml`, mark the entry test `#[ignore]`,
and append it to the `cargo test` invocation in `scripts/run_fuzz.sh`.

---

## ✍️ Git Commit Etiquette & AI Attribution

When formatting Git commit messages:

* Summarize the changes clearly.
* **Co-author Attribution**: If you are an AI assistant coauthoring or generating the changes, append the appropriate `Co-authored-by` trailer at the bottom of the commit message. For example, for **Antigravity**:

  ```text
  Co-authored-by: Antigravity <antigravity-noreply@google.com>
  ```

---

## 🏗️ Architectural Constraints

Keep the database's layered, bottom-up design in mind. Lower-level crates must never depend on higher-level crates:

1. **`dtdb_storage`** (LSM-Tree Key-Value Store)
2. **`dtdb_relational`** (Tabular Rows & Transactions)
3. **`dtdb_sql`** (AST Parsing, Query Planning, Optimization, and Physical Volcano execution)
4. **`dtdb_api`** (In-Process and gRPC Client API, Server, and SQL CLI prompt)

### Architecture Decision Records

Design decisions with lasting structural impact are recorded under
[`docs/adr/`](docs/adr/). Consult them before reworking the areas they cover, and
add a new numbered ADR when making a comparable decision.

* [ADR 0001 — Unified persistence for non-LSM metadata files](docs/adr/0001-unified-metadata-persistence.md):
  the three-layer scheme (`atomic_write`, `FramedLog`, `SnapshotLog` in
  `dtdb_storage`) behind every non-LSM metadata file. **Implemented.**
  Full-rewrite files (`statistics.bin`, `schema.bin`, options) go through
  `atomic_write`; append-only logs (the storage WAL, `transactions.log`) share
  the checksummed, header-versioned `FramedLog`; the manifest is a
  `SnapshotLog<Manifest>` stored as `manifest/CURRENT` + `snapshot.<gen>` +
  `log.<gen>`. On-disk formats changed incompatibly (pre-release, no migration).

### Known Design & Transaction Constraints

* **No DDL in Transactions**: DDL statements (e.g., `CREATE TABLE`, `DROP TABLE`) are **strictly prohibited** within transactions. This is a primary architectural constraint.
* **Single-Statement Executions**: Standard query execution (`execute()`) rejects queries with multiple semicolon-separated statements. Multi-statement execution must be routed through the transaction API (`run_in_transaction` or gRPC transaction stream).
* **Append-Only & Write-Once Persistence**: All database file writes must be strictly append-only. Once a file is closed, it must never be opened for write again. In-place file truncation (e.g., `set_len(0)` or `reset()`) is strictly prohibited. To clear/recycle logs or metadata:
  * For WAL segments and manifest logs (`log.<gen>`), roll over to a fresh numbered/generation segment and delete the old closed file.
  * For transaction logs (`transactions.log`), close the file descriptor, delete the old file from disk, and recreate it at the same path.

---

## 🎨 Code Style Guidelines

* **Preserve Documentation**: Maintain existing comments and docstrings unless explicitly asked to modify them.
* **Avoid Placeholders**: Never write placeholder code or comments like `// TODO: implement later` unless authorized. Keep all implementations complete.
* **Strict Type Safety**: Use strongly-typed wrapper structures such as `DbKey` and `DbValue` rather than generic collections when passing keys or row columns around.
