# Guide for AI Agents 🤖

Welcome! If you are an AI assistant contributing to **DuctTapeDB**, please follow these rules, guidelines, and constraints to ensure high-quality contributions and maintain codebase consistency.

---

## 🚦 Verification Checklist (Must Pass Before Commit)

Always run the following commands and verify they pass cleanly before proposing or committing any changes:

1. **Format and Lints**:
   Ensure the code compiles without warnings and passes all Clippy guidelines:

   ```bash
   cargo fmt --check
   cargo clippy --all-targets -- -D warnings
   ```

2. **Unit & Integration Tests**:
   Ensure all tests compile and pass:

   ```bash
   RUSTFLAGS="-D warnings" cargo test
   ```

3. **ThreadSanitizer (TSAN) Checks**:
   Since DuctTapeDB has multi-threaded operations in its storage engine (e.g., background compaction spawner), verify that no thread/concurrency issues are introduced:

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

### Known Design & Transaction Constraints

* **No DDL in Transactions**: DDL statements (e.g., `CREATE TABLE`, `DROP TABLE`) are **strictly prohibited** within transactions. This is a primary architectural constraint.
* **Single-Statement Executions**: Standard query execution (`execute()`) rejects queries with multiple semicolon-separated statements. Multi-statement execution must be routed through the transaction API (`run_in_transaction` or gRPC transaction stream).

---

## 🎨 Code Style Guidelines

* **Preserve Documentation**: Maintain existing comments and docstrings unless explicitly asked to modify them.
* **Avoid Placeholders**: Never write placeholder code or comments like `// TODO: implement later` unless authorized. Keep all implementations complete.
* **Strict Type Safety**: Use strongly-typed wrapper structures such as `DbKey` and `DbValue` rather than generic collections when passing keys or row columns around.
