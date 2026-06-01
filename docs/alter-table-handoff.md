# ALTER TABLE — work-in-progress handoff

This is a working handoff for the lazy `ALTER TABLE` feature. It is **not** an
ADR (the design of record is [ADR 0003](adr/0003-lazy-alter-table.md)); it's a
"where things stand / what's next" note so the work can resume in a fresh
session with local file access. Delete it once the feature lands.

## TL;DR

- **Branch:** `claude/dtdb-alter-table-Sss6W` (push target; do not push elsewhere).
- **Design of record:** `docs/adr/0003-lazy-alter-table.md` (status: Accepted).
- **Done:** commit 1 (foundation) + commit 2 (Phase 1 SQL: ADD / RENAME).
- **Next:** commit 3 (SSTable layout blob + `ValueRewriter`, all passthrough),
  then commit 4 (DROP COLUMN + the STW/compaction seam).
- **Scope decisions (locked):** ignore `ALTER COLUMN TYPE` entirely (future
  offline tool); lazy wherever the read-path cost is bounded; reject anything
  needing an O(rows) scan or a deep architectural change.

## The one-paragraph mental model

Rows are positional `Vec<DbValue>` bincode'd into an opaque `DbValue::Bytes`,
keyed by PK; nothing in a stored row records which schema produced it. Lazy
ALTER changes only metadata and makes the **read path** reconcile old rows
against the current schema. Every column now has a stable `id`; that id lets a
*stored layout* (the ordered column-ids a set of positional bytes corresponds
to) be described independently of the current column order. Phase 1 keeps the
"prefix invariant" (a stored row is always a prefix of the live schema, so
reconciliation is a pure tail-pad). Phase 2 attaches a layout descriptor to each
write-once SSTable and rewrites old→current layout during compaction, so dropped
columns physically erode on the normal compaction schedule instead of leaking
forever.

## What is DONE

### Commit 1 — `20a41d3` — stable ids + reconcile-on-read (foundation)

- `Column.id: u32` and `Schema.next_column_id: u32` (monotonic high-water mark).
  Ids are assigned at the `Schema::new` / `new_with_options` chokepoint
  (incoming columns renumbered `0..n`); callers never set ids themselves.
  Both use `#[serde(default)]`.
- `Schema::reconcile_row` (tail-pads a short row with per-column defaults / NULL)
  and `Schema::default_row_values` (full-width vec seeded with defaults).
- Wired into the read path so **no short row escapes a `Table` read**:
  - `dtdb_relational/src/database.rs` `TableScanIterator::advance` — single-group
    fast path now calls `reconcile_row`; multi-group merge path seeds with
    `default_row_values()` instead of bare NULL.
  - `merge_rows` (covers `get` / `multi_get` / `filtered_scan`) seeds with
    defaults.
- ~53 `Column { .. }` literals across src + tests gained `id: 0` (ephemeral
  query-output columns; id is meaningless for them).

### Commit 2 — `b5ccd2a` — Phase 1 SQL (ADD / RENAME)

- `dtdb_relational/src/database.rs`:
  - `wait_for_table_quiescent(table_name)` — factored-out spin-wait shared by
    the new methods (the existing create_index/drop_index/drop_table loops were
    left inline; consolidating them is optional cleanup).
  - `alter_table_add_column(table, Column)` — COW via `Arc::make_mut`, assigns
    the stable id, validates, quiesces, atomic `schema.bin` swap, version bump,
    resets the cloned `relative_indices` cache. O(1) STW, no row rewrite.
  - `alter_table_rename_column(table, old, new)` — metadata-only; also rewrites
    matching `IndexDefinition.columns` entries.
- `dtdb_sql/src/planner.rs`: `SqlStatement::AlterTable { table_name, op }`,
  `enum AlterOp { AddColumn(Column), RenameColumn { old_name, new_name } }`,
  the `Statement::AlterTable` arm (one operation per statement; DROP/ALTER
  COLUMN return an explicit "Unsupported ALTER TABLE operation" error), and the
  `column_def_to_column` helper (maps a sqlparser `ColumnDef`; rejects PK).
- `dtdb_sql/src/engine.rs`: `ExecutionResult::AlterTable`, dispatch in
  `execute_planned`, ALTER added to `substitute_params` (no-op) and `is_ddl`.
- `dtdb_sql/src/bin/dtdb_sql_cli.rs`, `dtdb_api/src/in_process.rs`,
  `dtdb_api/src/server.rs`: render the new result variant.
- `docs/sql_support.md`: ALTER TABLE section.
- Tests: 6 in `dtdb_sql/tests/sql_tests.rs`, 5 in
  `dtdb_relational/tests/relational_tests.rs` (incl. a multi-locality-group add
  through the merge path, and a survives-reopen durability check).

### Validation rules implemented (Phase 1)

- ADD: reject duplicate column name; reject NOT NULL without default on a
  non-empty table (allowed if empty); reject non-default locality group; reject
  PRIMARY KEY (in the planner).
- RENAME: reject missing source name; reject colliding target name; no-op rename
  (old == new) is accepted.

## What is NEXT

### Commit 3 — SSTable layout blob + `ValueRewriter` (ALL PASSTHROUGH)

Land the format change and the compaction hook as a **no-op** before any layout
ever differs, so regressions show up against known-good behavior under the
fuzz/TSAN targets.

- `dtdb_storage/src/sstable.rs`: add `layout: Vec<u8>` to `IndexBlock` (opaque to
  storage; round-tripped only). Thread it through `SstableWriter::create` /
  `finish_internal` and expose `SstableReader.layout`. **Backwards compat is
  explicitly dropped** — no `serde(default)` gymnastics, old `.sst` files need
  not read.
- `dtdb_storage`: define `trait ValueRewriter: Send + Sync { fn rewrite(&self,
  src_layout: &[u8], dst_layout: &[u8], value: &DbValue) -> Result<DbValue>; }`
  and inject it into the engine the same way `Arc<dyn Executor>` is
  (`open_with_executor`-style second arg). **Fast path: when
  `src_layout == dst_layout`, return `value.clone()` with no deserialize** —
  this is every SSTable until a layout actually diverges.
- `dtdb_storage/src/engine.rs` `compact_level` (~line 827): create the output
  writer with the engine's *current target layout*, and run each surviving
  merged value through the rewriter before `SstableWriter::append`. Snapshot the
  target layout at the **top** of `compact_level` (see seam below).
- Relational side: the blob is the bincode of `struct StoredLayout { column_ids:
  Vec<u32> }`; the `ValueRewriter` impl lives in `dtdb_relational` (this is the
  deliberate layering inversion the ADR signs off on) and maps src→dst positions
  by column id (defaults for ids new in dst, drop ids absent from dst).
- For commit 3 specifically, target always equals source, so the rewriter is
  pure passthrough and `StoredLayout` is just "current columns in order."

### Commit 4 — DROP COLUMN + the STW/compaction seam

- `Database::alter_table_drop_column`: remove the column (retire its id; never
  reuse), bump the engine target layout, **flush the memtable**, atomic
  `schema.bin` swap, version bump. Reads remap the dropped id out; compaction
  erases its bytes over time (self-healing).
- Planner: turn the DROP COLUMN arm from "unsupported error" into a real
  `AlterOp::DropColumn`.
- Phase 2 also generally requires the read path to remap by **layout** (not just
  tail-pad), because once DROP exists the prefix invariant no longer holds. Plan
  for `reconcile_row` to grow a layout-aware variant, or for rows to be
  normalized via the SSTable's `StoredLayout` at the storage boundary.

## The seam to keep in mind (STW swap vs. background compaction)

ALTER is stop-the-world at the **relational** layer (it quiesces
`active_table_access`), but compaction runs as a background task inside
`dtdb_storage` that is **not** a table accessor — so a compaction can be in
flight when the schema swaps. The contract that makes Phase 2 safe:

1. **Flush before publish.** ALTER calls `flush_memtable` inside the STW window
   (as `create_index` already does), so no positional bytes remain in a
   WAL/memtable without a file-level layout descriptor.
2. **Compaction snapshots its target layout at the top of `compact_level`** and
   tags its output with that snapshot. Invariant: "output is tagged with
   whatever target was current when this compaction started," never "the
   globally-latest layout." A compaction that started before the swap may
   legally emit a file tagged with the previous layout; a later compaction
   rewrites it forward. Sound because the id space is monotonic and every value
   is expressible in any later layout (this is exactly why ALTER TYPE is
   excluded).

`compaction_mutex` already serializes compactions; the target-layout handoff is
an independent cheap `Mutex<Vec<u8>>` swap.

## Gotchas / known issues to carry forward

- **Quiesce hang on a leaked transaction.** The ALTER quiesce spin-waits on
  `active_table_access`, and a `Transaction` that is constructed but not dropped
  (e.g. a shadowed `let tx = ...`) holds its reader slot forever, hanging the
  ALTER. This is the same latent issue as the existing TODO in
  `dtdb_relational/src/transaction.rs` (slots release on `Drop`, not on
  commit/rollback). Production callers go through `run_in_transaction` (drops
  promptly), so it is not a runtime regression, but **white-box tests must scope
  each transaction in a block** before a later ALTER — the existing `drop_table`
  tests do the same. Fixing it (release the slot on commit/rollback) would also
  bound the STW and is worth doing alongside Phase 2.
- **`relative_indices` cache.** `Schema::clone` copies the populated
  `OnceLock` group→index cache; any operation that changes the column set must
  reset it (`add_column` already does). Keep this in mind for DROP.
- **`dtdb_api` was not compiled in the cloud env** (no `protoc`). The two API
  match arms added in commit 2 (`in_process.rs`, `server.rs`) are eyeballed, not
  compiler-checked. **Run `cargo build -p dtdb_api` locally first.**

## How to verify locally (AGENTS.md checklist)

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
RUSTFLAGS="-D warnings" cargo test
./scripts/run_tsan.sh        # storage has background threads; matters for commit 3/4
./scripts/run_fuzz.sh        # exercises sstable_reader / wal_recovery; gate for commit 3
```

In the cloud session only the three core crates were exercised
(`-p dtdb_relational -p dtdb_sql -p dtdb_storage`); `dtdb_api` / `dtdb_fuzz`
need `protoc` installed. Locally, run the full workspace.

## Key files (quick map)

| Area | File |
|---|---|
| Schema, Column, ids, reconcile | `dtdb_relational/src/schema.rs` |
| Catalog, ALTER methods, scan iterator | `dtdb_relational/src/database.rs` |
| Row (positional bytes) | `dtdb_relational/src/row.rs` |
| Planner AST + ALTER arm | `dtdb_sql/src/planner.rs` |
| Execution dispatch, is_ddl, ExecutionResult | `dtdb_sql/src/engine.rs` |
| SSTable format (commit 3 target) | `dtdb_storage/src/sstable.rs` |
| Compaction loop (commit 3 hook) | `dtdb_storage/src/engine.rs` (`compact_level`) |
| Manifest / engine wiring | `dtdb_storage/src/manifest.rs`, `engine.rs` |
| Design of record | `docs/adr/0003-lazy-alter-table.md` |
| SQL docs | `docs/sql_support.md` |
