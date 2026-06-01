# ADR 0003: Lazy ALTER TABLE via stable column ids and self-healing SSTable layouts

- **Status:** Accepted
- **Date:** 2026-06-01
- **Deciders:** dtdb maintainers

## Context

dtdb supports `CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`, and `DROP INDEX`,
but not `ALTER TABLE`. We want to add column-level alterations — at minimum
`ADD COLUMN` and `RENAME COLUMN`, eventually `DROP COLUMN`.

Two properties of the current design frame the whole problem:

1. **Rows are positional and schema-version-free.** A `Row` is a
   `Vec<DbValue>` ([`row.rs`](../../dtdb_relational/src/row.rs)), bincode-
   serialized into a `DbValue::Bytes` and keyed by primary key. *Nothing in a
   stored row records which schema produced it* — column `i` is whatever
   `schema.columns[i]` currently says it is. The serialized bytes for a row are
   opaque to the storage layer, which only ever sees `DbKey -> DbValue::Bytes`.

2. **DDL already has a "stop-the-world" (STW) mechanism, and it is cheap.**
   `create_index` / `drop_index` / `drop_table`
   ([`database.rs`](../../dtdb_relational/src/database.rs)) all follow the same
   shape: write-lock the catalog (`tables.write()`), copy-on-write the affected
   `Table` via `Arc::make_mut` (so in-flight transactions keep their old
   `Arc<Table>` snapshot), spin-wait until the table has no active readers
   (`active_table_access`), make any new on-disk data durable, then atomically
   swap `schema.bin` and bump `schema_version` to invalidate the plan cache.

The maintainer constraint is explicit: **a short STW is fine; a multi-hour STW
is not.** An eager ALTER that rewrites every row inside the STW window is
O(rows) and unacceptable on large tables. We therefore want alterations to be
**as lazy as possible** — change metadata, keep the STW O(1), and let the read
path reconcile already-stored rows against the current schema.

Two operations are explicitly **out of scope** for this ADR:

- **`ALTER COLUMN TYPE` and tightening nullability on a populated table.** These
  require an O(rows) validate/rewrite no matter what. Deferred to a future
  offline-only tool.
- Anything requiring per-row type coercion. The lazy scheme below is only sound
  because every layout transition (add / drop / rename) is a *total function* on
  a monotonic column-id space — old bytes plus the new layout plus per-column
  defaults are always sufficient. Type changes break that property, which is
  exactly why they are excluded.

### The lazy-reconciliation wrinkle

Making the read path reconcile stored rows against the current schema is the
core of the work. The `TableScanIterator` single-locality-group **fast path**
([`database.rs`](../../dtdb_relational/src/database.rs), `advance`) returns
`Row::from_bytes(&bytes)` *directly* as the full row. If `ADD COLUMN` only
changes metadata, old rows decode with `N` values while the schema has `N+1`
columns, and everything downstream that indexes `row.values[i]` is wrong or
out of bounds. (The multi-group merge path already null-fills to
`columns.len()`, so it tolerates trailing-missing columns by accident — but the
fast path does not.) Lazy ALTER therefore requires turning the read path into a
deliberate schema-reconciling step.

### The DROP COLUMN reclamation problem

A naive lazy `DROP COLUMN` keeps the dropped column's byte position reserved
forever (a "positional tombstone") so the remaining columns stay aligned. That
is cheap at drop time but is a **permanent space ratchet**: the dead bytes live
*inside* each row's opaque value, below the layer that does any garbage
collection. LSM compaction merges whole `key -> value` entries and never
reinterprets value bytes, so it copies the dead column forward verbatim,
forever. Even an ordinary `UPDATE` re-serializes the row from the current
schema, which still lists the (tombstoned) column, so the slot is preserved.
The only things that reclaim the space are a full-table rewrite (the deferred
offline tool) or a design where the serialized value can legitimately omit the
dropped column. We want DROP's dead data to **fade on the normal compaction
schedule** rather than persist.

## Decision

Adopt a two-phase design. **Phase 1** makes `ADD` / `RENAME` lazy with an O(1)
STW and a reconciling read path. **Phase 2** adds a per-SSTable layout
descriptor and a compaction-time value rewriter, so that old layouts — and in
particular dropped columns — are physically erased as SSTables are rewritten by
normal compaction, with no central registry to garbage-collect.

### Stable column ids (the load-bearing primitive)

Every column gets a **stable `id: u32`** that is never reused within a table.
`Schema` carries a monotonic `next_column_id` high-water mark. Ids are assigned
by the catalog at the single chokepoint where a catalog schema is built
(`Schema::new` / `new_with_options` normalize incoming columns to `0..n` and set
`next_column_id = n`); `ADD COLUMN` mints `next_column_id` and increments it;
`DROP COLUMN` retires an id permanently.

Crucially, **rows stay positional** — we do *not* tag individual rows with ids.
The id is metadata that lets a *stored layout* (the ordered list of column-ids a
set of positional bytes corresponds to) be described independently of the
*current* `schema.columns` order. Most `Column` values in the codebase are
ephemeral query-output columns (projection/join output schemas, the EXPLAIN
"Query Plan" schema); their id is meaningless and simply defaults to 0 — only
columns persisted in `schema.bin` ever participate in id-based reconciliation.

### Phase 1 — pad-on-read, O(1) STW

- **Read-path reconciliation, contained to the storage boundary.** No
  short/mismatched row escapes `Table`'s read methods. A single
  `Schema::reconcile_row` helper normalizes a stored row to current full width,
  filling absent trailing columns with their `default_value` (or `NULL`). It is
  applied at the merge point (`merge_rows`, covering `get` / `multi_get` /
  `filtered_scan` / the multi-group scan path) and in the single-group scan
  fast path. Under Phase 1 the prefix invariant holds (stored rows are always a
  prefix of `columns`), so reconciliation is a pure tail-extend.

- **`Database::alter_table_add_column` / `alter_table_rename_column`** mirror
  `create_index`'s skeleton minus the engine/index build: `tables.write()` →
  `Arc::make_mut` → quiesce on `active_table_access` → atomic `schema.bin` swap
  → publish in-memory → `schema_version.fetch_add(1)`. No new data is written,
  so the single atomic `schema.bin` rename is the entire durability story, and
  the STW is O(1).

- **Validation.** Reject `ADD COLUMN NOT NULL` with no default on a non-empty
  table (no value to give existing rows); reject name clashes; reject `RENAME`
  onto an existing name. `RENAME` also fixes up `IndexDefinition.columns` and
  any `locality_group` references in lockstep.

- **SQL wiring.** A new `SqlStatement::AlterTable { table, op }` with
  `AlterOp::{AddColumn, RenameColumn}`, a `Statement::AlterTable` arm in
  `LogicalPlanner::plan` (returning an explicit "not yet supported" error for
  `DropColumn` / `AlterColumn` rather than a silent fallthrough), a dispatch arm
  in `execute_planned`, a new `ExecutionResult::AlterTable`, and adding
  `Statement::AlterTable` to `is_ddl` so ALTER is rejected inside transactions
  like every other DDL.

### Phase 2 — per-SSTable layout descriptor + compaction rewriter

This is where we deliberately break the layering rule that *a lower-level crate
never depends on a higher-level one*. `dtdb_storage`'s compaction must turn
old-layout value bytes into current-layout value bytes, which is relational
knowledge. We accept the inversion because the alternative (teaching storage the
schema, or re-implementing row reconciliation below the relational layer) is
worse, and the coupling is expressed as a single injected trait object,
precedented by the existing `Arc<dyn Executor>` injection.

- **SSTables self-describe their layout.** `IndexBlock`
  ([`sstable.rs`](../../dtdb_storage/src/sstable.rs)) gains a `layout: Vec<u8>`
  field — **opaque to storage**, which only round-trips it. The relational layer
  defines its meaning (the bincode of an ordered `Vec<u32>` of column-ids that
  the file's positional rows correspond to) and is the only code that decodes
  it. This keeps the layering inversion to exactly one place (the rewriter
  callback) and keeps the SSTable format schema-agnostic.

- **`ValueRewriter` injected into the engine.** A
  `trait ValueRewriter: Send + Sync { fn rewrite(&self, src_layout: &[u8],
  dst_layout: &[u8], value: &DbValue) -> Result<DbValue>; }` is injected like
  the executor. The relational implementation maps `src` positions to `dst`
  positions by column-id (defaults for ids new in `dst`, drop for ids absent
  from `dst`) and reserializes. **Fast path:** when `src_layout == dst_layout`
  the rewrite is a `clone` with no deserialization — which is every SSTable of
  every table nobody has altered, so steady-state overhead is a slice compare.

- **Compaction rewrites forward.** `compact_level`
  ([`engine.rs`](../../dtdb_storage/src/engine.rs)) creates its output writer
  with the engine's *current target layout* and runs each surviving merged value
  through the rewriter before `append`. Old layouts physically disappear as their
  SSTables are rewritten downward; a dropped column's bytes erode on the normal
  compaction cadence and are gone once the table fully compacts. This is the
  self-healing property; there is no central layout→file registry to GC because
  the descriptor lives in the file and dies with it.

- **DROP COLUMN** then falls out: remove the column from `schema.columns`
  (id retired), bump the engine target layout, flush the memtable, atomic
  `schema.bin` swap, version bump. Reads remap the dropped id out; compaction
  erases it over time.

### The STW-swap vs. background-compaction seam

ALTER is STW at the *relational* layer (quiesced `active_table_access`), but
compaction runs as a background task inside `dtdb_storage` that is **not** a
table accessor, so a compaction can be in flight when the schema swaps. The
contract that makes this safe:

1. **Flush before publish.** ALTER calls `flush_memtable` as part of the STW
   window (as `create_index` already does for an analogous durability reason),
   so no positional bytes remain in a WAL/memtable without a file-level layout
   descriptor. Every old-layout byte lives in an SSTable with an explicit blob.
2. **Compaction snapshots its target layout at the top of `compact_level`** and
   tags its output with that snapshot. The invariant is "output is tagged with
   whatever target was current when this compaction started," never "tagged with
   the globally-latest layout." A compaction that started before the swap may
   legally emit a file tagged with the *previous* layout; a later compaction
   re-rewrites it forward. This is sound precisely because the id space is
   monotonic and every value is expressible in any later layout.

`compaction_mutex` already serializes compactions; the target-layout handoff is
an independent cheap `Mutex<Vec<u8>>` swap. The only ordering rule is
flush-before-publish in ALTER, for which the relational STW already provides a
clean window.

## Consequences

### Positive

- `ADD` / `RENAME` ship with an O(1) STW and no row rewrites — the laziness the
  constraint demands.
- `DROP COLUMN`'s dead bytes self-heal on the normal compaction schedule instead
  of becoming a permanent space ratchet. No registry to garbage-collect.
- Steady-state overhead is a slice compare: every unaltered table's SSTables are
  passthrough in the rewriter, and the read path's reconciliation is a length
  check that only does work on legacy-width rows.
- Stable column ids are a reusable primitive (they also make future column
  reorder / re-add well-defined).

### Negative / costs

- **We break the strict layering rule** that a lower crate never depends on a
  higher one: `dtdb_storage` compaction calls a relational-supplied
  `ValueRewriter`. Contained to one injected trait object and one opaque
  `Vec<u8>` on the SSTable; storage still never names `Schema`/`Column`.
- The SSTable format change and the compaction hook land in the most
  safety-critical, most-fuzzed code in the repo (`sstable_reader`,
  `wal_recovery`, compaction, TSAN). This is why Phase 2 lands the format change
  and rewriter as a **no-op passthrough first** (target always equals source),
  proving the plumbing against known-good behavior before any layout diverges.
- The read-path reconciliation must be airtight: no code path may read
  `row.values[i]` assuming full width on a possibly-short legacy row. Normalizing
  at the storage boundary is what de-risks this; an audit of the executor/expr
  layer for direct positional indexing on freshly-read rows is the gate before
  merging Phase 1.

### On-disk format changes

dtdb is pre-release and we **explicitly drop backwards compatibility**. The
`schema.bin` format changes (columns gain `id`, schema gains `next_column_id`)
and the SSTable `IndexBlock` gains `layout`; old SSTables and schemas written by
previous code are **not** readable, and we add no migration shims. A clean format
beats a compatibility layer at this stage (consistent with ADR 0001).

## Rejected alternatives

- **Eager ALTER (rewrite all rows in the STW window).** Simplest semantics —
  the read path stays untouched because stored rows always match the live schema
  — but the STW is O(rows), violating the "no multi-hour STW" constraint. Kept
  only as the model for the deferred offline `ALTER COLUMN TYPE` tool, which is
  inherently O(rows) anyway.

- **Positional tombstones for DROP COLUMN** (keep the dropped column's slot
  reserved forever). O(1) at drop time, but a permanent space ratchet that
  compaction never reclaims (see Context). Rejected because "forever artifact"
  is exactly what we are trying to avoid; the per-SSTable layout descriptor gives
  the same O(1) drop *with* self-healing reclamation.

- **Id-tagged rows (self-describing rows).** Make each stored row carry its
  column ids so it is self-describing. Rejected: it inflates every row on disk
  and rewrites the hot serialization path, when attaching one descriptor per
  *SSTable* (write-once by definition) achieves the same reconciliation at the
  file granularity for negligible cost.

- **Teach `dtdb_storage` about `Schema` directly** (push the relational schema
  down so storage can reconcile without a callback). Rejected as a deeper and
  more permanent layering violation than a single opaque blob plus one injected
  rewriter trait; it would entangle the storage format with relational types.

## Implementation sketch (suggested order)

1. **Stable ids + reconcile-on-read** (this commit): add `Column.id` and
   `Schema.next_column_id`, assign ids at the `Schema::new` chokepoint, add
   `Schema::reconcile_row`, and wire it into the merge point and the single-group
   scan fast path. No SQL surface yet; testable through the relational API.
2. **Phase 1 SQL**: `ADD` / `RENAME` end-to-end — planner arm, dispatch,
   `is_ddl`, `ExecutionResult`, validation, and tests (old-row padding,
   mixed-width scan, NOT-NULL rejection, crash/reopen after add,
   snapshot-isolation old-`Arc` visibility).
3. **SSTable `layout` blob + `ValueRewriter`, all passthrough** (target always
   equals source). Lands the format change and the compaction hook as a no-op,
   proven under the existing fuzz/TSAN targets before any layout differs.
4. **DROP COLUMN**: real (non-passthrough) rewrites plus the STW/compaction seam
   tests (alter while a compaction is mid-flight; reopen mid-erosion).

Each step is a self-contained, independently shippable commit.
