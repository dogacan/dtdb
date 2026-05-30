# ADR 0001: Unified persistence for non-LSM metadata files

- **Status:** Accepted — implemented
- **Date:** 2026-05-30
- **Deciders:** dtdb maintainers

## Context

Outside the LSM-tree data path, dtdb persists a handful of metadata and log
files. Each was written by hand, at a different time, and they have drifted into
**four distinct durability patterns** of varying quality. The full inventory:

| File | Scope | Write pattern today | Frequency | Atomic? | Checksummed? |
|---|---|---|---|---|---|
| `wal.log` | per engine | append-only, `len + xxh32 + bincode` | per write | n/a | ✅ |
| sstables | per engine | immutable, write-tmp → fsync → rename | flush / compaction | ✅ | (block CRCs) |
| `manifest.bin` | per engine | **full rewrite**, tmp → fsync → rename → dir-fsync | every flush / compaction | ✅ | ❌ |
| `transactions.log` | per db | append-only, `len + bincode` | per txn | n/a | ❌ |
| `schema.bin` | per table | full rewrite, tmp → rename | DDL | ✅ | ❌ |
| `statistics.bin` | per table | **full rewrite, bare `fs::write`** | `create_table`, `ANALYZE` | ❌ | ❌ |
| `db_options.bin` / `options.bin` | db / engine | bare `fs::write`, **write-once** | never mutated | ❌ | ❌ |
| spill files | query exec | temp, deleted after use | per spill | n/a (ephemeral) | ❌ |

Concrete problems this has created:

1. **`transactions.log` has no checksums** ([`database.rs`](../../dtdb_relational/src/database.rs)
   `append_record`). Recovery stops at a truncated tail, but a bit-flip in the
   middle of a frame is silently deserialized. The storage `wal.log`
   ([`wal.rs`](../../dtdb_storage/src/wal.rs)) solved this exact problem with an
   `xxh32` per frame — `transactions.log` reimplemented the framing without it.

2. **`statistics.bin` is written non-atomically** with a bare `fs::write`
   ([`database.rs`](../../dtdb_relational/src/database.rs) `analyze_table` /
   `create_table`) — no temp file, no fsync, no rename. A crash mid-write leaves
   a torn file that `bincode` rejects on the next open. `schema.bin`, written by
   the same layer a few lines away, *does* use atomic tmp+rename. Two "full
   rewrite" files, two different safety levels.

3. **`manifest.bin` is reloaded from disk on every mutation.** The engine does
   `Manifest::load` → mutate → `Manifest::save` on every flush and every
   compaction ([`engine.rs`](../../dtdb_storage/src/engine.rs), three call
   sites). That is a full read-modify-write of the whole manifest, plus two
   fsyncs and a rename, for what is logically a one-line edit ("sstable (L,id)
   added" / "removed"). The live state is already in memory under
   `manifest_mutex`; the disk round-trip is pure overhead.

4. **No shared vocabulary.** Each file invents its own framing, atomicity story,
   and recovery loop. Fixing a durability bug means finding and fixing it in N
   places (see problems 1 and 2, which are the *same* class of bug in two
   files).

This is **not primarily a performance problem.** The metadata files are small
and most are written infrequently. The motivation is **architectural
consistency and correctness**: one well-tested implementation of "append a
record" and "atomically replace a snapshot," instead of four hand-rolled ones.
A modest efficiency win (eliminating the per-mutation manifest reload) falls out
for free, and future optimizations (e.g. block-aligned padding, group commit on
the manifest log) become a single-place change rather than four.

### Prior art

This is the classic **"checkpoint + redo log"** / **"snapshot + delta log"**
pattern. The canonical real-world instance is exactly our manifest case:
**RocksDB's `MANIFEST`** is an append-only log of `VersionEdit` records (sstable
added at level L / sstable deleted), with a `CURRENT` file naming the live
manifest, periodically compacted by writing a fresh snapshot. We are
deliberately copying that design for the manifest.

## Decision

Introduce **three layered primitives** in `dtdb_storage`, and migrate the
existing files onto them. The layering matches a simple taxonomy: everything we
persist is either a *snapshot*, a *log*, or *both with compaction*.

### Layer 0 — `atomic_write` (snapshot replacement)

A single function that replaces a whole file durably:

```
write to <path>.tmp  →  fsync(tmp)  →  rename(tmp, path)  →  fsync(parent dir)
```

This is what `manifest.rs` and `schema.rs` already do correctly; we lift it into
one helper so every snapshot file gets the same guarantees.

- **Clients:** `schema.bin`, `statistics.bin` (**fixes problem 2**),
  `db_options.bin` / `options.bin`, and the snapshot half of the manifest.
- **Out of scope to change semantics:** these files stay full-rewrite. Layer 0
  only unifies *how* the rewrite is made durable.

### Layer 1 — `FramedLog` (append-only log)

The framing/recovery primitive, extracted from today's `wal.rs`:

- Frame = `len: u32` + `checksum: xxh32` + `bincode(payload)`.
- In-memory size counter (no `fstat` on the hot path).
- Configurable fsync policy (per-append vs. interval — already in `wal.rs`).
- Recovery replays frames and **stops at the first truncated / checksum-
  mismatched / undeserializable frame** (already implemented and unit-tested in
  `wal.rs::recover_from_reader`).
- Generic over the payload type `E: Serialize + DeserializeOwned`.

- **Clients:** `wal.log` (the existing implementation *is* this), and
  `transactions.log` (**fixes problem 1** — gains checksums by construction).
  The checksummed framing is the **only** on-disk format; we do not read the
  legacy unchecksummed format. dtdb is pre-release and we prefer a clean format
  over a compatibility shim (see "Consequences → On-disk format changes").
- **Note:** `transactions.log` stays a *plain* `FramedLog` client. Its
  checkpoint folds redo records into the storage engines and discards the log;
  its "snapshot" lives in the engines, not in a manifest-style snapshot file. It
  therefore does **not** use Layer 2 — see "Rejected alternatives."

### Layer 2 — `SnapshotLog<State, Edit>` (snapshot + delta + compaction)

Built on Layers 0 and 1. Models a piece of state maintained as `base snapshot +
replayed edits`, compacted periodically.

```rust
/// State that can be rebuilt from a base snapshot plus a stream of edits.
trait Snapshotable {
    type Edit: Serialize + DeserializeOwned;
    fn apply(&mut self, edit: &Self::Edit);
}

struct SnapshotLog<S: Snapshotable> {
    state: S,            // authoritative in-memory copy
    log: FramedLog<S::Edit>,
    generation: u64,
    // ... paths, fsync policy
}

impl<S: Snapshotable> SnapshotLog<S> {
    fn append(&mut self, edit: S::Edit) -> Result<()>;  // apply in-mem + FramedLog::append
    fn compact(&mut self) -> Result<()>;                // write new snapshot, start empty log
    fn open(dir: &Path) -> Result<Self>;                // load snapshot, replay log
}
```

**Crash-atomic compaction** (the hard part the naive sketch skips). A
monotonically increasing `generation` ties each snapshot to its log:

- On-disk layout: `snapshot.<gen>` + `log.<gen>` + a `CURRENT` pointer file
  (Layer 0 atomic write) naming the live `<gen>`.
- `compact()`: build the new snapshot in memory, `atomic_write` it as
  `snapshot.<gen+1>`, create an empty `log.<gen+1>`, then flip `CURRENT` to
  `<gen+1>` (the single atomic commit point), then unlink the old pair.
- `open()`: read `CURRENT`, load `snapshot.<gen>`, replay `log.<gen>`.

This closes the truncate-in-place crash window: there is never a moment where a
log can be replayed against a snapshot that already contains its edits, because
the log and snapshot are versioned together and committed atomically by the
`CURRENT` flip. (Today's `manifest.bin` avoids this only because it has no log
at all; `transactions.log` avoids it only by relying on idempotent LSM replay.)

- **Client:** `manifest` — and likely the *only* client. `Manifest` becomes the
  `State`; edits are `Add(level, id)` / `Remove(level, id)`. **Fixes problem 3:**
  the live `Manifest` stays in memory, a flush/compaction appends one small edit
  frame instead of reloading and rewriting the whole file. The manifest is
  compacted when its log exceeds a size/count threshold.

## Consequences

### Positive

- One implementation each of "append a framed record" and "atomically replace a
  snapshot." Durability bugs get fixed once.
- `transactions.log` gains checksums; `statistics.bin` gains atomicity — both
  for free, as a side effect of adopting the shared primitives.
- Manifest mutations drop from full read-modify-write to a single small append;
  the per-mutation `Manifest::load` disappears.
- Future optimizations (block-aligned padding for high-frequency append
  streams, group commit) live in one place. **Note we are not doing these now** —
  see "Non-goals."

### Negative / costs

- Net-new abstraction surface (three primitives) in `dtdb_storage`. Mitigated by
  the fact that two of the three are near-mechanical extractions of code that
  already exists.
- **On-disk format changes.** dtdb is pre-release, so we take backwards-
  incompatible changes deliberately, favoring a clean format over compatibility
  shims. Two formats change:
  - **`transactions.log`**: new checksummed framing is the only format; the
    legacy format is not read. A database written by an older build is not
    recoverable — acceptable at this stage.
  - **`manifest.bin`**: single file → `snapshot.<gen>` + `log.<gen>` +
    `CURRENT`. No migration code; old layouts are not read. The manifest is in
    any case rebuildable from the sstable set on disk.
- Touching the recovery path is inherently risk-sensitive; needs crash-injection
  test coverage (we already have `dtdb_storage/tests/crash_safety_tests.rs` to
  extend).

### Non-goals (explicitly deferred)

- **Block-aligned / padded appends to dodge sub-page read-modify-write.** Real,
  but it does not remove the fsync latency that dominates a synchronous metadata
  write — and on macOS (`FsyncMethod::Fullfsync`, `F_FULLFSYNC`) reducing fsync
  *count* dominates reducing fsync *size*. Reach for group commit before block
  padding. The unified layer makes either a one-place change later.
- **Incremental statistics.** `statistics.bin` stays a recomputed snapshot
  (Layer 0). Turning it into a delta log is a separate project.
- **Schema as an edit log.** DDL is rare enough that atomic full-rewrite
  (Layer 0) is fine; no Layer 2 treatment.

## Rejected alternatives

- **A single `AppendOnlyBase` class for everything** (the original sketch). Two
  problems. (1) It bakes in a `map<key, value>` model, but the manifest is a
  *set* with add/remove and the txn log is a *sequence of records* — the shared
  thing is an opaque **edit**, not a KV pair. (2) It forces `transactions.log`
  and `manifest` under one `compact(base, appends) -> new base` signature, but
  their compaction semantics genuinely differ: the manifest produces a new
  compacted snapshot, while the txn log's checkpoint discards the log and folds
  records into a *different* subsystem (the engines). The three-layer split lets
  them share framing (Layer 1) without pretending their compaction is the same.

- **Leave `manifest.bin` as full-rewrite, only fix the two bugs (Layers 0+1
  only).** Cheapest, and it fixes both live bugs. **Rejected:** it leaves the
  per-mutation manifest reload (problem 3) in place and keeps the manifest on a
  fifth bespoke pattern. We want Layer 2 done — the shared `SnapshotLog` is what
  prevents the *next* such bug and is the architecturally consistent endpoint.

## Implementation sketch (suggested order)

1. **Layer 0 `atomic_write`** + migrate `statistics.bin` (fixes a live bug,
   smallest blast radius, no format change).
2. **Layer 1 `FramedLog`**: extract from `wal.rs`, keep `wal.log` behavior
   byte-identical, then migrate `transactions.log` onto it. This changes the
   `transactions.log` format (gains checksums); the new format is the only one
   read — no compatibility path.
3. **Layer 2 `SnapshotLog`** + migrate the manifest to the
   `snapshot.<gen>` + `log.<gen>` + `CURRENT` layout, with crash-injection
   tests. Old `manifest.bin` files are not read.

Each step is a self-contained commit, independently shippable and independently
valuable; together they complete the unification.
