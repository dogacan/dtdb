# ADR 0006: Accelerating LIKE with prefix range scans and trigram-index intersection

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** dtdb maintainers

## Context

`LIKE` is currently always evaluated row-by-row. The planner has no rule for it:
`extract_bounds_for_column` ([`optimizer.rs`](../../dtdb_sql/src/optimizer.rs))
handles `Eq` / `Gt` / `GtEq` / `Lt` / `LtEq` but has no `Operator::Like` arm, and
the full-text path only triggers on a `MATCH` predicate
(`has_match_predicate_for_col`). So every `LIKE` compiles to a full scan with a
residual filter that runs `like_match` — a cached, anchored regex — against each
row ([`expr.rs`](../../dtdb_sql/src/expr.rs), `like_match` /
`like_pattern_to_regex`). On a large table that is O(rows) every time.

Two facts about the existing engine make a much better plan reachable without new
storage primitives:

1. **A FULLTEXT index is already a sorted token→pk map.** Index entries are
   `Composite([String(token), pk])` ([`transaction.rs`](../../dtdb_relational/src/transaction.rs),
   the FTS write path and `eval_fulltext_query`). A single-token lookup is the
   range scan `[Composite(token, min_pk), Composite(token, max_pk)]`, and
   `eval_fulltext_query` already resolves boolean queries by **posting-list
   intersection** (`And`) and **union** (`Or`) over those scans. That set-algebra
   primitive is exactly what an infix `LIKE` needs.

2. **Tokenizers are pluggable by name.** The index def carries an optional
   `tokenizer: Option<String>` ([`schema.rs`](../../dtdb_relational/src/schema.rs),
   `IndexDefinition`), resolved through a global registry
   ([`tokenizer.rs`](../../dtdb_relational/src/tokenizer.rs)). Only
   `SimpleTokenizer` (whitespace split + lowercase) exists today, but a new
   tokenizer is a registry insert plus a trait impl.

Three properties of dtdb's `LIKE` semantics constrain the design:

- **Case-sensitive.** `like_pattern_to_regex` emits `(?s)^…$` with **no** `(?i)`
  flag. `LIKE 'A%'` does not match `"abc"`.
- **`%` and `_` are the only wildcards, and there is no `ESCAPE`.** The planner
  destructures `SqlExpr::Like { negated, expr, pattern, .. }`
  ([`planner.rs`](../../dtdb_sql/src/planner.rs)) and discards the escape field via
  `..`. There is no way to match a literal `%`/`_`. This *simplifies* literal-run
  extraction: `%` and `_` are unconditionally special, everything else is literal.
- **Statistics today are per-index aggregates, not distributions.** `ANALYZE`
  (`analyze_table`, [`database.rs`](../../dtdb_relational/src/database.rs)) scans
  every index and records `IndexStats { entry_count, unique_values,
  avg_rows_per_value }`. For an FTS index `unique_values` is the distinct-trigram
  count and `avg_rows_per_value` is the *average* posting-list length. An average
  is the one statistic that cannot see the violent frequency skew of trigrams
  (`the` ≫ `zxq`), which is precisely what determines whether an infix plan is a
  win or a disaster.

### Prefix vs. infix are two different problems

The motivating example `LIKE 'ab%'` is a **prefix** match, and the right tool for
it is *not* a trigram index — it is an ordinary ordered-index **range scan**
(`'ab' <= col < 'ac'`), which is exact (no false positives, no recheck) and needs
no tokenizer at all. A 2-character prefix cannot even form a trigram, so a trigram
index would not help that query regardless.

Where a trigram index is *uniquely* able to help is the **infix** case
(`LIKE '%foobar%'`): a leading `%` destroys prefix anchoring, so an ordered index
is useless, but `"foobar"` decomposes into the trigrams `{foo, oob, oba, bar}`,
each an exact token lookup, intersected via the path that already exists.

### Why "detect an active tokenizer" is the wrong trigger

Soundness is a property of the **specific tokenizer**, not of "a tokenizer being
configured on the column":

- `SimpleTokenizer` tokenizes whole words, so it cannot answer `LIKE '%ell%'` at
  all — there is no `ell` token for `"hello"`.
- Any **lossy** normalization (punctuation stripping, stemming, accent folding)
  produces **false negatives**: a tokenizer that drops `.` never indexes the
  trigram `a.b`, so `LIKE '%a.b%'` would return an empty result even when rows
  match. That is a correctness bug, not a missed optimization.

The optimization is sound only if, for every string that LIKE-matches the pattern,
**every required token of the pattern is guaranteed present in the index**. That
is a contract a tokenizer either offers or does not — the planner must *ask*, not
sniff a name.

## Decision

Three independent pieces, shippable in order. Pieces 1 and 2/4 are orthogonal;
piece 3 is what keeps piece 4 from backfiring.

### 1. Prefix `LIKE` → range scan (no tokenizer)

Teach `extract_bounds_for_column` (and `extract_key_range` for the primary key) an
`Operator::Like` arm. When the right-hand side is a literal whose first wildcard
(`%` or `_`) is preceded by a non-empty literal run `P`, emit the half-open range
`[P, P⁺)` where `P⁺` increments `P`'s last code point (the same successor trick
already used for `Gt` on strings, which appends `\0`). The residual `LIKE` filter
**stays** above the scan, because the range is a superset for patterns like
`'ab%c'` (range `[ab, ac)` is correct for the prefix, but `%c` still must be
checked). For a pure-prefix pattern `'ab%'` the range is exact, but keeping the
filter unconditionally is simpler and always correct.

This is exact, needs no FTS index, and serves the motivating example. It is
independent of everything below.

### 2. A `TrigramTokenizer` and a tokenizer **capability contract**

Add a `TrigramTokenizer` (character 3-grams, lowercased to match the only existing
normalization convention; padding for prefix/suffix anchoring is optional and can
come later). Crucially, add a capability the planner can query rather than a name
it sniffs — an optional trait method, e.g.:

```rust
/// Returned by a tokenizer that can soundly accelerate LIKE.
struct LikePlan {
    /// Tokens that must all be present (ANDed) for a row to possibly match.
    required: Vec<String>,
    /// Always true for trigram acceleration: token membership is necessary,
    /// not sufficient, so the candidate rows must be re-checked with like_match.
    needs_recheck: bool,
}

trait Tokenizer {
    fn tokenize(&self, text: &str) -> Vec<String>;
    /// Compile a LIKE pattern into a sound token query, or None if this
    /// tokenizer cannot accelerate it (default: None).
    fn plan_like(&self, _pattern: &str) -> Option<LikePlan> { None }
}
```

`SimpleTokenizer` keeps the default (`None`). `TrigramTokenizer::plan_like` splits
the pattern into maximal literal runs (terminated by **both** `%` and `_`), and
for each run of length ≥ 3 emits its trigrams; runs shorter than 3 contribute no
token. If **no** run yields a trigram (e.g. `'%ab%'`, `'%a%b%'`), it returns
`None` and the planner falls back to a full scan. The pattern's trigrams are
lowercased to probe the lowercased index; the case-sensitive `like_match` recheck
then removes the over-matches this introduces.

### 3. Most-common-values (MCV) statistics, to defuse the cost footgun

A naive infix plan for `LIKE '%the%'` intersects enormous posting lists and is
slower than a full scan. The intersection size is bounded by the **rarest**
required trigram, so the planner must be able to ask "is at least one required
trigram selective?" — which `avg_rows_per_value` cannot answer.

Extend `IndexStats` with a bounded most-common-prefix list:

```rust
pub struct IndexStats {
    pub entry_count: u64,
    pub unique_values: u64,
    pub avg_rows_per_value: f64,
    /// Top-K key-prefixes by frequency, descending. The "prefix" is the index
    /// key minus its trailing pk — exactly what `unique_values` already dedups
    /// on. For a 1-column FTS index it is a `[String(token)]`, i.e. a
    /// trigram→frequency table; for a B-tree index, an MCV list on the value.
    pub most_common: Vec<McvEntry>,
}
pub struct McvEntry { pub prefix: Vec<DbKey>, pub count: u64 }
```

`analyze_table`'s existing index scan already groups by this prefix into a
`HashSet`; swap that for a `HashMap<Vec<DbKey>, u64>` of counts (same cardinality,
so no worse on memory), then `select_nth_unstable` + `truncate` to the top `K`
(≈256). The planner reads it via the `IndexStats` it *already* fetches in
`estimate_index_scan_cost`. Frequency of a non-MCV prefix is bounded above by the
least-frequent MCV entry (anything more frequent would be in the list), so a
trigram absent from the MCV list is treated as selective.

Because statistics are postcard-serialized and loaded with
`if let Ok(stats) = postcard::from_bytes::<TableStatistics>(…)`
([`database.rs`](../../dtdb_relational/src/database.rs)), adding a field changes the
wire layout and old `statistics.bin` files simply fail to decode and are skipped —
the background `ANALYZE` recomputes them. No migration shim is needed (consistent
with ADR 0001 / 0003's pre-release stance).

### 4. The optimizer rule and cost model for infix `LIKE`

When a `LIKE` predicate targets a column whose FTS index has a tokenizer that
returns `Some(LikePlan)`, construct a candidate plan of
`Filter(like_match) over [trigram intersection scan]`, reusing the
`eval_fulltext_query` AND path. The residual `Filter` is **mandatory and
unconditional** — trigram membership is necessary but not sufficient — and it also
makes the write-buffer story trivial: index candidates and uncommitted rows both
pass through one precise `like_match`, instead of the separate `eval_row_fts_query`
re-tokenization the `MATCH` path needs.

Cost is keyed off the MCV stats:

```text
candidate_rows  = min over required trigrams of  posting_len_upper_bound(t)
postings_touched = sum over required trigrams of posting_len_upper_bound(t)
cost ≈ postings_touched · POSTING_SCAN_FACTOR        // walk posting lists
     + candidate_rows  · RECHECK_CPU_FACTOR          // like_match recheck
     + candidate_rows  · row_fetch_random_io         // pk lookups (cf. table_io)
```

Chosen only when cheaper than the full-scan estimate. `%zxq%` → tiny
`candidate_rows` → chosen; `%the%` → huge → full scan wins. With no stats at all,
default to **not** choosing the trigram plan (conservative, matches Postgres
behavior when pg_trgm stats are absent).

To avoid round-tripping through the FTS query grammar (where space means AND and
quotes mean phrase), the trigram query is passed as a typed `FullTextQuery` /
token list on a logical node, not synthesized back into a `query_str` string the
way the `MATCH` path carries it.

## Consequences

### Positive

- Prefix `LIKE` becomes an exact range scan with no recheck and no FTS dependency.
- Infix `LIKE` on a trigram-indexed column becomes a posting-list intersection +
  recheck, reusing the set-algebra primitive that already exists.
- The MCV list is a **general** statistics upgrade: it also sharpens equality-cost
  estimation on skewed B-tree indexes, not just trigram LIKE.
- The mandatory recheck unifies the write-buffer path through a single precise
  predicate.

### Negative / costs

- **The trigger is a tokenizer capability, and that contract must be honored.** A
  tokenizer that advertises `plan_like` but is lossy would silently return wrong
  rows. The contract — "every required token is present for every matching
  string" — must be documented and only implemented by faithful n-gram tokenizers.
- The cost model gains real dependence on fresh statistics. Stale MCV lists are
  tolerable (relative trigram skew is very stable across inserts), but a never-
  analyzed table gets no infix acceleration until the background `ANALYZE` runs.
- `analyze_table` does one extra `select_nth` pass; the counting `HashMap` is the
  same cardinality as today's `HashSet`, so the pre-existing concern about a huge
  distinct-prefix set on a giant FTS index is unchanged, not worsened. A streaming
  count-min-sketch + top-K heap is the escape hatch if it ever bites.

### On-disk format changes

`IndexStats` gains `most_common`, changing the `statistics.bin` postcard layout.
Old stats files fail to decode and are silently skipped, then recomputed — no
shim. No SSTable or `schema.bin` format change. dtdb is pre-release and drops
backwards compatibility by policy (ADR 0001).

## Prior art

The question "should a full-text index accelerate `LIKE`?" has two defensible
answers in the wild, and this ADR consciously picks one.

- **PostgreSQL — keep them strictly separate (three mechanisms).**
  - Prefix `LIKE 'ab%'` is served by a **B-tree with the `text_pattern_ops`
    operator class** (or any B-tree under the `C` locale), which sorts by raw
    bytes so the prefix is a contiguous range.
  - Infix `LIKE '%ab%'` (and `ILIKE`, regex, similarity) is served by
    **`pg_trgm`** — a *separate* extension exposing a **trigram GIN/GiST index**
    (`gin_trgm_ops`). For patterns too short to yield a trigram, pg_trgm declines
    and the planner falls back to a scan.
  - Linguistic search (`tsvector` + `@@`, with stemming and stop-words) is a
    **third** mechanism that does **not** help `LIKE` at all.

  The split is not arbitrary: `tsvector` is **lossy** (stemming, stop-words, case
  folding), so it physically cannot answer a substring `LIKE` soundly — which is
  exactly why pg_trgm had to be a separate, faithful index.

- **MySQL — `FULLTEXT` is `MATCH … AGAINST` only.** It never accelerates `LIKE`;
  prefix `LIKE` rides an ordinary B-tree leftmost-prefix, and there is no native
  infix-`LIKE` index.

- **SQLite FTS5 — one full-text index, specialized by tokenizer (the model dtdb
  follows).** FTS5 with the **`trigram` tokenizer** supports substring matching
  and lets the `LIKE` / `GLOB` operators use the index directly, while FTS5 with a
  word tokenizer serves `MATCH`. Same physical inverted index; the tokenizer
  decides what it can answer.

dtdb takes SQLite's unification, not PostgreSQL's separation: one
`IndexType::FullText` structure parameterized by a tokenizer. The mapping onto
PostgreSQL's vocabulary is exact — **`IndexType` is the access method, and the
tokenizer is the operator class** — and `plan_like` (Decision §2) is the
planner's operator-class match: "can this index answer a `LIKE`?". This keeps the
single index type sound precisely because the **lossiness boundary that forced
PostgreSQL to split** is enforced one level down, at the tokenizer: a faithful
n-gram tokenizer returns `Some(LikePlan)` and behaves like pg_trgm; a lossy /
word tokenizer keeps the `None` default and behaves like `tsvector`. The cost
model's "no stats ⇒ don't pick the trigram plan" rule (Decision §4) also mirrors
pg_trgm's behavior when its statistics are absent.

The one inherited cost is PostgreSQL's "wrong operator class" footgun: a
`FULLTEXT` index built with a word tokenizer is *correct* for `LIKE` but silently
falls back to a scan, so the tokenizer requirement must be documented and made
visible through `EXPLAIN`.

## Rejected alternatives

- **Use a trigram index for the prefix case** (the original framing). A plain
  range scan is strictly better for `'ab%'`: exact, no recheck, no FTS index, and
  it works for prefixes shorter than the trigram length. Trigrams are reserved for
  the infix case they alone can serve.
- **Trigger on "a tokenizer is configured" / sniff the tokenizer name.** Unsound:
  `SimpleTokenizer` can't serve infix LIKE, and a lossy tokenizer produces false
  negatives. Replaced by an explicit `plan_like` capability the planner queries.
- **Drop the recheck and trust trigram membership.** Unsound — membership is
  necessary, not sufficient (the trigrams can come from disjoint positions). The
  residual `Filter(like_match)` is non-negotiable.
- **Rely on `avg_rows_per_value` for infix cost.** An average averages away the
  exact skew that causes the footgun; `%the%` and `%zxq%` would look identical.
  The MCV list is the minimum statistic that distinguishes them.
- **Synthesize an FTS `query_str` for the trigram plan.** Fragile: the FTS grammar
  assigns meaning to spaces (AND), quotes (phrase), and operators, so trigrams
  containing those would be mis-parsed. Carry a typed token query instead.

## Implementation sketch (suggested order)

1. **Prefix `LIKE` range scan** — the `Operator::Like` arm in
   `extract_bounds_for_column` + `extract_key_range`, keeping the residual filter.
   Smallest, exact, independent; testable through SQL immediately.
2. **MCV statistics** — extend `IndexStats` with `most_common`, switch the
   `analyze_table` dedup set to a counting map + top-K, and add a
   `posting_len_upper_bound` helper on `IndexStats`. No planner behavior change
   yet; verifiable by inspecting computed stats.
3. **`TrigramTokenizer` + `plan_like` capability** — the tokenizer, the trait
   method (default `None`), and unit tests for run-splitting, the length-3 guard,
   and the `None` fallbacks (`'%ab%'`, `'%a%b%'`).
4. **Infix `LIKE` optimizer rule + cost model** — recognize trigram-capable FTS
   indexes, build `Filter(like_match)` over the reused intersection path with a
   typed token query, and gate selection on the MCV-based cost. End-to-end tests:
   `%substring%` correctness (including false-positive rows the recheck must drop),
   case-sensitivity, write-buffer rows, and the `%common%` → full-scan choice.

Each step is a self-contained, independently shippable commit.
