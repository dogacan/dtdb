# ADR 0005: Subquery support via compile-time folding and derived-table re-aliasing

- **Status:** Accepted
- **Date:** 2026-06-02
- **Deciders:** dtdb maintainers

## Context

dtdb supports a structured subset of SQL ([`sql_support.md`](../sql_support.md))
but no subqueries of any kind. We want the common cases:

- **Scalar subqueries** — `WHERE x = (SELECT MAX(y) FROM t2)`
- **`IN` / `NOT IN` subqueries** — `WHERE x IN (SELECT id FROM t2)`
- **`EXISTS` / `NOT EXISTS`** — `WHERE EXISTS (SELECT 1 FROM t2 WHERE …)`
- **Derived tables** — `FROM (SELECT … FROM t2) AS d`

Three properties of the current design frame the whole problem:

1. **`Expr::eval` is row-local and has no execution context.** Its signature is
   `eval(&self, row: &Row, schema: &Schema)`
   ([`expr.rs`](../../dtdb_sql/src/expr.rs)) — it can see one tuple and a schema,
   nothing else. A subquery, by contrast, must *run another query against
   storage* during evaluation. There is no transaction, executor, or catalog
   handle anywhere near `eval`.

2. **`dtdb_sql::expr` is a leaf module by dependency.** It imports only
   `dtdb_relational` and `dtdb_storage` types; it knows nothing about the engine,
   the physical operators, or `Transaction`. Teaching `eval` to execute a query
   would invert that layering (expr → engine → expr) and would change the
   `eval`/`bind_columns` signatures at the ~15 call sites across
   [`physical.rs`](../../dtdb_sql/src/physical.rs) and
   [`engine.rs`](../../dtdb_sql/src/engine.rs).

3. **A subquery's *correlation* is what decides its cost.** An **uncorrelated**
   subquery (no reference to the outer row) yields the same result for every
   outer tuple, so it can be evaluated *once*. A **correlated** subquery
   (`… WHERE t2.k = t1.k`) depends on the outer tuple and must be re-evaluated
   per row, or decorrelated into a join. The first is cheap; the second fights
   assumption (1) head-on.

Two facts work in our favor:

- **The parser already produces the AST.** dtdb is on `sqlparser 0.62`, which
  emits `Expr::Subquery`, `Expr::InSubquery`, `Expr::Exists`, and
  `TableFactor::Derived`. They currently fall into two catch-all `Err` arms in
  [`planner.rs`](../../dtdb_sql/src/planner.rs) (`plan_expr`'s "Unsupported
  expression" and `plan_table_factor`'s "Unsupported table factor"). No grammar
  work is required.

- **The transaction is available at physical-compile time.**
  `compile_physical(plan, tx, …)` ([`engine.rs`](../../dtdb_sql/src/engine.rs))
  already threads the live `Transaction`. Anything we want to execute *before*
  the hot `next()` loop — including a nested subquery — can run here.

The maintainer constraint is explicit: **ship the common (uncorrelated) cases
cheaply and keep `Expr::eval` pure; do not silently mis-handle the correlated
cases — detect and reject them.**

Correlated subqueries are **out of scope** for this ADR. Supporting them properly
needs either an execution context inside `eval` (the layering inversion above) or
decorrelation into semi-join / anti-join / dependent-join operators that the
engine does not have (it has sort-merge, cross, and hash-aggregate joins only).
Both are deferred to a future ADR; here they are a *detected error*.

## Decision

Adopt a three-tier design. **Tier 1** handles expression-position subqueries
(scalar / `IN` / `EXISTS`) by folding *uncorrelated* ones to constants at
compile time, so `Expr::eval` never executes a query. **Tier 2** handles derived
tables in `FROM` by recursing in the planner and re-aliasing the output schema —
the Volcano compiler already composes nested plan subtrees. **Tier 3** detects
correlation at plan time and returns a clear error.

### `plan_expr` becomes a method

Planning a subquery body requires catalog access (`self.database.get_table`) to
resolve its tables. The free function `plan_expr`
([`planner.rs`](../../dtdb_sql/src/planner.rs)) is converted to
`LogicalPlanner::plan_expr(&self, …)`. Its ~30 call sites are all already inside
`impl LogicalPlanner`, so the change is mechanical (`plan_expr(…)` →
`self.plan_expr(…)`); the `pub use planner::plan_expr` re-export is dropped (no
callers exist outside `dtdb_sql`).

### Subqueries carry an already-planned `LogicalPlan`, not raw AST

Three new `Expr` variants ([`expr.rs`](../../dtdb_sql/src/expr.rs)):

```rust
ScalarSubquery(Box<crate::logical::LogicalPlan>),
InSubquery { expr: Box<Expr>, subquery: Box<crate::logical::LogicalPlan>, negated: bool },
Exists    { subquery: Box<crate::logical::LogicalPlan>, negated: bool },
```

They hold a **planned `LogicalPlan` subtree**, not the `sqlparser` AST. This is
deliberate: `Expr` derives `Serialize`/`Deserialize` for plan caching, and
`LogicalPlan` already derives both, whereas the `sqlparser` `Query` type does not
(its `serde` feature is off) — embedding raw AST would break the derive.
`logical.rs` already imports `Expr`; the reverse reference (`expr.rs` naming
`crate::logical::LogicalPlan`) forms an intra-crate module cycle, which Rust
permits, and `Box` keeps the type finite.

The four exhaustive `match self` arms in `expr.rs` and `infer_expr_type` in
[`logical.rs`](../../dtdb_sql/src/logical.rs) gain cases for the new variants —
the compiler enumerates exactly the sites that must handle a subquery:

- `collect_columns` — recurse into the subplan (the correlation check needs it).
- `bind_columns` — bind only the outer-facing `InSubquery.expr`; the subplan is
  bound during its own compilation.
- `substitute_params` — recurse so bound parameters reach the subplan
  (`LogicalPlan::substitute_params` already exists).
- `eval` — **hard error.** Reaching `eval` with a subquery node means folding was
  skipped; this mirrors the existing `Expr::Parameter` guard.
- `infer_expr_type` — each variant is typed as the expression it *folds into*,
  so the fold pass is type-preserving: `ScalarSubquery` → the subplan's single
  output-column type; `InSubquery` → `Int` (it folds to `InList` / `Not`, which
  follow dtdb's existing Int-for-boolean convention — declared `Int` though they
  evaluate to `DbValue::Bool`); `Exists` → `Bool` (it folds to a boolean
  literal). Typing `InSubquery` as `Bool` here would make a projection column's
  type flip `Bool`→`Int` across folding, which we avoid. The deeper question of
  whether boolean expressions should be declared `Bool` rather than `Int` is a
  pre-existing, repo-wide convention left untouched by this ADR.

### Parameter binding has two paths; both must descend into subqueries

A prepared statement binds bind-parameters one of two ways. The cached-plan path
(`PreparedPlan::Planned`) substitutes at the *plan* level via
`LogicalPlan`/`Expr::substitute_params` — covered by the `substitute_params` arm
above. The fallback path (`PreparedPlan::Ast`, taken when
`LogicalPlanner::plan` returns `Err` at prepare time) caches the raw `sqlparser`
AST and binds at the *AST* level on each execution via `bind_statement` /
`bind_expr` ([`parameters.rs`](../../dtdb_sql/src/parameters.rs)). `bind_expr`'s
wildcard arm silently skips `SqlExpr::Subquery` / `InSubquery` / `Exists`, so a
parameter nested in an expression-position subquery (e.g.
`… WHERE x = (SELECT y FROM t2 WHERE z = :p)`) would reach execution unbound and
fail with `Unbound parameter`. `bind_expr` therefore gains arms that recurse via
the existing `bind_query` (already used by `bind_table_factor` for `Derived`
tables, so FROM-clause subqueries are the mirror that already works) and bind the
outer-facing `InSubquery` left-hand side. This is currently masked — the planner
rejects subqueries, so such a statement errors at planning regardless — and
becomes reachable the moment the planner accepts subqueries, so it lands together
with the planner wiring.

### Tier 1 — fold uncorrelated subqueries to constants at compile time

A new pass `SqlEngine::fold_subqueries(plan, tx)` runs on the optimized plan
immediately before `compile_physical`, in both `execute_planned` and
`execute_planned_streaming`. It walks the plan; for every contained `Expr` it
replaces each subquery node by compiling and draining its subplan through the
existing `compile_physical(subplan, tx, …)`:

- **`ScalarSubquery`** → require exactly one output column; 0 rows ⇒
  `Literal(Null)`, 1 row ⇒ that value, >1 rows ⇒ error (standard SQL).
- **`InSubquery`** → drain the single output column into the existing
  `Expr::InList { expr, list }`, whose three-valued NULL logic already gives
  correct `IN` / `NOT IN` semantics; wrap in `Not` when `negated`.
- **`Exists`** → non-empty ⇒ `Literal(Bool(true))` (or `false`), `negated` flips.

After folding, the predicate / projection is an ordinary constant-bearing `Expr`
and the existing `eval` path runs unchanged. `eval` stays pure; the engine layer
— which legitimately owns `Transaction` — is the only place that executes a
subquery.

### Tier 2 — derived tables via planner recursion + re-aliasing

`plan_table_factor` ([`planner.rs`](../../dtdb_sql/src/planner.rs)) gains a
`TableFactor::Derived { subquery, alias, .. }` arm: plan the inner query with
`self.plan_query(...)`, then wrap it in a re-aliasing `Projection` that renames
each output column to `alias.col` (or the column aliases from `AS d(a, b)` when
present) so outer references like `d.col` resolve. A derived table **requires**
an alias — `None` is an error. Because `plan_from` delegates to
`plan_table_factor`, derived tables work in joins for free, and the physical
compiler already composes nested operator subtrees, so no new physical operator
is needed.

### Tier 3 — detect correlation and reject

In `plan_expr`, after a subquery's `LogicalPlan` is built, validate that **every
column it references resolves within its own scope** by binding each node's
expressions against that node's input schema (those schemas are already stored in
the `Scan` / `Join` / `Projection` / … nodes — no `tx` needed). Any column that
fails inner resolution means the subquery is correlated (or references a
nonexistent column). To produce a precise message, the outer scope's schema is
threaded into subquery planning (a small scope stack passed down the
subquery-planning path only); a reference that resolves against the outer scope
yields:

```
correlated subqueries are not supported (column 'x' refers to the outer query)
```

`LATERAL` derived tables are correlated by definition and are rejected by the
same path.

## Consequences

### Positive

- **The cheap 80% is genuinely cheap.** Uncorrelated scalar / `IN` / `EXISTS` and
  derived tables cover the bulk of real subquery use, and land without touching
  the hot `eval`/`next()` path.
- **`Expr::eval` stays pure and row-local.** No signature change at its ~15 call
  sites; the leaf-module layering of `dtdb_sql::expr` is preserved. Subquery
  execution lives only where `Transaction` legitimately does.
- **`IN` reuses `Expr::InList` verbatim**, including its three-valued NULL logic,
  so `NOT IN (subquery)` is correct by construction.
- **Plan caching is unaffected.** New variants serialize via the existing derives;
  derived tables are ordinary `LogicalPlan` subtrees.
- **Correlated queries fail loudly**, with a message that names the offending
  column, instead of returning a wrong answer.

### Negative / costs

- **The fold runs per execution, not once per cached plan.** A subquery's result
  can change between runs (different snapshot / data), so `fold_subqueries` must
  execute against the live `tx` on each compile; we cache the *unfolded* plan.
  This is a correctness requirement, not an oversight — flagged in-code so it is
  not "optimized" into the cached plan.
- **No index pushdown through a folded scalar in v1.** `id = (SELECT MAX(id) …)`
  evaluates the subquery to a literal but does not then become a primary-key
  point-lookup; the outer scan stays a full scan. Correctness is unaffected;
  noted as a follow-up (fold before outer optimization).
- **Correlation detection must not false-reject.** A valid uncorrelated subquery
  whose inner column shares a name with an outer column must still resolve
  inner-first; the inner-scope computation around projection aliases is the
  accuracy risk and is gated by explicit regression tests.
- **Correlated subqueries remain unsupported** — a real functional gap, accepted
  here and deferred to a future decorrelation ADR.

## Rejected alternatives

- **Thread an execution context into `Expr::eval`** (pass `tx` + outer row so any
  subquery, correlated or not, runs inline). Rejected: it changes the
  `eval`/`bind_columns` signatures at ~15 call sites and inverts the
  `expr → engine → expr` layering, for a feature (correlation) we are explicitly
  deferring. Folding keeps the change at the engine boundary.

- **Decorrelate into semi-join / anti-join / dependent-join operators** (the
  "production" answer for correlated `IN`/`EXISTS`/scalar). Rejected *for now*:
  the engine has none of these operators, and the rewrite is the bulk of the
  difficulty. This is the natural content of the future ADR that lifts the Tier 3
  restriction; folding does not preclude it.

- **Embed the raw `sqlparser` `Query` in `Expr`** (defer planning to compile
  time). Rejected: `Expr` derives `Serialize`/`Deserialize` for the plan cache
  and `sqlparser`'s types do not implement serde here, so the derive would break.
  Planning the subquery into a `LogicalPlan` at plan time keeps `Expr`
  self-contained and serializable.

- **Re-execute correlated subqueries naively, per outer row** (bind outer columns
  as parameters via the existing `Parameter` machinery and re-run the subplan).
  This is a viable *first* implementation of Tier 3 and the `Parameter`
  infrastructure makes it tractable, but it is O(outer × inner) and still needs
  the context-in-`eval` plumbing or a dedicated apply operator. Deferred with the
  rest of correlation rather than half-shipped.

## Implementation sketch (suggested order)

1. **Add the three `Expr` variants** plus the four exhaustive `expr.rs` arms and
   the `infer_expr_type` arm. `eval` errors on a subquery node for now. Compiles
   with no behavior change. *(This commit.)*
2. **Make `plan_expr` a method** and wire `Subquery` / `InSubquery` / `Exists`
   planning via `self.plan_query`, *without* the correlation check yet. Also add
   the matching `bind_expr` arms in [`parameters.rs`](../../dtdb_sql/src/parameters.rs)
   so the `PreparedPlan::Ast` fallback binds parameters nested inside subqueries
   — this gap becomes reachable exactly when the planner starts accepting them.
3. **Add `fold_subqueries`** before `compile_physical` → uncorrelated Tier 1
   works end-to-end.
4. **Add correlation detection** → Tier 3 rejected with the specific message.
5. **Add `TableFactor::Derived`** with re-aliasing → Tier 2.
6. **Docs + tests**: scalar (0-row → NULL, >1-row → error), `IN`/`NOT IN` with
   NULL semantics, `EXISTS`/`NOT EXISTS`, derived tables (standalone, in a join,
   missing-alias error), correlated-rejection for each shape, and the
   shared-column-name non-regression. Update [`sql_support.md`](../sql_support.md)
   with a Subqueries section documenting the correlated limitation.

Each step is a self-contained, independently shippable commit.
