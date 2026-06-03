use crate::expr::{Expr, Operator};
use crate::logical::{AggregateExpr, JoinType, LogicalPlan, PlanKey, format_logical_plan};
use crate::optimizer::Optimizer;
use crate::physical::{
    PhysicalCrossJoin, PhysicalDistinct, PhysicalFilter, PhysicalFullTextScan,
    PhysicalHashAggregate, PhysicalIndexScan, PhysicalLimit, PhysicalOperator, PhysicalProjection,
    PhysicalSeqScan, PhysicalSetOp, PhysicalSort, PhysicalSortMergeJoin, PhysicalSortedAggregate,
};
use crate::planner::{LogicalPlanner, SqlStatement};
use dtdb_relational::{DataType, Database, Row, Schema, Transaction};
use dtdb_storage::{DbKey, DbValue, LruCache};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashSet;
use std::sync::{Arc, Mutex, RwLock};

/// Capacity (in entries) of the per-engine parsed-statement cache. SQL text is
/// typically a small, fixed set of templates (the user-facing `sql_query!`
/// macro requires compile-time literal strings), so this comfortably holds a
/// real application's working set; pathological dynamic SQL is bounded by LRU
/// eviction. Entries are stored with size 1 so the byte budget is an entry count.
const PARSE_CACHE_CAPACITY: usize = 1024;
const PLAN_CACHE_CAPACITY: usize = 1024;

/// Represents the tabular or DDL execution output of a SQL query.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionResult {
    CreateTable,
    DropTable,
    CreateIndex,
    DropIndex,
    AlterTable,
    Insert { count: usize },
    Delete { count: usize },
    Update { count: usize },
    Select { schema: Schema, rows: Vec<Row> },
    Analyze,
}

/// Represents either a completed output or a streaming physical operator for a SQL query.
pub enum ExecutionStreamingResult {
    Complete(ExecutionResult),
    Streaming {
        schema: Schema,
        operator: Box<dyn PhysicalOperator>,
    },
}

fn is_plan_sorted_by(plan: &LogicalPlan, group_by: &[Expr]) -> bool {
    if group_by.len() != 1 {
        return false;
    }
    if let Expr::Column(group_col, _) = &group_by[0]
        && let Some(child_sort_col) = Optimizer::get_plan_sort_key(plan)
    {
        return dtdb_relational::column_names_match(group_col, &child_sort_col);
    }
    false
}

/// The pieces of a single-column primary-key point lookup, matched out of an
/// optimized [`LogicalPlan`] by [`match_point_get`]. Borrows from the plan.
struct PointGetPlan<'a> {
    /// Optional top projection: `(expressions, output schema)`.
    projection: Option<(&'a [Expr], &'a Schema)>,
    /// Optional residual filter retained above the scan by the optimizer.
    filter: Option<&'a Expr>,
    table_name: &'a str,
    /// Schema of the underlying `Scan` (the full table schema), against which
    /// the filter/projection expressions are bound and evaluated.
    scan_schema: &'a Schema,
    /// The exact primary-key value to fetch.
    key: DbKey,
}

/// Recognizes an optimized plan that is a single-column primary-key point
/// lookup: an optional `Projection` over an optional `Filter` over a `Scan`
/// whose pushed-down `range` is a single key (`start == end`).
///
/// A `Scan` only carries a pushed-down `range` from primary-key extraction
/// (`extract_key_range`), which is gated on a single-column primary key, so a
/// single-key range is always a complete primary-key value. Returns `None` for
/// any other shape (range scans, full scans, secondary-index/`IndexScan`
/// lookups, joins, aggregates, composite keys, …), in which case callers fall
/// back to the general Volcano path.
fn match_point_get<'a>(
    plan: &'a LogicalPlan,
    params: &std::collections::HashMap<String, DbValue>,
) -> Option<PointGetPlan<'a>> {
    let (projection, after_proj) = match plan {
        LogicalPlan::Projection {
            source,
            expressions,
            schema,
            ..
        } => (Some((expressions.as_slice(), schema)), source.as_ref()),
        other => (None, other),
    };
    let (filter, after_filter) = match after_proj {
        LogicalPlan::Filter { source, predicate } => (Some(predicate), source.as_ref()),
        other => (None, other),
    };
    match after_filter {
        LogicalPlan::Scan {
            table_name,
            schema,
            range: Some((start, end)),
        } => {
            let resolve = |key: &PlanKey| -> Option<DbKey> {
                match key {
                    PlanKey::Value(v) => Some(v.clone()),
                    PlanKey::Parameter(name) => {
                        let val = params.get(name)?;
                        match val {
                            DbValue::Int(v) => Some(DbKey::Int(*v)),
                            DbValue::String(s) => Some(DbKey::String(s.clone())),
                            DbValue::Date(d) => Some(DbKey::Date(*d)),
                            DbValue::Time(t) => Some(DbKey::Time(*t)),
                            DbValue::Timestamp(ts) => Some(DbKey::Timestamp(*ts)),
                            DbValue::Decimal(dec) => Some(DbKey::Decimal(*dec)),
                            _ => None,
                        }
                    }
                }
            };
            let start_key = resolve(start)?;
            let end_key = resolve(end)?;
            if start_key == end_key {
                Some(PointGetPlan {
                    projection,
                    filter,
                    table_name,
                    scan_schema: schema,
                    key: start_key,
                })
            } else {
                None
            }
        }
        _ => None,
    }
}

/// How a [`PreparedStatement`]'s work is cached.
#[derive(Debug, Clone)]
enum PreparedPlan {
    /// The statement was planned once with its parameters left symbolic
    /// (`Expr::Parameter`). Each execution clones this plan, substitutes the
    /// bound values, then optimizes and runs it — skipping both parsing and
    /// logical planning.
    Planned(Box<SqlStatement>),
    /// Fallback for statements whose parameters cannot survive planning — e.g.
    /// placeholders in `INSERT ... VALUES`, which the planner materializes to
    /// concrete values. The parsed AST is cached and bound on each execution,
    /// so only parsing is skipped (the original prepared-statement behavior).
    /// Boxed because a sqlparser `Statement` is far larger than a planned one.
    Ast(Box<Statement>),
}

/// A parsed, reusable SQL statement produced by [`SqlEngine::prepare`].
///
/// Preparing parses and preprocesses the SQL once, and — where possible — plans
/// it too, leaving parameters symbolic. [`SqlEngine::execute_prepared`] then
/// runs it many times with fresh bindings, skipping the parse (always) and the
/// logical planning (when the plan could be cached). Optimization and physical
/// execution still run on every call.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    plan: PreparedPlan,
    sql: String,
    /// Catalog version this statement's cached [`PreparedPlan::Planned`] was
    /// built against. A cached logical plan resolves table and column
    /// references against the schema at `prepare()` time, so DDL run afterwards
    /// (which bumps [`Database::schema_version`]) can leave it stale.
    /// `execute_prepared` compares this against the live version and re-plans on
    /// a mismatch. Unused by the [`PreparedPlan::Ast`] fallback, which already
    /// re-plans against the live catalog on every execution.
    catalog_version: u64,
}

impl PreparedStatement {
    /// The original SQL text this statement was prepared from.
    pub fn sql(&self) -> &str {
        &self.sql
    }
}

/// SqlEngine orchestrates the parser, logical planner, optimizer, and physical execution pipeline.
pub struct SqlEngine {
    database: Arc<Database>,
    /// Caches parsed ASTs keyed on the preprocessed SQL text, so re-executing
    /// the same statement (parameterized via placeholders) parses only once.
    /// Parsing is context-free (independent of schema), so a text key is always
    /// valid; planning is intentionally NOT cached here, so each execution still
    /// resolves against the current schema.
    parse_cache: Mutex<LruCache<String, Arc<Statement>>>,
    /// Caches pre-optimized logical plans keyed on the preprocessed SQL text,
    /// so re-preparing or executing the same statement template bypasses planning/optimization.
    plan_cache: RwLock<LruCache<String, (PreparedPlan, u64)>>,
}

impl SqlEngine {
    pub fn new(database: Arc<Database>) -> Self {
        Self {
            database,
            parse_cache: Mutex::new(LruCache::new(PARSE_CACHE_CAPACITY)),
            plan_cache: RwLock::new(LruCache::new(PLAN_CACHE_CAPACITY)),
        }
    }

    /// Returns the number of cached plans in the registry.
    pub fn plan_cache_len(&self) -> usize {
        self.plan_cache.read().unwrap().len()
    }

    /// Returns true if the plan cache is empty.
    pub fn plan_cache_is_empty(&self) -> bool {
        self.plan_cache.read().unwrap().is_empty()
    }

    /// Returns the parsed AST for `preprocessed`, reusing a cached parse when
    /// available. On a miss, parses (outside the lock) and caches the result.
    /// Validates the single-statement constraint exactly as the uncached path.
    fn cached_parse(&self, preprocessed: String) -> Result<Arc<Statement>, String> {
        {
            let mut cache = self.parse_cache.lock().unwrap();
            if let Some(stmt) = cache.get(&preprocessed) {
                return Ok(stmt.clone());
            }
        }
        let dialect = GenericDialect {};
        let mut statements =
            Parser::parse_sql(&dialect, &preprocessed).map_err(|e| e.to_string())?;
        if statements.is_empty() {
            return Err("No SQL statements found".to_string());
        }
        if statements.len() > 1 {
            return Err(
                "Multiple SQL statements in a single execute() call are not allowed. \
                 Use DuctTapeDbClient::run_in_transaction() or the Transaction RPC to \
                 execute multiple statements within a single transaction."
                    .to_string(),
            );
        }
        let stmt = Arc::new(statements.remove(0));
        let mut cache = self.parse_cache.lock().unwrap();
        cache.insert(preprocessed, stmt.clone(), 1);
        Ok(stmt)
    }

    /// Parses and preprocesses `sql` once into a reusable [`PreparedStatement`].
    ///
    /// Like [`execute`](Self::execute), this accepts exactly one statement. Use
    /// parameter placeholders (e.g. `WHERE id = :id`) for the values that vary
    /// between executions, then supply them to [`execute_prepared`](Self::execute_prepared).
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement, String> {
        let preprocessed = preprocess_sql(sql);
        let schema_version = self.database.schema_version();

        // 1. Check if the pre-optimized plan is in the cache and matches the current schema version.
        {
            let mut cache = self.plan_cache.write().unwrap();
            if let Some((plan, catalog_version)) = cache
                .get(&preprocessed)
                .filter(|(_, cv)| *cv == schema_version)
            {
                return Ok(PreparedStatement {
                    plan: plan.clone(),
                    sql: sql.to_string(),
                    catalog_version: *catalog_version,
                });
            }
        }

        let dialect = GenericDialect {};
        let mut statements =
            Parser::parse_sql(&dialect, &preprocessed).map_err(|e| e.to_string())?;
        if statements.is_empty() {
            return Err("No SQL statements found".to_string());
        }
        if statements.len() > 1 {
            return Err(
                "Multiple SQL statements in a single prepare() call are not allowed.".to_string(),
            );
        }
        let statement = statements.remove(0);

        // Try to plan now, with parameters left symbolic, so execution can skip
        // logical planning. If planning can't represent the parameters (e.g.
        // placeholders in INSERT ... VALUES, which the planner materializes to
        // values), fall back to caching the AST and binding on each execution.
        let plan = match LogicalPlanner::new(self.database.clone()).plan(&statement) {
            Ok(planned) => {
                let optimized = match planned {
                    SqlStatement::Query(q) => {
                        SqlStatement::Query(Optimizer::new(self.database.clone()).optimize(q))
                    }
                    SqlStatement::InsertSelect {
                        table_name,
                        columns,
                        query,
                    } => SqlStatement::InsertSelect {
                        table_name,
                        columns,
                        query: Optimizer::new(self.database.clone()).optimize(query),
                    },
                    other => other,
                };
                PreparedPlan::Planned(Box::new(optimized))
            }
            Err(_) => PreparedPlan::Ast(Box::new(statement)),
        };

        // Cache the newly planned statement in the registry.
        {
            let mut cache = self.plan_cache.write().unwrap();
            cache.insert(preprocessed, (plan.clone(), schema_version), 1);
        }

        Ok(PreparedStatement {
            plan,
            sql: sql.to_string(),
            catalog_version: schema_version,
        })
    }

    /// Re-plans `prepared` from its original SQL when the catalog has changed
    /// since it was prepared, returning a plan to use for the current execution.
    ///
    /// Returns `None` when the cached plan is still valid (the common case), so
    /// the caller uses `prepared.plan` directly and avoids re-parsing. A stale
    /// [`PreparedPlan::Planned`] is rebuilt for this call only; the cached plan
    /// itself is not refreshed (`prepared` is shared and immutable), so the
    /// next execution will re-plan again until the statement is re-prepared.
    fn refreshed_plan(&self, prepared: &PreparedStatement) -> Result<Option<PreparedPlan>, String> {
        // The Ast fallback already re-plans against the live catalog on every
        // execution, so only a cached Planned variant can go stale.
        if !matches!(prepared.plan, PreparedPlan::Planned(_))
            || prepared.catalog_version == self.database.schema_version()
        {
            return Ok(None);
        }

        let preprocessed = preprocess_sql(&prepared.sql);
        let schema_version = self.database.schema_version();

        // 1. Check if another thread has already cached the fresh plan
        {
            let mut cache = self.plan_cache.write().unwrap();
            if let Some((plan, _)) = cache
                .get(&preprocessed)
                .filter(|(_, cv)| *cv == schema_version)
            {
                return Ok(Some(plan.clone()));
            }
        }

        let dialect = GenericDialect {};
        let statement = Parser::parse_sql(&dialect, &preprocessed)
            .map_err(|e| e.to_string())?
            .into_iter()
            .next()
            .ok_or_else(|| "No SQL statements found".to_string())?;

        let plan = match LogicalPlanner::new(self.database.clone()).plan(&statement) {
            Ok(planned) => {
                let optimized = match planned {
                    SqlStatement::Query(q) => {
                        SqlStatement::Query(Optimizer::new(self.database.clone()).optimize(q))
                    }
                    SqlStatement::InsertSelect {
                        table_name,
                        columns,
                        query,
                    } => SqlStatement::InsertSelect {
                        table_name,
                        columns,
                        query: Optimizer::new(self.database.clone()).optimize(query),
                    },
                    other => other,
                };
                PreparedPlan::Planned(Box::new(optimized))
            }
            Err(_) => PreparedPlan::Ast(Box::new(statement)),
        };

        // Cache the newly refreshed plan in the registry.
        {
            let mut cache = self.plan_cache.write().unwrap();
            cache.insert(preprocessed, (plan.clone(), schema_version), 1);
        }

        Ok(Some(plan))
    }

    /// Executes a previously [`prepare`](Self::prepare)d statement within `tx`,
    /// binding `params`. Reuses the cached parse, and — when the plan was
    /// cacheable — the cached logical plan, substituting the bound values into
    /// it during physical compilation.
    pub fn execute_prepared(
        &self,
        prepared: &PreparedStatement,
        tx: &Transaction,
        params: &std::collections::HashMap<String, DbValue>,
    ) -> Result<ExecutionResult, String> {
        let refreshed = self.refreshed_plan(prepared)?;
        match refreshed.as_ref().unwrap_or(&prepared.plan) {
            PreparedPlan::Planned(planned) => self.execute_planned(planned, tx, params),
            PreparedPlan::Ast(statement) => {
                self.execute_statement((**statement).clone(), tx, params)
            }
        }
    }

    /// Executes a previously [`prepare`](Self::prepare)d statement within `tx` in streaming mode.
    pub fn execute_prepared_streaming(
        &self,
        prepared: &PreparedStatement,
        tx: &Transaction,
        params: &std::collections::HashMap<String, DbValue>,
    ) -> Result<ExecutionStreamingResult, String> {
        let refreshed = self.refreshed_plan(prepared)?;
        match refreshed.as_ref().unwrap_or(&prepared.plan) {
            PreparedPlan::Planned(planned) => self.execute_planned_streaming(planned, tx, params),
            PreparedPlan::Ast(statement) => {
                self.execute_statement_streaming((**statement).clone(), tx, params)
            }
        }
    }

    /// Returns the temp directory for spill files, creating it if needed.
    fn temp_dir(&self) -> std::path::PathBuf {
        let dir = self.database.dir_path().join("_tmp");
        let _ = std::fs::create_dir_all(&dir);
        dir
    }

    /// Returns the per-operator memory budget in bytes used to trigger disk spilling.
    fn memory_budget(&self) -> usize {
        self.database
            .options
            .memory_budget
            .unwrap_or(8 * 1024 * 1024)
    }

    /// Checks if the SQL string contains a DDL statement (CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX).
    ///
    /// Parses through the shared cache so a statement that is then executed via
    /// `execute_with_params` is parsed only once (the client wrapper calls this
    /// immediately before executing). Multi-statement/empty input fails
    /// `cached_parse` and is treated as non-DDL, matching the downstream
    /// rejection of such inputs.
    pub fn is_ddl(&self, sql: &str) -> bool {
        let preprocessed = preprocess_sql(sql);
        if let Ok(statement) = self.cached_parse(preprocessed) {
            return matches!(
                &*statement,
                sqlparser::ast::Statement::CreateTable(_)
                    | sqlparser::ast::Statement::Drop { .. }
                    | sqlparser::ast::Statement::CreateIndex(_)
                    | sqlparser::ast::Statement::AlterTable(_)
            );
        }
        false
    }

    /// Parses, plans, optimizes, and executes a single SQL statement within the given transaction.
    ///
    /// # Single-Statement Restriction
    ///
    /// This method intentionally rejects inputs containing multiple SQL statements
    /// (e.g. "INSERT ...; INSERT ...;"). This is a deliberate design choice:
    ///
    /// - **Atomicity**: Each `execute()` call runs as a single auto-committed transaction.
    ///   Silently executing only the first statement of a multi-statement input would
    ///   cause data loss. Executing all statements without transaction boundaries would
    ///   leave partial results on failure.
    ///
    /// - **Explicit transactions**: Callers who need to execute multiple statements
    ///   atomically should use `DuctTapeDbClient::run_in_transaction()` (client API)
    ///   or the `Transaction` bidirectional streaming RPC, which provide proper
    ///   transaction boundaries with commit/rollback semantics.
    ///
    /// This restriction ensures that every `execute()` call has clear, predictable
    /// Parses, plans, optimizes, and executes a single SQL statement within the given transaction.
    ///
    /// # Single-Statement Restriction
    ///
    /// This method intentionally rejects inputs containing multiple SQL statements
    /// (e.g. "INSERT ...; INSERT ...;"). This is a deliberate design choice:
    ///
    /// - **Atomicity**: Each `execute()` call runs as a single auto-committed transaction.
    ///   Silently executing only the first statement of a multi-statement input would
    ///   cause data loss. Executing all statements without transaction boundaries would
    ///   leave partial results on failure.
    ///
    /// - **Explicit transactions**: Callers who need to execute multiple statements
    ///   atomically should use `DuctTapeDbClient::run_in_transaction()` (client API)
    ///   or the `Transaction` bidirectional streaming RPC, which provide proper
    ///   transaction boundaries with commit/rollback semantics.
    ///
    /// This restriction ensures that every `execute()` call has clear, predictable
    /// atomicity semantics: exactly one statement, exactly one transaction.
    pub fn execute(&self, sql: &str, tx: &Transaction) -> Result<ExecutionResult, String> {
        self.execute_with_params(sql, tx, &std::collections::HashMap::new())
    }

    /// Parses, plans, optimizes, and executes a single SQL statement with parameters within the given transaction.
    pub fn execute_with_params(
        &self,
        sql: &str,
        tx: &Transaction,
        params: &std::collections::HashMap<String, DbValue>,
    ) -> Result<ExecutionResult, String> {
        // Parse via the cache: re-executing the same (parameterized) statement
        // reuses the cached AST instead of re-parsing. The single-statement and
        // empty-input checks live in `cached_parse`, matching the prior path.
        let preprocessed = preprocess_sql(sql);
        let statement = self.cached_parse(preprocessed)?;
        // Clone the cached AST: `execute_statement` binds parameters into it, so
        // each execution needs its own owned copy; the cached entry stays
        // pristine (symbolic placeholders) for the next caller.
        self.execute_statement((*statement).clone(), tx, params)
    }

    /// Parses, plans, optimizes, and executes a single SQL statement with parameters inside the transaction in streaming mode.
    pub fn execute_streaming(
        &self,
        sql: &str,
        tx: &Transaction,
        params: &std::collections::HashMap<String, DbValue>,
    ) -> Result<ExecutionStreamingResult, String> {
        let preprocessed = preprocess_sql(sql);
        let statement = self.cached_parse(preprocessed)?;
        self.execute_statement_streaming((*statement).clone(), tx, params)
    }

    /// Binds parameters into an already-parsed statement, then plans,
    /// optimizes, and executes it. Shared by `execute_with_params` (which
    /// parses fresh each call) and `execute_prepared` (which reuses a cached
    /// parse), so the parameter/plan/execute behavior is identical either way.
    fn execute_statement(
        &self,
        mut statement: Statement,
        tx: &Transaction,
        params: &std::collections::HashMap<String, DbValue>,
    ) -> Result<ExecutionResult, String> {
        crate::parameters::bind_statement(&mut statement, params)?;
        let planned_stmt = LogicalPlanner::new(self.database.clone()).plan(&statement)?;
        let optimized = match planned_stmt {
            SqlStatement::Query(q) => {
                SqlStatement::Query(Optimizer::new(self.database.clone()).optimize(q))
            }
            SqlStatement::InsertSelect {
                table_name,
                columns,
                query,
            } => SqlStatement::InsertSelect {
                table_name,
                columns,
                query: Optimizer::new(self.database.clone()).optimize(query),
            },
            other => other,
        };
        self.execute_planned(&optimized, tx, params)
    }

    /// Streaming counterpart to `execute_statement`.
    fn execute_statement_streaming(
        &self,
        mut statement: Statement,
        tx: &Transaction,
        params: &std::collections::HashMap<String, DbValue>,
    ) -> Result<ExecutionStreamingResult, String> {
        crate::parameters::bind_statement(&mut statement, params)?;
        let planned_stmt = LogicalPlanner::new(self.database.clone()).plan(&statement)?;
        let optimized = match planned_stmt {
            SqlStatement::Query(q) => {
                SqlStatement::Query(Optimizer::new(self.database.clone()).optimize(q))
            }
            SqlStatement::InsertSelect {
                table_name,
                columns,
                query,
            } => SqlStatement::InsertSelect {
                table_name,
                columns,
                query: Optimizer::new(self.database.clone()).optimize(query),
            },
            other => other,
        };
        self.execute_planned_streaming(&optimized, tx, params)
    }

    /// Optimizes, compiles, and executes an already-planned statement in streaming mode.
    fn execute_planned_streaming(
        &self,
        planned_stmt: &SqlStatement,
        tx: &Transaction,
        params: &std::collections::HashMap<String, DbValue>,
    ) -> Result<ExecutionStreamingResult, String> {
        match planned_stmt {
            SqlStatement::Query(optimized_plan) => {
                // Fold uncorrelated subqueries to constants first (ADR 0005), so a
                // folded literal can even enable the point-get fast path below.
                let folded = self.fold_subqueries(optimized_plan, tx, params)?;
                let optimized_plan = folded.as_ref();

                // 1a. Fast path: primary-key point lookups skip the Volcano
                // operator tree entirely (see `execute_point_get`).
                if let Some(result) = self.execute_point_get(optimized_plan, tx, params)? {
                    return Ok(ExecutionStreamingResult::Complete(result));
                }

                // Collect referenced columns
                let mut cols = HashSet::new();
                optimized_plan.collect_columns(&mut cols);
                let cols_vec: Vec<String> = cols.into_iter().collect();

                // 2. Compile to Volcano Physical Plan
                let physical_op =
                    self.compile_physical(optimized_plan, tx, Some(&cols_vec), params)?;

                Ok(ExecutionStreamingResult::Streaming {
                    schema: physical_op.schema().clone(),
                    operator: physical_op,
                })
            }
            SqlStatement::Explain(logical_plan) => {
                let result =
                    self.execute_planned(&SqlStatement::Explain(logical_plan.clone()), tx, params)?;
                Ok(ExecutionStreamingResult::Complete(result))
            }
            other => {
                let result = self.execute_planned(other, tx, params)?;
                Ok(ExecutionStreamingResult::Complete(result))
            }
        }
    }

    /// Optimizes, compiles, and executes an already-planned statement. Shared by
    /// the parse-then-plan path (`execute_statement`) and the prepared path
    /// (`execute_prepared`), which supplies a cached plan with parameters
    /// already substituted.
    fn execute_planned(
        &self,
        planned_stmt: &SqlStatement,
        tx: &Transaction,
        params: &std::collections::HashMap<String, DbValue>,
    ) -> Result<ExecutionResult, String> {
        match planned_stmt {
            SqlStatement::CreateTable { name, schema } => {
                self.database
                    .create_table(name, schema.clone())
                    .map_err(|e| e.to_string())?;
                Ok(ExecutionResult::CreateTable)
            }
            SqlStatement::DropTable { name } => {
                self.database.drop_table(name).map_err(|e| e.to_string())?;
                Ok(ExecutionResult::DropTable)
            }
            SqlStatement::CreateIndex {
                table_name,
                index_name,
                columns,
                index_type,
                tokenizer,
            } => {
                self.database
                    .create_index(
                        table_name,
                        index_name,
                        columns.clone(),
                        *index_type,
                        tokenizer.clone(),
                    )
                    .map_err(|e| e.to_string())?;
                Ok(ExecutionResult::CreateIndex)
            }
            SqlStatement::DropIndex {
                table_name,
                index_name,
            } => {
                self.database
                    .drop_index(table_name, index_name)
                    .map_err(|e| e.to_string())?;
                Ok(ExecutionResult::DropIndex)
            }
            SqlStatement::AlterTable { table_name, op } => {
                match op {
                    crate::planner::AlterOp::AddColumn(column) => {
                        self.database
                            .alter_table_add_column(table_name, column.clone())
                            .map_err(|e| e.to_string())?;
                    }
                    crate::planner::AlterOp::RenameColumn { old_name, new_name } => {
                        self.database
                            .alter_table_rename_column(table_name, old_name, new_name)
                            .map_err(|e| e.to_string())?;
                    }
                    crate::planner::AlterOp::DropColumn(column_name) => {
                        self.database
                            .alter_table_drop_column(table_name, column_name)
                            .map_err(|e| e.to_string())?;
                    }
                }
                Ok(ExecutionResult::AlterTable)
            }
            SqlStatement::Insert {
                table_name,
                columns,
                rows,
            } => {
                let table = self
                    .database
                    .get_table(table_name)
                    .map_err(|e| e.to_string())?;

                let schema = table.schema.clone();
                let mut insert_count = 0;

                for row_vals in rows {
                    // Start with default values for each column.
                    let mut aligned_vals = Vec::with_capacity(schema.columns.len());
                    for col in &schema.columns {
                        aligned_vals.push(col.default_value.clone().unwrap_or(DbValue::Null));
                    }

                    // Map provided row values to the correct column indices.
                    for (col_idx, col_name) in columns.iter().enumerate() {
                        let schema_idx = schema
                            .column_index(col_name)
                            .ok_or_else(|| format!("Column '{}' not found in schema", col_name))?;
                        aligned_vals[schema_idx] = row_vals[col_idx].clone();
                    }

                    // Handle auto-increment columns.
                    for (col_idx, col) in schema.columns.iter().enumerate() {
                        if col.is_auto_increment {
                            if matches!(aligned_vals[col_idx], DbValue::Null) {
                                let next_val = self
                                    .database
                                    .next_sequence_value(table_name)
                                    .map_err(|e| e.to_string())?;
                                aligned_vals[col_idx] = DbValue::Int(next_val);
                            } else if let DbValue::Int(explicit_val) = aligned_vals[col_idx] {
                                self.database
                                    .update_sequence_value(table_name, explicit_val)
                                    .map_err(|e| e.to_string())?;
                            }
                        }
                    }

                    let full_row = Row::new(aligned_vals);

                    // Extract the primary key value.
                    let pk_key = schema
                        .extract_primary_key(&full_row)
                        .map_err(|e| e.to_string())?;

                    if tx
                        .get(table_name, &pk_key)
                        .map_err(|e| e.to_string())?
                        .is_some()
                    {
                        return Err("Duplicate primary key".to_string());
                    }

                    tx.put(table_name, pk_key, full_row)
                        .map_err(|e| e.to_string())?;
                    insert_count += 1;
                }

                Ok(ExecutionResult::Insert {
                    count: insert_count,
                })
            }
            SqlStatement::InsertSelect {
                table_name,
                columns,
                query: optimized_plan,
            } => {
                let folded = self.fold_subqueries(optimized_plan, tx, params)?;
                let optimized_plan = folded.as_ref();

                let table = self
                    .database
                    .get_table(table_name)
                    .map_err(|e| e.to_string())?;

                let schema = table.schema.clone();
                let mut insert_count = 0;

                // Collect referenced columns
                let mut cols = HashSet::new();
                optimized_plan.collect_columns(&mut cols);
                let cols_vec: Vec<String> = cols.into_iter().collect();

                // 2. Compile to Volcano Physical Plan
                let mut physical_op =
                    self.compile_physical(optimized_plan, tx, Some(&cols_vec), params)?;

                // 3. Validate columns count match
                let subquery_schema = physical_op.schema();
                if columns.len() != subquery_schema.columns.len() {
                    return Err(format!(
                        "INSERT INTO ... SELECT col count mismatch: target columns: {}, SELECT columns: {}",
                        columns.len(),
                        subquery_schema.columns.len()
                    ));
                }

                // 4. Consume Volcano Iterator streaming rows and insert them
                while let Some(row) = physical_op.next()? {
                    let mut aligned_vals = Vec::with_capacity(schema.columns.len());
                    for col in &schema.columns {
                        aligned_vals.push(col.default_value.clone().unwrap_or(DbValue::Null));
                    }

                    for (col_idx, col_name) in columns.iter().enumerate() {
                        let schema_idx = schema
                            .column_index(col_name)
                            .ok_or_else(|| format!("Column '{}' not found in schema", col_name))?;
                        aligned_vals[schema_idx] = row.values[col_idx].clone();
                    }

                    // Handle auto-increment columns.
                    for (col_idx, col) in schema.columns.iter().enumerate() {
                        if col.is_auto_increment {
                            if matches!(aligned_vals[col_idx], DbValue::Null) {
                                let next_val = self
                                    .database
                                    .next_sequence_value(table_name)
                                    .map_err(|e| e.to_string())?;
                                aligned_vals[col_idx] = DbValue::Int(next_val);
                            } else if let DbValue::Int(explicit_val) = aligned_vals[col_idx] {
                                self.database
                                    .update_sequence_value(table_name, explicit_val)
                                    .map_err(|e| e.to_string())?;
                            }
                        }
                    }

                    let full_row = Row::new(aligned_vals);
                    let pk_key = schema
                        .extract_primary_key(&full_row)
                        .map_err(|e| e.to_string())?;

                    if tx
                        .get(table_name, &pk_key)
                        .map_err(|e| e.to_string())?
                        .is_some()
                    {
                        return Err("Duplicate primary key".to_string());
                    }

                    tx.put(table_name, pk_key, full_row)
                        .map_err(|e| e.to_string())?;
                    insert_count += 1;
                }

                Ok(ExecutionResult::Insert {
                    count: insert_count,
                })
            }
            SqlStatement::Delete { table_name, filter } => {
                let table = self
                    .database
                    .get_table(table_name)
                    .map_err(|e| e.to_string())?;

                let scan_plan = LogicalPlan::Scan {
                    table_name: table_name.clone(),
                    schema: table.schema.clone(),
                    range: None,
                };

                let plan = match filter {
                    Some(pred) => {
                        // Fold any subquery in the WHERE predicate (ADR 0005).
                        let mut pred = pred.clone();
                        self.fold_expr(&mut pred, tx, params)?;
                        LogicalPlan::Filter {
                            source: Box::new(scan_plan),
                            predicate: pred,
                        }
                    }
                    None => scan_plan,
                };

                let optimized_plan = Optimizer::new(self.database.clone()).optimize(plan);
                let mut physical_op = self.compile_physical(&optimized_plan, tx, None, params)?;

                let mut delete_count = 0;
                while let Some(row) = physical_op.next()? {
                    let pk_key = table
                        .schema
                        .extract_primary_key(&row)
                        .map_err(|e| e.to_string())?;
                    tx.delete(table_name, pk_key).map_err(|e| e.to_string())?;
                    delete_count += 1;
                }

                Ok(ExecutionResult::Delete {
                    count: delete_count,
                })
            }
            SqlStatement::Update {
                table_name,
                assignments,
                filter,
            } => {
                let table = self
                    .database
                    .get_table(table_name)
                    .map_err(|e| e.to_string())?;

                let scan_plan = LogicalPlan::Scan {
                    table_name: table_name.clone(),
                    schema: table.schema.clone(),
                    range: None,
                };

                let plan = match filter {
                    Some(pred) => {
                        // Fold any subquery in the WHERE predicate (ADR 0005).
                        let mut pred = pred.clone();
                        self.fold_expr(&mut pred, tx, params)?;
                        LogicalPlan::Filter {
                            source: Box::new(scan_plan),
                            predicate: pred,
                        }
                    }
                    None => scan_plan,
                };

                let optimized_plan = Optimizer::new(self.database.clone()).optimize(plan);
                let mut physical_op = self.compile_physical(&optimized_plan, tx, None, params)?;

                // Clone and substitute parameter bindings in assignments once.
                let mut bound_assignments = Vec::with_capacity(assignments.len());
                for (col_name, expr) in assignments {
                    let mut bound_expr = expr.clone();
                    bound_expr.substitute_params(params)?;
                    self.fold_expr(&mut bound_expr, tx, params)?;
                    bound_assignments.push((col_name, bound_expr));
                }

                let mut update_count = 0;
                while let Some(row) = physical_op.next()? {
                    let mut updated_values = row.values.clone();
                    for (col_name, expr) in &bound_assignments {
                        let col_idx = table
                            .schema
                            .column_index(col_name)
                            .ok_or_else(|| format!("Column '{}' not found in schema", col_name))?;
                        let new_val = expr.eval(&row, &table.schema)?;
                        updated_values[col_idx] = new_val;
                    }

                    let old_pk_key = table
                        .schema
                        .extract_primary_key(&row)
                        .map_err(|e| e.to_string())?;

                    let updated_row = Row::new(updated_values);
                    let new_pk_key = table
                        .schema
                        .extract_primary_key(&updated_row)
                        .map_err(|e| e.to_string())?;

                    if old_pk_key != new_pk_key {
                        tx.delete(table_name, old_pk_key)
                            .map_err(|e| e.to_string())?;
                        tx.put(table_name, new_pk_key, updated_row)
                            .map_err(|e| e.to_string())?;
                    } else {
                        tx.put(table_name, new_pk_key, updated_row)
                            .map_err(|e| e.to_string())?;
                    }
                    update_count += 1;
                }

                Ok(ExecutionResult::Update {
                    count: update_count,
                })
            }
            SqlStatement::Query(optimized_plan) => {
                // Fold uncorrelated subqueries to constants first (ADR 0005), so a
                // folded literal can even enable the point-get fast path below.
                let folded = self.fold_subqueries(optimized_plan, tx, params)?;
                let optimized_plan = folded.as_ref();

                // 1a. Fast path: primary-key point lookups skip the Volcano
                // operator tree entirely (see `execute_point_get`).
                if let Some(result) = self.execute_point_get(optimized_plan, tx, params)? {
                    return Ok(result);
                }

                // Collect referenced columns
                let mut cols = HashSet::new();
                optimized_plan.collect_columns(&mut cols);
                let cols_vec: Vec<String> = cols.into_iter().collect();

                // 2. Compile to Volcano Physical Plan
                let mut physical_op =
                    self.compile_physical(optimized_plan, tx, Some(&cols_vec), params)?;

                // 3. Consume Volcano Iterator streaming rows
                let mut results = Vec::new();
                while let Some(row) = physical_op.next()? {
                    results.push(row);
                }

                Ok(ExecutionResult::Select {
                    schema: physical_op.schema().clone(),
                    rows: results,
                })
            }
            SqlStatement::Explain(logical_plan) => {
                // 1. Optimize the Logical Plan
                let optimized_plan =
                    Optimizer::new(self.database.clone()).optimize(logical_plan.clone());

                // 2. Format logical plans as strings
                let logical_str = format_logical_plan(logical_plan);
                let opt_logical_str = format_logical_plan(&optimized_plan);

                // 3. Describe the physical plan. Point lookups bypass the
                // Volcano tree (see `execute_point_get`), so report that here
                // rather than compiling operators that would never run.
                let physical_str = if let Some(pg) = match_point_get(&optimized_plan, params) {
                    format!(
                        "- PhysicalPointGet: table={}, key={:?}{}\n",
                        pg.table_name,
                        pg.key,
                        if pg.filter.is_some() {
                            " (residual filter)"
                        } else {
                            ""
                        }
                    )
                } else {
                    // Collect referenced columns
                    let mut cols = HashSet::new();
                    optimized_plan.collect_columns(&mut cols);
                    let cols_vec: Vec<String> = cols.into_iter().collect();

                    let physical_op =
                        self.compile_physical(&optimized_plan, tx, Some(&cols_vec), params)?;
                    let mut s = String::new();
                    physical_op.explain(0, &mut s);
                    s
                };

                // 4. Wrap the result in a select output with "Query Plan" schema column
                let schema = Schema::new(vec![dtdb_relational::Column {
                    // Ephemeral EXPLAIN output column; id is unused (not persisted).
                    id: 0,
                    name: "Query Plan".to_string(),
                    data_type: DataType::String,
                    is_primary_key: false,
                    is_nullable: true,
                    locality_group: None,
                    default_value: None,
                    is_auto_increment: false,
                }]);

                let plan_info = format!(
                    "--- Logical Plan ---\n{}\n--- Optimized Plan ---\n{}\n--- Physical Plan ---\n{}",
                    logical_str.trim_end(),
                    opt_logical_str.trim_end(),
                    physical_str.trim_end()
                );

                let rows = vec![Row::new(vec![DbValue::string(plan_info)])];

                Ok(ExecutionResult::Select { schema, rows })
            }
            SqlStatement::Analyze { table_name } => {
                self.database
                    .analyze_table(table_name, tx)
                    .map_err(|e| e.to_string())?;
                Ok(ExecutionResult::Analyze)
            }
        }
    }

    /// Fast path for single-column primary-key point lookups
    /// (`SELECT ... FROM t WHERE pk = <value>`).
    ///
    /// The cost-based optimizer compiles such a query to
    /// `Projection -> Filter -> Scan { range: Some((k, k)) }`. The residual
    /// `Filter` is retained even when the key range is exact, as a correctness
    /// safety net (the pushed-down range can be an over-approximation in
    /// general). For this shape the general Volcano path would build a
    /// range-scan iterator plus boxed `Projection`/`Filter`/`SeqScan` operators
    /// to produce at most a single row.
    ///
    /// Instead, when the optimized plan matches that shape with `start == end`
    /// on the primary-key `Scan`, we issue one [`Transaction::get_projected`]
    /// (a bloom-filter-short-circuited point get) and evaluate the residual
    /// filter and projection inline. The same `bind_columns`/`eval` calls and
    /// filter-truthiness rule as [`PhysicalFilter`]/[`PhysicalProjection`] are
    /// reused, so results are identical to the general path.
    ///
    /// A `Scan` only ever carries a pushed-down `range` from primary-key
    /// extraction (`extract_key_range`), which is gated on a *single-column*
    /// primary key (`Schema::primary_key_index`). So a single-key `Scan` range
    /// is always a full primary-key value that `get` can resolve directly;
    /// composite keys and secondary-index lookups never reach this path.
    ///
    /// Returns `Ok(None)` when the plan is not a point lookup, in which case the
    /// caller falls back to the general compile-and-execute path.
    fn execute_point_get(
        &self,
        plan: &LogicalPlan,
        tx: &Transaction,
        params: &std::collections::HashMap<String, DbValue>,
    ) -> Result<Option<ExecutionResult>, String> {
        let Some(pg) = match_point_get(plan, params) else {
            return Ok(None);
        };

        // Columns to read = union of those referenced by the projection and the
        // residual filter. This mirrors the locality-group I/O hint the general
        // path pushes down to the scan. Row width is always the full schema
        // (locality-group merge), so expression evaluation below is unaffected.
        let mut needed = HashSet::new();
        if let Some((exprs, _)) = &pg.projection {
            for e in exprs.iter() {
                e.collect_columns(&mut needed);
            }
        }
        if let Some(pred) = pg.filter {
            pred.collect_columns(&mut needed);
        }
        let needed_cols: Vec<String> = needed.into_iter().collect();

        let row = tx
            .get_projected(pg.table_name, &pg.key, Some(&needed_cols))
            .map_err(|e| e.to_string())?;

        let output_schema = match &pg.projection {
            Some((_, schema)) => (*schema).clone(),
            None => pg.scan_schema.clone(),
        };

        let mut rows = Vec::new();
        if let Some(row) = row {
            // Residual filter, using the identical truthiness rule as
            // `PhysicalFilter::next`.
            let passes = match pg.filter {
                Some(pred) => {
                    let mut pred = pred.clone();
                    pred.substitute_params(params)?;
                    pred.bind_columns(pg.scan_schema)
                        .map_err(|e| e.to_string())?;
                    match pred.eval(&row, pg.scan_schema)? {
                        DbValue::Bool(b) => b,
                        DbValue::Int(v) => v != 0,
                        _ => false,
                    }
                }
                None => true,
            };
            if passes {
                let out_row = match &pg.projection {
                    Some((expressions, _)) => {
                        let mut vals = Vec::with_capacity(expressions.len());
                        for e in expressions.iter() {
                            let mut e = (*e).clone();
                            e.substitute_params(params)?;
                            e.bind_columns(pg.scan_schema)
                                .map_err(|er| er.to_string())?;
                            vals.push(e.eval(&row, pg.scan_schema)?);
                        }
                        Row::new(vals)
                    }
                    None => row,
                };
                rows.push(out_row);
            }
        }

        Ok(Some(ExecutionResult::Select {
            schema: output_schema,
            rows,
        }))
    }

    /// Replaces every *uncorrelated* expression subquery in `plan` with a
    /// constant, by executing each subquery once against `tx` (ADR 0005).
    ///
    /// Returns a borrow when the plan has no subqueries (the common case, no
    /// allocation), or an owned, folded copy otherwise. The original plan —
    /// which may be a cached, parameterized plan shared across executions — is
    /// never mutated; folding is per-execution because a subquery's result can
    /// change between runs.
    fn fold_subqueries<'a>(
        &self,
        plan: &'a LogicalPlan,
        tx: &Transaction,
        params: &std::collections::HashMap<String, DbValue>,
    ) -> Result<std::borrow::Cow<'a, LogicalPlan>, String> {
        if !plan_contains_subquery(plan) {
            return Ok(std::borrow::Cow::Borrowed(plan));
        }
        let mut owned = plan.clone();
        self.fold_plan(&mut owned, tx, params)?;
        Ok(std::borrow::Cow::Owned(owned))
    }

    /// Recursively folds every subquery in the expressions carried by `plan`.
    fn fold_plan(
        &self,
        plan: &mut LogicalPlan,
        tx: &Transaction,
        params: &std::collections::HashMap<String, DbValue>,
    ) -> Result<(), String> {
        match plan {
            LogicalPlan::Scan { .. }
            | LogicalPlan::IndexScan { .. }
            | LogicalPlan::FullTextScan { .. } => {}
            LogicalPlan::Filter { source, predicate } => {
                self.fold_plan(source, tx, params)?;
                self.fold_expr(predicate, tx, params)?;
            }
            LogicalPlan::Projection {
                source,
                expressions,
                ..
            } => {
                self.fold_plan(source, tx, params)?;
                for e in expressions {
                    self.fold_expr(e, tx, params)?;
                }
            }
            LogicalPlan::Join {
                left,
                right,
                condition,
                ..
            } => {
                self.fold_plan(left, tx, params)?;
                self.fold_plan(right, tx, params)?;
                self.fold_expr(condition, tx, params)?;
            }
            LogicalPlan::Aggregate {
                source,
                group_by,
                aggrs,
                ..
            } => {
                self.fold_plan(source, tx, params)?;
                for e in group_by {
                    self.fold_expr(e, tx, params)?;
                }
                for a in aggrs {
                    self.fold_expr(aggregate_expr_mut(a), tx, params)?;
                }
            }
            LogicalPlan::Sort { source, keys } => {
                self.fold_plan(source, tx, params)?;
                for (e, _) in keys {
                    self.fold_expr(e, tx, params)?;
                }
            }
            LogicalPlan::Limit { source, .. } | LogicalPlan::Distinct { source } => {
                self.fold_plan(source, tx, params)?;
            }
            LogicalPlan::SetOp { left, right, .. } => {
                self.fold_plan(left, tx, params)?;
                self.fold_plan(right, tx, params)?;
            }
        }
        Ok(())
    }

    /// Recursively folds every subquery node inside a single expression.
    fn fold_expr(
        &self,
        expr: &mut Expr,
        tx: &Transaction,
        params: &std::collections::HashMap<String, DbValue>,
    ) -> Result<(), String> {
        match expr {
            Expr::BinaryOp { left, right, .. } => {
                self.fold_expr(left, tx, params)?;
                self.fold_expr(right, tx, params)?;
                return Ok(());
            }
            Expr::Not(inner) | Expr::IsNull(inner) => {
                self.fold_expr(inner, tx, params)?;
                return Ok(());
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(o) = operand {
                    self.fold_expr(o, tx, params)?;
                }
                for c in conditions {
                    self.fold_expr(c, tx, params)?;
                }
                for r in results {
                    self.fold_expr(r, tx, params)?;
                }
                if let Some(e) = else_result {
                    self.fold_expr(e, tx, params)?;
                }
                return Ok(());
            }
            Expr::Function { args, .. } => {
                for a in args {
                    self.fold_expr(a, tx, params)?;
                }
                return Ok(());
            }
            Expr::InList { expr: e, list } => {
                self.fold_expr(e, tx, params)?;
                for it in list {
                    self.fold_expr(it, tx, params)?;
                }
                return Ok(());
            }
            Expr::Cast { expr, .. } => {
                self.fold_expr(expr, tx, params)?;
                return Ok(());
            }
            Expr::Column(..) | Expr::Literal(_) | Expr::Parameter(_) | Expr::Match { .. } => {
                return Ok(());
            }
            // Only the subquery variants fall through to the rewrite below.
            Expr::ScalarSubquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. } => {}
        }
        let replacement = self.fold_subquery_node(expr, tx, params)?;
        *expr = replacement;
        Ok(())
    }

    /// Executes a single subquery node and returns the constant it folds to.
    /// Correlated subqueries are rejected earlier in planning (ADR 0005); any
    /// node reaching here is uncorrelated and self-contained.
    fn fold_subquery_node(
        &self,
        expr: &mut Expr,
        tx: &Transaction,
        params: &std::collections::HashMap<String, DbValue>,
    ) -> Result<Expr, String> {
        match expr {
            Expr::ScalarSubquery(subquery) => {
                self.fold_plan(subquery, tx, params)?;
                let (schema, mut rows) = self.exec_subplan(subquery, tx, params)?;
                if schema.columns.len() != 1 {
                    return Err("scalar subquery must return exactly one column".to_string());
                }
                match rows.len() {
                    0 => Ok(Expr::Literal(DbValue::Null)),
                    1 => Ok(Expr::Literal(
                        rows.remove(0)
                            .values
                            .into_iter()
                            .next()
                            .unwrap_or(DbValue::Null),
                    )),
                    _ => Err("scalar subquery returned more than one row".to_string()),
                }
            }
            Expr::InSubquery {
                expr: lhs,
                subquery,
                negated,
            } => {
                self.fold_expr(lhs, tx, params)?;
                self.fold_plan(subquery, tx, params)?;
                let (schema, rows) = self.exec_subplan(subquery, tx, params)?;
                if schema.columns.len() != 1 {
                    return Err("IN subquery must return exactly one column".to_string());
                }
                let list = rows
                    .into_iter()
                    .map(|r| Expr::Literal(r.values.into_iter().next().unwrap_or(DbValue::Null)))
                    .collect();
                let in_list = Expr::InList {
                    expr: lhs.clone(),
                    list,
                };
                Ok(if *negated {
                    Expr::Not(Box::new(in_list))
                } else {
                    in_list
                })
            }
            Expr::Exists { subquery, negated } => {
                self.fold_plan(subquery, tx, params)?;
                let (_schema, rows) = self.exec_subplan(subquery, tx, params)?;
                let exists = !rows.is_empty();
                Ok(Expr::Literal(DbValue::Bool(if *negated {
                    !exists
                } else {
                    exists
                })))
            }
            _ => unreachable!("fold_subquery_node called on a non-subquery expression"),
        }
    }

    /// Optimizes, compiles, and fully drains a subquery's plan, returning its
    /// output schema and all result rows.
    fn exec_subplan(
        &self,
        subplan: &LogicalPlan,
        tx: &Transaction,
        params: &std::collections::HashMap<String, DbValue>,
    ) -> Result<(Schema, Vec<Row>), String> {
        let optimized = Optimizer::new(self.database.clone()).optimize(subplan.clone());
        let mut op = self.compile_physical(&optimized, tx, None, params)?;
        let schema = op.schema().clone();
        let mut rows = Vec::new();
        while let Some(r) = op.next()? {
            rows.push(r);
        }
        Ok((schema, rows))
    }

    /// Compiles a logical plan node into a Volcano Physical Operator tree.
    fn compile_physical(
        &self,
        plan: &LogicalPlan,
        tx: &Transaction,
        columns: Option<&[String]>,
        params: &std::collections::HashMap<String, DbValue>,
    ) -> Result<Box<dyn PhysicalOperator>, String> {
        match plan {
            LogicalPlan::IndexScan {
                table_name,
                index_name,
                schema,
                range,
            } => {
                // Calculate scan range bounds.
                let (start, end) = match range {
                    Some((start_key, end_key)) => {
                        let resolve = |key: &PlanKey| -> Result<DbKey, String> {
                            match key {
                                PlanKey::Value(v) => Ok(v.clone()),
                                PlanKey::Parameter(name) => {
                                    let val = params
                                        .get(name)
                                        .ok_or_else(|| format!("Unbound parameter: {}", name))?;
                                    match val {
                                        DbValue::Int(v) => Ok(DbKey::Int(*v)),
                                        DbValue::String(s) => Ok(DbKey::String(s.clone())),
                                        DbValue::Date(d) => Ok(DbKey::Date(*d)),
                                        DbValue::Time(t) => Ok(DbKey::Time(*t)),
                                        DbValue::Timestamp(ts) => Ok(DbKey::Timestamp(*ts)),
                                        DbValue::Decimal(dec) => Ok(DbKey::Decimal(*dec)),
                                        _ => Err(format!(
                                            "Unsupported key parameter type for {}",
                                            name
                                        )),
                                    }
                                }
                            }
                        };
                        (resolve(start_key)?, resolve(end_key)?)
                    }
                    None => {
                        let idx_def = schema
                            .indexes
                            .iter()
                            .find(|idx| idx.name == *index_name)
                            .ok_or_else(|| {
                                format!(
                                    "Index '{}' not found in schema during compilation",
                                    index_name
                                )
                            })?;
                        let col_name = idx_def
                            .columns
                            .first()
                            .ok_or_else(|| format!("Index '{}' defines no columns", index_name))?;
                        let col = schema
                            .columns
                            .iter()
                            .find(|c| c.matches_name(col_name))
                            .ok_or_else(|| {
                                format!("Indexed column '{}' not found in schema", col_name)
                            })?;
                        // Full-index-scan bounds must bracket the index's
                        // composite keys, whose leading component is a `DbKey`
                        // of the column's type. Untyped string bounds would not
                        // bracket Date/Time/Timestamp/Decimal keys, yielding an
                        // empty scan. Keep in sync with `Schema`'s key bounds.
                        match col.data_type {
                            DataType::Int => (DbKey::Int(i64::MIN), DbKey::Int(i64::MAX)),
                            DataType::Bool => (DbKey::Bool(false), DbKey::Bool(true)),
                            DataType::Date => (
                                DbKey::Date(chrono::NaiveDate::MIN),
                                DbKey::Date(chrono::NaiveDate::MAX),
                            ),
                            DataType::Time => (
                                DbKey::Time(chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap()),
                                DbKey::Time(
                                    chrono::NaiveTime::from_hms_nano_opt(23, 59, 59, 999_999_999)
                                        .unwrap(),
                                ),
                            ),
                            DataType::Timestamp => (
                                DbKey::Timestamp(chrono::NaiveDateTime::MIN),
                                DbKey::Timestamp(chrono::NaiveDateTime::MAX),
                            ),
                            DataType::Decimal => (
                                DbKey::Decimal(rust_decimal::Decimal::MIN),
                                DbKey::Decimal(rust_decimal::Decimal::MAX),
                            ),
                            _ => (DbKey::string(""), DbKey::string("\u{10ffff}")),
                        }
                    }
                };

                let rows = tx
                    .index_scan(table_name, index_name, &start, &end, columns)
                    .map_err(|e| e.to_string())?;

                Ok(Box::new(PhysicalIndexScan::new(schema.clone(), rows)))
            }
            LogicalPlan::FullTextScan {
                table_name,
                index_name,
                schema,
                query_str,
            } => {
                let rows = tx
                    .fulltext_scan(table_name, index_name, query_str, columns)
                    .map_err(|e| e.to_string())?;

                Ok(Box::new(PhysicalFullTextScan::new(
                    schema.clone(),
                    query_str.clone(),
                    rows,
                )))
            }
            LogicalPlan::Scan {
                table_name,
                schema,
                range,
            } => {
                // Calculate scan range bounds.
                let (start, end) = match range {
                    Some((start_key, end_key)) => {
                        let resolve = |key: &PlanKey| -> Result<DbKey, String> {
                            match key {
                                PlanKey::Value(v) => Ok(v.clone()),
                                PlanKey::Parameter(name) => {
                                    let val = params
                                        .get(name)
                                        .ok_or_else(|| format!("Unbound parameter: {}", name))?;
                                    match val {
                                        DbValue::Int(v) => Ok(DbKey::Int(*v)),
                                        DbValue::String(s) => Ok(DbKey::String(s.clone())),
                                        DbValue::Date(d) => Ok(DbKey::Date(*d)),
                                        DbValue::Time(t) => Ok(DbKey::Time(*t)),
                                        DbValue::Timestamp(ts) => Ok(DbKey::Timestamp(*ts)),
                                        DbValue::Decimal(dec) => Ok(DbKey::Decimal(*dec)),
                                        _ => Err(format!(
                                            "Unsupported key parameter type for {}",
                                            name
                                        )),
                                    }
                                }
                            }
                        };
                        (resolve(start_key)?, resolve(end_key)?)
                    }
                    None => schema.primary_key_bounds().map_err(|e| e.to_string())?,
                };

                let iter = tx
                    .scan_iter(table_name, &start, &end, columns)
                    .map_err(|e| e.to_string())?;

                struct RelationalIteratorAdapter {
                    iter: dtdb_relational::TransactionScanIterator,
                }

                impl Iterator for RelationalIteratorAdapter {
                    type Item = Result<Row, String>;

                    fn next(&mut self) -> Option<Self::Item> {
                        match self.iter.next() {
                            Ok(Some(row)) => Some(Ok(row)),
                            Ok(None) => None,
                            Err(e) => Some(Err(e.to_string())),
                        }
                    }
                }

                Ok(Box::new(PhysicalSeqScan::from_iter(
                    schema.clone(),
                    Box::new(RelationalIteratorAdapter { iter }),
                )))
            }
            LogicalPlan::Filter { source, predicate } => {
                let mut predicate = predicate.clone();
                predicate.substitute_params(params)?;
                let mut predicate_cols = HashSet::new();
                predicate.collect_columns(&mut predicate_cols);
                let child_cols = parent_cols_union(columns, &predicate_cols);
                let src_op = self.compile_physical(source, tx, child_cols.as_deref(), params)?;
                predicate
                    .bind_columns(src_op.schema())
                    .map_err(|e| e.to_string())?;
                Ok(Box::new(PhysicalFilter::new(src_op, predicate)))
            }
            LogicalPlan::Projection {
                source,
                expressions,
                field_names,
                ..
            } => {
                let mut expressions = expressions.clone();
                for expr in &mut expressions {
                    expr.substitute_params(params)?;
                }
                let mut child_needed = HashSet::new();
                for expr in &expressions {
                    expr.collect_columns(&mut child_needed);
                }
                let child_cols: Vec<String> = child_needed.into_iter().collect();
                let src_op = self.compile_physical(source, tx, Some(&child_cols), params)?;
                for expr in &mut expressions {
                    expr.bind_columns(src_op.schema())
                        .map_err(|e| e.to_string())?;
                }
                let proj_schema = LogicalPlan::new_projection(
                    LogicalPlan::Scan {
                        table_name: "".to_string(),
                        schema: src_op.schema().clone(),
                        range: None,
                    },
                    expressions.clone(),
                    field_names.clone(),
                )
                .schema();

                Ok(Box::new(PhysicalProjection::new(
                    src_op,
                    expressions,
                    proj_schema,
                )))
            }
            LogicalPlan::Limit {
                source,
                limit,
                offset,
            } => {
                let src_op = self.compile_physical(source, tx, columns, params)?;
                Ok(Box::new(PhysicalLimit::new(src_op, *limit, *offset)))
            }
            LogicalPlan::Sort { source, keys } => {
                let mut keys = keys.clone();
                for (expr, _) in &mut keys {
                    expr.substitute_params(params)?;
                }
                let mut key_cols = HashSet::new();
                for (expr, _) in &keys {
                    expr.collect_columns(&mut key_cols);
                }
                let child_cols = parent_cols_union(columns, &key_cols);
                let src_op = self.compile_physical(source, tx, child_cols.as_deref(), params)?;
                for (expr, _) in &mut keys {
                    expr.bind_columns(src_op.schema())
                        .map_err(|e| e.to_string())?;
                }
                Ok(Box::new(PhysicalSort::new(
                    src_op,
                    keys,
                    self.temp_dir(),
                    self.memory_budget(),
                )))
            }
            LogicalPlan::Join {
                left,
                right,
                condition,
                join_type,
                ..
            } => {
                let mut condition = condition.clone();
                condition.substitute_params(params)?;

                let left_schema = left.schema();
                let right_schema = right.schema();

                let (left_op, right_op) = if let Some(cols) = columns {
                    let mut cond_cols = HashSet::new();
                    condition.collect_columns(&mut cond_cols);
                    let all_needed: HashSet<String> =
                        cols.iter().cloned().chain(cond_cols).collect();

                    let mut left_cols = Vec::new();
                    let mut right_cols = Vec::new();
                    for col in &all_needed {
                        if schema_contains_col(&left_schema, col) {
                            left_cols.push(col.clone());
                        }
                        if schema_contains_col(&right_schema, col) {
                            right_cols.push(col.clone());
                        }
                    }

                    let l_op = self.compile_physical(left, tx, Some(&left_cols), params)?;
                    let r_op = self.compile_physical(right, tx, Some(&right_cols), params)?;
                    (l_op, r_op)
                } else {
                    let l_op = self.compile_physical(left, tx, None, params)?;
                    let r_op = self.compile_physical(right, tx, None, params)?;
                    (l_op, r_op)
                };

                let joined_schema = LogicalPlan::new_join(
                    LogicalPlan::Scan {
                        table_name: "".to_string(),
                        schema: left_op.schema().clone(),
                        range: None,
                    },
                    LogicalPlan::Scan {
                        table_name: "".to_string(),
                        schema: right_op.schema().clone(),
                        range: None,
                    },
                    condition.clone(),
                    *join_type,
                )
                .schema();

                if *join_type == JoinType::Cross {
                    Ok(Box::new(PhysicalCrossJoin::new(
                        left_op,
                        right_op,
                        joined_schema,
                    )))
                } else {
                    // Extract left_on and right_on join keys from equality join condition
                    let (l_expr, r_expr) = match &condition {
                        Expr::BinaryOp {
                            left: l,
                            op: Operator::Eq,
                            right: r,
                        } => ((**l).clone(), (**r).clone()),
                        other => {
                            return Err(format!(
                                "Only equality join conditions supported, got {:?}",
                                other
                            ));
                        }
                    };

                    let mut l_cols = HashSet::new();
                    l_expr.collect_columns(&mut l_cols);
                    let mut r_cols = HashSet::new();
                    r_expr.collect_columns(&mut r_cols);

                    let left_schema = left_op.schema();
                    let right_schema = right_op.schema();

                    let l_in_left = l_cols
                        .iter()
                        .all(|col| schema_contains_col(left_schema, col));
                    let r_in_right = r_cols
                        .iter()
                        .all(|col| schema_contains_col(right_schema, col));

                    let r_in_left = r_cols
                        .iter()
                        .all(|col| schema_contains_col(left_schema, col));
                    let l_in_right = l_cols
                        .iter()
                        .all(|col| schema_contains_col(right_schema, col));

                    let (mut left_on, mut right_on) = if l_in_left && r_in_right {
                        (l_expr, r_expr)
                    } else if r_in_left && l_in_right {
                        (r_expr, l_expr)
                    } else {
                        // Fallback: use original order
                        (l_expr, r_expr)
                    };

                    left_on.bind_columns(left_schema)?;
                    right_on.bind_columns(right_schema)?;

                    Ok(Box::new(PhysicalSortMergeJoin::new(
                        left_op,
                        right_op,
                        left_on,
                        right_on,
                        *join_type,
                        joined_schema,
                        self.temp_dir(),
                        self.memory_budget(),
                    )))
                }
            }
            LogicalPlan::Aggregate {
                source,
                group_by,
                aggrs,
                field_names,
                ..
            } => {
                let mut group_by = group_by.clone();
                for expr in &mut group_by {
                    expr.substitute_params(params)?;
                }
                let mut aggrs = aggrs.clone();
                for aggr in &mut aggrs {
                    match aggr {
                        crate::logical::AggregateExpr::Count { expr, .. }
                        | crate::logical::AggregateExpr::Sum { expr, .. }
                        | crate::logical::AggregateExpr::Min { expr, .. }
                        | crate::logical::AggregateExpr::Max { expr, .. }
                        | crate::logical::AggregateExpr::Avg { expr, .. } => {
                            expr.substitute_params(params)?;
                        }
                    }
                }
                let use_sorted_agg = is_plan_sorted_by(source, &group_by);
                let mut child_needed = HashSet::new();
                for expr in &group_by {
                    expr.collect_columns(&mut child_needed);
                }
                for aggr in &aggrs {
                    match aggr {
                        crate::logical::AggregateExpr::Count { expr, .. }
                        | crate::logical::AggregateExpr::Sum { expr, .. }
                        | crate::logical::AggregateExpr::Min { expr, .. }
                        | crate::logical::AggregateExpr::Max { expr, .. }
                        | crate::logical::AggregateExpr::Avg { expr, .. } => {
                            expr.collect_columns(&mut child_needed)
                        }
                    }
                }
                let child_cols: Vec<String> = child_needed.into_iter().collect();
                let src_op = self.compile_physical(source, tx, Some(&child_cols), params)?;
                for expr in &mut group_by {
                    expr.bind_columns(src_op.schema())
                        .map_err(|e| e.to_string())?;
                }
                for aggr in &mut aggrs {
                    match aggr {
                        crate::logical::AggregateExpr::Count { expr, .. }
                        | crate::logical::AggregateExpr::Sum { expr, .. }
                        | crate::logical::AggregateExpr::Min { expr, .. }
                        | crate::logical::AggregateExpr::Max { expr, .. }
                        | crate::logical::AggregateExpr::Avg { expr, .. } => {
                            expr.bind_columns(src_op.schema())
                                .map_err(|e| e.to_string())?;
                        }
                    }
                }
                let aggr_schema = LogicalPlan::new_aggregate(
                    LogicalPlan::Scan {
                        table_name: "".to_string(),
                        schema: src_op.schema().clone(),
                        range: None,
                    },
                    group_by.clone(),
                    aggrs.clone(),
                    field_names.clone(),
                )
                .schema();

                if use_sorted_agg {
                    Ok(Box::new(PhysicalSortedAggregate::new(
                        src_op,
                        group_by,
                        aggrs,
                        aggr_schema,
                    )))
                } else {
                    Ok(Box::new(PhysicalHashAggregate::new(
                        src_op,
                        group_by,
                        aggrs,
                        aggr_schema,
                        self.temp_dir(),
                        self.memory_budget(),
                    )))
                }
            }
            LogicalPlan::SetOp {
                left,
                right,
                op,
                all,
            } => {
                let left_op = self.compile_physical(left, tx, columns, params)?;
                let right_op = self.compile_physical(right, tx, columns, params)?;
                let schema = left_op.schema().clone();
                Ok(Box::new(PhysicalSetOp::new(
                    left_op,
                    right_op,
                    *op,
                    *all,
                    schema,
                    self.temp_dir(),
                    self.memory_budget(),
                )))
            }
            LogicalPlan::Distinct { source } => {
                let src_op = self.compile_physical(source, tx, columns, params)?;
                Ok(Box::new(PhysicalDistinct::new(
                    src_op,
                    self.temp_dir(),
                    self.memory_budget(),
                )))
            }
        }
    }
}

fn schema_contains_col(schema: &Schema, col_name: &str) -> bool {
    schema.matches_column(col_name)
}

/// Returns the mutable inner expression of an aggregate call (the `col` in
/// `SUM(col)`), so the fold pass can rewrite subqueries used as aggregate
/// arguments.
fn aggregate_expr_mut(aggr: &mut AggregateExpr) -> &mut Expr {
    match aggr {
        AggregateExpr::Count { expr, .. }
        | AggregateExpr::Sum { expr, .. }
        | AggregateExpr::Min { expr, .. }
        | AggregateExpr::Max { expr, .. }
        | AggregateExpr::Avg { expr, .. } => expr,
    }
}

/// Whether any expression anywhere in `plan` contains a subquery node. Used to
/// skip the clone-and-fold pass for the common no-subquery case.
fn plan_contains_subquery(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Scan { .. }
        | LogicalPlan::IndexScan { .. }
        | LogicalPlan::FullTextScan { .. } => false,
        LogicalPlan::Filter { source, predicate } => {
            expr_contains_subquery(predicate) || plan_contains_subquery(source)
        }
        LogicalPlan::Projection {
            source,
            expressions,
            ..
        } => expressions.iter().any(expr_contains_subquery) || plan_contains_subquery(source),
        LogicalPlan::Join {
            left,
            right,
            condition,
            ..
        } => {
            expr_contains_subquery(condition)
                || plan_contains_subquery(left)
                || plan_contains_subquery(right)
        }
        LogicalPlan::Aggregate {
            source,
            group_by,
            aggrs,
            ..
        } => {
            group_by.iter().any(expr_contains_subquery)
                || aggrs
                    .iter()
                    .any(|a| expr_contains_subquery(aggregate_expr_ref(a)))
                || plan_contains_subquery(source)
        }
        LogicalPlan::Sort { source, keys } => {
            keys.iter().any(|(e, _)| expr_contains_subquery(e)) || plan_contains_subquery(source)
        }
        LogicalPlan::Limit { source, .. } | LogicalPlan::Distinct { source } => {
            plan_contains_subquery(source)
        }
        LogicalPlan::SetOp { left, right, .. } => {
            plan_contains_subquery(left) || plan_contains_subquery(right)
        }
    }
}

/// Shared-reference counterpart of [`aggregate_expr_mut`].
fn aggregate_expr_ref(aggr: &AggregateExpr) -> &Expr {
    match aggr {
        AggregateExpr::Count { expr, .. }
        | AggregateExpr::Sum { expr, .. }
        | AggregateExpr::Min { expr, .. }
        | AggregateExpr::Max { expr, .. }
        | AggregateExpr::Avg { expr, .. } => expr,
    }
}

fn expr_contains_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::ScalarSubquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. } => true,
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_subquery(left) || expr_contains_subquery(right)
        }
        Expr::Not(inner) | Expr::IsNull(inner) => expr_contains_subquery(inner),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand.as_deref().is_some_and(expr_contains_subquery)
                || conditions.iter().any(expr_contains_subquery)
                || results.iter().any(expr_contains_subquery)
                || else_result.as_deref().is_some_and(expr_contains_subquery)
        }
        Expr::Function { args, .. } => args.iter().any(expr_contains_subquery),
        Expr::InList { expr, list } => {
            expr_contains_subquery(expr) || list.iter().any(expr_contains_subquery)
        }
        Expr::Cast { expr, .. } => expr_contains_subquery(expr),
        Expr::Column(..) | Expr::Literal(_) | Expr::Parameter(_) | Expr::Match { .. } => false,
    }
}

fn parent_cols_union(
    parent_cols: Option<&[String]>,
    extra_cols: &HashSet<String>,
) -> Option<Vec<String>> {
    parent_cols.map(|cols| {
        let mut set: HashSet<String> = cols.iter().cloned().collect();
        for col in extra_cols {
            set.insert(col.clone());
        }
        set.into_iter().collect()
    })
}

fn preprocess_sql(sql: &str) -> String {
    let sql_trimmed = sql.trim();
    let sql_upper = sql_trimmed.to_ascii_uppercase();
    if sql_upper.starts_with("CREATE FULLTEXT INDEX") {
        let rest = sql_trimmed["CREATE FULLTEXT INDEX".len()..].trim();
        let rest_upper = rest.to_ascii_uppercase();
        if let Some(on_idx) = rest_upper.find(" ON ") {
            let idx_name = rest[..on_idx].trim();
            let after_on = rest[on_idx + " ON ".len()..].trim();
            if let Some(paren_idx) = after_on.find('(') {
                let tbl_name = after_on[..paren_idx].trim();
                let after_paren = &after_on[paren_idx..];
                if let Some(rparen_idx) = after_paren.find(')') {
                    let col_name = after_paren[1..rparen_idx].trim();
                    let after_cols = after_paren[rparen_idx + 1..].trim();
                    let after_cols_upper = after_cols.to_ascii_uppercase();
                    let tokenizer = if after_cols_upper.starts_with("USING ") {
                        after_cols["USING ".len()..].trim()
                    } else {
                        "simple"
                    };
                    return format!(
                        "CREATE INDEX {} ON {} USING {} ({})",
                        idx_name, tbl_name, tokenizer, col_name
                    );
                }
            }
        }
    }
    sql.to_string()
}
