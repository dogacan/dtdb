use crate::expr::{Expr, Operator};
use crate::logical::{JoinType, LogicalPlan, format_logical_plan};
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
use std::sync::{Arc, Mutex};

/// Capacity (in entries) of the per-engine parsed-statement cache. SQL text is
/// typically a small, fixed set of templates (the user-facing `sql_query!`
/// macro requires compile-time literal strings), so this comfortably holds a
/// real application's working set; pathological dynamic SQL is bounded by LRU
/// eviction. Entries are stored with size 1 so the byte budget is an entry count.
const PARSE_CACHE_CAPACITY: usize = 1024;

/// Represents the tabular or DDL execution output of a SQL query.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionResult {
    CreateTable,
    DropTable,
    CreateIndex,
    DropIndex,
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
    key: &'a DbKey,
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
fn match_point_get(plan: &LogicalPlan) -> Option<PointGetPlan<'_>> {
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
        } if start == end => Some(PointGetPlan {
            projection,
            filter,
            table_name,
            scan_schema: schema,
            key: start,
        }),
        _ => None,
    }
}

/// Substitutes bound parameter values into a planned statement, turning its
/// symbolic `Expr::Parameter` nodes into concrete literals before execution.
fn substitute_statement_params(
    planned: &mut SqlStatement,
    params: &std::collections::HashMap<String, DbValue>,
) -> Result<(), String> {
    match planned {
        SqlStatement::Query(plan) | SqlStatement::Explain(plan) => plan.substitute_params(params),
        SqlStatement::InsertSelect { query, .. } => query.substitute_params(params),
        SqlStatement::Delete { filter, .. } => {
            if let Some(f) = filter {
                f.substitute_params(params)?;
            }
            Ok(())
        }
        SqlStatement::Update {
            assignments,
            filter,
            ..
        } => {
            for (_, e) in assignments.iter_mut() {
                e.substitute_params(params)?;
            }
            if let Some(f) = filter {
                f.substitute_params(params)?;
            }
            Ok(())
        }
        // Insert holds concrete values (parameters can't survive planning into
        // it); DDL and Analyze carry no expressions.
        SqlStatement::Insert { .. }
        | SqlStatement::CreateTable { .. }
        | SqlStatement::DropTable { .. }
        | SqlStatement::CreateIndex { .. }
        | SqlStatement::DropIndex { .. }
        | SqlStatement::Analyze { .. } => Ok(()),
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
}

impl SqlEngine {
    pub fn new(database: Arc<Database>) -> Self {
        Self {
            database,
            parse_cache: Mutex::new(LruCache::new(PARSE_CACHE_CAPACITY)),
        }
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
            Ok(planned) => PreparedPlan::Planned(Box::new(planned)),
            Err(_) => PreparedPlan::Ast(Box::new(statement)),
        };

        // Record the catalog version the plan was built against so later DDL can
        // be detected and the plan re-built (see `PreparedStatement::catalog_version`).
        Ok(PreparedStatement {
            plan,
            sql: sql.to_string(),
            catalog_version: self.database.schema_version(),
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
        let dialect = GenericDialect {};
        let statement = Parser::parse_sql(&dialect, &preprocessed)
            .map_err(|e| e.to_string())?
            .into_iter()
            .next()
            .ok_or_else(|| "No SQL statements found".to_string())?;
        Ok(Some(
            match LogicalPlanner::new(self.database.clone()).plan(&statement) {
                Ok(planned) => PreparedPlan::Planned(Box::new(planned)),
                Err(_) => PreparedPlan::Ast(Box::new(statement)),
            },
        ))
    }

    /// Executes a previously [`prepare`](Self::prepare)d statement within `tx`,
    /// binding `params`. Reuses the cached parse, and — when the plan was
    /// cacheable — the cached logical plan, substituting the bound values into
    /// it before optimization and execution.
    pub fn execute_prepared(
        &self,
        prepared: &PreparedStatement,
        tx: &Transaction,
        params: &std::collections::HashMap<String, DbValue>,
    ) -> Result<ExecutionResult, String> {
        let refreshed = self.refreshed_plan(prepared)?;
        match refreshed.as_ref().unwrap_or(&prepared.plan) {
            PreparedPlan::Planned(planned) => {
                let mut planned = (**planned).clone();
                substitute_statement_params(&mut planned, params)?;
                self.execute_planned(planned, tx)
            }
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
            PreparedPlan::Planned(planned) => {
                let mut planned = (**planned).clone();
                substitute_statement_params(&mut planned, params)?;
                self.execute_planned_streaming(planned, tx)
            }
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
        self.execute_planned(planned_stmt, tx)
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
        self.execute_planned_streaming(planned_stmt, tx)
    }

    /// Optimizes, compiles, and executes an already-planned statement in streaming mode.
    fn execute_planned_streaming(
        &self,
        planned_stmt: SqlStatement,
        tx: &Transaction,
    ) -> Result<ExecutionStreamingResult, String> {
        match planned_stmt {
            SqlStatement::Query(logical_plan) => {
                // 1. Optimize the Logical Plan
                let optimized_plan = Optimizer::new(self.database.clone()).optimize(logical_plan);

                // 1a. Fast path: primary-key point lookups skip the Volcano
                // operator tree entirely (see `execute_point_get`).
                if let Some(result) = self.execute_point_get(&optimized_plan, tx)? {
                    return Ok(ExecutionStreamingResult::Complete(result));
                }

                // Collect referenced columns
                let mut cols = HashSet::new();
                optimized_plan.collect_columns(&mut cols);
                let cols_vec: Vec<String> = cols.into_iter().collect();

                // 2. Compile to Volcano Physical Plan
                let physical_op = self.compile_physical(optimized_plan, tx, Some(&cols_vec))?;

                Ok(ExecutionStreamingResult::Streaming {
                    schema: physical_op.schema().clone(),
                    operator: physical_op,
                })
            }
            SqlStatement::Explain(logical_plan) => {
                let result = self.execute_planned(SqlStatement::Explain(logical_plan), tx)?;
                Ok(ExecutionStreamingResult::Complete(result))
            }
            other => {
                let result = self.execute_planned(other, tx)?;
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
        planned_stmt: SqlStatement,
        tx: &Transaction,
    ) -> Result<ExecutionResult, String> {
        match planned_stmt {
            SqlStatement::CreateTable { name, schema } => {
                self.database
                    .create_table(&name, schema)
                    .map_err(|e| e.to_string())?;
                Ok(ExecutionResult::CreateTable)
            }
            SqlStatement::DropTable { name } => {
                self.database.drop_table(&name).map_err(|e| e.to_string())?;
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
                    .create_index(&table_name, &index_name, columns, index_type, tokenizer)
                    .map_err(|e| e.to_string())?;
                Ok(ExecutionResult::CreateIndex)
            }
            SqlStatement::DropIndex {
                table_name,
                index_name,
            } => {
                self.database
                    .drop_index(&table_name, &index_name)
                    .map_err(|e| e.to_string())?;
                Ok(ExecutionResult::DropIndex)
            }
            SqlStatement::Insert {
                table_name,
                columns,
                rows,
            } => {
                let table = self
                    .database
                    .get_table(&table_name)
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
                                    .next_sequence_value(&table_name)
                                    .map_err(|e| e.to_string())?;
                                aligned_vals[col_idx] = DbValue::Int(next_val);
                            } else if let DbValue::Int(explicit_val) = aligned_vals[col_idx] {
                                self.database
                                    .update_sequence_value(&table_name, explicit_val)
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
                        .get(&table_name, &pk_key)
                        .map_err(|e| e.to_string())?
                        .is_some()
                    {
                        return Err("Duplicate primary key".to_string());
                    }

                    tx.put(&table_name, pk_key, full_row)
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
                query,
            } => {
                let table = self
                    .database
                    .get_table(&table_name)
                    .map_err(|e| e.to_string())?;

                let schema = table.schema.clone();
                let mut insert_count = 0;

                // 1. Optimize the Logical Plan
                let optimized_plan = Optimizer::new(self.database.clone()).optimize(query);

                // Collect referenced columns
                let mut cols = HashSet::new();
                optimized_plan.collect_columns(&mut cols);
                let cols_vec: Vec<String> = cols.into_iter().collect();

                // 2. Compile to Volcano Physical Plan
                let mut physical_op = self.compile_physical(optimized_plan, tx, Some(&cols_vec))?;

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
                                    .next_sequence_value(&table_name)
                                    .map_err(|e| e.to_string())?;
                                aligned_vals[col_idx] = DbValue::Int(next_val);
                            } else if let DbValue::Int(explicit_val) = aligned_vals[col_idx] {
                                self.database
                                    .update_sequence_value(&table_name, explicit_val)
                                    .map_err(|e| e.to_string())?;
                            }
                        }
                    }

                    let full_row = Row::new(aligned_vals);
                    let pk_key = schema
                        .extract_primary_key(&full_row)
                        .map_err(|e| e.to_string())?;

                    if tx
                        .get(&table_name, &pk_key)
                        .map_err(|e| e.to_string())?
                        .is_some()
                    {
                        return Err("Duplicate primary key".to_string());
                    }

                    tx.put(&table_name, pk_key, full_row)
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
                    .get_table(&table_name)
                    .map_err(|e| e.to_string())?;

                let scan_plan = LogicalPlan::Scan {
                    table_name: table_name.clone(),
                    schema: table.schema.clone(),
                    range: None,
                };

                let plan = match filter {
                    Some(pred) => LogicalPlan::Filter {
                        source: Box::new(scan_plan),
                        predicate: pred,
                    },
                    None => scan_plan,
                };

                let optimized_plan = Optimizer::new(self.database.clone()).optimize(plan);
                let mut physical_op = self.compile_physical(optimized_plan, tx, None)?;

                let mut delete_count = 0;
                while let Some(row) = physical_op.next()? {
                    let pk_key = table
                        .schema
                        .extract_primary_key(&row)
                        .map_err(|e| e.to_string())?;
                    tx.delete(&table_name, pk_key).map_err(|e| e.to_string())?;
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
                    .get_table(&table_name)
                    .map_err(|e| e.to_string())?;

                let scan_plan = LogicalPlan::Scan {
                    table_name: table_name.clone(),
                    schema: table.schema.clone(),
                    range: None,
                };

                let plan = match filter {
                    Some(pred) => LogicalPlan::Filter {
                        source: Box::new(scan_plan),
                        predicate: pred,
                    },
                    None => scan_plan,
                };

                let optimized_plan = Optimizer::new(self.database.clone()).optimize(plan);
                let mut physical_op = self.compile_physical(optimized_plan, tx, None)?;

                let mut update_count = 0;
                while let Some(row) = physical_op.next()? {
                    let mut updated_values = row.values.clone();
                    for (col_name, expr) in &assignments {
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
                        tx.delete(&table_name, old_pk_key)
                            .map_err(|e| e.to_string())?;
                        tx.put(&table_name, new_pk_key, updated_row)
                            .map_err(|e| e.to_string())?;
                    } else {
                        tx.put(&table_name, new_pk_key, updated_row)
                            .map_err(|e| e.to_string())?;
                    }
                    update_count += 1;
                }

                Ok(ExecutionResult::Update {
                    count: update_count,
                })
            }
            SqlStatement::Query(logical_plan) => {
                // 1. Optimize the Logical Plan
                let optimized_plan = Optimizer::new(self.database.clone()).optimize(logical_plan);

                // 1a. Fast path: primary-key point lookups skip the Volcano
                // operator tree entirely (see `execute_point_get`).
                if let Some(result) = self.execute_point_get(&optimized_plan, tx)? {
                    return Ok(result);
                }

                // Collect referenced columns
                let mut cols = HashSet::new();
                optimized_plan.collect_columns(&mut cols);
                let cols_vec: Vec<String> = cols.into_iter().collect();

                // 2. Compile to Volcano Physical Plan
                let mut physical_op = self.compile_physical(optimized_plan, tx, Some(&cols_vec))?;

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
                let logical_str = format_logical_plan(&logical_plan);
                let opt_logical_str = format_logical_plan(&optimized_plan);

                // 3. Describe the physical plan. Point lookups bypass the
                // Volcano tree (see `execute_point_get`), so report that here
                // rather than compiling operators that would never run.
                let physical_str = if let Some(pg) = match_point_get(&optimized_plan) {
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

                    let physical_op = self.compile_physical(optimized_plan, tx, Some(&cols_vec))?;
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

                let rows = vec![Row::new(vec![DbValue::String(plan_info)])];

                Ok(ExecutionResult::Select { schema, rows })
            }
            SqlStatement::Analyze { table_name } => {
                self.database
                    .analyze_table(&table_name, tx)
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
    ) -> Result<Option<ExecutionResult>, String> {
        let Some(pg) = match_point_get(plan) else {
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
            .get_projected(pg.table_name, pg.key, Some(&needed_cols))
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

    /// Compiles a logical plan node into a Volcano Physical Operator tree.
    fn compile_physical(
        &self,
        plan: LogicalPlan,
        tx: &Transaction,
        columns: Option<&[String]>,
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
                    Some(r) => r,
                    None => {
                        let idx_def = schema
                            .indexes
                            .iter()
                            .find(|idx| idx.name == index_name)
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
                        match col.data_type {
                            DataType::Int => (DbKey::Int(i64::MIN), DbKey::Int(i64::MAX)),
                            DataType::Bool => (DbKey::Bool(false), DbKey::Bool(true)),
                            _ => (
                                DbKey::String("".to_string()),
                                DbKey::String("\u{10ffff}".to_string()),
                            ),
                        }
                    }
                };

                let rows = tx
                    .index_scan(&table_name, &index_name, &start, &end, columns)
                    .map_err(|e| e.to_string())?;

                Ok(Box::new(PhysicalIndexScan::new(schema, rows)))
            }
            LogicalPlan::FullTextScan {
                table_name,
                index_name,
                schema,
                query_str,
            } => {
                let rows = tx
                    .fulltext_scan(&table_name, &index_name, &query_str, columns)
                    .map_err(|e| e.to_string())?;

                Ok(Box::new(PhysicalFullTextScan::new(schema, query_str, rows)))
            }
            LogicalPlan::Scan {
                table_name,
                schema,
                range,
            } => {
                // Calculate scan range bounds.
                let (start, end) = match range {
                    Some(r) => r,
                    None => schema.primary_key_bounds().map_err(|e| e.to_string())?,
                };

                let iter = tx
                    .scan_iter(&table_name, &start, &end, columns)
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
                    schema,
                    Box::new(RelationalIteratorAdapter { iter }),
                )))
            }
            LogicalPlan::Filter {
                source,
                mut predicate,
            } => {
                let mut predicate_cols = HashSet::new();
                predicate.collect_columns(&mut predicate_cols);
                let child_cols = parent_cols_union(columns, &predicate_cols);
                let src_op = self.compile_physical(*source, tx, child_cols.as_deref())?;
                predicate
                    .bind_columns(src_op.schema())
                    .map_err(|e| e.to_string())?;
                Ok(Box::new(PhysicalFilter::new(src_op, predicate)))
            }
            LogicalPlan::Projection {
                source,
                mut expressions,
                field_names,
                ..
            } => {
                let mut child_needed = HashSet::new();
                for expr in &expressions {
                    expr.collect_columns(&mut child_needed);
                }
                let child_cols: Vec<String> = child_needed.into_iter().collect();
                let src_op = self.compile_physical(*source, tx, Some(&child_cols))?;
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
                let src_op = self.compile_physical(*source, tx, columns)?;
                Ok(Box::new(PhysicalLimit::new(src_op, limit, offset)))
            }
            LogicalPlan::Sort { source, mut keys } => {
                let mut key_cols = HashSet::new();
                for (expr, _) in &keys {
                    expr.collect_columns(&mut key_cols);
                }
                let child_cols = parent_cols_union(columns, &key_cols);
                let src_op = self.compile_physical(*source, tx, child_cols.as_deref())?;
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

                    let l_op = self.compile_physical(*left, tx, Some(&left_cols))?;
                    let r_op = self.compile_physical(*right, tx, Some(&right_cols))?;
                    (l_op, r_op)
                } else {
                    let l_op = self.compile_physical(*left, tx, None)?;
                    let r_op = self.compile_physical(*right, tx, None)?;
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
                    join_type,
                )
                .schema();

                if join_type == JoinType::Cross {
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
                        } => ((*l).clone(), (*r).clone()),
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
                        *left_on,
                        *right_on,
                        join_type,
                        joined_schema,
                        self.temp_dir(),
                        self.memory_budget(),
                    )))
                }
            }
            LogicalPlan::Aggregate {
                source,
                mut group_by,
                mut aggrs,
                field_names,
                ..
            } => {
                let use_sorted_agg = is_plan_sorted_by(&source, &group_by);
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
                let src_op = self.compile_physical(*source, tx, Some(&child_cols))?;
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
                let left_op = self.compile_physical(*left, tx, columns)?;
                let right_op = self.compile_physical(*right, tx, columns)?;
                let schema = left_op.schema().clone();
                Ok(Box::new(PhysicalSetOp::new(
                    left_op,
                    right_op,
                    op,
                    all,
                    schema,
                    self.temp_dir(),
                    self.memory_budget(),
                )))
            }
            LogicalPlan::Distinct { source } => {
                let src_op = self.compile_physical(*source, tx, columns)?;
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
