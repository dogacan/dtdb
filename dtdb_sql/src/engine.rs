use crate::SqlQuery;
use crate::expr::{Expr, Operator};
use crate::logical::{LogicalPlan, format_logical_plan};
use crate::optimizer::Optimizer;
use crate::physical::{
    PhysicalFilter, PhysicalHashAggregate, PhysicalHashJoin, PhysicalLimit, PhysicalOperator,
    PhysicalProjection, PhysicalSeqScan, PhysicalSort,
};
use crate::planner::{LogicalPlanner, SqlStatement};
use dtdb_relational::{DataType, Database, Row, Schema, Transaction};
use dtdb_storage::{DbKey, DbValue};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashSet;
use std::sync::Arc;

/// Represents the tabular or DDL execution output of a SQL query.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionResult {
    CreateTable,
    DropTable,
    Insert { count: usize },
    Delete { count: usize },
    Update { count: usize },
    Select { schema: Schema, rows: Vec<Row> },
}

/// SqlEngine orchestrates the parser, logical planner, optimizer, and physical execution pipeline.
pub struct SqlEngine {
    database: Arc<Database>,
}

impl SqlEngine {
    pub fn new(database: Arc<Database>) -> Self {
        Self { database }
    }

    /// Checks if the SQL string contains a DDL statement (CREATE TABLE or DROP TABLE).
    pub fn is_ddl(&self, sql: &str) -> bool {
        let dialect = GenericDialect {};
        if let Ok(statements) = Parser::parse_sql(&dialect, sql)
            && !statements.is_empty()
        {
            return matches!(
                statements[0],
                sqlparser::ast::Statement::CreateTable { .. }
                    | sqlparser::ast::Statement::Drop { .. }
            );
        }
        false
    }

    /// Executes a parameterized SqlQuery safely by interpolating bound parameters first.
    pub fn execute_query(
        &self,
        query: &SqlQuery,
        tx: &Transaction,
    ) -> Result<ExecutionResult, String> {
        let sql = query.interpolate()?;
        self.execute(&sql, tx)
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
    /// atomicity semantics: exactly one statement, exactly one transaction.
    pub fn execute(&self, sql: &str, tx: &Transaction) -> Result<ExecutionResult, String> {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;
        if statements.is_empty() {
            return Err("No SQL statements found".to_string());
        }

        // Reject multi-statement inputs. Callers who need to run multiple statements
        // in a single transaction must use RunInTransaction or the Transaction RPC.
        // See the doc comment above for the full rationale.
        if statements.len() > 1 {
            return Err(
                "Multiple SQL statements in a single execute() call are not allowed. \
                 Use DuctTapeDbClient::run_in_transaction() or the Transaction RPC to \
                 execute multiple statements within a single transaction."
                    .to_string(),
            );
        }

        let statement = &statements[0];
        let planned_stmt = LogicalPlanner::new(self.database.clone()).plan(statement)?;

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
                    // Start with NULL values for each column.
                    let mut aligned_vals = vec![DbValue::Null; schema.columns.len()];

                    // Map provided row values to the correct column indices.
                    for (col_idx, col_name) in columns.iter().enumerate() {
                        let schema_idx = schema
                            .column_index(col_name)
                            .ok_or_else(|| format!("Column '{}' not found in schema", col_name))?;
                        aligned_vals[schema_idx] = row_vals[col_idx].clone();
                    }

                    let full_row = Row::new(aligned_vals);

                    // Extract the primary key value.
                    let pk_idx = schema
                        .primary_key_index()
                        .ok_or_else(|| "Primary key not defined".to_string())?;
                    let pk_val = &full_row.values[pk_idx];

                    let pk_key = match pk_val {
                        DbValue::Int(v) => DbKey::Int(*v),
                        DbValue::String(s) => DbKey::String(s.clone()),
                        other => return Err(format!("Invalid primary key type: {:?}", other)),
                    };

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

                let optimized_plan = Optimizer::new().optimize(plan);
                let mut physical_op = self.compile_physical(optimized_plan, tx, None)?;

                let mut delete_count = 0;
                let pk_idx = table
                    .schema
                    .primary_key_index()
                    .ok_or_else(|| "Primary key not defined".to_string())?;

                let mut keys_to_delete = Vec::new();
                while let Some(row) = physical_op.next()? {
                    let pk_val = &row.values[pk_idx];
                    let pk_key = match pk_val {
                        DbValue::Int(v) => DbKey::Int(*v),
                        DbValue::String(s) => DbKey::String(s.clone()),
                        other => return Err(format!("Invalid primary key type: {:?}", other)),
                    };
                    keys_to_delete.push(pk_key);
                }

                for pk_key in keys_to_delete {
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

                let optimized_plan = Optimizer::new().optimize(plan);
                let mut physical_op = self.compile_physical(optimized_plan, tx, None)?;

                let mut update_count = 0;
                let pk_idx = table
                    .schema
                    .primary_key_index()
                    .ok_or_else(|| "Primary key not defined".to_string())?;

                let mut updates = Vec::new();
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

                    let old_pk_val = &row.values[pk_idx];
                    let old_pk_key = match old_pk_val {
                        DbValue::Int(v) => DbKey::Int(*v),
                        DbValue::String(s) => DbKey::String(s.clone()),
                        other => return Err(format!("Invalid primary key type: {:?}", other)),
                    };

                    let new_pk_val = &updated_values[pk_idx];
                    let new_pk_key = match new_pk_val {
                        DbValue::Int(v) => DbKey::Int(*v),
                        DbValue::String(s) => DbKey::String(s.clone()),
                        other => return Err(format!("Invalid primary key type: {:?}", other)),
                    };

                    let updated_row = Row::new(updated_values);
                    updates.push((old_pk_key, new_pk_key, updated_row));
                }

                for (old_pk, new_pk, updated_row) in updates {
                    if old_pk != new_pk {
                        tx.delete(&table_name, old_pk).map_err(|e| e.to_string())?;
                        tx.put(&table_name, new_pk, updated_row)
                            .map_err(|e| e.to_string())?;
                    } else {
                        tx.put(&table_name, new_pk, updated_row)
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
                let optimized_plan = Optimizer::new().optimize(logical_plan);

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
                let optimized_plan = Optimizer::new().optimize(logical_plan.clone());

                // 2. Format logical plans as strings
                let logical_str = format_logical_plan(&logical_plan);
                let opt_logical_str = format_logical_plan(&optimized_plan);

                // Collect referenced columns
                let mut cols = HashSet::new();
                optimized_plan.collect_columns(&mut cols);
                let cols_vec: Vec<String> = cols.into_iter().collect();

                // 3. Compile physical and explain it
                let physical_op = self.compile_physical(optimized_plan, tx, Some(&cols_vec))?;
                let mut physical_str = String::new();
                physical_op.explain(0, &mut physical_str);

                // 4. Wrap the result in a select output with "Query Plan" schema column
                let schema = Schema::new(vec![dtdb_relational::Column {
                    name: "Query Plan".to_string(),
                    data_type: DataType::String,
                    is_primary_key: false,
                    is_nullable: true,
                    locality_group: None,
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
        }
    }

    /// Compiles a logical plan node into a Volcano Physical Operator tree.
    fn compile_physical(
        &self,
        plan: LogicalPlan,
        tx: &Transaction,
        columns: Option<&[String]>,
    ) -> Result<Box<dyn PhysicalOperator>, String> {
        match plan {
            LogicalPlan::Scan {
                table_name,
                schema,
                range,
            } => {
                // Calculate scan range bounds.
                let (start, end) = match range {
                    Some(r) => r,
                    None => {
                        let pk_idx = schema.primary_key_index().ok_or_else(|| {
                            "No primary key found for Scan compilation".to_string()
                        })?;
                        let pk_col = &schema.columns[pk_idx];
                        match pk_col.data_type {
                            DataType::Int => (DbKey::Int(i64::MIN), DbKey::Int(i64::MAX)),
                            _ => (
                                DbKey::String("".to_string()),
                                DbKey::String("\u{10ffff}".to_string()),
                            ),
                        }
                    }
                };

                let rows = tx
                    .filtered_scan_projected(&table_name, &start, &end, columns, |_| true)
                    .map_err(|e| e.to_string())?;

                Ok(Box::new(PhysicalSeqScan::new(schema, rows)))
            }
            LogicalPlan::Filter { source, predicate } => {
                let src_op = self.compile_physical(*source, tx, columns)?;
                Ok(Box::new(PhysicalFilter::new(src_op, predicate)))
            }
            LogicalPlan::Projection {
                source,
                expressions,
                field_names,
            } => {
                let src_op = self.compile_physical(*source, tx, columns)?;
                let proj_schema = LogicalPlan::Projection {
                    source: Box::new(LogicalPlan::Scan {
                        table_name: "".to_string(),
                        schema: src_op.schema().clone(),
                        range: None,
                    }),
                    expressions: expressions.clone(),
                    field_names: field_names.clone(),
                }
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
            LogicalPlan::Sort { source, keys } => {
                let src_op = self.compile_physical(*source, tx, columns)?;
                Ok(Box::new(PhysicalSort::new(src_op, keys)))
            }
            LogicalPlan::Join {
                left,
                right,
                condition,
                join_type,
            } => {
                let left_op = self.compile_physical(*left, tx, columns)?;
                let right_op = self.compile_physical(*right, tx, columns)?;

                // Extract left_on and right_on join keys from equality join condition
                let (left_on, right_on) = match &condition {
                    Expr::BinaryOp {
                        left: l,
                        op: Operator::Eq,
                        right: r,
                    } => (l.clone(), r.clone()),
                    other => {
                        return Err(format!(
                            "Only equality join conditions supported, got {:?}",
                            other
                        ));
                    }
                };

                let joined_schema = LogicalPlan::Join {
                    left: Box::new(LogicalPlan::Scan {
                        table_name: "".to_string(),
                        schema: left_op.schema().clone(),
                        range: None,
                    }),
                    right: Box::new(LogicalPlan::Scan {
                        table_name: "".to_string(),
                        schema: right_op.schema().clone(),
                        range: None,
                    }),
                    condition: condition.clone(),
                    join_type,
                }
                .schema();

                Ok(Box::new(PhysicalHashJoin::new(
                    left_op,
                    right_op,
                    *left_on,
                    *right_on,
                    join_type,
                    joined_schema,
                )))
            }
            LogicalPlan::Aggregate {
                source,
                group_by,
                aggrs,
                field_names,
            } => {
                let src_op = self.compile_physical(*source, tx, columns)?;
                let aggr_schema = LogicalPlan::Aggregate {
                    source: Box::new(LogicalPlan::Scan {
                        table_name: "".to_string(),
                        schema: src_op.schema().clone(),
                        range: None,
                    }),
                    group_by: group_by.clone(),
                    aggrs: aggrs.clone(),
                    field_names: field_names.clone(),
                }
                .schema();

                Ok(Box::new(PhysicalHashAggregate::new(
                    src_op,
                    group_by,
                    aggrs,
                    aggr_schema,
                )))
            }
        }
    }
}
