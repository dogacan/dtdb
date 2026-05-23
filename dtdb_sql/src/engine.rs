use std::sync::Arc;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use dtdb_storage::{DbKey, DbValue};
use dtdb_relational::{DataType, Database, Row, Schema, Transaction};
use crate::expr::{Expr, Operator};
use crate::logical::LogicalPlan;
use crate::optimizer::Optimizer;
use crate::planner::{LogicalPlanner, SqlStatement};
use crate::physical::{
    PhysicalFilter, PhysicalHashAggregate, PhysicalHashJoin, PhysicalLimit,
    PhysicalOperator, PhysicalProjection, PhysicalSeqScan, PhysicalSort,
};

/// Represents the tabular or DDL execution output of a SQL query.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionResult {
    CreateTable,
    DropTable,
    Insert { count: usize },
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

    /// Parses, plans, optimizes, and executes a SQL query within the given transaction.
    pub fn execute(&self, sql: &str, tx: &Transaction) -> Result<ExecutionResult, String> {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;
        if statements.is_empty() {
            return Err("No SQL statements found".to_string());
        }

        // We execute the first statement in the query block
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
                    // Start with default values for each column.
                    let mut aligned_vals = vec![DbValue::Int(0); schema.columns.len()];
                    for (i, col) in schema.columns.iter().enumerate() {
                        aligned_vals[i] = match col.data_type {
                            DataType::Int => DbValue::Int(0),
                            DataType::Float => DbValue::Float(0.0),
                            DataType::String => DbValue::String("".to_string()),
                            DataType::Bytes => DbValue::Bytes(Vec::new()),
                        };
                    }

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

                    tx.put(&table_name, pk_key, full_row).map_err(|e| e.to_string())?;
                    insert_count += 1;
                }

                Ok(ExecutionResult::Insert { count: insert_count })
            }
            SqlStatement::Query(logical_plan) => {
                // 1. Optimize the Logical Plan
                let optimized_plan = Optimizer::new().optimize(logical_plan);

                // 2. Compile to Volcano Physical Plan
                let mut physical_op = self.compile_physical(optimized_plan, tx)?;

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
        }
    }

    /// Compiles a logical plan node into a Volcano Physical Operator tree.
    fn compile_physical(
        &self,
        plan: LogicalPlan,
        tx: &Transaction,
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
                        let pk_idx = schema
                            .primary_key_index()
                            .ok_or_else(|| "No primary key found for Scan compilation".to_string())?;
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
                    .filtered_scan(&table_name, &start, &end, |_| true)
                    .map_err(|e| e.to_string())?;

                Ok(Box::new(PhysicalSeqScan::new(schema, rows)))
            }
            LogicalPlan::Filter { source, predicate } => {
                let src_op = self.compile_physical(*source, tx)?;
                Ok(Box::new(PhysicalFilter::new(src_op, predicate)))
            }
            LogicalPlan::Projection {
                source,
                expressions,
                field_names,
            } => {
                let src_op = self.compile_physical(*source, tx)?;
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
            LogicalPlan::Limit { source, limit } => {
                let src_op = self.compile_physical(*source, tx)?;
                Ok(Box::new(PhysicalLimit::new(src_op, limit)))
            }
            LogicalPlan::Sort { source, keys } => {
                let src_op = self.compile_physical(*source, tx)?;
                Ok(Box::new(PhysicalSort::new(src_op, keys)))
            }
            LogicalPlan::Join {
                left,
                right,
                condition,
            } => {
                let left_op = self.compile_physical(*left, tx)?;
                let right_op = self.compile_physical(*right, tx)?;

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
                        ))
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
                }
                .schema();

                Ok(Box::new(PhysicalHashJoin::new(
                    left_op,
                    right_op,
                    *left_on,
                    *right_on,
                    joined_schema,
                )))
            }
            LogicalPlan::Aggregate {
                source,
                group_by,
                aggrs,
                field_names,
            } => {
                let src_op = self.compile_physical(*source, tx)?;
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
