use crate::expr::{Expr, Operator};
use crate::logical::LogicalPlan;
use dtdb_relational::{DataType, Schema};
use dtdb_storage::DbKey;
use dtdb_storage::DbValue;

/// Logical Optimizer applies rule-based optimizations to a LogicalPlan.
pub struct Optimizer;

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Optimizer {
    pub fn new() -> Self {
        Self
    }

    /// Optimizes a LogicalPlan recursively.
    pub fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        self.optimize_node(plan)
    }

    fn optimize_node(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter { source, predicate } => {
                let opt_source = self.optimize_node(*source);
                match opt_source {
                    LogicalPlan::Scan {
                        table_name,
                        schema,
                        range: _,
                    } => {
                        // Rule-based secondary index selection:
                        // Search for a matching predicate on the leading column of any index.
                        let mut index_match = None;
                        for index in &schema.indexes {
                            if let Some(col_name) = index.columns.first()
                                && let Some(col) = schema.columns.iter().find(|c| {
                                    c.name == *col_name
                                        || c.name.ends_with(&format!(".{}", col_name))
                                })
                                && let Some((start, end)) =
                                    extract_bounds_for_column(&predicate, &col.name, &col.data_type)
                            {
                                index_match = Some((index.name.clone(), start, end));
                                break;
                            }
                        }

                        if let Some((index_name, start, end)) = index_match {
                            LogicalPlan::Filter {
                                source: Box::new(LogicalPlan::IndexScan {
                                    table_name,
                                    index_name,
                                    schema,
                                    range: Some((start, end)),
                                }),
                                predicate,
                            }
                        } else if let Some((start, end)) = extract_key_range(&predicate, &schema) {
                            LogicalPlan::Filter {
                                source: Box::new(LogicalPlan::Scan {
                                    table_name,
                                    schema,
                                    range: Some((start, end)),
                                }),
                                predicate,
                            }
                        } else {
                            LogicalPlan::Filter {
                                source: Box::new(LogicalPlan::Scan {
                                    table_name,
                                    schema,
                                    range: None,
                                }),
                                predicate,
                            }
                        }
                    }
                    other => LogicalPlan::Filter {
                        source: Box::new(other),
                        predicate,
                    },
                }
            }
            LogicalPlan::Projection {
                source,
                expressions,
                field_names,
            } => LogicalPlan::Projection {
                source: Box::new(self.optimize_node(*source)),
                expressions,
                field_names,
            },
            LogicalPlan::Join {
                left,
                right,
                condition,
                join_type,
            } => LogicalPlan::Join {
                left: Box::new(self.optimize_node(*left)),
                right: Box::new(self.optimize_node(*right)),
                condition,
                join_type,
            },
            LogicalPlan::Aggregate {
                source,
                group_by,
                aggrs,
                field_names,
            } => LogicalPlan::Aggregate {
                source: Box::new(self.optimize_node(*source)),
                group_by,
                aggrs,
                field_names,
            },
            LogicalPlan::Sort { source, keys } => LogicalPlan::Sort {
                source: Box::new(self.optimize_node(*source)),
                keys,
            },
            LogicalPlan::Limit {
                source,
                limit,
                offset,
            } => LogicalPlan::Limit {
                source: Box::new(self.optimize_node(*source)),
                limit,
                offset,
            },
            other => other,
        }
    }
}

enum Boundary {
    Lower(DbValue),
    Upper(DbValue),
}

fn get_column_boundary(expr: &Expr, col_name: &str) -> Option<Boundary> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if let Some(lit) = get_column_comparison(left, right, col_name) {
                match op {
                    Operator::GtEq | Operator::Gt => Some(Boundary::Lower(lit.clone())),
                    Operator::LtEq | Operator::Lt => Some(Boundary::Upper(lit.clone())),
                    _ => None,
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

fn get_column_comparison<'a>(
    left: &'a Expr,
    right: &'a Expr,
    col_name: &str,
) -> Option<&'a DbValue> {
    match (left, right) {
        (Expr::Column(name), Expr::Literal(lit))
            if name == col_name
                || name.ends_with(&format!(".{}", col_name))
                || col_name.ends_with(&format!(".{}", name)) =>
        {
            Some(lit)
        }
        (Expr::Literal(lit), Expr::Column(name))
            if name == col_name
                || name.ends_with(&format!(".{}", col_name))
                || col_name.ends_with(&format!(".{}", name)) =>
        {
            Some(lit)
        }
        _ => None,
    }
}

fn val_to_key(val: &DbValue) -> Option<DbKey> {
    match val {
        DbValue::Int(v) => Some(DbKey::Int(*v)),
        DbValue::String(s) => Some(DbKey::String(s.clone())),
        _ => None,
    }
}

/// Helper to examine an expression tree and extract range constraints for the given column.
fn extract_bounds_for_column(
    predicate: &Expr,
    col_name: &str,
    col_type: &DataType,
) -> Option<(DbKey, DbKey)> {
    match predicate {
        Expr::BinaryOp {
            left,
            op: Operator::Eq,
            right,
        } => {
            if let Some(lit) = get_column_comparison(left, right, col_name) {
                let key = val_to_key(lit)?;
                return Some((key.clone(), key));
            }
        }
        Expr::BinaryOp {
            left,
            op: Operator::And,
            right,
        } => {
            let boundary_l = get_column_boundary(left, col_name);
            let boundary_r = get_column_boundary(right, col_name);

            match (boundary_l, boundary_r) {
                (Some(Boundary::Lower(l_lit)), Some(Boundary::Upper(u_lit)))
                | (Some(Boundary::Upper(u_lit)), Some(Boundary::Lower(l_lit))) => {
                    let k_start = val_to_key(&l_lit)?;
                    let k_end = val_to_key(&u_lit)?;
                    return Some((k_start, k_end));
                }
                _ => {}
            }
        }
        Expr::BinaryOp { left, op, right } => {
            if let Some(lit) = get_column_comparison(left, right, col_name) {
                let key = val_to_key(lit)?;
                match op {
                    Operator::GtEq | Operator::Gt => {
                        let start = if matches!(op, Operator::Gt) {
                            match key {
                                DbKey::Int(v) => DbKey::Int(v + 1),
                                DbKey::String(s) => DbKey::String(s + "\0"),
                                _ => key,
                            }
                        } else {
                            key
                        };
                        let end = match col_type {
                            DataType::Int => DbKey::Int(i64::MAX),
                            _ => DbKey::String("\u{10ffff}".to_string()),
                        };
                        return Some((start, end));
                    }
                    Operator::LtEq | Operator::Lt => {
                        let start = match col_type {
                            DataType::Int => DbKey::Int(i64::MIN),
                            _ => DbKey::String("".to_string()),
                        };
                        let end = if matches!(op, Operator::Lt) {
                            match key {
                                DbKey::Int(v) => DbKey::Int(v - 1),
                                DbKey::String(s) => DbKey::String(s),
                                _ => key,
                            }
                        } else {
                            key
                        };
                        return Some((start, end));
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
    None
}

/// Helper to examine an expression tree and extract range constraints for the primary key.
fn extract_key_range(predicate: &Expr, schema: &Schema) -> Option<(DbKey, DbKey)> {
    let pk_idx = schema.primary_key_index()?;
    let pk_col = &schema.columns[pk_idx];
    extract_bounds_for_column(predicate, &pk_col.name, &pk_col.data_type)
}
