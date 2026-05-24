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
                        // Extract bounds if the predicate filters on the primary key
                        if let Some((start, end)) = extract_key_range(&predicate, &schema) {
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

fn get_boundary(expr: &Expr, pk_name: &str) -> Option<Boundary> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if let Some(lit) = get_pk_comparison(left, right, pk_name) {
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

fn get_pk_comparison<'a>(left: &'a Expr, right: &'a Expr, pk_name: &str) -> Option<&'a DbValue> {
    match (left, right) {
        (Expr::Column(name), Expr::Literal(lit))
            if name == pk_name
                || name.ends_with(&format!(".{}", pk_name))
                || pk_name.ends_with(&format!(".{}", name)) =>
        {
            Some(lit)
        }
        (Expr::Literal(lit), Expr::Column(name))
            if name == pk_name
                || name.ends_with(&format!(".{}", pk_name))
                || pk_name.ends_with(&format!(".{}", name)) =>
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

/// Helper to examine an expression tree and extract range constraints for the primary key.
fn extract_key_range(predicate: &Expr, schema: &Schema) -> Option<(DbKey, DbKey)> {
    let pk_idx = schema.primary_key_index()?;
    let pk_col = &schema.columns[pk_idx];

    match predicate {
        Expr::BinaryOp {
            left,
            op: Operator::Eq,
            right,
        } => {
            if let Some(lit) = get_pk_comparison(left, right, &pk_col.name) {
                let key = val_to_key(lit)?;
                return Some((key.clone(), key));
            }
        }
        Expr::BinaryOp {
            left,
            op: Operator::And,
            right,
        } => {
            // E.g., id >= 10 AND id <= 20
            let boundary_l = get_boundary(left, &pk_col.name);
            let boundary_r = get_boundary(right, &pk_col.name);

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
            if let Some(lit) = get_pk_comparison(left, right, &pk_col.name) {
                let key = val_to_key(lit)?;
                match op {
                    Operator::GtEq | Operator::Gt => {
                        let start = if matches!(op, Operator::Gt) {
                            match key {
                                DbKey::Int(v) => DbKey::Int(v + 1),
                                DbKey::String(s) => DbKey::String(s + "\0"),
                            }
                        } else {
                            key
                        };
                        let end = match pk_col.data_type {
                            DataType::Int => DbKey::Int(i64::MAX),
                            _ => DbKey::String("\u{10ffff}".to_string()),
                        };
                        return Some((start, end));
                    }
                    Operator::LtEq | Operator::Lt => {
                        let start = match pk_col.data_type {
                            DataType::Int => DbKey::Int(i64::MIN),
                            _ => DbKey::String("".to_string()),
                        };
                        let end = if matches!(op, Operator::Lt) {
                            match key {
                                DbKey::Int(v) => DbKey::Int(v - 1),
                                DbKey::String(s) => DbKey::String(s),
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
