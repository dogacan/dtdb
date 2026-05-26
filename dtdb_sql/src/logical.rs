use crate::expr::{Expr, Operator};
use dtdb_relational::{Column, DataType, Schema};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Cross,
}

/// AggregateExpr represents aggregate functions (COUNT, SUM, MIN, MAX, AVG).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum AggregateExpr {
    Count(Expr),
    Sum(Expr),
    Min(Expr),
    Max(Expr),
    Avg(Expr),
}

/// LogicalPlan represents relational algebra logical operations.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    Scan {
        table_name: String,
        schema: Schema,
        // Optional key range bounds (start_key, end_key) pushed down by the optimizer.
        range: Option<(dtdb_storage::DbKey, dtdb_storage::DbKey)>,
    },
    IndexScan {
        table_name: String,
        index_name: String,
        schema: Schema,
        // Optional key range bounds (start_key, end_key) on the indexed columns pushed down by the optimizer.
        range: Option<(dtdb_storage::DbKey, dtdb_storage::DbKey)>,
    },
    Filter {
        source: Box<LogicalPlan>,
        predicate: Expr,
    },
    Projection {
        source: Box<LogicalPlan>,
        expressions: Vec<Expr>,
        field_names: Vec<String>,
        schema: Schema,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        condition: Expr,
        join_type: JoinType,
        schema: Schema,
    },
    Aggregate {
        source: Box<LogicalPlan>,
        group_by: Vec<Expr>,
        aggrs: Vec<AggregateExpr>,
        field_names: Vec<String>,
        schema: Schema,
    },
    Sort {
        source: Box<LogicalPlan>,
        keys: Vec<(Expr, bool)>, // (expression, asc: true = ASC, false = DESC)
    },
    Limit {
        source: Box<LogicalPlan>,
        limit: Option<usize>,
        offset: usize,
    },
    SetOp {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        op: SetOpType,
        all: bool,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpType {
    Union,
    Intersect,
    Except,
}

impl LogicalPlan {
    pub fn new_projection(
        source: LogicalPlan,
        expressions: Vec<Expr>,
        field_names: Vec<String>,
    ) -> Self {
        let source_schema = source.schema();
        let mut cols = Vec::new();
        for (name, expr) in field_names.iter().zip(expressions.iter()) {
            let dt = infer_expr_type(expr, &source_schema);
            cols.push(Column {
                name: name.clone(),
                data_type: dt,
                is_primary_key: false,
                is_nullable: true,
                locality_group: None,
                default_value: None,
                is_auto_increment: false,
            });
        }
        let schema = Schema::new(cols);
        LogicalPlan::Projection {
            source: Box::new(source),
            expressions,
            field_names,
            schema,
        }
    }

    pub fn new_join(
        left: LogicalPlan,
        right: LogicalPlan,
        condition: Expr,
        join_type: JoinType,
    ) -> Self {
        let mut cols = left.schema().columns;
        cols.extend(right.schema().columns.clone());
        let schema = Schema::new(cols);
        LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            condition,
            join_type,
            schema,
        }
    }

    pub fn new_aggregate(
        source: LogicalPlan,
        group_by: Vec<Expr>,
        aggrs: Vec<AggregateExpr>,
        field_names: Vec<String>,
    ) -> Self {
        let source_schema = source.schema();
        let mut cols = Vec::new();

        // 1. Group-by keys.
        for (idx, expr) in group_by.iter().enumerate() {
            let dt = match expr {
                Expr::Column(col_name, _) => {
                    let pos = source_schema.columns.iter().position(|col| {
                        col.name == *col_name || dtdb_relational::schema::ends_with_dot_suffix(col_name, &col.name)
                    });
                    if let Some(i) = pos {
                        source_schema.columns[i].data_type
                    } else {
                        DataType::String
                    }
                }
                _ => DataType::String,
            };
            cols.push(Column {
                name: field_names[idx].clone(),
                data_type: dt,
                is_primary_key: false,
                is_nullable: true,
                locality_group: None,
                default_value: None,
                is_auto_increment: false,
            });
        }

        // 2. Aggregate functions.
        let start_idx = group_by.len();
        for (idx, aggr) in aggrs.iter().enumerate() {
            let dt = match aggr {
                AggregateExpr::Count(_) => DataType::Int,
                AggregateExpr::Sum(expr)
                | AggregateExpr::Min(expr)
                | AggregateExpr::Max(expr)
                | AggregateExpr::Avg(expr) => match expr {
                    Expr::Column(col_name, _) => {
                        let pos = source_schema.columns.iter().position(|col| {
                            col.name == *col_name || dtdb_relational::schema::ends_with_dot_suffix(col_name, &col.name)
                        });
                        if let Some(i) = pos {
                            source_schema.columns[i].data_type
                        } else {
                            DataType::Float
                        }
                    }
                    _ => DataType::Float,
                },
            };
            cols.push(Column {
                name: field_names[start_idx + idx].clone(),
                data_type: dt,
                is_primary_key: false,
                is_nullable: true,
                locality_group: None,
                default_value: None,
                is_auto_increment: false,
            });
        }

        let schema = Schema::new(cols);
        LogicalPlan::Aggregate {
            source: Box::new(source),
            group_by,
            aggrs,
            field_names,
            schema,
        }
    }

    /// Derives the output Schema generated by this logical operator.
    pub fn schema(&self) -> Schema {
        match self {
            LogicalPlan::Scan { schema, .. } => schema.clone(),
            LogicalPlan::Filter { source, .. } | LogicalPlan::Limit { source, .. } => {
                source.schema()
            }
            LogicalPlan::Projection { schema, .. } => schema.clone(),
            LogicalPlan::Join { schema, .. } => schema.clone(),
            LogicalPlan::Aggregate { schema, .. } => schema.clone(),
            LogicalPlan::Sort { source, .. } => source.schema(),
            LogicalPlan::IndexScan { schema, .. } => schema.clone(),
            LogicalPlan::SetOp { left, .. } => left.schema(),
        }
    }

    /// Recursively collects all column names referenced in this logical plan.
    pub fn collect_columns(&self, columns: &mut HashSet<String>) {
        match self {
            LogicalPlan::Scan { .. } => {}
            LogicalPlan::Filter { source, predicate } => {
                predicate.collect_columns(columns);
                source.collect_columns(columns);
            }
            LogicalPlan::Projection {
                source,
                expressions,
                ..
            } => {
                for expr in expressions {
                    expr.collect_columns(columns);
                }
                source.collect_columns(columns);
            }
            LogicalPlan::Join {
                left,
                right,
                condition,
                ..
            } => {
                condition.collect_columns(columns);
                left.collect_columns(columns);
                right.collect_columns(columns);
            }
            LogicalPlan::Aggregate {
                source,
                group_by,
                aggrs,
                ..
            } => {
                for expr in group_by {
                    expr.collect_columns(columns);
                }
                for aggr in aggrs {
                    match aggr {
                        AggregateExpr::Count(expr)
                        | AggregateExpr::Sum(expr)
                        | AggregateExpr::Min(expr)
                        | AggregateExpr::Max(expr)
                        | AggregateExpr::Avg(expr) => expr.collect_columns(columns),
                    }
                }
                source.collect_columns(columns);
            }
            LogicalPlan::Sort { source, keys } => {
                for (expr, _) in keys {
                    expr.collect_columns(columns);
                }
                source.collect_columns(columns);
            }
            LogicalPlan::Limit { source, .. } => {
                source.collect_columns(columns);
            }
            LogicalPlan::IndexScan { .. } => {}
            LogicalPlan::SetOp { left, right, .. } => {
                left.collect_columns(columns);
                right.collect_columns(columns);
            }
        }
    }
}

pub fn format_logical_plan(plan: &LogicalPlan) -> String {
    let mut out = String::new();
    format_logical_node(plan, 0, &mut out);
    out
}

fn format_logical_node(node: &LogicalPlan, indent: usize, out: &mut String) {
    let indent_str = "  ".repeat(indent);
    match node {
        LogicalPlan::Scan {
            table_name, range, ..
        } => {
            let range_str = match range {
                Some((s, e)) => format!("range=[{:?}, {:?}]", s, e),
                None => "range=all".to_string(),
            };
            out.push_str(&format!(
                "{}- Scan: table={}, {}\n",
                indent_str, table_name, range_str
            ));
        }
        LogicalPlan::Filter { source, predicate } => {
            out.push_str(&format!(
                "{}- Filter: condition={:?}\n",
                indent_str, predicate
            ));
            format_logical_node(source, indent + 1, out);
        }
        LogicalPlan::Projection {
            source,
            field_names,
            ..
        } => {
            out.push_str(&format!(
                "{}- Projection: fields={:?}\n",
                indent_str, field_names
            ));
            format_logical_node(source, indent + 1, out);
        }
        LogicalPlan::Join {
            left,
            right,
            condition,
            join_type,
            ..
        } => {
            out.push_str(&format!(
                "{}- HashJoin: type={:?}, condition={:?}\n",
                indent_str, join_type, condition
            ));
            out.push_str(&format!("{}  left:\n", indent_str));
            format_logical_node(left, indent + 2, out);
            out.push_str(&format!("{}  right:\n", indent_str));
            format_logical_node(right, indent + 2, out);
        }
        LogicalPlan::Aggregate {
            source,
            group_by,
            aggrs,
            field_names,
            ..
        } => {
            out.push_str(&format!(
                "{}- HashAggregate: group_by={:?}, aggregates={:?}, output_names={:?}\n",
                indent_str, group_by, aggrs, field_names
            ));
            format_logical_node(source, indent + 1, out);
        }
        LogicalPlan::Sort { source, keys } => {
            let keys_str: Vec<String> = keys
                .iter()
                .map(|(expr, asc)| format!("{:?} {}", expr, if *asc { "ASC" } else { "DESC" }))
                .collect();
            out.push_str(&format!("{}- Sort: keys={:?}\n", indent_str, keys_str));
            format_logical_node(source, indent + 1, out);
        }
        LogicalPlan::Limit {
            source,
            limit,
            offset,
        } => {
            let limit_str = match limit {
                Some(lim) => lim.to_string(),
                None => "none".to_string(),
            };
            out.push_str(&format!(
                "{}- Limit: count={}, offset={}\n",
                indent_str, limit_str, offset
            ));
            format_logical_node(source, indent + 1, out);
        }
        LogicalPlan::IndexScan {
            table_name,
            index_name,
            range,
            ..
        } => {
            let range_str = match range {
                Some((s, e)) => format!("range=[{:?}, {:?}]", s, e),
                None => "range=all".to_string(),
            };
            out.push_str(&format!(
                "{}- IndexScan: table={}, index={}, {}\n",
                indent_str, table_name, index_name, range_str
            ));
        }
        LogicalPlan::SetOp {
            left,
            right,
            op,
            all,
        } => {
            out.push_str(&format!(
                "{}- SetOp: op={:?}, all={}\n",
                indent_str, op, all
            ));
            out.push_str(&format!("{}  left:\n", indent_str));
            format_logical_node(left, indent + 2, out);
            out.push_str(&format!("{}  right:\n", indent_str));
            format_logical_node(right, indent + 2, out);
        }
    }
}

fn infer_expr_type(expr: &Expr, source_schema: &Schema) -> DataType {
    match expr {
        Expr::Literal(val) => match val {
            dtdb_storage::DbValue::Int(_) => DataType::Int,
            dtdb_storage::DbValue::Float(_) => DataType::Float,
            dtdb_storage::DbValue::String(_) => DataType::String,
            dtdb_storage::DbValue::Bytes(_) => DataType::Bytes,
            dtdb_storage::DbValue::Bool(_) => DataType::Bool,
            dtdb_storage::DbValue::Null => DataType::Null,
        },
        Expr::Column(col_name, _) => {
            let idx = source_schema.columns.iter().position(|col| {
                col.name == *col_name
                    || dtdb_relational::schema::ends_with_dot_suffix(col_name, &col.name)
                    || dtdb_relational::schema::ends_with_dot_suffix(&col.name, col_name)
            });
            if let Some(i) = idx {
                source_schema.columns[i].data_type
            } else {
                DataType::String // Fallback
            }
        }
        Expr::BinaryOp { op, left, .. } => {
            match op {
                Operator::Add | Operator::Sub | Operator::Mul | Operator::Div => {
                    infer_expr_type(left, source_schema)
                }
                _ => DataType::Int, // Logical/comparison operators return Int (0 or 1)
            }
        }
        Expr::Case {
            results,
            else_result,
            ..
        } => {
            if let Some(first_res) = results.first() {
                infer_expr_type(first_res, source_schema)
            } else if let Some(else_res) = else_result {
                infer_expr_type(else_res, source_schema)
            } else {
                DataType::Int // Fallback
            }
        }
        Expr::Function { name, args } => {
            let name_upper = name.to_uppercase();
            match name_upper.as_str() {
                "LENGTH" => DataType::Int,
                "SUBSTR" | "SUBSTRING" => DataType::String,
                "COALESCE" => {
                    if let Some(first_arg) = args.first() {
                        infer_expr_type(first_arg, source_schema)
                    } else {
                        DataType::Int // Fallback
                    }
                }
                _ => DataType::String, // Fallback
            }
        }
        Expr::Not(_) | Expr::IsNull(_) | Expr::InList { .. } => DataType::Int,
    }
}
