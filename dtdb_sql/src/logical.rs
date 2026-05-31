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

/// PlanKey represents a parameterizable key range boundary in LogicalPlan scans.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum PlanKey {
    Value(dtdb_storage::DbKey),
    Parameter(String),
}

/// AggregateExpr represents aggregate functions (COUNT, SUM, MIN, MAX, AVG).
///
/// `distinct` records whether the call used the `DISTINCT` quantifier
/// (e.g. `COUNT(DISTINCT col)`), which deduplicates input values before
/// aggregating. It is meaningful for COUNT/SUM/AVG; for MIN/MAX it has no
/// effect on the result and the executor ignores it.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum AggregateExpr {
    Count { expr: Expr, distinct: bool },
    Sum { expr: Expr, distinct: bool },
    Min { expr: Expr, distinct: bool },
    Max { expr: Expr, distinct: bool },
    Avg { expr: Expr, distinct: bool },
}

/// LogicalPlan represents relational algebra logical operations.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    Scan {
        table_name: String,
        schema: Schema,
        // Optional key range bounds (start_key, end_key) pushed down by the optimizer.
        range: Option<(PlanKey, PlanKey)>,
    },
    IndexScan {
        table_name: String,
        index_name: String,
        schema: Schema,
        // Optional key range bounds (start_key, end_key) on the indexed columns pushed down by the optimizer.
        range: Option<(PlanKey, PlanKey)>,
    },
    FullTextScan {
        table_name: String,
        index_name: String,
        schema: Schema,
        query_str: String,
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
    Distinct {
        source: Box<LogicalPlan>,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpType {
    Union,
    Intersect,
    Except,
}

impl LogicalPlan {
    /// Recursively replaces every [`Expr::Parameter`] in the plan with its
    /// bound value from `params`. Called at execution time to turn a cached,
    /// parameterized plan into a concrete one before optimization/compilation.
    pub fn substitute_params(
        &mut self,
        params: &std::collections::HashMap<String, dtdb_storage::DbValue>,
    ) -> Result<(), String> {
        match self {
            LogicalPlan::Scan { range, .. } | LogicalPlan::IndexScan { range, .. } => {
                if let Some((start, end)) = range {
                    let subst = |key: &mut PlanKey| -> Result<(), String> {
                        if let PlanKey::Parameter(name) = key {
                            let val = params
                                .get(name)
                                .ok_or_else(|| format!("Unbound parameter: {name}"))?;
                            let concrete = match val {
                                dtdb_storage::DbValue::Int(v) => dtdb_storage::DbKey::Int(*v),
                                dtdb_storage::DbValue::String(s) => {
                                    dtdb_storage::DbKey::String(s.clone())
                                }
                                _ => {
                                    return Err(format!(
                                        "Unsupported key parameter type for {name}"
                                    ));
                                }
                            };
                            *key = PlanKey::Value(concrete);
                        }
                        Ok(())
                    };
                    subst(start)?;
                    subst(end)?;
                }
                Ok(())
            }
            LogicalPlan::FullTextScan { .. } => Ok(()),
            LogicalPlan::Filter { source, predicate } => {
                source.substitute_params(params)?;
                predicate.substitute_params(params)
            }
            LogicalPlan::Projection {
                source,
                expressions,
                ..
            } => {
                source.substitute_params(params)?;
                for e in expressions {
                    e.substitute_params(params)?;
                }
                Ok(())
            }
            LogicalPlan::Join {
                left,
                right,
                condition,
                ..
            } => {
                left.substitute_params(params)?;
                right.substitute_params(params)?;
                condition.substitute_params(params)
            }
            LogicalPlan::Aggregate {
                source,
                group_by,
                aggrs,
                ..
            } => {
                source.substitute_params(params)?;
                for g in group_by {
                    g.substitute_params(params)?;
                }
                for aggr in aggrs {
                    let (AggregateExpr::Count { expr, .. }
                    | AggregateExpr::Sum { expr, .. }
                    | AggregateExpr::Min { expr, .. }
                    | AggregateExpr::Max { expr, .. }
                    | AggregateExpr::Avg { expr, .. }) = aggr;
                    expr.substitute_params(params)?;
                }
                Ok(())
            }
            LogicalPlan::Sort { source, keys } => {
                source.substitute_params(params)?;
                for (e, _) in keys {
                    e.substitute_params(params)?;
                }
                Ok(())
            }
            LogicalPlan::Limit { source, .. } | LogicalPlan::Distinct { source } => {
                source.substitute_params(params)
            }
            LogicalPlan::SetOp { left, right, .. } => {
                left.substitute_params(params)?;
                right.substitute_params(params)
            }
        }
    }

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
                    let pos = source_schema
                        .columns
                        .iter()
                        .position(|col| col.matches_name(col_name));
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
                AggregateExpr::Count { .. } => DataType::Int,
                // AVG always produces a fractional value; runtime emits Float
                // regardless of input type, so the schema must match.
                AggregateExpr::Avg { .. } => DataType::Float,
                AggregateExpr::Sum { expr, .. }
                | AggregateExpr::Min { expr, .. }
                | AggregateExpr::Max { expr, .. } => match expr {
                    Expr::Column(col_name, _) => {
                        let pos = source_schema
                            .columns
                            .iter()
                            .position(|col| col.matches_name(col_name));
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
            LogicalPlan::Filter { source, .. }
            | LogicalPlan::Limit { source, .. }
            | LogicalPlan::Distinct { source } => source.schema(),
            LogicalPlan::Projection { schema, .. } => schema.clone(),
            LogicalPlan::Join { schema, .. } => schema.clone(),
            LogicalPlan::Aggregate { schema, .. } => schema.clone(),
            LogicalPlan::Sort { source, .. } => source.schema(),
            LogicalPlan::IndexScan { schema, .. } => schema.clone(),
            LogicalPlan::FullTextScan { schema, .. } => schema.clone(),
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
                        AggregateExpr::Count { expr, .. }
                        | AggregateExpr::Sum { expr, .. }
                        | AggregateExpr::Min { expr, .. }
                        | AggregateExpr::Max { expr, .. }
                        | AggregateExpr::Avg { expr, .. } => expr.collect_columns(columns),
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
            LogicalPlan::FullTextScan { .. } => {}
            LogicalPlan::SetOp { left, right, .. } => {
                left.collect_columns(columns);
                right.collect_columns(columns);
            }
            LogicalPlan::Distinct { source } => {
                source.collect_columns(columns);
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
        LogicalPlan::FullTextScan {
            table_name,
            index_name,
            query_str,
            ..
        } => {
            out.push_str(&format!(
                "{}- FullTextScan: table={}, index={}, query={:?}\n",
                indent_str, table_name, index_name, query_str
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
        LogicalPlan::Distinct { source } => {
            out.push_str(&format!("{}- Distinct\n", indent_str));
            format_logical_node(source, indent + 1, out);
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
            let idx = source_schema
                .columns
                .iter()
                .position(|col| col.matches_name(col_name));
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
        Expr::Match { .. } => DataType::Bool,
        // A parameter's type is unknown until it is bound to a value.
        Expr::Parameter(_) => DataType::Null,
    }
}
