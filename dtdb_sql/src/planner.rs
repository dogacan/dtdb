use crate::expr::{Expr, Operator};
use crate::logical::{AggregateExpr, JoinType, LogicalPlan};
use dtdb_relational::{Column, DataType, Database, Schema};
use dtdb_storage::DbValue;
use sqlparser::ast::{
    BinaryOperator, ColumnOption, DataType as SqlDataType, Expr as SqlExpr, FunctionArg,
    FunctionArgExpr, Query, SelectItem, Statement, TableFactor, Value as SqlValue,
};
use std::sync::Arc;

/// Represents a parsed and planned SQL statement.
pub enum SqlStatement {
    CreateTable {
        name: String,
        schema: Schema,
    },
    DropTable {
        name: String,
    },
    Insert {
        table_name: String,
        columns: Vec<String>,
        rows: Vec<Vec<DbValue>>,
    },
    Delete {
        table_name: String,
        filter: Option<Expr>,
    },
    Update {
        table_name: String,
        assignments: Vec<(String, Expr)>,
        filter: Option<Expr>,
    },
    Query(LogicalPlan),
    Explain(LogicalPlan),
}

/// LogicalPlanner translates sqlparser AST Statements into SqlStatements.
pub struct LogicalPlanner {
    database: Arc<Database>,
}

impl LogicalPlanner {
    pub fn new(database: Arc<Database>) -> Self {
        Self { database }
    }

    /// Plans a parsed sqlparser AST Statement.
    pub fn plan(&self, statement: &Statement) -> Result<SqlStatement, String> {
        match statement {
            Statement::CreateTable {
                name,
                columns,
                with_options,
                ..
            } => {
                let table_name = name.to_string();
                let mut cols = Vec::new();

                let mut locality_map = std::collections::HashMap::new();
                let mut locality_group_options = std::collections::HashMap::new();
                for opt in with_options {
                    if opt.name.value == "locality_groups"
                        && let sqlparser::ast::Value::SingleQuotedString(ref val_str) = opt.value
                    {
                        for part in val_str.split(';') {
                            let part = part.trim();
                            if part.is_empty() {
                                continue;
                            }
                            let sub_parts: Vec<&str> = part.split(':').collect();
                            if sub_parts.len() >= 2 {
                                let group_name = sub_parts[0].trim().to_string();
                                for col_name in sub_parts[1].split(',') {
                                    let col_name = col_name.trim().to_string();
                                    locality_map.insert(col_name, group_name.clone());
                                }
                                if sub_parts.len() == 3 {
                                    let opts =
                                        locality_group_options.entry(group_name).or_insert_with(
                                            dtdb_relational::LocalityGroupOptions::default,
                                        );
                                    for opt_pair in sub_parts[2].split(',') {
                                        let kv: Vec<&str> = opt_pair.split('=').collect();
                                        if kv.len() == 2 {
                                            let key = kv[0].trim();
                                            let val = kv[1].trim();
                                            match key {
                                                "compression" => {
                                                    let comp = match val.to_lowercase().as_str() {
                                                        "lz4" => dtdb_storage::CompressionType::Lz4,
                                                        "uncompressed" => dtdb_storage::CompressionType::Uncompressed,
                                                        _ => return Err(format!("Unknown compression type: {}", val)),
                                                    };
                                                    opts.compression = Some(comp);
                                                }
                                                "memtable_size_limit" => {
                                                    opts.memtable_size_limit = Some(
                                                        val.parse::<usize>().map_err(|e| {
                                                            format!(
                                                                "Invalid memtable_size_limit: {}",
                                                                e
                                                            )
                                                        })?,
                                                    );
                                                }
                                                "block_size_limit" => {
                                                    opts.block_size_limit = Some(
                                                        val.parse::<usize>().map_err(|e| {
                                                            format!(
                                                                "Invalid block_size_limit: {}",
                                                                e
                                                            )
                                                        })?,
                                                    );
                                                }
                                                "wal_size_limit" => {
                                                    opts.wal_size_limit = Some(
                                                        val.parse::<usize>().map_err(|e| {
                                                            format!("Invalid wal_size_limit: {}", e)
                                                        })?,
                                                    );
                                                }
                                                "l0_compaction_threshold" => {
                                                    opts.l0_compaction_threshold = Some(val.parse::<usize>().map_err(|e| format!("Invalid l0_compaction_threshold: {}", e))?);
                                                }
                                                "sstable_target_size" => {
                                                    opts.sstable_target_size = Some(
                                                        val.parse::<usize>().map_err(|e| {
                                                            format!(
                                                                "Invalid sstable_target_size: {}",
                                                                e
                                                            )
                                                        })?,
                                                    );
                                                }
                                                "base_level_size_limit" => {
                                                    opts.base_level_size_limit = Some(
                                                        val.parse::<usize>().map_err(|e| {
                                                            format!(
                                                                "Invalid base_level_size_limit: {}",
                                                                e
                                                            )
                                                        })?,
                                                    );
                                                }
                                                "level_size_multiplier" => {
                                                    opts.level_size_multiplier = Some(
                                                        val.parse::<usize>().map_err(|e| {
                                                            format!(
                                                                "Invalid level_size_multiplier: {}",
                                                                e
                                                            )
                                                        })?,
                                                    );
                                                }
                                                "max_level" => {
                                                    opts.max_level =
                                                        Some(val.parse::<usize>().map_err(
                                                            |e| format!("Invalid max_level: {}", e),
                                                        )?);
                                                }
                                                "block_cache_capacity" => {
                                                    opts.block_cache_capacity = Some(
                                                        val.parse::<usize>().map_err(|e| {
                                                            format!(
                                                                "Invalid block_cache_capacity: {}",
                                                                e
                                                            )
                                                        })?,
                                                    );
                                                }
                                                other => {
                                                    return Err(format!(
                                                        "Unknown locality group option: {}",
                                                        other
                                                    ));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                for col in columns {
                    let dt = match &col.data_type {
                        SqlDataType::Integer(_) | SqlDataType::Int(_) | SqlDataType::BigInt(_) => {
                            DataType::Int
                        }
                        SqlDataType::Float(_) | SqlDataType::Double | SqlDataType::Real => {
                            DataType::Float
                        }
                        SqlDataType::Text
                        | SqlDataType::Varchar(_)
                        | SqlDataType::Char(_)
                        | SqlDataType::String => DataType::String,
                        SqlDataType::Bytea | SqlDataType::Blob(_) => DataType::Bytes,
                        other => return Err(format!("Unsupported SQL data type: {:?}", other)),
                    };

                    let is_pk = col
                        .options
                        .iter()
                        .any(|opt| matches!(opt.option, ColumnOption::Unique { is_primary: true }));

                    let is_nullable = !is_pk
                        && !col
                            .options
                            .iter()
                            .any(|opt| matches!(opt.option, ColumnOption::NotNull));

                    let group = locality_map.get(&col.name.value).cloned();

                    cols.push(Column {
                        name: col.name.value.clone(),
                        data_type: dt,
                        is_primary_key: is_pk,
                        is_nullable,
                        locality_group: group,
                    });
                }

                Ok(SqlStatement::CreateTable {
                    name: table_name,
                    schema: Schema::new_with_options(cols, locality_group_options),
                })
            }
            Statement::Drop {
                object_type, names, ..
            } => {
                // E.g., DROP TABLE names[0]
                if matches!(object_type, sqlparser::ast::ObjectType::Table) && !names.is_empty() {
                    let table_name = names[0].to_string();
                    Ok(SqlStatement::DropTable { name: table_name })
                } else {
                    Err("Only DROP TABLE statement is supported".to_string())
                }
            }
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => {
                let table_str = table_name.to_string();
                let table = self
                    .database
                    .get_table(&table_str)
                    .map_err(|e| e.to_string())?;

                let col_names = if columns.is_empty() {
                    // Default to all columns in schema order
                    table
                        .schema
                        .columns
                        .iter()
                        .map(|c| c.name.clone())
                        .collect()
                } else {
                    columns.iter().map(|c| c.value.clone()).collect()
                };

                let mut rows = Vec::new();
                if let sqlparser::ast::SetExpr::Values(values) = &*source.body {
                    for row_exprs in &values.rows {
                        let mut row_vals = Vec::new();
                        for expr in row_exprs {
                            match plan_expr(expr)? {
                                Expr::Literal(val) => row_vals.push(val),
                                other => {
                                    return Err(format!(
                                        "INSERT expects literal values, got expression {:?}",
                                        other
                                    ));
                                }
                            }
                        }
                        rows.push(row_vals);
                    }
                }

                Ok(SqlStatement::Insert {
                    table_name: table_str,
                    columns: col_names,
                    rows,
                })
            }
            Statement::Delete {
                from, selection, ..
            } => {
                if from.is_empty() {
                    return Err("DELETE statement requires a table name".to_string());
                }
                let name_str = match &from[0].relation {
                    TableFactor::Table { name, .. } => name.to_string(),
                    other => {
                        return Err(format!("Unsupported table factor in DELETE: {:?}", other));
                    }
                };
                let filter = match selection {
                    Some(expr) => Some(plan_expr(expr)?),
                    None => None,
                };
                Ok(SqlStatement::Delete {
                    table_name: name_str,
                    filter,
                })
            }
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => {
                let name_str = match &table.relation {
                    TableFactor::Table { name, .. } => name.to_string(),
                    other => {
                        return Err(format!("Unsupported table factor in UPDATE: {:?}", other));
                    }
                };
                let mut my_assignments = Vec::new();
                for assign in assignments {
                    let col_name = assign
                        .id
                        .iter()
                        .map(|i| i.value.clone())
                        .collect::<Vec<_>>()
                        .join(".");
                    let planned_expr = plan_expr(&assign.value)?;
                    my_assignments.push((col_name, planned_expr));
                }
                let filter = match selection {
                    Some(expr) => Some(plan_expr(expr)?),
                    None => None,
                };
                Ok(SqlStatement::Update {
                    table_name: name_str,
                    assignments: my_assignments,
                    filter,
                })
            }
            Statement::Query(query) => {
                let logical_plan = self.plan_query(query)?;
                Ok(SqlStatement::Query(logical_plan))
            }
            Statement::Explain { statement, .. } => {
                let inner = self.plan(statement)?;
                match inner {
                    SqlStatement::Query(plan) => Ok(SqlStatement::Explain(plan)),
                    _ => Err("Can only EXPLAIN SELECT queries".to_string()),
                }
            }
            other => Err(format!("Unsupported SQL statement: {:?}", other)),
        }
    }

    /// Plans a SELECT query into a LogicalPlan.
    fn plan_query(&self, query: &Query) -> Result<LogicalPlan, String> {
        let select = match &*query.body {
            sqlparser::ast::SetExpr::Select(select) => select,
            other => return Err(format!("Unsupported query body: {:?}", other)),
        };

        // 1. Plan FROM clause (Leaf scan / joins)
        let mut plan = self.plan_from(&select.from)?;

        // 2. Plan JOIN clause
        if !select.from.is_empty() {
            let relation = &select.from[0];
            for join in &relation.joins {
                let right_scan = self.plan_table_factor(&join.relation)?;
                let (join_cond, join_type) = match &join.join_operator {
                    sqlparser::ast::JoinOperator::Inner(sqlparser::ast::JoinConstraint::On(
                        expr,
                    )) => (plan_expr(expr)?, JoinType::Inner),
                    sqlparser::ast::JoinOperator::LeftOuter(
                        sqlparser::ast::JoinConstraint::On(expr),
                    ) => (plan_expr(expr)?, JoinType::Left),
                    other => return Err(format!("Unsupported join type: {:?}", other)),
                };
                plan = LogicalPlan::Join {
                    left: Box::new(plan),
                    right: Box::new(right_scan),
                    condition: join_cond,
                    join_type,
                };
            }
        }

        // 3. Plan WHERE clause (Filter)
        if let Some(selection) = &select.selection {
            let predicate = plan_expr(selection)?;
            plan = LogicalPlan::Filter {
                source: Box::new(plan),
                predicate,
            };
        }

        // 4. Plan GROUP BY / aggregations
        let has_groupby = !select.group_by.is_empty();
        let has_aggrs = select_items_have_aggrs(&select.projection);

        if has_groupby || has_aggrs {
            // Aggregate planning
            let group_exprs = select
                .group_by
                .iter()
                .map(plan_expr)
                .collect::<Result<Vec<_>, String>>()?;

            let mut aggr_exprs = Vec::new();
            let mut field_names = Vec::new();

            // First group-by field names
            for expr in &group_exprs {
                match expr {
                    Expr::Column(name) => field_names.push(name.clone()),
                    _ => field_names.push("group_key".to_string()),
                }
            }

            // Extract aggregates from select projection items
            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        if let Ok(planned) = plan_expr(expr)
                            && let Some(pos) = group_exprs.iter().position(|ge| ge == &planned)
                        {
                            if let SelectItem::ExprWithAlias { alias, .. } = item {
                                field_names[pos] = alias.value.clone();
                            }
                            continue;
                        }

                        let alias = match item {
                            SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
                            _ => expr.to_string(),
                        };
                        extract_aggregates(expr, &mut aggr_exprs, &mut field_names, alias)?;
                    }
                    SelectItem::Wildcard(_) => {
                        return Err("Wildcards not allowed in GROUP BY / Aggregations".to_string());
                    }
                    _ => {}
                }
            }

            plan = LogicalPlan::Aggregate {
                source: Box::new(plan),
                group_by: group_exprs,
                aggrs: aggr_exprs,
                field_names,
            };

            // Plan ORDER BY (Sort) for aggregate query
            if !query.order_by.is_empty() {
                let mut sort_keys = Vec::new();
                for sort_expr in &query.order_by {
                    let expr = plan_expr(&sort_expr.expr)?;
                    let asc = sort_expr.asc.unwrap_or(true);
                    sort_keys.push((expr, asc));
                }
                plan = LogicalPlan::Sort {
                    source: Box::new(plan),
                    keys: sort_keys,
                };
            }
        } else {
            // Plan ORDER BY (Sort) BEFORE standard SELECT projection
            if !query.order_by.is_empty() {
                let mut sort_keys = Vec::new();
                for sort_expr in &query.order_by {
                    let expr = plan_expr(&sort_expr.expr)?;
                    let asc = sort_expr.asc.unwrap_or(true);
                    sort_keys.push((expr, asc));
                }
                plan = LogicalPlan::Sort {
                    source: Box::new(plan),
                    keys: sort_keys,
                };
            }

            // 5. Plan standard SELECT projection
            let mut expressions = Vec::new();
            let mut field_names = Vec::new();

            let source_schema = plan.schema();

            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        let planned_expr = plan_expr(expr)?;
                        let name = match &planned_expr {
                            Expr::Column(name) => name.clone(),
                            _ => expr.to_string(),
                        };
                        expressions.push(planned_expr);
                        field_names.push(name);
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        let planned_expr = plan_expr(expr)?;
                        expressions.push(planned_expr);
                        field_names.push(alias.value.clone());
                    }
                    SelectItem::Wildcard(_) => {
                        // Expand wildcard to all fields in the source schema
                        for col in &source_schema.columns {
                            expressions.push(Expr::Column(col.name.clone()));
                            field_names.push(col.name.clone());
                        }
                    }
                    _ => return Err(format!("Unsupported select item: {:?}", item)),
                }
            }

            plan = LogicalPlan::Projection {
                source: Box::new(plan),
                expressions,
                field_names,
            };
        }

        // 7. Plan LIMIT and OFFSET
        let limit = if let Some(limit_expr) = &query.limit {
            let l = match limit_expr {
                SqlExpr::Value(SqlValue::Number(s, _)) => {
                    s.parse::<usize>().map_err(|e| e.to_string())?
                }
                other => return Err(format!("Unsupported limit expression: {:?}", other)),
            };
            Some(l)
        } else {
            None
        };

        let offset = if let Some(offset_val) = &query.offset {
            match &offset_val.value {
                SqlExpr::Value(SqlValue::Number(s, _)) => {
                    s.parse::<usize>().map_err(|e| e.to_string())?
                }
                other => return Err(format!("Unsupported offset expression: {:?}", other)),
            }
        } else {
            0
        };

        if limit.is_some() || offset > 0 {
            plan = LogicalPlan::Limit {
                source: Box::new(plan),
                limit,
                offset,
            };
        }

        Ok(plan)
    }

    fn plan_from(&self, from: &[sqlparser::ast::TableWithJoins]) -> Result<LogicalPlan, String> {
        if from.is_empty() {
            return Err("SELECT requires a FROM table source".to_string());
        }

        // In this simple planner, we support a single table factor relation.
        // Joins are handled separately as sibling nodes in `from`.
        self.plan_table_factor(&from[0].relation)
    }

    fn plan_table_factor(&self, factor: &TableFactor) -> Result<LogicalPlan, String> {
        match factor {
            TableFactor::Table { name, .. } => {
                let name_str = name.to_string();
                let table = self
                    .database
                    .get_table(&name_str)
                    .map_err(|e| e.to_string())?;

                Ok(LogicalPlan::Scan {
                    table_name: name_str,
                    schema: table.schema,
                    range: None, // Configured later by optimizer
                })
            }
            other => Err(format!("Unsupported table factor in FROM: {:?}", other)),
        }
    }
}

/// Helper to plan sqlparser scalar expression to custom Expr.
pub fn plan_expr(expr: &SqlExpr) -> Result<Expr, String> {
    match expr {
        SqlExpr::Identifier(ident) => {
            if ident.quote_style == Some('"') {
                Ok(Expr::Literal(DbValue::String(ident.value.clone())))
            } else {
                Ok(Expr::Column(ident.value.clone()))
            }
        }
        SqlExpr::CompoundIdentifier(parts) => {
            let name = parts
                .iter()
                .map(|p| p.value.as_str())
                .collect::<Vec<_>>()
                .join(".");
            Ok(Expr::Column(name))
        }
        SqlExpr::Value(val) => {
            let db_val = match val {
                SqlValue::Number(num_str, _) => {
                    if let Ok(i) = num_str.parse::<i64>() {
                        DbValue::Int(i)
                    } else if let Ok(f) = num_str.parse::<f64>() {
                        DbValue::Float(f)
                    } else {
                        return Err(format!("Invalid numeric literal: {}", num_str));
                    }
                }
                SqlValue::SingleQuotedString(s) => DbValue::String(s.clone()),
                SqlValue::Boolean(b) => DbValue::Int(if *b { 1 } else { 0 }),
                SqlValue::Null => DbValue::Null,
                other => return Err(format!("Unsupported SQL value type: {:?}", other)),
            };
            Ok(Expr::Literal(db_val))
        }
        SqlExpr::Like {
            negated,
            expr,
            pattern,
            escape_char: _,
        } => {
            if *negated {
                return Err("NOT LIKE is not supported yet".to_string());
            }
            Ok(Expr::BinaryOp {
                left: Box::new(plan_expr(expr)?),
                op: Operator::Like,
                right: Box::new(plan_expr(pattern)?),
            })
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let my_op = match op {
                BinaryOperator::Eq => Operator::Eq,
                BinaryOperator::Gt => Operator::Gt,
                BinaryOperator::Lt => Operator::Lt,
                BinaryOperator::GtEq => Operator::GtEq,
                BinaryOperator::LtEq => Operator::LtEq,
                BinaryOperator::NotEq => Operator::NotEq,
                BinaryOperator::And => Operator::And,
                BinaryOperator::Or => Operator::Or,
                BinaryOperator::Plus => Operator::Add,
                BinaryOperator::Minus => Operator::Sub,
                BinaryOperator::Multiply => Operator::Mul,
                BinaryOperator::Divide => Operator::Div,
                other => return Err(format!("Unsupported operator: {:?}", other)),
            };
            Ok(Expr::BinaryOp {
                left: Box::new(plan_expr(left)?),
                op: my_op,
                right: Box::new(plan_expr(right)?),
            })
        }
        SqlExpr::Nested(inner) => plan_expr(inner),
        SqlExpr::UnaryOp { op, expr } => {
            let inner = plan_expr(expr)?;
            match op {
                sqlparser::ast::UnaryOperator::Minus => match inner {
                    Expr::Literal(DbValue::Int(i)) => Ok(Expr::Literal(DbValue::Int(-i))),
                    Expr::Literal(DbValue::Float(f)) => Ok(Expr::Literal(DbValue::Float(-f))),
                    other => Ok(Expr::BinaryOp {
                        left: Box::new(Expr::Literal(DbValue::Int(0))),
                        op: Operator::Sub,
                        right: Box::new(other),
                    }),
                },
                sqlparser::ast::UnaryOperator::Plus => Ok(inner),
                other => Err(format!("Unsupported unary operator: {:?}", other)),
            }
        }
        SqlExpr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            let planned_operand = match operand {
                Some(expr) => Some(Box::new(plan_expr(expr)?)),
                None => None,
            };
            let planned_conditions = conditions
                .iter()
                .map(plan_expr)
                .collect::<Result<Vec<_>, String>>()?;
            let planned_results = results
                .iter()
                .map(plan_expr)
                .collect::<Result<Vec<_>, String>>()?;
            let planned_else = match else_result {
                Some(expr) => Some(Box::new(plan_expr(expr)?)),
                None => None,
            };
            Ok(Expr::Case {
                operand: planned_operand,
                conditions: planned_conditions,
                results: planned_results,
                else_result: planned_else,
            })
        }
        SqlExpr::Function(func) => {
            let name = func.name.to_string();
            let name_upper = name.to_uppercase();
            if matches!(name_upper.as_str(), "COUNT" | "SUM" | "MIN" | "MAX" | "AVG") {
                return Err(format!(
                    "Aggregate function {} cannot be used in scalar expression context",
                    name
                ));
            }
            if matches!(
                name_upper.as_str(),
                "SUBSTR" | "SUBSTRING" | "LENGTH" | "COALESCE"
            ) {
                let mut args = Vec::new();
                for arg in &func.args {
                    match arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(inner_expr)) => {
                            args.push(plan_expr(inner_expr)?);
                        }
                        other => {
                            return Err(format!("Unsupported function argument type: {:?}", other));
                        }
                    }
                }
                Ok(Expr::Function { name, args })
            } else {
                Err(format!("Unsupported or unrecognized function: {}", name))
            }
        }
        other => Err(format!("Unsupported expression: {:?}", other)),
    }
}

/// Helper to check if select list contains aggregate functions.
fn select_items_have_aggrs(projection: &[SelectItem]) -> bool {
    for item in projection {
        if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item
            && has_aggregate_function(expr)
        {
            return true;
        }
    }
    false
}

fn has_aggregate_function(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::Function(func) => {
            let name = func.name.to_string().to_uppercase();
            matches!(name.as_str(), "COUNT" | "SUM" | "MIN" | "MAX" | "AVG")
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            has_aggregate_function(left) || has_aggregate_function(right)
        }
        SqlExpr::Nested(inner) => has_aggregate_function(inner),
        _ => false,
    }
}

/// Extracts aggregate function calls from a select expression.
fn extract_aggregates(
    expr: &SqlExpr,
    aggrs: &mut Vec<AggregateExpr>,
    field_names: &mut Vec<String>,
    alias: String,
) -> Result<(), String> {
    match expr {
        SqlExpr::Function(func) => {
            let name = func.name.to_string().to_uppercase();
            let arg_expr = if func.args.is_empty() {
                Expr::Literal(DbValue::Int(1))
            } else {
                match &func.args[0] {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(inner_expr)) => {
                        plan_expr(inner_expr)?
                    }
                    FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                        Expr::Literal(DbValue::Int(1))
                    }
                    other => return Err(format!("Unsupported function argument: {:?}", other)),
                }
            };

            let aggr = match name.as_str() {
                "COUNT" => AggregateExpr::Count(arg_expr),
                "SUM" => AggregateExpr::Sum(arg_expr),
                "MIN" => AggregateExpr::Min(arg_expr),
                "MAX" => AggregateExpr::Max(arg_expr),
                "AVG" => AggregateExpr::Avg(arg_expr),
                other => return Err(format!("Unsupported aggregate function: {}", other)),
            };

            aggrs.push(aggr);
            field_names.push(alias);
            Ok(())
        }
        SqlExpr::Nested(inner) => extract_aggregates(inner, aggrs, field_names, alias),
        other => Err(format!(
            "Only aggregate function calls allowed in SELECT list when grouping: {:?}",
            other
        )),
    }
}
