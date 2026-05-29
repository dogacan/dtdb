use dtdb_storage::DbValue;
use sqlparser::ast::{
    Expr as SqlExpr, FunctionArg, FunctionArgExpr, JoinConstraint, JoinOperator, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor, Value as SqlValue, Values,
};
use std::collections::HashMap;

/// Convert DbValue to a sqlparser SqlExpr literal.
fn db_value_to_sql_expr(val: &DbValue) -> SqlExpr {
    match val {
        DbValue::Int(i) => SqlExpr::Value(SqlValue::Number(i.to_string(), false).into()),
        DbValue::Float(f) => SqlExpr::Value(SqlValue::Number(f.to_string(), false).into()),
        DbValue::Bool(b) => SqlExpr::Value(SqlValue::Boolean(*b).into()),
        DbValue::String(s) => SqlExpr::Value(SqlValue::SingleQuotedString(s.clone()).into()),
        DbValue::Null => SqlExpr::Value(SqlValue::Null.into()),
        DbValue::Bytes(bytes) => {
            let mut hex = String::new();
            for b in bytes {
                hex.push_str(&format!("{:02x}", b));
            }
            SqlExpr::Value(SqlValue::HexStringLiteral(hex).into())
        }
    }
}

/// Recursively binds parameter values to placeholders in a SqlExpr.
pub fn bind_expr(expr: &mut SqlExpr, params: &HashMap<String, DbValue>) -> Result<(), String> {
    match expr {
        SqlExpr::Identifier(ident) if ident.value.starts_with('@') => {
            let name = &ident.value[1..];
            if let Some(val) = params.get(name) {
                *expr = db_value_to_sql_expr(val);
            } else {
                return Err(format!("Unbound parameter: @{}", name));
            }
        }
        SqlExpr::Value(value_with_span) => {
            if let SqlValue::Placeholder(placeholder) = &**value_with_span {
                let mut lookup_name = placeholder.clone();
                if lookup_name.starts_with(':') {
                    lookup_name = lookup_name[1..].to_string();
                }
                if let Some(val) = params.get(&lookup_name) {
                    *expr = db_value_to_sql_expr(val);
                } else if let Some(val) = params.get(placeholder) {
                    *expr = db_value_to_sql_expr(val);
                } else {
                    return Err(format!("Unbound parameter: {}", placeholder));
                }
            }
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            bind_expr(left, params)?;
            bind_expr(right, params)?;
        }
        SqlExpr::UnaryOp { expr: inner, .. } => {
            bind_expr(inner, params)?;
        }
        SqlExpr::Nested(inner) => {
            bind_expr(inner, params)?;
        }
        SqlExpr::IsNull(inner) => {
            bind_expr(inner, params)?;
        }
        SqlExpr::IsNotNull(inner) => {
            bind_expr(inner, params)?;
        }
        SqlExpr::Between {
            expr: e, low, high, ..
        } => {
            bind_expr(e, params)?;
            bind_expr(low, params)?;
            bind_expr(high, params)?;
        }
        SqlExpr::Function(func) => {
            if let sqlparser::ast::FunctionArguments::List(arg_list) = &mut func.args {
                for arg in &mut arg_list.args {
                    match arg {
                        FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(e),
                            ..
                        } => {
                            bind_expr(e, params)?;
                        }
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                            bind_expr(e, params)?;
                        }
                        _ => {}
                    }
                }
            }
        }
        SqlExpr::Like {
            expr: e, pattern, ..
        } => {
            bind_expr(e, params)?;
            bind_expr(pattern, params)?;
        }
        SqlExpr::InList { expr: e, list, .. } => {
            bind_expr(e, params)?;
            for item in list {
                bind_expr(item, params)?;
            }
        }
        SqlExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                bind_expr(op, params)?;
            }
            for cond in conditions {
                bind_expr(&mut cond.condition, params)?;
                bind_expr(&mut cond.result, params)?;
            }
            if let Some(else_res) = else_result {
                bind_expr(else_res, params)?;
            }
        }
        SqlExpr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            bind_expr(expr, params)?;
            if let Some(from_expr) = substring_from {
                bind_expr(from_expr, params)?;
            }
            if let Some(for_expr) = substring_for {
                bind_expr(for_expr, params)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn bind_join_constraint(
    constraint: &mut JoinConstraint,
    params: &HashMap<String, DbValue>,
) -> Result<(), String> {
    if let JoinConstraint::On(expr) = constraint {
        bind_expr(expr, params)?;
    }
    Ok(())
}

fn bind_table_factor(
    factor: &mut TableFactor,
    params: &HashMap<String, DbValue>,
) -> Result<(), String> {
    match factor {
        TableFactor::Derived { subquery, .. } => {
            bind_query(subquery, params)?;
        }
        TableFactor::TableFunction { expr, .. } => {
            bind_expr(expr, params)?;
        }
        _ => {}
    }
    Ok(())
}

fn bind_select(select: &mut Select, params: &HashMap<String, DbValue>) -> Result<(), String> {
    for item in &mut select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                bind_expr(expr, params)?;
            }
            _ => {}
        }
    }
    for from_item in &mut select.from {
        bind_table_factor(&mut from_item.relation, params)?;
        for join in &mut from_item.joins {
            bind_table_factor(&mut join.relation, params)?;
            match &mut join.join_operator {
                JoinOperator::Inner(constraint)
                | JoinOperator::Join(constraint)
                | JoinOperator::Left(constraint)
                | JoinOperator::LeftOuter(constraint)
                | JoinOperator::Right(constraint)
                | JoinOperator::RightOuter(constraint)
                | JoinOperator::FullOuter(constraint)
                | JoinOperator::CrossJoin(constraint)
                | JoinOperator::Semi(constraint)
                | JoinOperator::LeftSemi(constraint)
                | JoinOperator::RightSemi(constraint) => bind_join_constraint(constraint, params)?,
                _ => {}
            }
        }
    }
    if let Some(expr) = &mut select.selection {
        bind_expr(expr, params)?;
    }
    match &mut select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, _) => {
            for expr in exprs {
                bind_expr(expr, params)?;
            }
        }
        sqlparser::ast::GroupByExpr::All(_) => {}
    }
    if let Some(expr) = &mut select.having {
        bind_expr(expr, params)?;
    }
    if let Some(expr) = &mut select.qualify {
        bind_expr(expr, params)?;
    }
    Ok(())
}

fn bind_values(values: &mut Values, params: &HashMap<String, DbValue>) -> Result<(), String> {
    for row in &mut values.rows {
        for expr in &mut row.content {
            bind_expr(expr, params)?;
        }
    }
    Ok(())
}

fn bind_set_expr(set_expr: &mut SetExpr, params: &HashMap<String, DbValue>) -> Result<(), String> {
    match set_expr {
        SetExpr::Select(select) => {
            bind_select(select, params)?;
        }
        SetExpr::Query(query) => {
            bind_query(query, params)?;
        }
        SetExpr::SetOperation { left, right, .. } => {
            bind_set_expr(left, params)?;
            bind_set_expr(right, params)?;
        }
        SetExpr::Values(values) => {
            bind_values(values, params)?;
        }
        _ => {}
    }
    Ok(())
}

fn bind_query(query: &mut Query, params: &HashMap<String, DbValue>) -> Result<(), String> {
    bind_set_expr(&mut query.body, params)?;
    if let Some(sqlparser::ast::OrderBy {
        kind: sqlparser::ast::OrderByKind::Expressions(exprs),
        ..
    }) = &mut query.order_by
    {
        for order_item in exprs {
            bind_expr(&mut order_item.expr, params)?;
        }
    }
    if let Some(limit_clause) = &mut query.limit_clause {
        match limit_clause {
            sqlparser::ast::LimitClause::LimitOffset {
                limit,
                offset,
                limit_by,
            } => {
                if let Some(limit_expr) = limit {
                    bind_expr(limit_expr, params)?;
                }
                if let Some(offset_struct) = offset {
                    bind_expr(&mut offset_struct.value, params)?;
                }
                for expr in limit_by {
                    bind_expr(expr, params)?;
                }
            }
            sqlparser::ast::LimitClause::OffsetCommaLimit { offset, limit } => {
                bind_expr(offset, params)?;
                bind_expr(limit, params)?;
            }
        }
    }
    Ok(())
}

/// Traverse a SQL AST statement and replace parameter placeholders with bound values.
pub fn bind_statement(
    statement: &mut Statement,
    params: &HashMap<String, DbValue>,
) -> Result<(), String> {
    match statement {
        Statement::Insert(insert) => {
            if let Some(source) = &mut insert.source {
                bind_query(source, params)?;
            }
        }
        Statement::Delete(delete) => {
            if let Some(expr) = &mut delete.selection {
                bind_expr(expr, params)?;
            }
        }
        Statement::Update(update) => {
            for assign in &mut update.assignments {
                bind_expr(&mut assign.value, params)?;
            }
            if let Some(expr) = &mut update.selection {
                bind_expr(expr, params)?;
            }
        }
        Statement::Query(query) => {
            bind_query(query, params)?;
        }
        Statement::Explain {
            statement: stmt, ..
        } => {
            bind_statement(stmt, params)?;
        }
        _ => {}
    }
    Ok(())
}
