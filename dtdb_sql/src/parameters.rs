use dtdb_storage::DbValue;
use sqlparser::ast::{
    Expr as SqlExpr, FunctionArg, FunctionArgExpr, JoinConstraint, JoinOperator, Offset, Query,
    Select, SelectItem, SetExpr, Statement, TableFactor, Value as SqlValue, Values,
};
use std::collections::HashMap;

/// Convert DbValue to a sqlparser SqlExpr literal.
fn db_value_to_sql_expr(val: &DbValue) -> SqlExpr {
    match val {
        DbValue::Int(i) => SqlExpr::Value(SqlValue::Number(i.to_string(), false)),
        DbValue::Float(f) => SqlExpr::Value(SqlValue::Number(f.to_string(), false)),
        DbValue::Bool(b) => SqlExpr::Value(SqlValue::Boolean(*b)),
        DbValue::String(s) => SqlExpr::Value(SqlValue::SingleQuotedString(s.clone())),
        DbValue::Null => SqlExpr::Value(SqlValue::Null),
        DbValue::Bytes(bytes) => {
            let mut hex = String::new();
            for b in bytes {
                hex.push_str(&format!("{:02x}", b));
            }
            SqlExpr::Value(SqlValue::HexStringLiteral(hex))
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
        SqlExpr::Value(SqlValue::Placeholder(placeholder)) => {
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
            for arg in &mut func.args {
                match arg {
                    FunctionArg::Named { arg: arg_expr, .. } => {
                        if let FunctionArgExpr::Expr(e) = arg_expr {
                            bind_expr(e, params)?;
                        }
                    }
                    FunctionArg::Unnamed(arg_expr) => {
                        if let FunctionArgExpr::Expr(e) = arg_expr {
                            bind_expr(e, params)?;
                        }
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
            results,
            else_result,
        } => {
            if let Some(op) = operand {
                bind_expr(op, params)?;
            }
            for cond in conditions {
                bind_expr(cond, params)?;
            }
            for res in results {
                bind_expr(res, params)?;
            }
            if let Some(else_res) = else_result {
                bind_expr(else_res, params)?;
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
            SelectItem::UnnamedExpr(expr) => {
                bind_expr(expr, params)?;
            }
            SelectItem::ExprWithAlias { expr, .. } => {
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
                JoinOperator::Inner(constraint) => bind_join_constraint(constraint, params)?,
                JoinOperator::LeftOuter(constraint) => bind_join_constraint(constraint, params)?,
                JoinOperator::RightOuter(constraint) => bind_join_constraint(constraint, params)?,
                JoinOperator::FullOuter(constraint) => bind_join_constraint(constraint, params)?,
                _ => {}
            }
        }
    }
    if let Some(expr) = &mut select.selection {
        bind_expr(expr, params)?;
    }
    for expr in &mut select.group_by {
        bind_expr(expr, params)?;
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
        for expr in row {
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
    for order_item in &mut query.order_by {
        bind_expr(&mut order_item.expr, params)?;
    }
    if let Some(expr) = &mut query.limit {
        bind_expr(expr, params)?;
    }
    if let Some(Offset { value, .. }) = &mut query.offset {
        bind_expr(value, params)?;
    }
    Ok(())
}

/// Traverse a SQL AST statement and replace parameter placeholders with bound values.
pub fn bind_statement(
    statement: &mut Statement,
    params: &HashMap<String, DbValue>,
) -> Result<(), String> {
    match statement {
        Statement::Insert { source, .. } => {
            bind_query(source, params)?;
        }
        Statement::Delete {
            selection: Some(expr),
            ..
        } => {
            bind_expr(expr, params)?;
        }
        Statement::Delete {
            selection: None, ..
        } => {}
        Statement::Update {
            assignments,
            selection,
            ..
        } => {
            for assign in assignments {
                bind_expr(&mut assign.value, params)?;
            }
            if let Some(expr) = selection {
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
