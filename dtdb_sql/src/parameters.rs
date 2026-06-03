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
        DbValue::String(s) => SqlExpr::Value(SqlValue::SingleQuotedString(s.to_string()).into()),
        DbValue::Null => SqlExpr::Value(SqlValue::Null.into()),
        DbValue::Bytes(bytes) => {
            let mut hex = String::new();
            for b in bytes.iter() {
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
        // Parameters can be nested inside subqueries (ADR 0005). The cached-plan
        // path binds these via `Expr::substitute_params`; this AST-fallback path
        // must descend into the subquery body the same way `bind_table_factor`
        // already does for `Derived` (FROM-clause) subqueries.
        SqlExpr::Subquery(query) => {
            bind_query(query, params)?;
        }
        SqlExpr::InSubquery {
            expr: lhs,
            subquery,
            ..
        } => {
            bind_expr(lhs, params)?;
            bind_query(subquery, params)?;
        }
        SqlExpr::Exists { subquery, .. } => {
            bind_query(subquery, params)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn params(pairs: &[(&str, DbValue)]) -> HashMap<String, DbValue> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    /// Parse one SQL statement, bind the given params, and render it back to text.
    fn bind_and_render(sql: &str, params: &HashMap<String, DbValue>) -> Result<String, String> {
        let dialect = GenericDialect {};
        let mut statements = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;
        let mut stmt = statements.remove(0);
        bind_statement(&mut stmt, params)?;
        Ok(stmt.to_string())
    }

    #[test]
    fn binds_named_placeholder_in_select_where() {
        let p = params(&[("id", DbValue::Int(42))]);
        let out = bind_and_render("SELECT * FROM t WHERE id = :id", &p).unwrap();
        assert!(out.contains("42"), "got: {out}");
        assert!(!out.contains(':'), "placeholder not substituted: {out}");
    }

    #[test]
    fn binds_at_identifier_parameter() {
        let p = params(&[("name", DbValue::string("alice"))]);
        let out = bind_and_render("SELECT * FROM t WHERE name = @name", &p).unwrap();
        assert!(out.contains("'alice'"), "got: {out}");
        assert!(!out.contains('@'), "got: {out}");
    }

    #[test]
    fn unbound_parameters_error() {
        let empty = params(&[]);
        assert!(bind_and_render("SELECT * FROM t WHERE id = :id", &empty).is_err());
        assert!(bind_and_render("SELECT * FROM t WHERE id = @id", &empty).is_err());
    }

    #[test]
    fn binds_all_db_value_types() {
        let p = params(&[
            ("i", DbValue::Int(1)),
            ("f", DbValue::Float(2.5)),
            ("b", DbValue::Bool(true)),
            ("s", DbValue::string("hi")),
            ("n", DbValue::Null),
            ("by", DbValue::bytes(vec![0xde, 0xad])),
        ]);
        let out = bind_and_render(
            "SELECT * FROM t WHERE i = :i AND f = :f AND b = :b AND s = :s AND n = :n AND by = :by",
            &p,
        )
        .unwrap();
        assert!(out.contains('1'));
        assert!(out.contains("2.5"));
        assert!(out.contains("true") || out.contains("TRUE"));
        assert!(out.contains("'hi'"));
        assert!(out.to_uppercase().contains("NULL"));
        // Bytes render as a hex string literal.
        assert!(out.to_lowercase().contains("dead"), "got: {out}");
    }

    #[test]
    fn binds_inside_expression_nodes() {
        // Exercise BinaryOp, Between, Function args, Like, InList, IsNull, Nested, UnaryOp, Case.
        let p = params(&[
            ("lo", DbValue::Int(1)),
            ("hi", DbValue::Int(10)),
            ("pat", DbValue::string("a%")),
            ("x", DbValue::Int(5)),
            ("y", DbValue::Int(6)),
            ("z", DbValue::Int(7)),
        ]);
        let sql = "SELECT * FROM t WHERE (a BETWEEN :lo AND :hi) \
                   AND b LIKE :pat \
                   AND c IN (:x, :y) \
                   AND ABS(:z) > 0 \
                   AND d IS NOT NULL \
                   AND -:x < 0";
        let out = bind_and_render(sql, &p).unwrap();
        assert!(!out.contains(':'), "unbound placeholders remain: {out}");
    }

    #[test]
    fn binds_in_insert_update_delete() {
        let p = params(&[
            ("v", DbValue::Int(9)),
            ("w", DbValue::string("x")),
            ("id", DbValue::Int(3)),
        ]);
        let insert = bind_and_render("INSERT INTO t (a) VALUES (:v)", &p).unwrap();
        assert!(
            insert.contains('9') && !insert.contains(':'),
            "got: {insert}"
        );

        let update = bind_and_render("UPDATE t SET a = :w WHERE id = :id", &p).unwrap();
        assert!(
            update.contains("'x'") && update.contains('3'),
            "got: {update}"
        );

        let delete = bind_and_render("DELETE FROM t WHERE id = :id", &p).unwrap();
        assert!(
            delete.contains('3') && !delete.contains(':'),
            "got: {delete}"
        );
    }

    #[test]
    fn binds_in_join_subquery_and_order_limit() {
        let p = params(&[
            ("j", DbValue::Int(1)),
            ("s", DbValue::Int(2)),
            ("n", DbValue::Int(5)),
        ]);
        let sql = "SELECT * FROM a JOIN (SELECT * FROM b WHERE b.x = :s) sub \
                   ON a.id = sub.id AND a.k = :j \
                   ORDER BY a.id \
                   LIMIT :n";
        let out = bind_and_render(sql, &p).unwrap();
        assert!(!out.contains(':'), "unbound placeholders remain: {out}");
    }

    #[test]
    fn binds_through_explain_and_set_operations() {
        let p = params(&[("x", DbValue::Int(1)), ("y", DbValue::Int(2))]);
        let explain = bind_and_render("EXPLAIN SELECT * FROM t WHERE a = :x", &p).unwrap();
        assert!(!explain.contains(':'), "got: {explain}");

        let union = bind_and_render(
            "SELECT a FROM t WHERE a = :x UNION SELECT b FROM u WHERE b = :y",
            &p,
        )
        .unwrap();
        assert!(!union.contains(':'), "got: {union}");
    }

    #[test]
    fn bind_expr_leaves_non_parameter_literals_untouched() {
        let p = params(&[]);
        // No placeholders at all -> unchanged and no error.
        let out = bind_and_render("SELECT * FROM t WHERE a = 1 AND b = 'x'", &p).unwrap();
        assert!(out.contains('1') && out.contains("'x'"));
    }
}
