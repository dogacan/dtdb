use dtdb_relational::{Row, Schema};
use dtdb_storage::DbValue;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Operator represents binary operations in SQL WHERE conditions.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    Eq,
    Gt,
    Lt,
    GtEq,
    LtEq,
    NotEq,
    And,
    Or,
    Like,
    Add,
    Sub,
    Mul,
    Div,
}

/// Expr represents a scalar expression in SQL (column names, literals, binary operations).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Expr {
    Column(String, #[serde(default)] Option<usize>),
    Literal(DbValue),
    /// A bind parameter placeholder (e.g. `:id`), carrying the parameter name.
    ///
    /// Parameters survive into the logical plan so a plan can be cached and
    /// reused across executions with different bound values. They must be
    /// substituted with a concrete [`Expr::Literal`] before execution;
    /// evaluating one directly is an error.
    Parameter(String),
    BinaryOp {
        left: Box<Expr>,
        op: Operator,
        right: Box<Expr>,
    },
    Case {
        operand: Option<Box<Expr>>,
        conditions: Vec<Expr>,
        results: Vec<Expr>,
        else_result: Option<Box<Expr>>,
    },
    Function {
        name: String,
        args: Vec<Expr>,
    },
    Not(Box<Expr>),
    IsNull(Box<Expr>),
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
    },
    Match {
        column: String,
        #[serde(default)]
        index: Option<usize>,
        query_str: String,
    },
}

impl Expr {
    /// Recursively collects all column names referenced in this expression.
    pub fn collect_columns(&self, columns: &mut HashSet<String>) {
        match self {
            Expr::Column(name, _) => {
                columns.insert(name.clone());
            }
            Expr::Literal(_) => {}
            Expr::Parameter(_) => {}
            Expr::BinaryOp { left, right, .. } => {
                left.collect_columns(columns);
                right.collect_columns(columns);
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(op) = operand {
                    op.collect_columns(columns);
                }
                for cond in conditions {
                    cond.collect_columns(columns);
                }
                for res in results {
                    res.collect_columns(columns);
                }
                if let Some(el) = else_result {
                    el.collect_columns(columns);
                }
            }
            Expr::Function { args, .. } => {
                for arg in args {
                    arg.collect_columns(columns);
                }
            }
            Expr::Not(inner) => {
                inner.collect_columns(columns);
            }
            Expr::IsNull(inner) => {
                inner.collect_columns(columns);
            }
            Expr::InList { expr, list } => {
                expr.collect_columns(columns);
                for item in list {
                    item.collect_columns(columns);
                }
            }
            Expr::Match { column, .. } => {
                columns.insert(column.clone());
            }
        }
    }

    pub fn bind_columns(&mut self, schema: &Schema) -> Result<(), String> {
        match self {
            Expr::Column(name, index) => {
                if index.is_none() {
                    *index = Some(resolve_column(schema, name)?);
                }
                Ok(())
            }
            Expr::Literal(_) => Ok(()),
            Expr::Parameter(_) => Ok(()),
            Expr::BinaryOp { left, right, .. } => {
                left.bind_columns(schema)?;
                right.bind_columns(schema)
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(op) = operand {
                    op.bind_columns(schema)?;
                }
                for cond in conditions {
                    cond.bind_columns(schema)?;
                }
                for res in results {
                    res.bind_columns(schema)?;
                }
                if let Some(el) = else_result {
                    el.bind_columns(schema)?;
                }
                Ok(())
            }
            Expr::Function { args, .. } => {
                for arg in args {
                    arg.bind_columns(schema)?;
                }
                Ok(())
            }
            Expr::Not(inner) => inner.bind_columns(schema),
            Expr::IsNull(inner) => inner.bind_columns(schema),
            Expr::InList { expr, list } => {
                expr.bind_columns(schema)?;
                for item in list {
                    item.bind_columns(schema)?;
                }
                Ok(())
            }
            Expr::Match { column, index, .. } => {
                if index.is_none() {
                    *index = Some(resolve_column(schema, column)?);
                }
                Ok(())
            }
        }
    }

    /// Recursively replaces each [`Expr::Parameter`] with the bound value from
    /// `params` (as an [`Expr::Literal`]). Errors if a referenced parameter has
    /// no binding. Used to turn a cached, parameterized plan into a concrete one
    /// at execution time.
    pub fn substitute_params(&mut self, params: &HashMap<String, DbValue>) -> Result<(), String> {
        match self {
            Expr::Parameter(name) => {
                let val = params
                    .get(name)
                    .ok_or_else(|| format!("Unbound parameter: {name}"))?;
                *self = Expr::Literal(val.clone());
                Ok(())
            }
            Expr::Column(..) | Expr::Literal(_) | Expr::Match { .. } => Ok(()),
            Expr::BinaryOp { left, right, .. } => {
                left.substitute_params(params)?;
                right.substitute_params(params)
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(op) = operand {
                    op.substitute_params(params)?;
                }
                for cond in conditions {
                    cond.substitute_params(params)?;
                }
                for res in results {
                    res.substitute_params(params)?;
                }
                if let Some(el) = else_result {
                    el.substitute_params(params)?;
                }
                Ok(())
            }
            Expr::Function { args, .. } => {
                for arg in args {
                    arg.substitute_params(params)?;
                }
                Ok(())
            }
            Expr::Not(inner) | Expr::IsNull(inner) => inner.substitute_params(params),
            Expr::InList { expr, list } => {
                expr.substitute_params(params)?;
                for item in list {
                    item.substitute_params(params)?;
                }
                Ok(())
            }
        }
    }
}

/// Returns the index of the column matching `name` in `schema`, or an error
/// if the lookup is ambiguous (matches more than one column) or not found.
///
/// Matching rules:
///   - exact match on `col.name`
///   - if `name` is qualified (e.g. `users.name`), match the trailing portion
///     against `col.name`
///   - if `col.name` is qualified, match its trailing portion against `name`
///
/// An ambiguous resolution is a hard error rather than silently picking the
/// first match: after `SELECT a.name, b.name FROM a JOIN b`, an unqualified
/// `name` in WHERE would otherwise bind to whichever column happened to be
/// listed first, which is both surprising and non-portable.
fn resolve_column(schema: &Schema, name: &str) -> Result<usize, String> {
    let mut matches = Vec::new();
    for (i, col) in schema.columns.iter().enumerate() {
        if col.matches_name(name) {
            matches.push(i);
        }
    }
    match matches.len() {
        0 => Err(format!("Column not found in schema: '{}'", name)),
        1 => Ok(matches[0]),
        _ => {
            let candidates: Vec<&str> = matches
                .iter()
                .map(|&i| schema.columns[i].name.as_str())
                .collect();
            Err(format!(
                "Ambiguous column reference '{}': matches {:?}",
                name, candidates
            ))
        }
    }
}

impl Expr {
    /// Evaluates the expression against a Row and its Schema.
    pub fn eval(&self, row: &Row, schema: &Schema) -> Result<DbValue, String> {
        match self {
            Expr::Literal(val) => Ok(val.clone()),
            Expr::Parameter(name) => Err(format!(
                "Unbound parameter '{name}' reached execution; parameters must be bound before the query runs"
            )),
            Expr::Column(name, index) => {
                let idx = if let Some(idx) = index {
                    *idx
                } else {
                    resolve_column(schema, name)?
                };

                row.get_by_index(idx)
                    .cloned()
                    .ok_or_else(|| format!("Index {} out of bounds for row values", idx))
            }
            Expr::Not(inner) => {
                let val = inner.eval(row, schema)?;
                if matches!(val, DbValue::Null) {
                    Ok(DbValue::Null)
                } else {
                    let b = to_bool(&val)?;
                    Ok(DbValue::Bool(!b))
                }
            }
            Expr::IsNull(inner) => {
                let val = inner.eval(row, schema)?;
                Ok(DbValue::Bool(matches!(val, DbValue::Null)))
            }
            Expr::InList { expr, list } => {
                let val = expr.eval(row, schema)?;
                if matches!(val, DbValue::Null) {
                    return Ok(DbValue::Null);
                }
                let mut has_null = false;
                for item in list {
                    let item_val = item.eval(row, schema)?;
                    if matches!(item_val, DbValue::Null) {
                        has_null = true;
                    } else if compare_values(&val, &item_val) == Ok(std::cmp::Ordering::Equal) {
                        return Ok(DbValue::Bool(true));
                    }
                }
                if has_null {
                    Ok(DbValue::Null)
                } else {
                    Ok(DbValue::Bool(false))
                }
            }
            Expr::BinaryOp { left, op, right } => {
                // Handle logical AND/OR with short-circuiting and three-valued logic
                if matches!(op, Operator::And | Operator::Or) {
                    let l_val = left.eval(row, schema)?;
                    return match op {
                        Operator::And => {
                            if matches!(l_val, DbValue::Null) {
                                let r_val = right.eval(row, schema)?;
                                if matches!(r_val, DbValue::Null) {
                                    Ok(DbValue::Null)
                                } else {
                                    let r_bool = to_bool(&r_val)?;
                                    if !r_bool {
                                        Ok(DbValue::Bool(false))
                                    } else {
                                        Ok(DbValue::Null)
                                    }
                                }
                            } else {
                                let l_bool = to_bool(&l_val)?;
                                if !l_bool {
                                    Ok(DbValue::Bool(false))
                                } else {
                                    let r_val = right.eval(row, schema)?;
                                    if matches!(r_val, DbValue::Null) {
                                        Ok(DbValue::Null)
                                    } else {
                                        let r_bool = to_bool(&r_val)?;
                                        Ok(DbValue::Bool(r_bool))
                                    }
                                }
                            }
                        }
                        Operator::Or => {
                            if matches!(l_val, DbValue::Null) {
                                let r_val = right.eval(row, schema)?;
                                if matches!(r_val, DbValue::Null) {
                                    Ok(DbValue::Null)
                                } else {
                                    let r_bool = to_bool(&r_val)?;
                                    if r_bool {
                                        Ok(DbValue::Bool(true))
                                    } else {
                                        Ok(DbValue::Null)
                                    }
                                }
                            } else {
                                let l_bool = to_bool(&l_val)?;
                                if l_bool {
                                    Ok(DbValue::Bool(true))
                                } else {
                                    let r_val = right.eval(row, schema)?;
                                    if matches!(r_val, DbValue::Null) {
                                        Ok(DbValue::Null)
                                    } else {
                                        let r_bool = to_bool(&r_val)?;
                                        Ok(DbValue::Bool(r_bool))
                                    }
                                }
                            }
                        }
                        _ => unreachable!(),
                    };
                }

                let l_val = left.eval(row, schema)?;
                let r_val = right.eval(row, schema)?;

                if matches!(l_val, DbValue::Null) || matches!(r_val, DbValue::Null) {
                    // Propagate NULL for arithmetic, comparison, and LIKE operations
                    return Ok(DbValue::Null);
                }

                match op {
                    Operator::And => unreachable!(),
                    Operator::Or => unreachable!(),
                    Operator::Like => {
                        let text = to_string_val(&l_val)?;
                        let pattern = to_string_val(&r_val)?;
                        let matched = like_match(&text, &pattern);
                        Ok(DbValue::Bool(matched))
                    }
                    Operator::Add => eval_arithmetic(
                        &l_val,
                        &r_val,
                        |a, b| {
                            a.checked_add(b)
                                .ok_or_else(|| "Integer overflow".to_string())
                        },
                        |a, b| Ok(a + b),
                    ),
                    Operator::Sub => eval_arithmetic(
                        &l_val,
                        &r_val,
                        |a, b| {
                            a.checked_sub(b)
                                .ok_or_else(|| "Integer overflow".to_string())
                        },
                        |a, b| Ok(a - b),
                    ),
                    Operator::Mul => eval_arithmetic(
                        &l_val,
                        &r_val,
                        |a, b| {
                            a.checked_mul(b)
                                .ok_or_else(|| "Integer overflow".to_string())
                        },
                        |a, b| Ok(a * b),
                    ),
                    Operator::Div => match r_val {
                        DbValue::Int(0) => Err("Division by zero".to_string()),
                        DbValue::Float(0.0) => Err("Division by zero".to_string()),
                        _ => eval_arithmetic(
                            &l_val,
                            &r_val,
                            |a, b| {
                                a.checked_div(b)
                                    .ok_or_else(|| "Integer overflow".to_string())
                            },
                            |a, b| Ok(a / b),
                        ),
                    },
                    other_op => {
                        let ordering = compare_values(&l_val, &r_val)?;
                        let matched = match other_op {
                            Operator::Eq => ordering == std::cmp::Ordering::Equal,
                            Operator::NotEq => ordering != std::cmp::Ordering::Equal,
                            Operator::Gt => ordering == std::cmp::Ordering::Greater,
                            Operator::Lt => ordering == std::cmp::Ordering::Less,
                            Operator::GtEq => {
                                ordering == std::cmp::Ordering::Greater
                                    || ordering == std::cmp::Ordering::Equal
                            }
                            Operator::LtEq => {
                                ordering == std::cmp::Ordering::Less
                                    || ordering == std::cmp::Ordering::Equal
                            }
                            _ => unreachable!(),
                        };
                        Ok(DbValue::Bool(matched))
                    }
                }
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if conditions.len() != results.len() {
                    return Err(
                        "CASE expression conditions and results length mismatch".to_string()
                    );
                }

                let mut matched_idx = None;
                if let Some(op_expr) = operand {
                    let op_val = op_expr.eval(row, schema)?;
                    for (i, cond_expr) in conditions.iter().enumerate() {
                        let cond_val = cond_expr.eval(row, schema)?;
                        if let Ok(ordering) = compare_values(&op_val, &cond_val)
                            && ordering == std::cmp::Ordering::Equal
                        {
                            matched_idx = Some(i);
                            break;
                        }
                    }
                } else {
                    for (i, cond_expr) in conditions.iter().enumerate() {
                        let cond_val = cond_expr.eval(row, schema)?;
                        if to_bool(&cond_val)? {
                            matched_idx = Some(i);
                            break;
                        }
                    }
                }

                if let Some(idx) = matched_idx {
                    results[idx].eval(row, schema)
                } else if let Some(else_expr) = else_result {
                    else_expr.eval(row, schema)
                } else {
                    Ok(DbValue::Null)
                }
            }
            Expr::Function { name, args } => {
                let name_upper = name.to_uppercase();
                match name_upper.as_str() {
                    "LENGTH" => {
                        if args.len() != 1 {
                            return Err(format!(
                                "LENGTH expects exactly 1 argument, got {}",
                                args.len()
                            ));
                        }
                        let val = args[0].eval(row, schema)?;
                        if matches!(val, DbValue::Null) {
                            return Ok(DbValue::Null);
                        }
                        let s = coerce_to_string(&val);
                        Ok(DbValue::Int(s.chars().count() as i64))
                    }
                    "SUBSTR" | "SUBSTRING" => {
                        if args.len() != 2 && args.len() != 3 {
                            return Err(format!(
                                "SUBSTR expects 2 or 3 arguments, got {}",
                                args.len()
                            ));
                        }
                        let val = args[0].eval(row, schema)?;
                        if matches!(val, DbValue::Null) {
                            return Ok(DbValue::Null);
                        }
                        let s = coerce_to_string(&val);
                        let start_val = args[1].eval(row, schema)?;
                        let start = match start_val {
                            DbValue::Int(i) => i,
                            other => {
                                return Err(format!(
                                    "SUBSTR start index must be integer, got {:?}",
                                    other
                                ));
                            }
                        };

                        let chars: Vec<char> = s.chars().collect();
                        let n = chars.len() as i64;
                        let start_idx = if start > 0 {
                            start - 1
                        } else if start == 0 {
                            -1
                        } else {
                            n + start
                        };

                        if args.len() == 2 {
                            let start_rust = start_idx.max(0) as usize;
                            if start_rust >= chars.len() {
                                Ok(DbValue::string(""))
                            } else {
                                Ok(DbValue::string(
                                    chars[start_rust..].iter().collect::<String>(),
                                ))
                            }
                        } else {
                            let len_val = args[2].eval(row, schema)?;
                            let length = match len_val {
                                DbValue::Int(i) => i,
                                other => {
                                    return Err(format!(
                                        "SUBSTR length must be integer, got {:?}",
                                        other
                                    ));
                                }
                            };
                            if length < 0 {
                                return Err(format!(
                                    "SUBSTR length must be non-negative, got {length}"
                                ));
                            }
                            if length == 0 {
                                Ok(DbValue::string(""))
                            } else {
                                // Saturating add: SUBSTR(s, 1, i64::MAX) used to panic
                                // (debug) or wrap to a negative value (release). We
                                // intentionally clamp the candidate end index to i64::MAX
                                // and then to the actual character count below, so the
                                // request degrades to "give me everything from start" no
                                // matter how absurd the requested length is.
                                let end_idx = start_idx.saturating_add(length);
                                let active_start = start_idx.max(0) as usize;
                                let active_end = end_idx.clamp(0, n) as usize;
                                if active_start < active_end && active_start < chars.len() {
                                    Ok(DbValue::string(
                                        chars[active_start..active_end].iter().collect::<String>(),
                                    ))
                                } else {
                                    Ok(DbValue::string(""))
                                }
                            }
                        }
                    }
                    "COALESCE" => {
                        if args.is_empty() {
                            return Err("COALESCE expects at least 1 argument".to_string());
                        }
                        let mut final_val = None;
                        for arg_expr in args {
                            let val = arg_expr.eval(row, schema)?;
                            if !matches!(val, DbValue::Null) {
                                return Ok(val);
                            }
                            final_val = Some(val);
                        }
                        Ok(final_val.unwrap_or(DbValue::Null))
                    }
                    "UPPER" => {
                        if args.len() != 1 {
                            return Err(format!(
                                "UPPER expects exactly 1 argument, got {}",
                                args.len()
                            ));
                        }
                        let val = args[0].eval(row, schema)?;
                        if matches!(val, DbValue::Null) {
                            return Ok(DbValue::Null);
                        }
                        let s = coerce_to_string(&val);
                        Ok(DbValue::string(s.to_uppercase()))
                    }
                    "LOWER" => {
                        if args.len() != 1 {
                            return Err(format!(
                                "LOWER expects exactly 1 argument, got {}",
                                args.len()
                            ));
                        }
                        let val = args[0].eval(row, schema)?;
                        if matches!(val, DbValue::Null) {
                            return Ok(DbValue::Null);
                        }
                        let s = coerce_to_string(&val);
                        Ok(DbValue::string(s.to_lowercase()))
                    }
                    "CONCAT" => {
                        if args.is_empty() {
                            return Err("CONCAT expects at least 1 argument".to_string());
                        }
                        let mut result = String::new();
                        for arg in args {
                            let val = arg.eval(row, schema)?;
                            if matches!(val, DbValue::Null) {
                                return Ok(DbValue::Null);
                            }
                            result.push_str(&coerce_to_string(&val));
                        }
                        Ok(DbValue::string(result))
                    }
                    "ABS" => {
                        if args.len() != 1 {
                            return Err(format!(
                                "ABS expects exactly 1 argument, got {}",
                                args.len()
                            ));
                        }
                        let val = args[0].eval(row, schema)?;
                        match val {
                            DbValue::Int(i) => Ok(DbValue::Int(i.abs())),
                            DbValue::Float(f) => Ok(DbValue::Float(f.abs())),
                            DbValue::Null => Ok(DbValue::Null),
                            other => Err(format!("ABS expects numeric argument, got {:?}", other)),
                        }
                    }
                    "ROUND" => {
                        if args.len() != 1 {
                            return Err(format!(
                                "ROUND expects exactly 1 argument, got {}",
                                args.len()
                            ));
                        }
                        let val = args[0].eval(row, schema)?;
                        match val {
                            DbValue::Int(i) => Ok(DbValue::Int(i)),
                            DbValue::Float(f) => Ok(DbValue::Float(f.round())),
                            DbValue::Null => Ok(DbValue::Null),
                            other => {
                                Err(format!("ROUND expects numeric argument, got {:?}", other))
                            }
                        }
                    }
                    other => Err(format!("Unsupported scalar function: {}", other)),
                }
            }
            Expr::Match {
                column,
                index,
                query_str,
            } => {
                let idx = if let Some(idx) = index {
                    *idx
                } else {
                    schema
                        .columns
                        .iter()
                        .position(|col| col.name == *column)
                        .ok_or_else(|| format!("Column not found in schema: '{}'", column))?
                };
                let val = row
                    .get_by_index(idx)
                    .cloned()
                    .ok_or_else(|| format!("Index {} out of bounds for row values", idx))?;

                if let DbValue::String(s) = val {
                    let tokenizer_name = schema
                        .indexes
                        .iter()
                        .find(|idx| {
                            idx.index_type == dtdb_relational::IndexType::FullText
                                && idx.columns.contains(column)
                        })
                        .and_then(|idx| idx.tokenizer.as_deref())
                        .unwrap_or("simple");

                    if let Some(tokenizer) = dtdb_relational::get_tokenizer(tokenizer_name) {
                        let query =
                            dtdb_relational::FullTextQuery::parse(query_str, tokenizer.as_ref())
                                .map_err(|e| e.to_string())?;
                        let tokens = tokenizer.tokenize(&s);
                        fn eval_match_query(
                            query: &dtdb_relational::FullTextQuery,
                            tokens: &[String],
                        ) -> bool {
                            match query {
                                dtdb_relational::FullTextQuery::Token(tok) => tokens.contains(tok),
                                dtdb_relational::FullTextQuery::And(left, right) => {
                                    eval_match_query(left, tokens)
                                        && eval_match_query(right, tokens)
                                }
                                dtdb_relational::FullTextQuery::Or(left, right) => {
                                    eval_match_query(left, tokens)
                                        || eval_match_query(right, tokens)
                                }
                                dtdb_relational::FullTextQuery::Phrase(phrase_tokens) => {
                                    if phrase_tokens.is_empty() {
                                        return true;
                                    }
                                    if tokens.len() < phrase_tokens.len() {
                                        return false;
                                    }
                                    for i in 0..=(tokens.len() - phrase_tokens.len()) {
                                        let sub_slice = &tokens[i..(i + phrase_tokens.len())];
                                        if sub_slice == phrase_tokens {
                                            return true;
                                        }
                                    }
                                    false
                                }
                            }
                        }
                        let matched = eval_match_query(&query, &tokens);
                        Ok(DbValue::Bool(matched))
                    } else {
                        Err(format!("Tokenizer '{}' not found", tokenizer_name))
                    }
                } else {
                    Ok(DbValue::Bool(false))
                }
            }
        }
    }
}

fn coerce_to_string(val: &DbValue) -> String {
    match val {
        DbValue::String(s) => s.to_string(),
        DbValue::Int(i) => i.to_string(),
        DbValue::Float(f) => f.to_string(),
        DbValue::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        DbValue::Bool(b) => b.to_string(),
        DbValue::Null => "NULL".to_string(),
    }
}

/// Coerces a DbValue to a boolean (Bool directly, Int not equal to 0, or Null treated as false).
fn to_bool(val: &DbValue) -> Result<bool, String> {
    match val {
        DbValue::Bool(b) => Ok(*b),
        DbValue::Int(v) => Ok(*v != 0),
        DbValue::Null => Ok(false),
        other => Err(format!("Cannot convert value to boolean: {:?}", other)),
    }
}

/// Coerces a DbValue to a String.
fn to_string_val(val: &DbValue) -> Result<String, String> {
    match val {
        DbValue::String(s) => Ok(s.to_string()),
        other => Err(format!("Expected string value, got: {:?}", other)),
    }
}

pub(crate) fn compare_values(l: &DbValue, r: &DbValue) -> Result<std::cmp::Ordering, String> {
    match (l, r) {
        (DbValue::Null, DbValue::Null) => Ok(std::cmp::Ordering::Equal),
        (DbValue::Null, _) => Ok(std::cmp::Ordering::Less),
        (_, DbValue::Null) => Ok(std::cmp::Ordering::Greater),
        (DbValue::Int(lv), DbValue::Int(rv)) => Ok(lv.cmp(rv)),
        (DbValue::Float(lv), DbValue::Float(rv)) => lv
            .partial_cmp(rv)
            .ok_or_else(|| "NaN float comparison".to_string()),
        (DbValue::Int(lv), DbValue::Float(rv)) => (*lv as f64)
            .partial_cmp(rv)
            .ok_or_else(|| "NaN float comparison".to_string()),
        (DbValue::Float(lv), DbValue::Int(rv)) => lv
            .partial_cmp(&(*rv as f64))
            .ok_or_else(|| "NaN float comparison".to_string()),
        (DbValue::Bool(lv), DbValue::Bool(rv)) => Ok(lv.cmp(rv)),
        (DbValue::Bool(lv), DbValue::Int(rv)) => Ok((*lv as i64).cmp(rv)),
        (DbValue::Int(lv), DbValue::Bool(rv)) => Ok(lv.cmp(&(*rv as i64))),
        (DbValue::String(lv), DbValue::String(rv)) => Ok(lv.cmp(rv)),
        (DbValue::Bytes(lv), DbValue::Bytes(rv)) => Ok(lv.cmp(rv)),
        (expected, actual) => Err(format!(
            "Type mismatch: cannot compare {:?} and {:?}",
            expected, actual
        )),
    }
}

/// Implements SQL LIKE matching by translating the pattern to a regular
/// expression and matching with the `regex` crate (RE2-style NFA/DFA,
/// guaranteed linear time in the input — no catastrophic backtracking).
///
/// - `%` matches zero or more of any characters.
/// - `_` matches exactly one of any character.
///
/// Compiled patterns are cached in a process-wide map so repeated executions
/// of the same query don't re-translate and re-compile on every row.
fn like_match(text: &str, pattern: &str) -> bool {
    match compiled_like_regex(pattern) {
        Some(re) => re.is_match(text),
        None => false,
    }
}

fn compiled_like_regex(pattern: &str) -> Option<std::sync::Arc<regex::Regex>> {
    use std::sync::{Arc, OnceLock, RwLock};
    static CACHE: OnceLock<RwLock<std::collections::HashMap<String, Arc<regex::Regex>>>> =
        OnceLock::new();
    let cache = CACHE.get_or_init(|| RwLock::new(std::collections::HashMap::new()));

    // Fast path: cached hit.
    if let Some(re) = cache.read().ok().and_then(|g| g.get(pattern).cloned()) {
        return Some(re);
    }

    let regex_src = like_pattern_to_regex(pattern);
    let compiled = match regex::Regex::new(&regex_src) {
        Ok(r) => Arc::new(r),
        Err(_) => return None,
    };
    if let Ok(mut g) = cache.write() {
        // Bound cache size to avoid unbounded memory growth from adversarial
        // workloads that issue many distinct patterns.
        if g.len() >= 1024 {
            g.clear();
        }
        g.entry(pattern.to_string())
            .or_insert_with(|| compiled.clone());
    }
    Some(compiled)
}

/// Translates a SQL LIKE pattern into an anchored regex. `%` becomes `.*`,
/// `_` becomes `.`, and every other character is escaped so regex
/// metacharacters embedded in user input cannot alter the match.
fn like_pattern_to_regex(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len() + 4);
    out.push_str("(?s)^"); // anchor + dotall so `.` matches newlines too
    for c in pattern.chars() {
        match c {
            '%' => out.push_str(".*"),
            '_' => out.push('.'),
            other => {
                // regex::escape on a single char would allocate; do it inline.
                if "\\.+*?()|[]{}^$".contains(other) {
                    out.push('\\');
                }
                out.push(other);
            }
        }
    }
    out.push('$');
    out
}

/// Helper to evaluate arithmetic operations on DbValues with promotion logic.
fn eval_arithmetic<FI, FF>(
    l: &DbValue,
    r: &DbValue,
    int_op: FI,
    float_op: FF,
) -> Result<DbValue, String>
where
    FI: FnOnce(i64, i64) -> Result<i64, String>,
    FF: FnOnce(f64, f64) -> Result<f64, String>,
{
    match (l, r) {
        (DbValue::Int(lv), DbValue::Int(rv)) => Ok(DbValue::Int(int_op(*lv, *rv)?)),
        (DbValue::Float(lv), DbValue::Float(rv)) => Ok(DbValue::Float(float_op(*lv, *rv)?)),
        (DbValue::Int(lv), DbValue::Float(rv)) => Ok(DbValue::Float(float_op(*lv as f64, *rv)?)),
        (DbValue::Float(lv), DbValue::Int(rv)) => Ok(DbValue::Float(float_op(*lv, *rv as f64)?)),
        (expected, actual) => Err(format!(
            "Cannot perform arithmetic on non-numeric types: {:?} and {:?}",
            expected, actual
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dtdb_relational::{Column, DataType};

    fn col(name: &str, dt: DataType) -> Column {
        Column {
            id: 0,
            name: name.to_string(),
            data_type: dt,
            is_primary_key: false,
            is_nullable: true,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        }
    }

    /// Schema with columns `id INT`, `name STRING`, `score FLOAT`.
    fn sample_schema() -> Schema {
        Schema::new(vec![
            col("id", DataType::Int),
            col("name", DataType::String),
            col("score", DataType::Float),
        ])
    }

    fn sample_row() -> Row {
        Row::new(vec![
            DbValue::Int(1),
            DbValue::string("alice"),
            DbValue::Float(9.5),
        ])
    }

    fn lit(v: DbValue) -> Box<Expr> {
        Box::new(Expr::Literal(v))
    }

    fn binop(l: Expr, op: Operator, r: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(l),
            op,
            right: Box::new(r),
        }
    }

    fn eval(expr: &Expr) -> Result<DbValue, String> {
        expr.eval(&sample_row(), &sample_schema())
    }

    // ----- literals and column access -----

    #[test]
    fn literal_and_column_eval() {
        let schema = sample_schema();
        let row = sample_row();
        assert_eq!(
            Expr::Literal(DbValue::Int(7)).eval(&row, &schema).unwrap(),
            DbValue::Int(7)
        );
        // Unbound column resolves by name.
        assert_eq!(
            Expr::Column("name".to_string(), None)
                .eval(&row, &schema)
                .unwrap(),
            DbValue::string("alice")
        );
        // Pre-bound index is used directly.
        assert_eq!(
            Expr::Column("ignored".to_string(), Some(0))
                .eval(&row, &schema)
                .unwrap(),
            DbValue::Int(1)
        );
    }

    #[test]
    fn column_errors() {
        let schema = sample_schema();
        let row = sample_row();
        // Unknown column name.
        assert!(
            Expr::Column("missing".to_string(), None)
                .eval(&row, &schema)
                .is_err()
        );
        // Bound index past the end of the row.
        assert!(
            Expr::Column("x".to_string(), Some(99))
                .eval(&row, &schema)
                .is_err()
        );
    }

    #[test]
    fn resolve_column_ambiguous_is_error() {
        // Two columns whose trailing portion is `id` -> ambiguous unqualified ref.
        let schema = Schema::new(vec![col("a.id", DataType::Int), col("b.id", DataType::Int)]);
        let err = Expr::Column("id".to_string(), None)
            .eval(&Row::new(vec![DbValue::Int(1), DbValue::Int(2)]), &schema)
            .unwrap_err();
        assert!(err.contains("Ambiguous"), "got: {err}");
    }

    // ----- comparisons -----

    #[test]
    fn comparison_operators() {
        assert_eq!(
            eval(&binop(
                Expr::Literal(DbValue::Int(1)),
                Operator::Eq,
                Expr::Literal(DbValue::Int(1))
            ))
            .unwrap(),
            DbValue::Bool(true)
        );
        assert_eq!(
            eval(&binop(
                Expr::Literal(DbValue::Int(1)),
                Operator::NotEq,
                Expr::Literal(DbValue::Int(2))
            ))
            .unwrap(),
            DbValue::Bool(true)
        );
        for (op, expect) in [
            (Operator::Gt, false),
            (Operator::Lt, true),
            (Operator::GtEq, false),
            (Operator::LtEq, true),
        ] {
            assert_eq!(
                eval(&binop(
                    Expr::Literal(DbValue::Int(1)),
                    op,
                    Expr::Literal(DbValue::Int(2))
                ))
                .unwrap(),
                DbValue::Bool(expect),
                "op {op:?}"
            );
        }
    }

    #[test]
    fn comparison_propagates_null() {
        assert_eq!(
            eval(&binop(
                Expr::Literal(DbValue::Null),
                Operator::Eq,
                Expr::Literal(DbValue::Int(1))
            ))
            .unwrap(),
            DbValue::Null
        );
    }

    #[test]
    fn compare_values_mixed_numeric_and_bool() {
        assert_eq!(
            compare_values(&DbValue::Int(2), &DbValue::Float(2.0)).unwrap(),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            compare_values(&DbValue::Float(1.0), &DbValue::Int(2)).unwrap(),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_values(&DbValue::Bool(true), &DbValue::Int(1)).unwrap(),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            compare_values(&DbValue::Int(0), &DbValue::Bool(false)).unwrap(),
            std::cmp::Ordering::Equal
        );
        // NULL sorts before everything; two NULLs are equal.
        assert_eq!(
            compare_values(&DbValue::Null, &DbValue::Int(1)).unwrap(),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_values(&DbValue::Int(1), &DbValue::Null).unwrap(),
            std::cmp::Ordering::Greater
        );
        // Incomparable types error.
        assert!(compare_values(&DbValue::string("a"), &DbValue::Int(1)).is_err());
        // NaN comparison errors.
        assert!(compare_values(&DbValue::Float(f64::NAN), &DbValue::Float(1.0)).is_err());
    }

    // ----- arithmetic -----

    #[test]
    fn arithmetic_add_sub_mul_div() {
        assert_eq!(
            eval(&binop(
                Expr::Literal(DbValue::Int(6)),
                Operator::Add,
                Expr::Literal(DbValue::Int(4))
            ))
            .unwrap(),
            DbValue::Int(10)
        );
        assert_eq!(
            eval(&binop(
                Expr::Literal(DbValue::Int(6)),
                Operator::Sub,
                Expr::Literal(DbValue::Int(4))
            ))
            .unwrap(),
            DbValue::Int(2)
        );
        assert_eq!(
            eval(&binop(
                Expr::Literal(DbValue::Int(6)),
                Operator::Mul,
                Expr::Literal(DbValue::Int(4))
            ))
            .unwrap(),
            DbValue::Int(24)
        );
        // Int / Float promotes to Float.
        assert_eq!(
            eval(&binop(
                Expr::Literal(DbValue::Int(9)),
                Operator::Div,
                Expr::Literal(DbValue::Float(2.0))
            ))
            .unwrap(),
            DbValue::Float(4.5)
        );
    }

    #[test]
    fn arithmetic_errors() {
        // Division by zero (int and float).
        assert!(
            eval(&binop(
                Expr::Literal(DbValue::Int(1)),
                Operator::Div,
                Expr::Literal(DbValue::Int(0))
            ))
            .is_err()
        );
        assert!(
            eval(&binop(
                Expr::Literal(DbValue::Float(1.0)),
                Operator::Div,
                Expr::Literal(DbValue::Float(0.0))
            ))
            .is_err()
        );
        // Integer overflow.
        assert!(
            eval(&binop(
                Expr::Literal(DbValue::Int(i64::MAX)),
                Operator::Add,
                Expr::Literal(DbValue::Int(1))
            ))
            .is_err()
        );
        // Non-numeric operands.
        assert!(
            eval(&binop(
                Expr::Literal(DbValue::string("x")),
                Operator::Add,
                Expr::Literal(DbValue::Int(1))
            ))
            .is_err()
        );
    }

    // ----- three-valued logic AND/OR -----

    #[test]
    fn three_valued_and() {
        let t = || Expr::Literal(DbValue::Bool(true));
        let f = || Expr::Literal(DbValue::Bool(false));
        let n = || Expr::Literal(DbValue::Null);
        assert_eq!(
            eval(&binop(t(), Operator::And, t())).unwrap(),
            DbValue::Bool(true)
        );
        assert_eq!(
            eval(&binop(t(), Operator::And, f())).unwrap(),
            DbValue::Bool(false)
        );
        // FALSE short-circuits regardless of right side.
        assert_eq!(
            eval(&binop(f(), Operator::And, n())).unwrap(),
            DbValue::Bool(false)
        );
        // TRUE AND NULL = NULL.
        assert_eq!(
            eval(&binop(t(), Operator::And, n())).unwrap(),
            DbValue::Null
        );
        // NULL AND TRUE = NULL; NULL AND FALSE = FALSE.
        assert_eq!(
            eval(&binop(n(), Operator::And, t())).unwrap(),
            DbValue::Null
        );
        assert_eq!(
            eval(&binop(n(), Operator::And, f())).unwrap(),
            DbValue::Bool(false)
        );
        assert_eq!(
            eval(&binop(n(), Operator::And, n())).unwrap(),
            DbValue::Null
        );
    }

    #[test]
    fn three_valued_or() {
        let t = || Expr::Literal(DbValue::Bool(true));
        let f = || Expr::Literal(DbValue::Bool(false));
        let n = || Expr::Literal(DbValue::Null);
        // TRUE short-circuits.
        assert_eq!(
            eval(&binop(t(), Operator::Or, n())).unwrap(),
            DbValue::Bool(true)
        );
        assert_eq!(
            eval(&binop(f(), Operator::Or, t())).unwrap(),
            DbValue::Bool(true)
        );
        assert_eq!(
            eval(&binop(f(), Operator::Or, f())).unwrap(),
            DbValue::Bool(false)
        );
        // FALSE OR NULL = NULL.
        assert_eq!(eval(&binop(f(), Operator::Or, n())).unwrap(), DbValue::Null);
        // NULL OR TRUE = TRUE; NULL OR FALSE = NULL.
        assert_eq!(
            eval(&binop(n(), Operator::Or, t())).unwrap(),
            DbValue::Bool(true)
        );
        assert_eq!(eval(&binop(n(), Operator::Or, f())).unwrap(), DbValue::Null);
        assert_eq!(eval(&binop(n(), Operator::Or, n())).unwrap(), DbValue::Null);
    }

    // ----- NOT / IS NULL / IN -----

    #[test]
    fn not_and_is_null() {
        assert_eq!(
            eval(&Expr::Not(lit(DbValue::Bool(true)))).unwrap(),
            DbValue::Bool(false)
        );
        // NOT NULL is NULL.
        assert_eq!(eval(&Expr::Not(lit(DbValue::Null))).unwrap(), DbValue::Null);
        assert_eq!(
            eval(&Expr::IsNull(lit(DbValue::Null))).unwrap(),
            DbValue::Bool(true)
        );
        assert_eq!(
            eval(&Expr::IsNull(lit(DbValue::Int(1)))).unwrap(),
            DbValue::Bool(false)
        );
    }

    #[test]
    fn in_list_semantics() {
        let in_list = |val: DbValue, items: Vec<DbValue>| Expr::InList {
            expr: lit(val),
            list: items.into_iter().map(Expr::Literal).collect(),
        };
        assert_eq!(
            eval(&in_list(
                DbValue::Int(2),
                vec![DbValue::Int(1), DbValue::Int(2)]
            ))
            .unwrap(),
            DbValue::Bool(true)
        );
        // Not present, no NULL -> false.
        assert_eq!(
            eval(&in_list(
                DbValue::Int(3),
                vec![DbValue::Int(1), DbValue::Int(2)]
            ))
            .unwrap(),
            DbValue::Bool(false)
        );
        // Not present but NULL in list -> NULL (unknown).
        assert_eq!(
            eval(&in_list(
                DbValue::Int(3),
                vec![DbValue::Int(1), DbValue::Null]
            ))
            .unwrap(),
            DbValue::Null
        );
        // NULL test value -> NULL.
        assert_eq!(
            eval(&in_list(DbValue::Null, vec![DbValue::Int(1)])).unwrap(),
            DbValue::Null
        );
    }

    // ----- LIKE -----

    #[test]
    fn like_matching() {
        let like = |text: &str, pat: &str| {
            eval(&binop(
                Expr::Literal(DbValue::string(text.to_string())),
                Operator::Like,
                Expr::Literal(DbValue::string(pat.to_string())),
            ))
            .unwrap()
        };
        assert_eq!(like("hello", "h%o"), DbValue::Bool(true));
        assert_eq!(like("hello", "h_llo"), DbValue::Bool(true));
        assert_eq!(like("hello", "world"), DbValue::Bool(false));
        // Regex metacharacters in the pattern are treated literally.
        assert_eq!(like("a.b", "a.b"), DbValue::Bool(true));
        assert_eq!(like("axb", "a.b"), DbValue::Bool(false));
    }

    #[test]
    fn like_pattern_translation_escapes_metachars() {
        assert_eq!(like_pattern_to_regex("a%b_c"), "(?s)^a.*b.c$");
        assert_eq!(like_pattern_to_regex("a.b"), "(?s)^a\\.b$");
    }

    // ----- CASE -----

    #[test]
    fn searched_case() {
        // CASE WHEN false THEN 'a' WHEN true THEN 'b' ELSE 'c' END
        let expr = Expr::Case {
            operand: None,
            conditions: vec![
                Expr::Literal(DbValue::Bool(false)),
                Expr::Literal(DbValue::Bool(true)),
            ],
            results: vec![
                Expr::Literal(DbValue::string("a")),
                Expr::Literal(DbValue::string("b")),
            ],
            else_result: Some(lit(DbValue::string("c"))),
        };
        assert_eq!(eval(&expr).unwrap(), DbValue::string("b"));
    }

    #[test]
    fn simple_case_with_operand_and_no_match() {
        // CASE 5 WHEN 1 THEN 'a' END  -> no match, no else -> NULL
        let expr = Expr::Case {
            operand: Some(lit(DbValue::Int(5))),
            conditions: vec![Expr::Literal(DbValue::Int(1))],
            results: vec![Expr::Literal(DbValue::string("a"))],
            else_result: None,
        };
        assert_eq!(eval(&expr).unwrap(), DbValue::Null);
    }

    #[test]
    fn case_length_mismatch_errors() {
        let expr = Expr::Case {
            operand: None,
            conditions: vec![Expr::Literal(DbValue::Bool(true))],
            results: vec![],
            else_result: None,
        };
        assert!(eval(&expr).is_err());
    }

    // ----- scalar functions -----

    fn func(name: &str, args: Vec<DbValue>) -> Expr {
        Expr::Function {
            name: name.to_string(),
            args: args.into_iter().map(Expr::Literal).collect(),
        }
    }

    #[test]
    fn string_functions() {
        assert_eq!(
            eval(&func("LENGTH", vec![DbValue::string("héllo")])).unwrap(),
            DbValue::Int(5)
        );
        assert_eq!(
            eval(&func("UPPER", vec![DbValue::string("abc")])).unwrap(),
            DbValue::string("ABC")
        );
        assert_eq!(
            eval(&func("LOWER", vec![DbValue::string("ABC")])).unwrap(),
            DbValue::string("abc")
        );
        assert_eq!(
            eval(&func("CONCAT", vec![DbValue::string("a"), DbValue::Int(2)])).unwrap(),
            DbValue::string("a2")
        );
        // case-insensitive name.
        assert_eq!(
            eval(&func("length", vec![DbValue::string("ab")])).unwrap(),
            DbValue::Int(2)
        );
    }

    #[test]
    fn substr_variants() {
        let s = || DbValue::string("hello");
        // 1-based start, no length: from 2nd char.
        assert_eq!(
            eval(&func("SUBSTR", vec![s(), DbValue::Int(2)])).unwrap(),
            DbValue::string("ello")
        );
        // start + length.
        assert_eq!(
            eval(&func(
                "SUBSTRING",
                vec![s(), DbValue::Int(1), DbValue::Int(3)]
            ))
            .unwrap(),
            DbValue::string("hel")
        );
        // negative start counts from end.
        assert_eq!(
            eval(&func("SUBSTR", vec![s(), DbValue::Int(-2)])).unwrap(),
            DbValue::string("lo")
        );
        // length 0 -> empty.
        assert_eq!(
            eval(&func("SUBSTR", vec![s(), DbValue::Int(1), DbValue::Int(0)])).unwrap(),
            DbValue::string("")
        );
        // absurd length saturates to "rest of string".
        assert_eq!(
            eval(&func(
                "SUBSTR",
                vec![s(), DbValue::Int(1), DbValue::Int(i64::MAX)]
            ))
            .unwrap(),
            DbValue::string("hello")
        );
        // start past the end -> empty.
        assert_eq!(
            eval(&func("SUBSTR", vec![s(), DbValue::Int(100)])).unwrap(),
            DbValue::string("")
        );
    }

    #[test]
    fn substr_errors() {
        let s = || DbValue::string("hello");
        // non-integer start.
        assert!(eval(&func("SUBSTR", vec![s(), DbValue::string("x")])).is_err());
        // negative length.
        assert!(
            eval(&func(
                "SUBSTR",
                vec![s(), DbValue::Int(1), DbValue::Int(-1)]
            ))
            .is_err()
        );
        // wrong arg count.
        assert!(eval(&func("SUBSTR", vec![s()])).is_err());
    }

    #[test]
    fn coalesce_and_numeric_functions() {
        assert_eq!(
            eval(&func(
                "COALESCE",
                vec![DbValue::Null, DbValue::Null, DbValue::Int(3)]
            ))
            .unwrap(),
            DbValue::Int(3)
        );
        // all null -> null.
        assert_eq!(
            eval(&func("COALESCE", vec![DbValue::Null, DbValue::Null])).unwrap(),
            DbValue::Null
        );
        assert_eq!(
            eval(&func("ABS", vec![DbValue::Int(-5)])).unwrap(),
            DbValue::Int(5)
        );
        assert_eq!(
            eval(&func("ABS", vec![DbValue::Float(-2.5)])).unwrap(),
            DbValue::Float(2.5)
        );
        assert_eq!(
            eval(&func("ROUND", vec![DbValue::Float(2.6)])).unwrap(),
            DbValue::Float(3.0)
        );
        assert_eq!(
            eval(&func("ROUND", vec![DbValue::Int(4)])).unwrap(),
            DbValue::Int(4)
        );
    }

    #[test]
    fn function_null_and_error_paths() {
        // NULL propagation through string functions.
        assert_eq!(
            eval(&func("LENGTH", vec![DbValue::Null])).unwrap(),
            DbValue::Null
        );
        assert_eq!(
            eval(&func("UPPER", vec![DbValue::Null])).unwrap(),
            DbValue::Null
        );
        // ABS/ROUND on non-numeric error.
        assert!(eval(&func("ABS", vec![DbValue::string("x")])).is_err());
        assert!(eval(&func("ROUND", vec![DbValue::string("x")])).is_err());
        // empty COALESCE / CONCAT.
        assert!(eval(&func("COALESCE", vec![])).is_err());
        assert!(eval(&func("CONCAT", vec![])).is_err());
        // unknown function.
        assert!(eval(&func("NOPE", vec![DbValue::Int(1)])).is_err());
        // wrong arity.
        assert!(eval(&func("UPPER", vec![DbValue::Int(1), DbValue::Int(2)])).is_err());
    }

    // ----- MATCH (full-text) -----

    #[test]
    fn match_full_text() {
        let schema = sample_schema();
        let row = Row::new(vec![
            DbValue::Int(1),
            DbValue::string("the quick brown fox"),
            DbValue::Float(0.0),
        ]);
        let m = |q: &str| {
            Expr::Match {
                column: "name".to_string(),
                index: None,
                query_str: q.to_string(),
            }
            .eval(&row, &schema)
            .unwrap()
        };
        assert_eq!(m("quick"), DbValue::Bool(true));
        assert_eq!(m("quick AND fox"), DbValue::Bool(true));
        assert_eq!(m("quick AND missing"), DbValue::Bool(false));
        assert_eq!(m("\"quick brown\""), DbValue::Bool(true));
        assert_eq!(m("\"brown quick\""), DbValue::Bool(false));

        // Non-string column never matches.
        let m_int = Expr::Match {
            column: "id".to_string(),
            index: None,
            query_str: "1".to_string(),
        }
        .eval(&row, &schema)
        .unwrap();
        assert_eq!(m_int, DbValue::Bool(false));
    }

    #[test]
    fn match_unknown_column_errors() {
        let schema = sample_schema();
        let row = sample_row();
        let err = Expr::Match {
            column: "missing".to_string(),
            index: None,
            query_str: "x".to_string(),
        }
        .eval(&row, &schema)
        .unwrap_err();
        assert!(err.contains("Column not found"), "got: {err}");
    }

    // ----- collect_columns / bind_columns -----

    #[test]
    fn collect_columns_walks_all_nodes() {
        let expr = Expr::Case {
            operand: Some(Box::new(Expr::Column("id".to_string(), None))),
            conditions: vec![binop(
                Expr::Column("name".to_string(), None),
                Operator::Eq,
                Expr::Literal(DbValue::Int(1)),
            )],
            results: vec![Expr::Function {
                name: "ABS".to_string(),
                args: vec![Expr::Column("score".to_string(), None)],
            }],
            else_result: Some(Box::new(Expr::InList {
                expr: Box::new(Expr::Not(Box::new(Expr::IsNull(Box::new(Expr::Column(
                    "id".to_string(),
                    None,
                )))))),
                list: vec![Expr::Match {
                    column: "name".to_string(),
                    index: None,
                    query_str: "x".to_string(),
                }],
            })),
        };
        let mut cols = HashSet::new();
        expr.collect_columns(&mut cols);
        assert!(cols.contains("id"));
        assert!(cols.contains("name"));
        assert!(cols.contains("score"));
    }

    #[test]
    fn bind_columns_resolves_indices() {
        let schema = sample_schema();
        let mut expr = binop(
            Expr::Column("name".to_string(), None),
            Operator::Eq,
            Expr::Literal(DbValue::string("alice")),
        );
        expr.bind_columns(&schema).unwrap();
        if let Expr::BinaryOp { left, .. } = &expr {
            assert_eq!(**left, Expr::Column("name".to_string(), Some(1)));
        } else {
            panic!("expected BinaryOp");
        }

        // Binding an unknown column errors.
        let mut bad = Expr::Column("missing".to_string(), None);
        assert!(bad.bind_columns(&schema).is_err());
    }
}
