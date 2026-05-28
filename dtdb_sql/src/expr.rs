use dtdb_relational::{Row, Schema};
use dtdb_storage::DbValue;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

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

    pub fn bind_columns(&mut self, schema: &Schema) {
        match self {
            Expr::Column(name, index) => {
                if index.is_none() {
                    let idx = schema.columns.iter().position(|col| {
                        if col.name == *name {
                            true
                        } else if let Some(pos) = name.rfind('.') {
                            col.name == name[pos + 1..]
                        } else if let Some(col_pos) = col.name.rfind('.') {
                            col.name[col_pos + 1..] == *name
                        } else {
                            false
                        }
                    });
                    *index = idx;
                }
            }
            Expr::Literal(_) => {}
            Expr::BinaryOp { left, right, .. } => {
                left.bind_columns(schema);
                right.bind_columns(schema);
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(op) = operand {
                    op.bind_columns(schema);
                }
                for cond in conditions {
                    cond.bind_columns(schema);
                }
                for res in results {
                    res.bind_columns(schema);
                }
                if let Some(el) = else_result {
                    el.bind_columns(schema);
                }
            }
            Expr::Function { args, .. } => {
                for arg in args {
                    arg.bind_columns(schema);
                }
            }
            Expr::Not(inner) => {
                inner.bind_columns(schema);
            }
            Expr::IsNull(inner) => {
                inner.bind_columns(schema);
            }
            Expr::InList { expr, list } => {
                expr.bind_columns(schema);
                for item in list {
                    item.bind_columns(schema);
                }
            }
            Expr::Match { column, index, .. } => {
                if index.is_none() {
                    let idx = schema.columns.iter().position(|col| {
                        if col.name == *column {
                            true
                        } else if let Some(pos) = column.rfind('.') {
                            col.name == column[pos + 1..]
                        } else if let Some(col_pos) = col.name.rfind('.') {
                            col.name[col_pos + 1..] == *column
                        } else {
                            false
                        }
                    });
                    *index = idx;
                }
            }
        }
    }

    /// Evaluates the expression against a Row and its Schema.
    pub fn eval(&self, row: &Row, schema: &Schema) -> Result<DbValue, String> {
        match self {
            Expr::Literal(val) => Ok(val.clone()),
            Expr::Column(name, index) => {
                let idx = if let Some(idx) = index {
                    *idx
                } else {
                    schema
                        .columns
                        .iter()
                        .position(|col| {
                            if col.name == *name {
                                true
                            } else if let Some(pos) = name.rfind('.') {
                                col.name == name[pos + 1..]
                            } else if let Some(col_pos) = col.name.rfind('.') {
                                col.name[col_pos + 1..] == *name
                            } else {
                                false
                            }
                        })
                        .ok_or_else(|| format!("Column not found in schema: '{}'", name))?
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
                                Ok(DbValue::String("".to_string()))
                            } else {
                                Ok(DbValue::String(chars[start_rust..].iter().collect()))
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
                                Ok(DbValue::String("".to_string()))
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
                                    Ok(DbValue::String(
                                        chars[active_start..active_end].iter().collect(),
                                    ))
                                } else {
                                    Ok(DbValue::String("".to_string()))
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
                        Ok(DbValue::String(s.to_uppercase()))
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
                        Ok(DbValue::String(s.to_lowercase()))
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
                        Ok(DbValue::String(result))
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
                            idx.index_type == dtdb_relational::schema::IndexType::FullText
                                && idx.columns.contains(column)
                        })
                        .and_then(|idx| idx.tokenizer.as_deref())
                        .unwrap_or("simple");

                    if let Some(tokenizer) =
                        dtdb_relational::tokenizer::get_tokenizer(tokenizer_name)
                    {
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
        DbValue::String(s) => s.clone(),
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
        DbValue::String(s) => Ok(s.clone()),
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
