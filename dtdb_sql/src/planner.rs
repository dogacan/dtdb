use crate::expr::{Expr, Operator};
use crate::logical::{AggregateExpr, JoinType, LogicalPlan, SetOpType};
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
    InsertSelect {
        table_name: String,
        columns: Vec<String>,
        query: LogicalPlan,
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
    CreateIndex {
        table_name: String,
        index_name: String,
        columns: Vec<String>,
    },
    DropIndex {
        table_name: String,
        index_name: String,
    },
    Analyze {
        table_name: String,
    },
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
                constraints,
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

                let mut pk_cols = std::collections::HashSet::new();
                for constraint in constraints {
                    if let sqlparser::ast::TableConstraint::Unique {
                        is_primary: true,
                        columns,
                        ..
                    } = constraint
                    {
                        for col_ident in columns {
                            pk_cols.insert(col_ident.value.clone());
                        }
                    }
                }

                for col in columns {
                    let dt = match &col.data_type {
                        SqlDataType::Integer(_) | SqlDataType::Int(_) | SqlDataType::BigInt(_) => {
                            DataType::Int
                        }
                        SqlDataType::Custom(name, _)
                            if {
                                let name_str = name.to_string().to_uppercase();
                                name_str == "SERIAL" || name_str == "BIGSERIAL"
                            } =>
                        {
                            DataType::Int
                        }
                        SqlDataType::Custom(name, _)
                            if {
                                let name_str = name.to_string().to_uppercase();
                                name_str == "BOOL"
                            } =>
                        {
                            DataType::Bool
                        }
                        SqlDataType::Float(_) | SqlDataType::Double | SqlDataType::Real => {
                            DataType::Float
                        }
                        SqlDataType::Boolean => DataType::Bool,
                        SqlDataType::Text
                        | SqlDataType::Varchar(_)
                        | SqlDataType::Char(_)
                        | SqlDataType::String => DataType::String,
                        SqlDataType::Bytea | SqlDataType::Blob(_) => DataType::Bytes,
                        other => return Err(format!("Unsupported SQL data type: {:?}", other)),
                    };

                    let mut is_pk = col
                        .options
                        .iter()
                        .any(|opt| matches!(opt.option, ColumnOption::Unique { is_primary: true }));
                    if pk_cols.contains(&col.name.value) {
                        is_pk = true;
                    }

                    let is_nullable = !is_pk
                        && !col
                            .options
                            .iter()
                            .any(|opt| matches!(opt.option, ColumnOption::NotNull));

                    let group = locality_map.get(&col.name.value).cloned();

                    let mut default_value = None;
                    for opt in &col.options {
                        if let ColumnOption::Default(ref default_expr) = opt.option {
                            default_value = Some(eval_default_expr(default_expr)?);
                        }
                    }

                    let mut is_auto_increment = false;
                    if let SqlDataType::Custom(name, _) = &col.data_type {
                        let name_str = name.to_string().to_uppercase();
                        if name_str == "SERIAL" || name_str == "BIGSERIAL" {
                            is_auto_increment = true;
                        }
                    }
                    for opt in &col.options {
                        if let ColumnOption::DialectSpecific(tokens) = &opt.option {
                            for token in tokens {
                                if let sqlparser::tokenizer::Token::Word(w) = token {
                                    let val = w.value.to_uppercase();
                                    if val == "AUTO_INCREMENT" || val == "AUTOINCREMENT" {
                                        is_auto_increment = true;
                                    }
                                }
                            }
                        }
                    }

                    cols.push(Column {
                        name: col.name.value.clone(),
                        data_type: dt,
                        is_primary_key: is_pk,
                        is_nullable,
                        locality_group: group,
                        default_value,
                        is_auto_increment,
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
                // E.g., DROP TABLE names[0] or DROP INDEX names[0]
                if matches!(object_type, sqlparser::ast::ObjectType::Table) && !names.is_empty() {
                    let table_name = names[0].to_string();
                    Ok(SqlStatement::DropTable { name: table_name })
                } else if matches!(object_type, sqlparser::ast::ObjectType::Index)
                    && !names.is_empty()
                {
                    let index_name = names[0].to_string();
                    // Look up the table containing this index
                    let tables = self.database.list_tables();
                    let mut found_table = None;
                    for table_name in tables {
                        if let Ok(table) = self.database.get_table(&table_name)
                            && table
                                .schema
                                .indexes
                                .iter()
                                .any(|idx| idx.name == index_name)
                        {
                            found_table = Some(table_name);
                            break;
                        }
                    }
                    if let Some(table_name) = found_table {
                        Ok(SqlStatement::DropIndex {
                            table_name,
                            index_name,
                        })
                    } else {
                        Err(format!("Index '{}' not found in database", index_name))
                    }
                } else {
                    Err("Only DROP TABLE and DROP INDEX statements are supported".to_string())
                }
            }
            Statement::CreateIndex {
                name,
                table_name,
                columns,
                ..
            } => {
                let index_name = name.to_string();
                let table_str = table_name.to_string();
                let mut col_names = Vec::new();
                for col in columns {
                    if let sqlparser::ast::Expr::Identifier(ident) = &col.expr {
                        col_names.push(ident.value.clone());
                    } else {
                        return Err(
                            "Only simple column identifiers are supported in CREATE INDEX"
                                .to_string(),
                        );
                    }
                }
                Ok(SqlStatement::CreateIndex {
                    table_name: table_str,
                    index_name,
                    columns: col_names,
                })
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

                match &*source.body {
                    sqlparser::ast::SetExpr::Values(values) => {
                        let mut rows = Vec::new();
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
                        Ok(SqlStatement::Insert {
                            table_name: table_str,
                            columns: col_names,
                            rows,
                        })
                    }
                    _ => {
                        let logical_plan = self.plan_query(source)?;
                        Ok(SqlStatement::InsertSelect {
                            table_name: table_str,
                            columns: col_names,
                            query: logical_plan,
                        })
                    }
                }
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
            Statement::Analyze { table_name, .. } => Ok(SqlStatement::Analyze {
                table_name: table_name.to_string(),
            }),
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
        use sqlparser::ast::SetExpr;
        match &*query.body {
            SetExpr::Select(select) => self.plan_select_query(select, query),
            other => {
                let mut plan = self.plan_set_expr(other)?;

                // Plan ORDER BY at the query level (after the set operation)
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

                // Plan LIMIT and OFFSET at the query level
                let limit = if let Some(limit_expr) = &query.limit {
                    let l = match limit_expr {
                        SqlExpr::Value(SqlValue::Number(s, _)) => {
                            s.parse::<usize>().map_err(|e| e.to_string())?
                        }
                        other_expr => {
                            return Err(format!("Unsupported limit expression: {:?}", other_expr));
                        }
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
                        other_expr => {
                            return Err(format!("Unsupported offset expression: {:?}", other_expr));
                        }
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
        }
    }

    fn plan_set_expr(&self, set_expr: &sqlparser::ast::SetExpr) -> Result<LogicalPlan, String> {
        use sqlparser::ast::SelectItem;
        use sqlparser::ast::SetExpr;

        match set_expr {
            SetExpr::Select(select) => {
                // 1. Plan FROM clause (Leaf scan / joins)
                let mut plan = self.plan_from(&select.from)?;

                // 2. Plan JOIN clause
                if !select.from.is_empty() {
                    let relation = &select.from[0];
                    for join in &relation.joins {
                        let right_scan = self.plan_table_factor(&join.relation)?;
                        let (join_cond, join_type) = match &join.join_operator {
                            sqlparser::ast::JoinOperator::Inner(
                                sqlparser::ast::JoinConstraint::On(expr),
                            ) => (plan_expr(expr)?, JoinType::Inner),
                            sqlparser::ast::JoinOperator::LeftOuter(
                                sqlparser::ast::JoinConstraint::On(expr),
                            ) => (plan_expr(expr)?, JoinType::Left),
                            sqlparser::ast::JoinOperator::CrossJoin => (
                                Expr::Literal(dtdb_storage::DbValue::Int(1)),
                                JoinType::Cross,
                            ),
                            sqlparser::ast::JoinOperator::Inner(
                                sqlparser::ast::JoinConstraint::None,
                            ) => (
                                Expr::Literal(dtdb_storage::DbValue::Int(1)),
                                JoinType::Cross,
                            ),
                            other => {
                                return Err(format!("Unsupported JOIN operator: {:?}", other));
                            }
                        };
                        plan = LogicalPlan::Join {
                            left: Box::new(plan),
                            right: Box::new(right_scan),
                            condition: join_cond,
                            join_type,
                        };
                    }
                }

                // 3. Plan WHERE clause
                if let Some(selection) = &select.selection {
                    plan = LogicalPlan::Filter {
                        source: Box::new(plan),
                        predicate: plan_expr(selection)?,
                    };
                }

                // 4. Plan GROUP BY / aggregations / HAVING
                let has_groupby = !select.group_by.is_empty();
                let has_aggrs = select_items_have_aggrs(&select.projection);
                let has_having = select.having.is_some();

                if has_groupby || has_aggrs || has_having {
                    // Aggregate planning
                    let group_exprs = select.group_by.iter().map(plan_expr).collect::<Result<
                        Vec<_>,
                        String,
                    >>(
                    )?;

                    let mut aggr_exprs = Vec::new();
                    let mut field_names = Vec::new();

                    // First group-by field names
                    for expr in &group_exprs {
                        match expr {
                            Expr::Column(name) => field_names.push(name.clone()),
                            _ => field_names.push("group_key".to_string()),
                        }
                    }

                    // Extract aggregates from select projection items by rewriting them
                    let mut rewritten_projection = Vec::new();
                    for item in &select.projection {
                        match item {
                            SelectItem::UnnamedExpr(expr) => {
                                let mut expr_mut = expr.clone();
                                rewrite_having_expr(
                                    &mut expr_mut,
                                    &mut aggr_exprs,
                                    &mut field_names,
                                )?;
                                rewritten_projection.push(SelectItem::UnnamedExpr(expr_mut));
                            }
                            SelectItem::ExprWithAlias { expr, alias } => {
                                let mut expr_mut = expr.clone();
                                rewrite_having_expr(
                                    &mut expr_mut,
                                    &mut aggr_exprs,
                                    &mut field_names,
                                )?;
                                rewritten_projection.push(SelectItem::ExprWithAlias {
                                    expr: expr_mut,
                                    alias: alias.clone(),
                                });
                            }
                            SelectItem::Wildcard(_) => {
                                return Err(
                                    "Wildcards not allowed in GROUP BY / Aggregations".to_string()
                                );
                            }
                            _ => return Err(format!("Unsupported select item: {:?}", item)),
                        }
                    }

                    // Extract aggregates from HAVING clause and rewrite the HAVING predicate
                    let planned_having = if let Some(having_expr) = &select.having {
                        let mut having_mut = having_expr.clone();
                        rewrite_having_expr(&mut having_mut, &mut aggr_exprs, &mut field_names)?;
                        Some(plan_expr(&having_mut)?)
                    } else {
                        None
                    };

                    plan = LogicalPlan::Aggregate {
                        source: Box::new(plan),
                        group_by: group_exprs,
                        aggrs: aggr_exprs,
                        field_names,
                    };

                    // Apply HAVING filter node if present
                    if let Some(having_pred) = planned_having {
                        plan = LogicalPlan::Filter {
                            source: Box::new(plan),
                            predicate: having_pred,
                        };
                    }

                    // Place a final Projection to select only the requested projection items
                    let mut expressions = Vec::new();
                    let mut projection_field_names = Vec::new();

                    for item in &rewritten_projection {
                        match item {
                            SelectItem::UnnamedExpr(expr) => {
                                let planned_expr = plan_expr(expr)?;
                                let name = match &planned_expr {
                                    Expr::Column(name) => name.clone(),
                                    _ => expr.to_string(),
                                };
                                expressions.push(planned_expr);
                                projection_field_names.push(name);
                            }
                            SelectItem::ExprWithAlias { expr, alias } => {
                                let planned_expr = plan_expr(expr)?;
                                expressions.push(planned_expr);
                                projection_field_names.push(alias.value.clone());
                            }
                            _ => unreachable!(),
                        }
                    }

                    plan = LogicalPlan::Projection {
                        source: Box::new(plan),
                        expressions,
                        field_names: projection_field_names,
                    };
                } else {
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

                Ok(plan)
            }
            SetExpr::Query(query) => self.plan_query(query),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => {
                let left_plan = self.plan_set_expr(left)?;
                let right_plan = self.plan_set_expr(right)?;

                validate_set_op_schemas(&left_plan.schema(), &right_plan.schema())?;

                let set_op_type = match op {
                    sqlparser::ast::SetOperator::Union => SetOpType::Union,
                    sqlparser::ast::SetOperator::Except => SetOpType::Except,
                    sqlparser::ast::SetOperator::Intersect => SetOpType::Intersect,
                };

                let all = matches!(set_quantifier, sqlparser::ast::SetQuantifier::All);

                Ok(LogicalPlan::SetOp {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    op: set_op_type,
                    all,
                })
            }
            other => Err(format!("Unsupported set expression: {:?}", other)),
        }
    }

    fn plan_select_query(
        &self,
        select: &sqlparser::ast::Select,
        query: &Query,
    ) -> Result<LogicalPlan, String> {
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
                    sqlparser::ast::JoinOperator::CrossJoin => (
                        Expr::Literal(dtdb_storage::DbValue::Int(1)),
                        JoinType::Cross,
                    ),
                    sqlparser::ast::JoinOperator::Inner(sqlparser::ast::JoinConstraint::None) => (
                        Expr::Literal(dtdb_storage::DbValue::Int(1)),
                        JoinType::Cross,
                    ),
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

        // 4. Plan GROUP BY / aggregations / HAVING
        let has_groupby = !select.group_by.is_empty();
        let has_aggrs = select_items_have_aggrs(&select.projection);
        let has_having = select.having.is_some();

        if has_groupby || has_aggrs || has_having {
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

            // Extract aggregates from select projection items by rewriting them
            let mut rewritten_projection = Vec::new();
            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        let mut expr_mut = expr.clone();
                        rewrite_having_expr(&mut expr_mut, &mut aggr_exprs, &mut field_names)?;
                        rewritten_projection.push(SelectItem::UnnamedExpr(expr_mut));
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        let mut expr_mut = expr.clone();
                        rewrite_having_expr(&mut expr_mut, &mut aggr_exprs, &mut field_names)?;
                        rewritten_projection.push(SelectItem::ExprWithAlias {
                            expr: expr_mut,
                            alias: alias.clone(),
                        });
                    }
                    SelectItem::Wildcard(_) => {
                        return Err("Wildcards not allowed in GROUP BY / Aggregations".to_string());
                    }
                    _ => return Err(format!("Unsupported select item: {:?}", item)),
                }
            }

            // Extract aggregates from HAVING clause and rewrite the HAVING predicate
            let planned_having = if let Some(having_expr) = &select.having {
                let mut having_mut = having_expr.clone();
                rewrite_having_expr(&mut having_mut, &mut aggr_exprs, &mut field_names)?;
                Some(plan_expr(&having_mut)?)
            } else {
                None
            };

            plan = LogicalPlan::Aggregate {
                source: Box::new(plan),
                group_by: group_exprs,
                aggrs: aggr_exprs,
                field_names,
            };

            // Apply HAVING filter node if present
            if let Some(having_pred) = planned_having {
                plan = LogicalPlan::Filter {
                    source: Box::new(plan),
                    predicate: having_pred,
                };
            }

            // Place a final Projection to select only the requested projection items
            let mut expressions = Vec::new();
            let mut projection_field_names = Vec::new();

            for item in &rewritten_projection {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        let planned_expr = plan_expr(expr)?;
                        let name = match &planned_expr {
                            Expr::Column(name) => name.clone(),
                            _ => expr.to_string(),
                        };
                        expressions.push(planned_expr);
                        projection_field_names.push(name);
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        let planned_expr = plan_expr(expr)?;
                        expressions.push(planned_expr);
                        projection_field_names.push(alias.value.clone());
                    }
                    _ => unreachable!(),
                }
            }

            plan = LogicalPlan::Projection {
                source: Box::new(plan),
                expressions,
                field_names: projection_field_names,
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
            TableFactor::Table { name, alias, .. } => {
                let name_str = name.to_string();
                let table = self
                    .database
                    .get_table(&name_str)
                    .map_err(|e| e.to_string())?;

                let qualifier = match alias {
                    Some(a) => a.name.value.clone(),
                    None => name_str.clone(),
                };

                let mut qualified_cols = table.schema.columns.clone();
                for col in &mut qualified_cols {
                    col.name = format!("{}.{}", qualifier, col.name);
                }

                let mut scan_schema = dtdb_relational::Schema::new_with_options(
                    qualified_cols,
                    table.schema.locality_group_options,
                );
                scan_schema.indexes = table.schema.indexes.clone();

                Ok(LogicalPlan::Scan {
                    table_name: name_str,
                    schema: scan_schema,
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
                SqlValue::Boolean(b) => DbValue::Bool(*b),
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
            let like_expr = Expr::BinaryOp {
                left: Box::new(plan_expr(expr)?),
                op: Operator::Like,
                right: Box::new(plan_expr(pattern)?),
            };
            if *negated {
                Ok(Expr::Not(Box::new(like_expr)))
            } else {
                Ok(like_expr)
            }
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
                sqlparser::ast::UnaryOperator::Not => Ok(Expr::Not(Box::new(inner))),
                other => Err(format!("Unsupported unary operator: {:?}", other)),
            }
        }
        SqlExpr::IsNull(inner) => Ok(Expr::IsNull(Box::new(plan_expr(inner)?))),
        SqlExpr::IsNotNull(inner) => Ok(Expr::Not(Box::new(Expr::IsNull(Box::new(plan_expr(
            inner,
        )?))))),
        SqlExpr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let planned_expr = plan_expr(expr)?;
            let planned_low = plan_expr(low)?;
            let planned_high = plan_expr(high)?;
            let between_expr = Expr::BinaryOp {
                left: Box::new(Expr::BinaryOp {
                    left: Box::new(planned_expr.clone()),
                    op: Operator::GtEq,
                    right: Box::new(planned_low),
                }),
                op: Operator::And,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(planned_expr),
                    op: Operator::LtEq,
                    right: Box::new(planned_high),
                }),
            };
            if *negated {
                Ok(Expr::Not(Box::new(between_expr)))
            } else {
                Ok(between_expr)
            }
        }
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            let planned_expr = plan_expr(expr)?;
            let planned_list = list
                .iter()
                .map(plan_expr)
                .collect::<Result<Vec<_>, String>>()?;
            let in_list_expr = Expr::InList {
                expr: Box::new(planned_expr),
                list: planned_list,
            };
            if *negated {
                Ok(Expr::Not(Box::new(in_list_expr)))
            } else {
                Ok(in_list_expr)
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
                "SUBSTR"
                    | "SUBSTRING"
                    | "LENGTH"
                    | "COALESCE"
                    | "UPPER"
                    | "LOWER"
                    | "CONCAT"
                    | "ABS"
                    | "ROUND"
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

fn eval_default_expr(expr: &SqlExpr) -> Result<DbValue, String> {
    match expr {
        SqlExpr::Value(val) => match val {
            SqlValue::Number(num_str, _) => {
                if let Ok(i) = num_str.parse::<i64>() {
                    Ok(DbValue::Int(i))
                } else if let Ok(f) = num_str.parse::<f64>() {
                    Ok(DbValue::Float(f))
                } else {
                    Err(format!("Invalid numeric literal for default: {}", num_str))
                }
            }
            SqlValue::SingleQuotedString(s) => Ok(DbValue::String(s.clone())),
            SqlValue::Boolean(b) => Ok(DbValue::Bool(*b)),
            SqlValue::Null => Ok(DbValue::Null),
            other => Err(format!("Unsupported value type for default: {:?}", other)),
        },
        SqlExpr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr: inner,
        } => {
            let val = eval_default_expr(inner)?;
            match val {
                DbValue::Int(i) => Ok(DbValue::Int(-i)),
                DbValue::Float(f) => Ok(DbValue::Float(-f)),
                _ => Err("Invalid negative default value".to_string()),
            }
        }
        SqlExpr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Plus,
            expr: inner,
        } => eval_default_expr(inner),
        other => Err(format!(
            "Unsupported expression type for default: {:?}",
            other
        )),
    }
}

fn rewrite_having_expr(
    expr: &mut SqlExpr,
    aggr_exprs: &mut Vec<AggregateExpr>,
    field_names: &mut Vec<String>,
) -> Result<(), String> {
    match expr {
        SqlExpr::Function(func) => {
            let name = func.name.to_string().to_uppercase();
            if matches!(name.as_str(), "COUNT" | "SUM" | "MIN" | "MAX" | "AVG") {
                let alias = func.to_string();

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
                    _ => unreachable!(),
                };

                if !aggr_exprs.contains(&aggr) {
                    aggr_exprs.push(aggr);
                    field_names.push(alias.clone());
                }

                *expr = SqlExpr::Identifier(sqlparser::ast::Ident::new(alias));
                Ok(())
            } else {
                for arg in &mut func.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner_expr)) = arg {
                        rewrite_having_expr(inner_expr, aggr_exprs, field_names)?;
                    }
                }
                Ok(())
            }
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            rewrite_having_expr(left, aggr_exprs, field_names)?;
            rewrite_having_expr(right, aggr_exprs, field_names)?;
            Ok(())
        }
        SqlExpr::Nested(inner) => rewrite_having_expr(inner, aggr_exprs, field_names),
        SqlExpr::UnaryOp { expr: inner, .. } => rewrite_having_expr(inner, aggr_exprs, field_names),
        SqlExpr::Case {
            conditions,
            results,
            else_result,
            ..
        } => {
            for cond in conditions {
                rewrite_having_expr(cond, aggr_exprs, field_names)?;
            }
            for res in results {
                rewrite_having_expr(res, aggr_exprs, field_names)?;
            }
            if let Some(el) = else_result {
                rewrite_having_expr(el, aggr_exprs, field_names)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn validate_set_op_schemas(left: &Schema, right: &Schema) -> Result<(), String> {
    if left.columns.len() != right.columns.len() {
        return Err(format!(
            "Set operation queries must have the same number of columns (left: {}, right: {})",
            left.columns.len(),
            right.columns.len()
        ));
    }
    for (i, (lc, rc)) in left.columns.iter().zip(right.columns.iter()).enumerate() {
        if lc.data_type != rc.data_type {
            return Err(format!(
                "Column {} type mismatch: left is {:?}, right is {:?}",
                i + 1,
                lc.data_type,
                rc.data_type
            ));
        }
    }
    Ok(())
}
