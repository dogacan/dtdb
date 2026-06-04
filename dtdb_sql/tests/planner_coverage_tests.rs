//! Targeted coverage for the logical planner paths not exercised elsewhere:
//!   * `eval_default_expr` — column DEFAULT clauses beyond plain literals
//!     (negative numbers, unary plus, CAST, typed temporal strings),
//!   * correlation detection (`collect_local_columns`) recursing through
//!     compound expressions (functions / CASE / CAST) inside a subquery.

use dtdb_relational::{Database, Transaction};
use dtdb_sql::{ExecutionResult, SqlEngine};
use dtdb_storage::DbValue;
use std::sync::Arc;
use tempfile::TempDir;

fn setup_engine() -> (TempDir, Arc<Database>, SqlEngine) {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    let engine = SqlEngine::new(db.clone());
    (temp_dir, db, engine)
}

fn select_rows(res: ExecutionResult) -> Vec<dtdb_relational::Row> {
    match res {
        ExecutionResult::Select { rows, .. } => rows,
        other => panic!("expected Select, got {other:?}"),
    }
}

// ----- column DEFAULT expressions -----

#[test]
fn column_defaults_support_negative_plus_cast_and_typed_literals() {
    let (_tmp, db, engine) = setup_engine();
    let tx = Transaction::new(1, db.clone());
    // Each DEFAULT exercises a distinct eval_default_expr branch:
    //   x: unary minus on an integer literal,
    //   y: unary minus on a float literal,
    //   z: unary plus,
    //   c: CAST(... AS INT),
    //   d: a typed temporal string literal (DATE '...').
    engine
        .execute(
            "CREATE TABLE t (\
                id INT PRIMARY KEY, \
                x INT DEFAULT -5, \
                y FLOAT DEFAULT -1.5, \
                z INT DEFAULT +3, \
                c INT DEFAULT CAST('7' AS INT), \
                d DATE DEFAULT DATE '2026-01-01')",
            &tx,
        )
        .unwrap();
    engine
        .execute("INSERT INTO t (id) VALUES (1)", &tx)
        .unwrap();
    tx.commit().unwrap();

    let tx = Transaction::new(2, db.clone());
    let rows = select_rows(
        engine
            .execute("SELECT x, y, z, c, d FROM t WHERE id = 1", &tx)
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], DbValue::Int(-5));
    assert_eq!(rows[0].values[1], DbValue::Float(-1.5));
    assert_eq!(rows[0].values[2], DbValue::Int(3));
    assert_eq!(rows[0].values[3], DbValue::Int(7));
    assert_eq!(
        rows[0].values[4],
        DbValue::Date(chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap())
    );
}

#[test]
fn unsupported_default_expression_is_rejected() {
    let (_tmp, _db, engine) = setup_engine();
    let tx = Transaction::new(1, _db.clone());
    // A non-constant default (a column reference) is not a supported default
    // expression and is rejected at planning time.
    let err = engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, x INT DEFAULT id)", &tx)
        .unwrap_err();
    assert!(
        err.contains("Unsupported expression type for default"),
        "got: {err}"
    );
}

// ----- correlation detection traversing compound expressions -----

fn seed_two_tables(db: &Arc<Database>, engine: &SqlEngine) {
    let tx = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)", &tx)
        .unwrap();
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, x INT)", &tx)
        .unwrap();
    engine
        .execute("INSERT INTO t (id, v) VALUES (1, 10), (2, 20)", &tx)
        .unwrap();
    engine
        .execute("INSERT INTO t2 (id, x) VALUES (1, 5), (2, 25)", &tx)
        .unwrap();
    tx.commit().unwrap();
}

#[test]
fn uncorrelated_subquery_with_function_case_and_cast_is_accepted() {
    let (_tmp, db, engine) = setup_engine();
    seed_two_tables(&db, &engine);
    let tx = Transaction::new(2, db.clone());

    // Each subquery references only its own column (t2.x), buried inside a
    // function / CASE / CAST. Correlation analysis must descend into those
    // expression nodes and conclude the subquery is uncorrelated.
    for sql in [
        "SELECT id FROM t WHERE v IN (SELECT x FROM t2 WHERE ABS(x) > 0)",
        "SELECT id FROM t WHERE v IN (SELECT x FROM t2 WHERE CASE WHEN x > 0 THEN 1 ELSE 0 END = 1)",
        "SELECT id FROM t WHERE v IN (SELECT x FROM t2 WHERE CAST(x AS INT) > 0)",
    ] {
        let res = engine.execute(sql, &tx);
        assert!(res.is_ok(), "expected ok for `{sql}`, got {res:?}");
    }
}

#[test]
fn correlated_column_buried_in_function_is_rejected() {
    let (_tmp, db, engine) = setup_engine();
    seed_two_tables(&db, &engine);
    let tx = Transaction::new(2, db.clone());

    // The outer column `t.id` is referenced inside a function argument in the
    // subquery — correlation detection must still find it and reject the query.
    let err = engine
        .execute(
            "SELECT id FROM t WHERE v IN (SELECT x FROM t2 WHERE ABS(t.id) = x)",
            &tx,
        )
        .unwrap_err();
    assert!(
        err.contains("correlated subqueries are not supported"),
        "got: {err}"
    );
}

// ----- CREATE TABLE Options & Column Option Parsing Errors -----

#[test]
fn test_create_table_locality_group_errors() {
    let (_tmp, db, engine) = setup_engine();
    let tx = Transaction::new(1, db.clone());

    // 1. Unknown compression type
    let err = engine
        .execute(
            "CREATE TABLE t_err (id INT PRIMARY KEY) WITH (locality_groups = 'g1:id:compression=invalid')",
            &tx,
        )
        .unwrap_err();
    assert!(err.contains("Unknown compression type: invalid"));

    // 2. Invalid memtable_size_limit
    let err = engine
        .execute(
            "CREATE TABLE t_err2 (id INT PRIMARY KEY) WITH (locality_groups = 'g1:id:memtable_size_limit=abc')",
            &tx,
        )
        .unwrap_err();
    assert!(err.contains("Invalid memtable_size_limit"));

    // 3. Unknown locality group option
    let err = engine
        .execute(
            "CREATE TABLE t_err3 (id INT PRIMARY KEY) WITH (locality_groups = 'g1:id:foo=123')",
            &tx,
        )
        .unwrap_err();
    assert!(err.contains("Unknown locality group option: foo"));

    // 4. Custom BOOL datatype (verifies line 1556-1557 in planner.rs)
    engine
        .execute("CREATE TABLE t_bool (id BOOL, flag \"BOOL\")", &tx)
        .unwrap();
}

// ----- CREATE/DROP INDEX and ALTER TABLE Parsing Errors -----

#[test]
fn test_create_drop_index_and_alter_table_errors() {
    let (_tmp, db, engine) = setup_engine();
    let tx = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)", &tx)
        .unwrap();

    // 1. CREATE INDEX with complex expression
    let err = engine
        .execute("CREATE INDEX idx ON t (id + 1)", &tx)
        .unwrap_err();
    assert!(err.contains("Only simple column identifiers are supported in CREATE INDEX"));

    // 2. DROP INDEX for a non-existent index
    let err = engine.execute("DROP INDEX non_existent", &tx).unwrap_err();
    assert!(err.contains("Index 'non_existent' not found in database"));

    // 3. Unsupported DROP statement object type
    let err = engine.execute("DROP SCHEMA s", &tx).unwrap_err();
    assert!(err.contains("Only DROP TABLE and DROP INDEX statements are supported"));

    // 4. ALTER TABLE with multiple operations
    let err = engine
        .execute("ALTER TABLE t ADD COLUMN a INT, DROP COLUMN v", &tx)
        .unwrap_err();
    assert!(err.contains("ALTER TABLE supports exactly one operation per statement"));

    // 5. ALTER TABLE DROP COLUMN with multiple columns (programmatically tested to bypass generic dialect parser restriction)
    let dialect = sqlparser::dialect::GenericDialect {};
    let mut statements =
        sqlparser::parser::Parser::parse_sql(&dialect, "ALTER TABLE t DROP COLUMN a").unwrap();
    let mut stmt = statements.remove(0);
    if let sqlparser::ast::Statement::AlterTable(ref mut at) = stmt
        && let sqlparser::ast::AlterTableOperation::DropColumn {
            ref mut column_names,
            ..
        } = at.operations[0]
    {
        column_names.push(sqlparser::ast::Ident::new("b"));
    }
    let planner = dtdb_sql::LogicalPlanner::new(db.clone());
    let err = planner.plan(&stmt).unwrap_err();
    assert_eq!(
        err,
        "ALTER TABLE DROP COLUMN supports dropping exactly one column"
    );

    // 6. Unsupported ALTER TABLE operation
    let err = engine
        .execute("ALTER TABLE t ALTER COLUMN v TYPE TEXT", &tx)
        .unwrap_err();
    assert!(err.contains("Unsupported ALTER TABLE operation"));

    // 7. ALTER TABLE ADD COLUMN with PRIMARY KEY
    let err = engine
        .execute("ALTER TABLE t ADD COLUMN a INT PRIMARY KEY", &tx)
        .unwrap_err();
    assert!(err.contains("ALTER TABLE ADD COLUMN cannot add a PRIMARY KEY column"));
}

// ----- INSERT / UPDATE Statements Edge Cases -----

#[test]
fn test_insert_update_edge_cases() {
    let (_tmp, db, engine) = setup_engine();
    let tx = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)", &tx)
        .unwrap();

    // 1. INSERT without column list (verifies schema column mapping, line 490-495)
    engine.execute("INSERT INTO t VALUES (1, 10)", &tx).unwrap();

    // 2. INSERT with invalid column identifier format
    let err = engine
        .execute("INSERT INTO t (id, v, extra.x) VALUES (2, 20, 30)", &tx)
        .unwrap_err();
    assert!(err.contains("Unsupported column name") || err.contains("Invalid column name"));

    // 3. UPDATE without a WHERE clause (verifies Selection None, line 620)
    engine.execute("UPDATE t SET v = 20", &tx).unwrap();

    let rows = select_rows(engine.execute("SELECT v FROM t WHERE id = 1", &tx).unwrap());
    assert_eq!(rows[0].values[0], DbValue::Int(20));
}

// ----- Query Expressions (Scalar and Set Operations) -----

#[test]
fn test_query_expressions_and_set_ops_edge_cases() {
    let (_tmp, db, engine) = setup_engine();
    let tx = Transaction::new(1, db.clone());
    engine
        .execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)", &tx)
        .unwrap();
    engine
        .execute("INSERT INTO t (id, v) VALUES (1, 10)", &tx)
        .unwrap();

    // 1. ORDER BY ALL (programmatically tested to bypass dialect parser restriction)
    let dialect = sqlparser::dialect::GenericDialect {};
    let mut statements = sqlparser::parser::Parser::parse_sql(&dialect, "SELECT * FROM t").unwrap();
    let mut stmt = statements.remove(0);
    if let sqlparser::ast::Statement::Query(ref mut query) = stmt {
        query.order_by = Some(sqlparser::ast::OrderBy {
            kind: sqlparser::ast::OrderByKind::All(sqlparser::ast::OrderByOptions::default()),
            interpolate: None,
        });
    }
    let planner = dtdb_sql::LogicalPlanner::new(db.clone());
    let err = planner.plan(&stmt).unwrap_err();
    assert_eq!(err, "ORDER BY ALL is not supported");

    // 2. SetExpr::Query (parenthesized query in UNION)
    let rows = select_rows(
        engine
            .execute("(SELECT id FROM t) UNION SELECT id FROM t", &tx)
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);

    // 3. Unsupported SetExpr (e.g. VALUES query body)
    let err = engine.execute("VALUES (1)", &tx).unwrap_err();
    assert!(err.contains("Unsupported set expression"));

    // 4. CROSS JOIN / JoinConstraint::None
    engine
        .execute("CREATE TABLE t2 (id INT PRIMARY KEY, x INT)", &tx)
        .unwrap();
    engine
        .execute("INSERT INTO t2 (id, x) VALUES (1, 100)", &tx)
        .unwrap();
    let rows = select_rows(
        engine
            .execute("SELECT t.id, t2.id FROM t JOIN t2", &tx)
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);

    // 5. Unsupported JOIN operator (e.g., RIGHT JOIN)
    let err = engine
        .execute("SELECT * FROM t RIGHT JOIN t2 ON t.id = t2.id", &tx)
        .unwrap_err();
    assert!(err.contains("Unsupported JOIN operator"));

    // 6. GROUP BY modifier (e.g. GROUP BY ROLLUP)
    let dialect = sqlparser::dialect::GenericDialect {};
    let mut statements =
        sqlparser::parser::Parser::parse_sql(&dialect, "SELECT v FROM t GROUP BY v").unwrap();
    let mut stmt = statements.remove(0);
    if let sqlparser::ast::Statement::Query(query) = &mut stmt
        && let sqlparser::ast::SetExpr::Select(select) = &mut *query.body
        && let sqlparser::ast::GroupByExpr::Expressions(_, modifiers) = &mut select.group_by
    {
        modifiers.push(sqlparser::ast::GroupByWithModifier::Rollup);
    }
    let planner = dtdb_sql::LogicalPlanner::new(db.clone());
    let err = planner.plan(&stmt).unwrap_err();
    assert_eq!(err, "GROUP BY modifiers are not supported");

    // 7. GROUP BY non-column expression (pushed as "group_key", line 918)
    let rows = select_rows(
        engine
            .execute("SELECT group_key FROM t GROUP BY v + 1", &tx)
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], DbValue::Int(11));

    // 8. CASE expression without ELSE (line 1360)
    let rows = select_rows(
        engine
            .execute("SELECT CASE WHEN v > 0 THEN 1 END FROM t", &tx)
            .unwrap(),
    );
    assert_eq!(rows[0].values[0], DbValue::Int(1));

    // 9. SUBSTRING with FROM syntax (line 1378)
    engine
        .execute("CREATE TABLE t_str (id INT PRIMARY KEY, s TEXT)", &tx)
        .unwrap();
    engine
        .execute("INSERT INTO t_str (id, s) VALUES (1, 'hello')", &tx)
        .unwrap();
    let rows = select_rows(
        engine
            .execute("SELECT SUBSTRING(s FROM 2 FOR 3) FROM t_str", &tx)
            .unwrap(),
    );
    assert_eq!(rows[0].values[0], DbValue::string("ell"));

    let rows2 = select_rows(
        engine
            .execute("SELECT SUBSTRING(s FROM 3) FROM t_str", &tx)
            .unwrap(),
    );
    assert_eq!(rows2[0].values[0], DbValue::string("llo"));

    // 10. Unsupported function argument (e.g. wildcard distinct)
    let err = engine
        .execute("SELECT COUNT(DISTINCT *) FROM t", &tx)
        .unwrap_err();
    assert!(err.contains("COUNT(DISTINCT *) is not supported"));

    let mut statements =
        sqlparser::parser::Parser::parse_sql(&dialect, "SELECT COUNT(v) FROM t").unwrap();
    let mut stmt = statements.remove(0);
    if let sqlparser::ast::Statement::Query(query) = &mut stmt
        && let sqlparser::ast::SetExpr::Select(select) = &mut *query.body
        && let sqlparser::ast::SelectItem::UnnamedExpr(sqlparser::ast::Expr::Function(func)) =
            &mut select.projection[0]
        && let sqlparser::ast::FunctionArguments::List(arg_list) = &mut func.args
    {
        arg_list.args.clear();
        arg_list.duplicate_treatment = Some(sqlparser::ast::DuplicateTreatment::Distinct);
    }
    let planner = dtdb_sql::LogicalPlanner::new(db.clone());
    let err2 = planner.plan(&stmt).unwrap_err();
    assert!(err2.contains("COUNT with DISTINCT requires a column argument"));

    // 11. TypedString DoubleQuotedString
    let rows = select_rows(
        engine
            .execute("SELECT DATE \"2026-06-04\" FROM t", &tx)
            .unwrap(),
    );
    assert_eq!(
        rows[0].values[0],
        DbValue::Date(chrono::NaiveDate::from_ymd_opt(2026, 6, 4).unwrap())
    );

    // 12. eval_default_expr null / invalid negative
    engine
        .execute(
            "CREATE TABLE t_def_null (id INT PRIMARY KEY, x INT DEFAULT NULL)",
            &tx,
        )
        .unwrap();
    let err = engine
        .execute(
            "CREATE TABLE t_def_neg (id INT PRIMARY KEY, x INT DEFAULT -'abc')",
            &tx,
        )
        .unwrap_err();
    assert!(
        err.contains("Invalid negative default value") || err.contains("Unsupported expression")
    );
}

// ----- Subquery Correlation Analysis Checks -----

#[test]
fn test_subquery_correlation_traversal_and_collect_local_columns() {
    let (_tmp, db, engine) = setup_engine();
    seed_two_tables(&db, &engine);
    let tx = Transaction::new(2, db.clone());

    // 1. Correlation inside Sort (ORDER BY) in subquery
    let err = engine
        .execute(
            "SELECT id FROM t WHERE v IN (SELECT x FROM t2 ORDER BY t.id)",
            &tx,
        )
        .unwrap_err();
    assert!(err.contains("correlated subqueries are not supported"));

    // 2. Correlation inside Limit in subquery (traversal verification)
    let err = engine
        .execute(
            "SELECT id FROM t WHERE v IN (SELECT x FROM t2 WHERE t.id = x LIMIT 1)",
            &tx,
        )
        .unwrap_err();
    assert!(err.contains("correlated subqueries are not supported"));

    // 3. Correlation inside Distinct in subquery
    let err = engine
        .execute(
            "SELECT id FROM t WHERE v IN (SELECT DISTINCT t.id FROM t2)",
            &tx,
        )
        .unwrap_err();
    assert!(err.contains("correlated subqueries are not supported"));

    // 4. Correlation inside SetOp in subquery
    let err = engine
        .execute(
            "SELECT id FROM t WHERE v IN (SELECT x FROM t2 WHERE t.id = 1 UNION SELECT x FROM t2)",
            &tx,
        )
        .unwrap_err();
    assert!(err.contains("correlated subqueries are not supported"));

    // 5. Uncorrelated Case operand & collect local columns inside Case
    let rows = select_rows(
        engine
            .execute(
                "SELECT id FROM t WHERE v IN (SELECT CASE x WHEN 5 THEN 10 ELSE 20 END FROM t2 WHERE x = 5)",
                &tx,
            )
            .unwrap(),
    );
    assert_eq!(rows.len(), 1); // matches x = 5 -> CASE yields 10, v = 10 matches row 1 of t

    // 6. Uncorrelated Match / IsNull / InList / InSubquery inside subquery
    let sql = "SELECT id FROM t WHERE v IN (SELECT x FROM t2 WHERE MATCH(x) AGAINST ('5') AND x IS NOT NULL AND x IN (5, 6) AND x IN (SELECT x FROM t2))";
    let dialect = sqlparser::dialect::GenericDialect {};
    let mut statements = sqlparser::parser::Parser::parse_sql(&dialect, sql).unwrap();
    let stmt = statements.remove(0);
    let planner = dtdb_sql::LogicalPlanner::new(db.clone());
    let _ = planner.plan(&stmt);
}
