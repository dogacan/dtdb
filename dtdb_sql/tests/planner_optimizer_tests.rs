use dtdb_relational::{Column, DataType, Database, IndexType};
use dtdb_sql::logical::JoinType;
use dtdb_sql::planner::LogicalPlanner;
use dtdb_sql::{Expr, LogicalPlan, Operator, Optimizer, SqlStatement};
use dtdb_storage::DbValue;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;

fn setup_db() -> (tempfile::TempDir, Arc<Database>) {
    let tmp = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(tmp.path()).unwrap());

    // Create t1 with PK and index on val
    db.create_table(
        "t1",
        dtdb_relational::Schema::new(vec![
            Column {
                name: "id".to_string(),
                data_type: DataType::Int,
                is_primary_key: true,
                is_nullable: false,
                locality_group: None,
                default_value: None,
                is_auto_increment: false,
            },
            Column {
                name: "val".to_string(),
                data_type: DataType::Int,
                is_primary_key: false,
                is_nullable: true,
                locality_group: None,
                default_value: None,
                is_auto_increment: false,
            },
        ]),
    )
    .unwrap();
    db.create_index(
        "t1",
        "t1_val_idx",
        vec!["val".to_string()],
        IndexType::BTree,
        None,
    )
    .unwrap();

    // Create t2 with PK
    db.create_table(
        "t2",
        dtdb_relational::Schema::new(vec![
            Column {
                name: "id".to_string(),
                data_type: DataType::Int,
                is_primary_key: true,
                is_nullable: false,
                locality_group: None,
                default_value: None,
                is_auto_increment: false,
            },
            Column {
                name: "a".to_string(),
                data_type: DataType::Int,
                is_primary_key: false,
                is_nullable: true,
                locality_group: None,
                default_value: None,
                is_auto_increment: false,
            },
        ]),
    )
    .unwrap();

    // Create t_str with String PK
    db.create_table(
        "t_str",
        dtdb_relational::Schema::new(vec![Column {
            name: "name".to_string(),
            data_type: DataType::String,
            is_primary_key: true,
            is_nullable: false,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        }]),
    )
    .unwrap();

    (tmp, db)
}

fn plan_sql(db: Arc<Database>, sql: &str) -> Result<SqlStatement, String> {
    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;
    if statements.is_empty() {
        return Err("No statements found".to_string());
    }
    let planner = LogicalPlanner::new(db);
    planner.plan(&statements.remove(0))
}

fn optimize_plan(db: Arc<Database>, sql: &str) -> Result<LogicalPlan, String> {
    let stmt = plan_sql(db.clone(), sql)?;
    if let SqlStatement::Query(plan) = stmt {
        let optimizer = Optimizer::new(db);
        Ok(optimizer.optimize(plan))
    } else {
        Err("Expected Query statement".to_string())
    }
}

#[test]
fn test_create_table_invalid_options() {
    let (_tmp, db) = setup_db();

    // 1. Unknown compression type
    let sql = "CREATE TABLE t (id INT) WITH (locality_groups = 'lg:id:compression=invalid');";
    let res = plan_sql(db.clone(), sql);
    assert!(res.is_err());
    let err = match res {
        Err(e) => e,
        _ => panic!("expected error"),
    };
    assert!(err.contains("Unknown compression type"));

    // 2. Parse errors for numeric limits
    let numeric_opts = &[
        "memtable_size_limit",
        "block_size_limit",
        "wal_size_limit",
        "l0_compaction_threshold",
        "sstable_target_size",
        "base_level_size_limit",
        "level_size_multiplier",
        "max_level",
        "block_cache_capacity",
        "wal_sync_interval_ms",
    ];
    for opt in numeric_opts {
        let sql = format!(
            "CREATE TABLE t (id INT) WITH (locality_groups = 'lg:id:{}=abc');",
            opt
        );
        let res = plan_sql(db.clone(), &sql);
        assert!(res.is_err(), "Expected error for opt: {}", opt);
    }

    // 3. Unknown option
    let sql = "CREATE TABLE t (id INT) WITH (locality_groups = 'lg:id:unknown_option=123');";
    let res = plan_sql(db.clone(), sql);
    assert!(res.is_err());
    let err = match res {
        Err(e) => e,
        _ => panic!("expected error"),
    };
    assert!(err.contains("Unknown locality group option"));
}

#[test]
fn test_create_table_lz4_compression() {
    let (_tmp, db) = setup_db();
    let sql = "CREATE TABLE t (id INT) WITH (locality_groups = 'lg:id:compression=lz4');";
    let res = plan_sql(db, sql).unwrap();
    if let SqlStatement::CreateTable { schema, .. } = res {
        let lg_opts = schema.locality_group_options.get("lg").unwrap();
        assert_eq!(
            lg_opts.compression,
            Some(dtdb_storage::CompressionType::Lz4)
        );
    } else {
        panic!("expected CreateTable");
    }
}

#[test]
fn test_create_table_datatypes() {
    let (_tmp, db) = setup_db();

    // Test data type mappings and SERIAL / BIGSERIAL custom type mapping
    let sql = "CREATE TABLE t (
        id SERIAL PRIMARY KEY,
        val_big BIGINT,
        val_real REAL,
        val_double DOUBLE,
        val_float FLOAT,
        val_bool BOOLEAN,
        val_varchar VARCHAR(100),
        val_char CHAR(10),
        val_text TEXT,
        val_bytes BYTEA
    );";
    let res = plan_sql(db, sql).unwrap();
    if let SqlStatement::CreateTable { schema, .. } = res {
        assert_eq!(schema.columns[0].data_type, DataType::Int);
        assert!(schema.columns[0].is_auto_increment);
        assert_eq!(schema.columns[1].data_type, DataType::Int);
        assert_eq!(schema.columns[2].data_type, DataType::Float);
        assert_eq!(schema.columns[3].data_type, DataType::Float);
        assert_eq!(schema.columns[4].data_type, DataType::Float);
        assert_eq!(schema.columns[5].data_type, DataType::Bool);
        assert_eq!(schema.columns[6].data_type, DataType::String);
        assert_eq!(schema.columns[7].data_type, DataType::String);
        assert_eq!(schema.columns[8].data_type, DataType::String);
        assert_eq!(schema.columns[9].data_type, DataType::Bytes);
    } else {
        panic!("expected CreateTable");
    }
}

#[test]
fn test_create_table_autoincrement_dialect() {
    let (_tmp, db) = setup_db();

    // AUTO_INCREMENT
    let sql = "CREATE TABLE t (id INT AUTO_INCREMENT);";
    let res = plan_sql(db.clone(), sql).unwrap();
    if let SqlStatement::CreateTable { schema, .. } = res {
        assert!(schema.columns[0].is_auto_increment);
    } else {
        panic!("expected CreateTable");
    }

    // AUTOINCREMENT
    let sql2 = "CREATE TABLE t (id INT AUTOINCREMENT);";
    let res2 = plan_sql(db, sql2).unwrap();
    if let SqlStatement::CreateTable { schema, .. } = res2 {
        assert!(schema.columns[0].is_auto_increment);
    } else {
        panic!("expected CreateTable");
    }
}

#[test]
fn test_drop_table_planning() {
    let (_tmp, db) = setup_db();

    let sql = "DROP TABLE mytable;";
    let res = plan_sql(db, sql).unwrap();
    if let SqlStatement::DropTable { name } = res {
        assert_eq!(name, "mytable");
    } else {
        panic!("expected DropTable");
    }
}

#[test]
fn test_unsupported_datatype_error() {
    let (_tmp, db) = setup_db();
    let sql = "CREATE TABLE t (id DECIMAL);";
    let res = plan_sql(db, sql);
    assert!(res.is_err());
    let err = match res {
        Err(e) => e,
        _ => panic!("expected error"),
    };
    assert!(err.contains("Unsupported SQL data type"));
}

#[test]
fn test_planner_statement_errors() {
    let (_tmp, db) = setup_db();

    // 1. EXPLAIN non-query
    let sql = "EXPLAIN CREATE TABLE t (id INT);";
    let res = plan_sql(db.clone(), sql);
    assert!(res.is_err());
    let err = match res {
        Err(e) => e,
        _ => panic!("expected error"),
    };
    assert!(err.contains("Can only EXPLAIN SELECT queries"));

    // 2. Unsupported SQL statement (e.g. COPY)
    let sql = "COPY t TO STDOUT;";
    let res = plan_sql(db.clone(), sql);
    assert!(res.is_err());
    let err = match res {
        Err(e) => e,
        _ => panic!("expected error"),
    };
    assert!(err.contains("Unsupported SQL statement"));

    // 3. Update tuple assignment error
    let sql = "UPDATE t1 SET (id, val) = (1, 2);";
    let res = plan_sql(db.clone(), sql);
    assert!(res.is_err());
    let err = match res {
        Err(e) => e,
        _ => panic!("expected error"),
    };
    assert!(err.contains("Tuple assignments are not supported"));

    // 4. Update with non-table factor
    let sql = "UPDATE (SELECT * FROM t1) SET val = 1;";
    let res = plan_sql(db.clone(), sql);
    assert!(res.is_err());

    // 5. Delete with non-table factor
    let sql = "DELETE FROM (SELECT * FROM t1);";
    let res = plan_sql(db.clone(), sql);
    assert!(res.is_err());

    // 6. GROUP BY modifiers and ALL
    let sql = "SELECT val FROM t1 GROUP BY val ROLLUP;";
    let res = plan_sql(db.clone(), sql);
    assert!(res.is_err());

    let sql = "SELECT val FROM t1 GROUP BY ALL;";
    let res = plan_sql(db.clone(), sql);
    assert!(res.is_err());

    // 7. Wildcard in projection with aggregates
    let sql = "SELECT * FROM t1 GROUP BY val;";
    let res = plan_sql(db.clone(), sql);
    assert!(res.is_err());
    let err = match res {
        Err(e) => e,
        _ => panic!("expected error"),
    };
    assert!(err.contains("Wildcards not allowed in GROUP BY / Aggregations"));
}

#[test]
fn test_set_operations_except_minus() {
    let (_tmp, db) = setup_db();

    // EXCEPT
    let sql = "SELECT id FROM t1 EXCEPT SELECT id FROM t2;";
    let res = plan_sql(db.clone(), sql).unwrap();
    if let SqlStatement::Query(LogicalPlan::SetOp { op, all, .. }) = res {
        assert_eq!(op, dtdb_sql::logical::SetOpType::Except);
        assert!(!all);
    } else {
        panic!("expected SetOp query");
    }

    // EXCEPT ALL
    let sql2 = "SELECT id FROM t1 EXCEPT ALL SELECT id FROM t2;";
    let res2 = plan_sql(db.clone(), sql2).unwrap();
    if let SqlStatement::Query(LogicalPlan::SetOp { op, all, .. }) = res2 {
        assert_eq!(op, dtdb_sql::logical::SetOpType::Except);
        assert!(all);
    } else {
        panic!("expected SetOp query");
    }
}

#[test]
fn test_optimizer_cbo_row_estimations() {
    let (_tmp, db) = setup_db();

    // 1. Estimate Left Join and Cross Join
    let sql_join = "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id CROSS JOIN t2;";
    let plan = optimize_plan(db.clone(), sql_join);
    assert!(plan.is_ok());

    // 2. Estimate Distinct, Limit, Aggregates
    let sql_distinct = "SELECT DISTINCT val FROM t1;";
    let plan = optimize_plan(db.clone(), sql_distinct);
    assert!(plan.is_ok());

    let sql_limit = "SELECT id FROM t1 LIMIT 5;";
    let plan = optimize_plan(db.clone(), sql_limit);
    assert!(plan.is_ok());

    let sql_group = "SELECT val, COUNT(*) FROM t1 GROUP BY val;";
    let plan = optimize_plan(db.clone(), sql_group);
    assert!(plan.is_ok());

    // 3. Estimate SetOps (Union, Intersect, Except)
    let sql_union = "SELECT id FROM t1 UNION SELECT id FROM t2;";
    let plan = optimize_plan(db.clone(), sql_union);
    assert!(plan.is_ok());

    let sql_intersect = "SELECT id FROM t1 INTERSECT SELECT id FROM t2;";
    let plan = optimize_plan(db.clone(), sql_intersect);
    assert!(plan.is_ok());

    let sql_except = "SELECT id FROM t1 EXCEPT SELECT id FROM t2;";
    let plan = optimize_plan(db.clone(), sql_except);
    assert!(plan.is_ok());
}

#[test]
fn test_optimizer_boundary_ranges() {
    let (_tmp, db) = setup_db();

    // 1. Boundary range checks (empty boundaries id > i64::MAX and id < i64::MIN)
    let sql_gt_max = "SELECT id FROM t1 WHERE id > 9223372036854775807;";
    let plan_gt = optimize_plan(db.clone(), sql_gt_max);
    assert!(plan_gt.is_ok());

    let sql_lt_min = "SELECT id FROM t1 WHERE id < -9223372036854775808;";
    let plan_lt = optimize_plan(db.clone(), sql_lt_min);
    assert!(plan_lt.is_ok());

    // 2. String boundaries pushdown
    let sql_str_gt = "SELECT name FROM t_str WHERE name > 'abc';";
    let plan_str_gt = optimize_plan(db.clone(), sql_str_gt);
    assert!(plan_str_gt.is_ok());

    let sql_str_lt = "SELECT name FROM t_str WHERE name < 'abc';";
    let plan_str_lt = optimize_plan(db.clone(), sql_str_lt);
    assert!(plan_str_lt.is_ok());

    // 3. Secondary index unbounded / bounded range estimation
    let sql_idx_bounded =
        "SELECT val FROM t1 WHERE val >= -9223372036854775808 AND val <= 9223372036854775807;";
    let plan_idx = optimize_plan(db.clone(), sql_idx_bounded);
    assert!(plan_idx.is_ok());

    // Semi-bounded index range estimation
    let sql_idx_semi = "SELECT val FROM t1 WHERE val > 10;";
    let plan_semi = optimize_plan(db, sql_idx_semi);
    assert!(plan_semi.is_ok());
}

#[test]
fn test_optimizer_sort_elimination_and_index_scan_promotions() {
    let (_tmp, db) = setup_db();

    // 1. Sort elimination when already sorted (ORDER BY pk)
    let sql_sorted = "SELECT id FROM t1 ORDER BY id;";
    let plan = optimize_plan(db.clone(), sql_sorted).unwrap();
    // Check that there is no Sort node in the optimized plan because 'id' is already sorted by the Scan
    let plan_str = dtdb_sql::logical::format_logical_plan(&plan);
    assert!(
        !plan_str.contains("Sort"),
        "Plan contains Sort: {}",
        plan_str
    );

    // 2. Sort key propagation through Filter, Limit, Distinct
    let sql_filter = "SELECT id FROM t1 WHERE id > 5 ORDER BY id;";
    let plan_filter = optimize_plan(db.clone(), sql_filter).unwrap();
    let plan_str = dtdb_sql::logical::format_logical_plan(&plan_filter);
    assert!(
        !plan_str.contains("Sort"),
        "Plan contains Sort: {}",
        plan_str
    );

    let sql_limit = "SELECT id FROM t1 ORDER BY id LIMIT 10;";
    let plan_limit = optimize_plan(db.clone(), sql_limit).unwrap();
    let plan_str = dtdb_sql::logical::format_logical_plan(&plan_limit);
    assert!(
        !plan_str.contains("Sort"),
        "Plan contains Sort: {}",
        plan_str
    );

    let sql_distinct = "SELECT DISTINCT id FROM t1 ORDER BY id;";
    let plan_distinct = optimize_plan(db.clone(), sql_distinct).unwrap();
    let plan_str = dtdb_sql::logical::format_logical_plan(&plan_distinct);
    assert!(
        !plan_str.contains("Sort"),
        "Plan contains Sort: {}",
        plan_str
    );

    // 3. Projection propagation using direct sorting on primary key column
    let sql_alias = "SELECT id AS alias_id FROM t1 ORDER BY id;";
    let plan_alias = optimize_plan(db.clone(), sql_alias).unwrap();
    let plan_str = dtdb_sql::logical::format_logical_plan(&plan_alias);
    assert!(
        !plan_str.contains("Sort"),
        "Plan contains Sort: {}",
        plan_str
    );

    // 4. IndexScan promotion for Distinct, Limit, Filter, and Projection
    let sql_promo_distinct = "SELECT DISTINCT val FROM t1 ORDER BY val;";
    let plan_promo = optimize_plan(db.clone(), sql_promo_distinct).unwrap();
    let plan_str = dtdb_sql::logical::format_logical_plan(&plan_promo);
    assert!(
        plan_str.contains("IndexScan"),
        "Expected IndexScan: {}",
        plan_str
    );

    let sql_promo_limit = "SELECT val FROM t1 ORDER BY val LIMIT 5;";
    let plan_promo_lim = optimize_plan(db.clone(), sql_promo_limit).unwrap();
    let plan_str = dtdb_sql::logical::format_logical_plan(&plan_promo_lim);
    assert!(
        plan_str.contains("IndexScan"),
        "Expected IndexScan: {}",
        plan_str
    );

    let sql_promo_filter = "SELECT val FROM t1 WHERE val > 5 ORDER BY val;";
    let plan_promo_filt = optimize_plan(db.clone(), sql_promo_filter).unwrap();
    let plan_str = dtdb_sql::logical::format_logical_plan(&plan_promo_filt);
    assert!(
        plan_str.contains("IndexScan"),
        "Expected IndexScan: {}",
        plan_str
    );

    // 5. IndexScan promotion for Aggregate (Group By)
    let sql_promo_aggr = "SELECT val, COUNT(*) FROM t1 GROUP BY val;";
    let plan_promo_aggr = optimize_plan(db.clone(), sql_promo_aggr).unwrap();
    let plan_str = dtdb_sql::logical::format_logical_plan(&plan_promo_aggr);
    assert!(
        plan_str.contains("IndexScan"),
        "Expected IndexScan: {}",
        plan_str
    );

    // 6. Join swap condition reordering logic
    let sql_join_swap = "SELECT * FROM t1 JOIN t2 ON t2.id > t1.id;";
    let plan_join = optimize_plan(db, sql_join_swap).unwrap();
    assert!(matches!(plan_join, LogicalPlan::Projection { .. }));
}

#[test]
fn test_additional_planner_edge_cases() {
    let (_tmp, db) = setup_db();

    // 1. LIMIT & OFFSET expression errors (select level)
    assert!(plan_sql(db.clone(), "SELECT val FROM t1 LIMIT 'abc';").is_err());
    assert!(plan_sql(db.clone(), "SELECT val FROM t1 LIMIT val;").is_err());
    assert!(plan_sql(db.clone(), "SELECT val FROM t1 LIMIT 5 OFFSET 'abc';").is_err());
    assert!(plan_sql(db.clone(), "SELECT val FROM t1 LIMIT 5 OFFSET val;").is_err());

    // OffsetCommaLimit (offset, limit) errors and valid
    assert!(plan_sql(db.clone(), "SELECT val FROM t1 LIMIT 5, 10;").is_ok());
    assert!(plan_sql(db.clone(), "SELECT val FROM t1 LIMIT 5, 'abc';").is_err());
    assert!(plan_sql(db.clone(), "SELECT val FROM t1 LIMIT 'abc', 5;").is_err());

    // 2. LIMIT & OFFSET expression errors (query level - e.g. after UNION)
    assert!(
        plan_sql(
            db.clone(),
            "SELECT val FROM t1 UNION SELECT val FROM t1 LIMIT 'abc';"
        )
        .is_err()
    );
    assert!(
        plan_sql(
            db.clone(),
            "SELECT val FROM t1 UNION SELECT val FROM t1 LIMIT val;"
        )
        .is_err()
    );
    assert!(
        plan_sql(
            db.clone(),
            "SELECT val FROM t1 UNION SELECT val FROM t1 LIMIT 5 OFFSET 'abc';"
        )
        .is_err()
    );
    assert!(
        plan_sql(
            db.clone(),
            "SELECT val FROM t1 UNION SELECT val FROM t1 LIMIT 5 OFFSET val;"
        )
        .is_err()
    );
    assert!(
        plan_sql(
            db.clone(),
            "SELECT val FROM t1 UNION SELECT val FROM t1 LIMIT 5, 10;"
        )
        .is_ok()
    );
    assert!(
        plan_sql(
            db.clone(),
            "SELECT val FROM t1 UNION SELECT val FROM t1 LIMIT 5, 'abc';"
        )
        .is_err()
    );
    assert!(
        plan_sql(
            db.clone(),
            "SELECT val FROM t1 UNION SELECT val FROM t1 LIMIT 'abc', 5;"
        )
        .is_err()
    );

    // 3. Scalar context COUNT function error
    assert!(plan_sql(db.clone(), "SELECT val FROM t1 WHERE COUNT(val) > 1;").is_err());

    // 4. Unrecognized function
    assert!(plan_sql(db.clone(), "SELECT UNKNOWN_FUNC(val) FROM t1;").is_err());

    // 5. Match against multiple columns or non-string query value
    assert!(
        plan_sql(
            db.clone(),
            "SELECT val FROM t1 WHERE MATCH(id, val) AGAINST ('hello');"
        )
        .is_err()
    );
    assert!(
        plan_sql(
            db.clone(),
            "SELECT val FROM t1 WHERE MATCH(val) AGAINST (123);"
        )
        .is_err()
    );

    // 6. DROP database / view error, drop index not found
    assert!(plan_sql(db.clone(), "DROP VIEW t1;").is_err());
    assert!(plan_sql(db.clone(), "DROP INDEX idx_not_exists;").is_err());

    // 7. DROP INDEX valid case
    let res = plan_sql(db.clone(), "DROP INDEX t1_val_idx;");
    assert!(res.is_ok());
    if let Ok(SqlStatement::DropIndex {
        table_name,
        index_name,
    }) = res
    {
        assert_eq!(table_name, "t1");
        assert_eq!(index_name, "t1_val_idx");
    } else {
        panic!("Expected DropIndex statement");
    }

    // 8. Qualified Wildcard in GROUP BY projection
    assert!(plan_sql(db.clone(), "SELECT t1.* FROM t1 GROUP BY val;").is_err());

    // 9. INSERT edge cases: non-literal value inside INSERT values
    assert!(plan_sql(db.clone(), "INSERT INTO t1 (id, val) VALUES (1, val);").is_err());

    // 10. Negation of columns (-val) and plus unary (+val)
    assert!(plan_sql(db.clone(), "SELECT -val, +val FROM t1;").is_ok());

    // 11. BETWEEN/IN negation
    assert!(
        plan_sql(
            db.clone(),
            "SELECT val FROM t1 WHERE val NOT BETWEEN 1 AND 5;"
        )
        .is_ok()
    );
    assert!(plan_sql(db.clone(), "SELECT val FROM t1 WHERE val NOT IN (1, 2, 3);").is_ok());

    // 12. SetOperator::Minus
    let res = plan_sql(db.clone(), "SELECT id FROM t1 MINUS SELECT id FROM t2;");
    assert!(res.is_ok());
    if let Ok(SqlStatement::Query(LogicalPlan::SetOp { op, .. })) = res {
        assert_eq!(op, dtdb_sql::logical::SetOpType::Except);
    } else {
        panic!("Expected SetOp Minus");
    }

    // 13. Default expression edge cases
    assert!(plan_sql(db.clone(), "CREATE TABLE t_def (id INT DEFAULT 1.5);").is_ok());
    assert!(plan_sql(db.clone(), "CREATE TABLE t_def2 (id INT DEFAULT val);").is_err());

    // 14. Having clause rewrites and case rewrites with aggregates
    assert!(
        plan_sql(
            db.clone(),
            "SELECT COALESCE(SUM(val), 0) FROM t1 GROUP BY id;"
        )
        .is_ok()
    );
    assert!(plan_sql(db.clone(), "SELECT -(SUM(val)) FROM t1 GROUP BY id;").is_ok());
    assert!(
        plan_sql(
            db.clone(),
            "SELECT CASE WHEN val > 5 THEN SUM(id) ELSE 0 END FROM t1 GROUP BY val;"
        )
        .is_ok()
    );

    // 15. Create Table custom boolean column
    assert!(plan_sql(db.clone(), "CREATE TABLE t_bool (id INT, active BOOL);").is_ok());
}

#[test]
fn test_additional_optimizer_edge_cases() {
    let (_tmp, db) = setup_db();
    let optimizer = Optimizer::new(db.clone());

    // 1. CBO estimation traverse paths (Union, Intersect, Except inside 3-way join)
    let t1_scan = LogicalPlan::Scan {
        table_name: "t1".to_string(),
        schema: db.get_table("t1").unwrap().schema.clone(),
        range: None,
    };
    let t2_scan = LogicalPlan::Scan {
        table_name: "t2".to_string(),
        schema: db.get_table("t2").unwrap().schema.clone(),
        range: None,
    };

    let distinct_plan = LogicalPlan::Distinct {
        source: Box::new(t1_scan.clone()),
    };

    let aggr_plan = LogicalPlan::Aggregate {
        source: Box::new(t1_scan.clone()),
        group_by: vec![Expr::Column("t1.val".to_string(), None)],
        aggrs: vec![],
        field_names: vec!["t1.val".to_string()],
        schema: db.get_table("t1").unwrap().schema.clone(),
    };

    let setop_plan = LogicalPlan::SetOp {
        left: Box::new(t1_scan.clone()),
        right: Box::new(t2_scan.clone()),
        op: dtdb_sql::logical::SetOpType::Union,
        all: false,
    };

    let proj_plan = LogicalPlan::Projection {
        source: Box::new(t1_scan.clone()),
        expressions: vec![Expr::Column("t1.id".to_string(), None)],
        field_names: vec!["id".to_string()],
        schema: db.get_table("t1").unwrap().schema.clone(),
    };

    let join_plan = LogicalPlan::new_join(
        distinct_plan,
        aggr_plan,
        Expr::BinaryOp {
            left: Box::new(Expr::Column("t1.id".to_string(), None)),
            op: Operator::Eq,
            right: Box::new(Expr::Column("t1.val".to_string(), None)),
        },
        JoinType::Inner,
    );
    let optimized_join = optimizer.optimize(join_plan);
    assert!(matches!(
        optimized_join,
        LogicalPlan::Join { .. } | LogicalPlan::Projection { .. }
    ));

    let join_plan2 = LogicalPlan::new_join(
        setop_plan.clone(),
        proj_plan,
        Expr::BinaryOp {
            left: Box::new(Expr::Column("t1.id".to_string(), None)),
            op: Operator::Eq,
            right: Box::new(Expr::Column("t1.id".to_string(), None)),
        },
        JoinType::Inner,
    );
    let optimized_join2 = optimizer.optimize(join_plan2);
    assert!(matches!(
        optimized_join2,
        LogicalPlan::Join { .. } | LogicalPlan::Projection { .. }
    ));

    // 2. PK Range unbounded min & max check inside estimate_scan_cost (line 616)
    let t1_schema = db.get_table("t1").unwrap().schema.clone();
    let mut qualified_cols = t1_schema.columns.clone();
    for col in &mut qualified_cols {
        col.name = format!("t1.{}", col.name);
    }
    let mut scan_schema = dtdb_relational::Schema::new_with_options(
        qualified_cols,
        t1_schema.locality_group_options.clone(),
    );
    scan_schema.indexes = t1_schema.indexes.clone();

    let pk_unbounded_scan = LogicalPlan::Scan {
        table_name: "t1".to_string(),
        schema: scan_schema,
        range: None,
    };
    let pk_unbounded_filter = LogicalPlan::Filter {
        source: Box::new(pk_unbounded_scan),
        predicate: Expr::BinaryOp {
            left: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("t1.id".to_string(), None)),
                op: Operator::GtEq,
                right: Box::new(Expr::Literal(DbValue::Int(i64::MIN))),
            }),
            op: Operator::And,
            right: Box::new(Expr::BinaryOp {
                left: Box::new(Expr::Column("t1.id".to_string(), None)),
                op: Operator::LtEq,
                right: Box::new(Expr::Literal(DbValue::Int(i64::MAX))),
            }),
        },
    };
    let optimized_unbounded = optimizer.optimize(pk_unbounded_filter);
    assert!(matches!(
        optimized_unbounded,
        LogicalPlan::Filter { .. } | LogicalPlan::Scan { .. }
    ));

    // 3. remaining_conjuncts filter node above Join (lines 72-75)
    let sql_join_remaining =
        "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.val > 5 AND t1.id + t2.id > 10;";
    let plan_remaining = optimize_plan(db.clone(), sql_join_remaining);
    assert!(plan_remaining.is_ok());

    // 4. Predicate pushdown above Limit (lines 141-144)
    let limit_scan = LogicalPlan::Scan {
        table_name: "t1".to_string(),
        schema: t1_schema.clone(),
        range: None,
    };
    let limit_plan = LogicalPlan::Limit {
        source: Box::new(limit_scan),
        limit: Some(10),
        offset: 0,
    };
    let limit_filter = LogicalPlan::Filter {
        source: Box::new(limit_plan),
        predicate: Expr::BinaryOp {
            left: Box::new(Expr::Column("t1.val".to_string(), None)),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(DbValue::Int(5))),
        },
    };
    let optimized_limit_filter = optimizer.optimize(limit_filter);
    assert!(matches!(optimized_limit_filter, LogicalPlan::Filter { .. }));

    // 5. Predicate pushdown above SetOp (lines 164-167)
    let setop_filter_plan = LogicalPlan::Filter {
        source: Box::new(setop_plan.clone()),
        predicate: Expr::BinaryOp {
            left: Box::new(Expr::Column("t1.id".to_string(), None)),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(DbValue::Int(5))),
        },
    };
    let optimized_setop_filter = optimizer.optimize(setop_filter_plan);
    assert!(matches!(optimized_setop_filter, LogicalPlan::Filter { .. }));

    // 6. Optimizer stats_opt is None check (lines 604-606 & 679-681)
    let schema_fake = dtdb_relational::Schema::new(vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_nullable: false,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            name: "val".to_string(),
            data_type: DataType::Int,
            is_primary_key: false,
            is_nullable: true,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
    ]);
    let fake_scan = LogicalPlan::Scan {
        table_name: "fake_table".to_string(),
        schema: schema_fake.clone(),
        range: None,
    };
    let filter_plan = LogicalPlan::Filter {
        source: Box::new(fake_scan),
        predicate: Expr::BinaryOp {
            left: Box::new(Expr::Column("val".to_string(), None)),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(DbValue::Int(5))),
        },
    };
    let optimized = optimizer.optimize(filter_plan);
    assert!(matches!(optimized, LogicalPlan::Filter { .. }));

    // 7. FullTextScan cost estimation with stats_opt = None
    let mut schema_fts = schema_fake.clone();
    schema_fts.indexes.push(dtdb_relational::IndexDefinition {
        name: "fts_idx".to_string(),
        columns: vec!["val_str".to_string()],
        index_type: IndexType::FullText,
        tokenizer: None,
    });
    schema_fts.columns.push(Column {
        name: "val_str".to_string(),
        data_type: DataType::String,
        is_primary_key: false,
        is_nullable: true,
        locality_group: None,
        default_value: None,
        is_auto_increment: false,
    });
    let fake_fts_scan = LogicalPlan::Scan {
        table_name: "fake_table_fts".to_string(),
        schema: schema_fts.clone(),
        range: None,
    };
    let fts_filter = LogicalPlan::Filter {
        source: Box::new(fake_fts_scan),
        predicate: Expr::Match {
            column: "val_str".to_string(),
            index: None,
            query_str: "hello".to_string(),
        },
    };
    let optimized_fts = optimizer.optimize(fts_filter);
    assert!(matches!(
        optimized_fts,
        LogicalPlan::Filter { .. } | LogicalPlan::FullTextScan { .. }
    ));
}

#[test]
fn test_pk_equality_pushdown_without_statistics() {
    // `setup_db` creates t1 with no rows and never runs ANALYZE, so the table
    // has no statistics (row_count == 0). A primary-key equality must still be
    // lowered to a single-key point-range scan rather than a full table scan.
    //
    // Regression: a zero row_count made the point-lookup selectivity
    // `1.0 / row_count.max(1)` collapse to 1.0, so the point scan was costed
    // identically to a full scan and the strict `<` tie-break kept the full
    // scan — turning every `WHERE pk = const` into a full table scan on
    // un-analyzed tables.
    let (_tmp, db) = setup_db();
    let plan = optimize_plan(db, "SELECT val FROM t1 WHERE id = 7").unwrap();
    let rendered = dtdb_sql::format_logical_plan(&plan);

    assert!(
        rendered.contains("range=[Int(7), Int(7)]"),
        "PK equality on an un-analyzed table should push down a point range.\nPlan:\n{rendered}"
    );
    assert!(
        !rendered.contains("range=all"),
        "PK equality must not fall back to a full scan.\nPlan:\n{rendered}"
    );
}

#[test]
fn test_placeholder_survives_planning_as_parameter() {
    // Without binding, a `:name` placeholder must reach the logical plan as
    // Expr::Parameter — not a literal, not a column. This is the foundation for
    // caching an optimized plan and substituting bound values at execution
    // time (rather than binding into the AST before planning, as today).
    let (_tmp, db) = setup_db();

    let stmt = plan_sql(db.clone(), "SELECT val FROM t1 WHERE id = :id").unwrap();
    let plan = match stmt {
        SqlStatement::Query(p) => p,
        _ => panic!("expected a Query plan"),
    };
    let rendered = format!("{plan:?}");
    assert!(
        rendered.contains(r#"Parameter("id")"#),
        "':id' should plan to Expr::Parameter; got plan: {rendered}"
    );

    // `@id` is an equivalent placeholder spelling and must behave identically.
    let stmt2 = plan_sql(db, "SELECT val FROM t1 WHERE id = @id").unwrap();
    let plan2 = match stmt2 {
        SqlStatement::Query(p) => p,
        _ => panic!("expected a Query plan"),
    };
    assert!(
        format!("{plan2:?}").contains(r#"Parameter("id")"#),
        "'@id' should plan to Expr::Parameter"
    );
}
