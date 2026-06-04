use dtdb_relational::{Column, DataType, Database, IndexDefinition, Row, Schema, Transaction};
use dtdb_storage::{DbKey, DbValue};
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn test_scan_iter_merge_coverage() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());

    let schema = Schema::new(vec![
        Column {
            id: 0,
            name: "id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_nullable: false,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            id: 0,
            name: "val".to_string(),
            data_type: DataType::Int,
            is_primary_key: false,
            is_nullable: true,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
    ]);
    db.create_table("t", schema.clone()).unwrap();

    let tx1 = Transaction::new(1, db.clone());
    tx1.put(
        "t",
        DbKey::Int(10),
        Row::new(vec![DbValue::Int(10), DbValue::Int(100)]),
    )
    .unwrap();
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    tx2.put(
        "t",
        DbKey::Int(5),
        Row::new(vec![DbValue::Int(5), DbValue::Int(50)]),
    )
    .unwrap();
    tx2.put(
        "t",
        DbKey::Int(10),
        Row::new(vec![DbValue::Int(10), DbValue::Int(105)]),
    )
    .unwrap();

    let mut it = tx2
        .scan_iter("t", &DbKey::Int(1), &DbKey::Int(20), None)
        .unwrap();

    let r1 = it.next().unwrap().unwrap();
    assert_eq!(r1.values[0], DbValue::Int(5));
    assert_eq!(r1.values[1], DbValue::Int(50));

    let r2 = it.next().unwrap().unwrap();
    assert_eq!(r2.values[0], DbValue::Int(10));
    assert_eq!(r2.values[1], DbValue::Int(105));

    assert!(it.next().unwrap().is_none());

    let tx3 = Transaction::new(3, db.clone());
    tx3.delete("t", DbKey::Int(10)).unwrap();
    let mut it3 = tx3
        .scan_iter("t", &DbKey::Int(1), &DbKey::Int(20), None)
        .unwrap();
    assert!(it3.next().unwrap().is_none());
}

#[test]
fn test_index_scan_write_buffer_coverage() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());

    let mut schema = Schema::new(vec![
        Column {
            id: 0,
            name: "id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_nullable: false,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            id: 0,
            name: "age".to_string(),
            data_type: DataType::Int,
            is_primary_key: false,
            is_nullable: true,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            id: 0,
            name: "score".to_string(),
            data_type: DataType::Int,
            is_primary_key: false,
            is_nullable: true,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
    ]);
    schema.indexes.push(IndexDefinition {
        name: "idx_age_score".to_string(),
        columns: vec!["age".to_string(), "score".to_string()],
        index_type: dtdb_relational::IndexType::BTree,
        tokenizer: None,
    });
    db.create_table("users", schema).unwrap();

    let tx1 = Transaction::new(1, db.clone());
    let err = tx1
        .index_scan(
            "users",
            "non_existent_idx",
            &DbKey::Int(10),
            &DbKey::Int(20),
            None,
        )
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("Index 'non_existent_idx' not found")
    );

    tx1.put(
        "users",
        DbKey::Int(1),
        Row::new(vec![DbValue::Int(1), DbValue::Int(25), DbValue::Int(100)]),
    )
    .unwrap();
    tx1.put(
        "users",
        DbKey::Int(2),
        Row::new(vec![DbValue::Int(2), DbValue::Int(30), DbValue::Int(90)]),
    )
    .unwrap();
    tx1.put(
        "users",
        DbKey::Int(3),
        Row::new(vec![DbValue::Int(3), DbValue::Null, DbValue::Int(80)]),
    )
    .unwrap();
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    let res = tx2
        .index_scan(
            "users",
            "idx_age_score",
            &DbKey::Int(20),
            &DbKey::Int(27),
            None,
        )
        .unwrap();
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].values[0], DbValue::Int(1));

    let comp_start = DbKey::composite(vec![DbKey::Int(30), DbKey::Int(90)]);
    let comp_end = DbKey::composite(vec![DbKey::Int(30), DbKey::Int(90)]);
    let res2 = tx2
        .index_scan("users", "idx_age_score", &comp_start, &comp_end, None)
        .unwrap();
    assert_eq!(res2.len(), 1);
    assert_eq!(res2[0].values[0], DbValue::Int(2));
}

#[test]
fn test_fts_scan_write_buffer_coverage() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());

    let mut schema = Schema::new(vec![
        Column {
            id: 0,
            name: "id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_nullable: false,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            id: 0,
            name: "doc".to_string(),
            data_type: DataType::String,
            is_primary_key: false,
            is_nullable: true,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
    ]);
    schema.indexes.push(IndexDefinition {
        name: "idx_fts".to_string(),
        columns: vec!["doc".to_string()],
        index_type: dtdb_relational::IndexType::FullText,
        tokenizer: Some("simple".to_string()),
    });
    db.create_table("docs", schema).unwrap();

    let tx = Transaction::new(1, db.clone());
    tx.put(
        "docs",
        DbKey::Int(1),
        Row::new(vec![
            DbValue::Int(1),
            DbValue::string("Rust is fast and safe"),
        ]),
    )
    .unwrap();
    tx.put(
        "docs",
        DbKey::Int(2),
        Row::new(vec![
            DbValue::Int(2),
            DbValue::string("Go is simple but fast"),
        ]),
    )
    .unwrap();

    let res_and = tx
        .fulltext_scan("docs", "idx_fts", "rust AND safe", None)
        .unwrap();
    assert_eq!(res_and.len(), 1);
    assert_eq!(res_and[0].values[0], DbValue::Int(1));

    let res_or = tx
        .fulltext_scan("docs", "idx_fts", "safe OR simple", None)
        .unwrap();
    assert_eq!(res_or.len(), 2);

    let res_phrase = tx
        .fulltext_scan("docs", "idx_fts", "\"is simple\"", None)
        .unwrap();
    assert_eq!(res_phrase.len(), 1);
    assert_eq!(res_phrase[0].values[0], DbValue::Int(2));

    let tx_commit = Transaction::new(2, db.clone());
    tx_commit
        .put(
            "docs",
            DbKey::Int(10),
            Row::new(vec![
                DbValue::Int(10),
                DbValue::string("Database engine implementation details"),
            ]),
        )
        .unwrap();
    tx_commit
        .put(
            "docs",
            DbKey::Int(20),
            Row::new(vec![
                DbValue::Int(20),
                DbValue::string("Transaction logs and OCC validation"),
            ]),
        )
        .unwrap();
    tx_commit.commit().unwrap();

    let tx_scan = Transaction::new(3, db.clone());
    let res_and_comm = tx_scan
        .fulltext_scan("docs", "idx_fts", "database AND details", None)
        .unwrap();
    assert_eq!(res_and_comm.len(), 1);
    assert_eq!(res_and_comm[0].values[0], DbValue::Int(10));

    let res_or_comm = tx_scan
        .fulltext_scan("docs", "idx_fts", "details OR validation", None)
        .unwrap();
    assert_eq!(res_or_comm.len(), 2);

    let res_phrase_comm = tx_scan
        .fulltext_scan("docs", "idx_fts", "\"occ validation\"", None)
        .unwrap();
    assert_eq!(res_phrase_comm.len(), 1);
    assert_eq!(res_phrase_comm[0].values[0], DbValue::Int(20));

    let res_phrase_single = tx_scan
        .fulltext_scan("docs", "idx_fts", "\"transaction\"", None)
        .unwrap();
    assert_eq!(res_phrase_single.len(), 1);
    assert_eq!(res_phrase_single[0].values[0], DbValue::Int(20));
}

#[test]
fn test_occ_index_maintenance_and_repeatable_read() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());

    let mut schema = Schema::new(vec![
        Column {
            id: 0,
            name: "id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_nullable: false,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            id: 0,
            name: "score".to_string(),
            data_type: DataType::Int,
            is_primary_key: false,
            is_nullable: true,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
    ]);
    schema.indexes.push(IndexDefinition {
        name: "idx_score".to_string(),
        columns: vec!["score".to_string()],
        index_type: dtdb_relational::IndexType::BTree,
        tokenizer: None,
    });
    db.create_table("t", schema).unwrap();

    let tx1 = Transaction::new(1, db.clone());
    tx1.put(
        "t",
        DbKey::Int(1),
        Row::new(vec![DbValue::Int(1), DbValue::Int(100)]),
    )
    .unwrap();
    tx1.put(
        "t",
        DbKey::Int(2),
        Row::new(vec![DbValue::Int(2), DbValue::Int(200)]),
    )
    .unwrap();
    tx1.commit().unwrap();

    let tx_rr = Transaction::new_with_isolation(
        2,
        db.clone(),
        dtdb_relational::IsolationLevel::RepeatableRead,
    );
    let res = tx_rr
        .multi_get_projected("t", &[DbKey::Int(1)], None)
        .unwrap();
    assert_eq!(res.len(), 1);

    tx_rr
        .put(
            "t",
            DbKey::Int(3),
            Row::new(vec![DbValue::Int(3), DbValue::Int(300)]),
        )
        .unwrap();
    let res2 = tx_rr
        .multi_get_projected("t", &[DbKey::Int(3)], None)
        .unwrap();
    assert_eq!(res2.len(), 1);

    let tx3 = Transaction::new(3, db.clone());
    tx3.put(
        "t",
        DbKey::Int(1),
        Row::new(vec![DbValue::Int(1), DbValue::Int(105)]),
    )
    .unwrap();
    tx3.delete("t", DbKey::Int(2)).unwrap();
    tx3.commit().unwrap();

    let tx_rr_scan = Transaction::new_with_isolation(
        4,
        db.clone(),
        dtdb_relational::IsolationLevel::RepeatableRead,
    );
    let mut it = tx_rr_scan
        .scan_iter("t", &DbKey::Int(1), &DbKey::Int(10), None)
        .unwrap();
    let _ = it.next().unwrap();
    let _ = it.next().unwrap();
}
