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

fn int_col(name: &str, pk: bool, nullable: bool) -> Column {
    Column {
        id: 0,
        name: name.to_string(),
        data_type: DataType::Int,
        is_primary_key: pk,
        is_nullable: nullable,
        locality_group: None,
        default_value: None,
        is_auto_increment: false,
    }
}

fn string_col(name: &str, nullable: bool) -> Column {
    Column {
        id: 0,
        name: name.to_string(),
        data_type: DataType::String,
        is_primary_key: false,
        is_nullable: nullable,
        locality_group: None,
        default_value: None,
        is_auto_increment: false,
    }
}

fn bool_col(name: &str, nullable: bool) -> Column {
    Column {
        id: 0,
        name: name.to_string(),
        data_type: DataType::Bool,
        is_primary_key: false,
        is_nullable: nullable,
        locality_group: None,
        default_value: None,
        is_auto_increment: false,
    }
}

/// Exercises full-text inverted-index maintenance when a previously committed,
/// FTS-indexed row is updated and then deleted. Both paths run read-before-write
/// during commit: the update must purge the old document's tokens before adding
/// the new ones, and the delete must purge them entirely.
#[test]
fn test_fts_index_maintenance_on_update_and_delete() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());

    let mut schema = Schema::new(vec![int_col("id", true, false), string_col("doc", true)]);
    schema.indexes.push(IndexDefinition {
        name: "idx_fts".to_string(),
        columns: vec!["doc".to_string()],
        index_type: dtdb_relational::IndexType::FullText,
        tokenizer: Some("simple".to_string()),
    });
    db.create_table("docs", schema).unwrap();

    // Commit an initial document.
    let tx1 = Transaction::new(1, db.clone());
    tx1.put(
        "docs",
        DbKey::Int(1),
        Row::new(vec![DbValue::Int(1), DbValue::string("hello world")]),
    )
    .unwrap();
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    assert_eq!(
        tx2.fulltext_scan("docs", "idx_fts", "hello", None)
            .unwrap()
            .len(),
        1
    );

    // Update the row to a new document. The old tokens (hello, world) must be
    // removed from the inverted index, and the new ones (goodbye, moon) added.
    let tx3 = Transaction::new(3, db.clone());
    tx3.put(
        "docs",
        DbKey::Int(1),
        Row::new(vec![DbValue::Int(1), DbValue::string("goodbye moon")]),
    )
    .unwrap();
    tx3.commit().unwrap();

    let tx4 = Transaction::new(4, db.clone());
    assert!(
        tx4.fulltext_scan("docs", "idx_fts", "hello", None)
            .unwrap()
            .is_empty(),
        "stale token 'hello' should have been purged on update"
    );
    assert!(
        tx4.fulltext_scan("docs", "idx_fts", "world", None)
            .unwrap()
            .is_empty(),
        "stale token 'world' should have been purged on update"
    );
    assert_eq!(
        tx4.fulltext_scan("docs", "idx_fts", "goodbye", None)
            .unwrap()
            .len(),
        1
    );

    // Delete the row. All of its tokens must be removed from the index.
    let tx5 = Transaction::new(5, db.clone());
    tx5.delete("docs", DbKey::Int(1)).unwrap();
    tx5.commit().unwrap();

    let tx6 = Transaction::new(6, db.clone());
    assert!(
        tx6.fulltext_scan("docs", "idx_fts", "goodbye", None)
            .unwrap()
            .is_empty(),
        "tokens should have been purged on delete"
    );
    assert!(
        tx6.fulltext_scan("docs", "idx_fts", "moon", None)
            .unwrap()
            .is_empty()
    );
}

/// `index_scan` must surface rows that live only in the transaction's own
/// uncommitted write buffer (read-your-own-writes), honoring the scan range,
/// skipping rows whose indexed columns are NULL, and returning results sorted by
/// the composite index key.
#[test]
fn test_index_scan_reads_uncommitted_composite_write_buffer() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());

    let mut schema = Schema::new(vec![
        int_col("id", true, false),
        int_col("age", false, true),
        int_col("score", false, true),
    ]);
    schema.indexes.push(IndexDefinition {
        name: "idx_age_score".to_string(),
        columns: vec!["age".to_string(), "score".to_string()],
        index_type: dtdb_relational::IndexType::BTree,
        tokenizer: None,
    });
    db.create_table("users", schema).unwrap();

    // All writes stay buffered (no commit) so the index engine is empty and the
    // scan must fall back to the write buffer for every match.
    let tx = Transaction::new(1, db.clone());
    tx.put(
        "users",
        DbKey::Int(1),
        Row::new(vec![DbValue::Int(1), DbValue::Int(25), DbValue::Int(100)]),
    )
    .unwrap();
    tx.put(
        "users",
        DbKey::Int(2),
        Row::new(vec![DbValue::Int(2), DbValue::Int(30), DbValue::Int(90)]),
    )
    .unwrap();
    // NULL in an indexed column: get_index_keys would not emit a key, so the scan
    // must skip this row.
    tx.put(
        "users",
        DbKey::Int(3),
        Row::new(vec![DbValue::Int(3), DbValue::Null, DbValue::Int(80)]),
    )
    .unwrap();
    tx.put(
        "users",
        DbKey::Int(4),
        Row::new(vec![DbValue::Int(4), DbValue::Int(25), DbValue::Int(50)]),
    )
    .unwrap();

    let res = tx
        .index_scan(
            "users",
            "idx_age_score",
            &DbKey::Int(20),
            &DbKey::Int(27),
            None,
        )
        .unwrap();
    // age=30 (id 2) is out of range; age=NULL (id 3) is excluded. Remaining rows
    // are sorted by (age, score): (25,50) then (25,100).
    let ids: Vec<_> = res.iter().map(|r| r.values[0].clone()).collect();
    assert_eq!(ids, vec![DbValue::Int(4), DbValue::Int(1)]);
}

/// Single-column index variant of the uncommitted write-buffer scan, covering the
/// single-key (non-composite) branch of the index-key extraction used for sorting.
#[test]
fn test_index_scan_reads_uncommitted_single_column_write_buffer() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());

    let mut schema = Schema::new(vec![
        int_col("id", true, false),
        int_col("val", false, true),
    ]);
    schema.indexes.push(IndexDefinition {
        name: "idx_val".to_string(),
        columns: vec!["val".to_string()],
        index_type: dtdb_relational::IndexType::BTree,
        tokenizer: None,
    });
    db.create_table("t", schema).unwrap();

    let tx = Transaction::new(1, db.clone());
    tx.put(
        "t",
        DbKey::Int(1),
        Row::new(vec![DbValue::Int(1), DbValue::Int(30)]),
    )
    .unwrap();
    tx.put(
        "t",
        DbKey::Int(2),
        Row::new(vec![DbValue::Int(2), DbValue::Int(10)]),
    )
    .unwrap();
    tx.put(
        "t",
        DbKey::Int(3),
        Row::new(vec![DbValue::Int(3), DbValue::Int(20)]),
    )
    .unwrap();

    let res = tx
        .index_scan("t", "idx_val", &DbKey::Int(5), &DbKey::Int(25), None)
        .unwrap();
    // val=30 is out of range; remaining sorted by val: 10 then 20.
    let ids: Vec<_> = res.iter().map(|r| r.values[0].clone()).collect();
    assert_eq!(ids, vec![DbValue::Int(2), DbValue::Int(3)]);
}

/// String-keyed variant of the uncommitted write-buffer scan. Besides covering
/// the `DbValue::String` arm of the write-buffer bound check, this exercises the
/// secondary sort tie-break: two buffered rows share the same indexed value, so
/// they must come back ordered by primary key.
#[test]
fn test_index_scan_write_buffer_string_index_pk_tiebreak() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());

    let mut schema = Schema::new(vec![int_col("id", true, false), string_col("name", true)]);
    schema.indexes.push(IndexDefinition {
        name: "idx_name".to_string(),
        columns: vec!["name".to_string()],
        index_type: dtdb_relational::IndexType::BTree,
        tokenizer: None,
    });
    db.create_table("t", schema).unwrap();

    // All rows stay buffered (no commit) so the scan must resolve them from the
    // write buffer. ids 1 and 3 share name "alice" to drive the PK tie-break.
    let tx = Transaction::new(1, db.clone());
    tx.put(
        "t",
        DbKey::Int(3),
        Row::new(vec![DbValue::Int(3), DbValue::string("alice")]),
    )
    .unwrap();
    tx.put(
        "t",
        DbKey::Int(1),
        Row::new(vec![DbValue::Int(1), DbValue::string("alice")]),
    )
    .unwrap();
    tx.put(
        "t",
        DbKey::Int(2),
        Row::new(vec![DbValue::Int(2), DbValue::string("bob")]),
    )
    .unwrap();
    // NULL indexed column: skipped, like the integer-keyed cases above.
    tx.put(
        "t",
        DbKey::Int(4),
        Row::new(vec![DbValue::Int(4), DbValue::Null]),
    )
    .unwrap();

    let res = tx
        .index_scan(
            "t",
            "idx_name",
            &DbKey::string("alice"),
            &DbKey::string("bob"),
            None,
        )
        .unwrap();
    // Sorted by name, then by PK for the "alice" tie: (alice,1), (alice,3), (bob,2).
    let ids: Vec<_> = res.iter().map(|r| r.values[0].clone()).collect();
    assert_eq!(ids, vec![DbValue::Int(1), DbValue::Int(3), DbValue::Int(2)]);
}

/// Bool-keyed variant of the uncommitted write-buffer scan, covering the
/// `DbValue::Bool` arm of the write-buffer bound check.
#[test]
fn test_index_scan_write_buffer_bool_index() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());

    let mut schema = Schema::new(vec![int_col("id", true, false), bool_col("active", true)]);
    schema.indexes.push(IndexDefinition {
        name: "idx_active".to_string(),
        columns: vec!["active".to_string()],
        index_type: dtdb_relational::IndexType::BTree,
        tokenizer: None,
    });
    db.create_table("t", schema).unwrap();

    let tx = Transaction::new(1, db.clone());
    tx.put(
        "t",
        DbKey::Int(1),
        Row::new(vec![DbValue::Int(1), DbValue::Bool(true)]),
    )
    .unwrap();
    tx.put(
        "t",
        DbKey::Int(2),
        Row::new(vec![DbValue::Int(2), DbValue::Bool(false)]),
    )
    .unwrap();
    tx.put(
        "t",
        DbKey::Int(3),
        Row::new(vec![DbValue::Int(3), DbValue::Bool(true)]),
    )
    .unwrap();

    let res = tx
        .index_scan(
            "t",
            "idx_active",
            &DbKey::Bool(false),
            &DbKey::Bool(true),
            None,
        )
        .unwrap();
    // Sorted by active (false < true), then PK: (false,2), (true,1), (true,3).
    let ids: Vec<_> = res.iter().map(|r| r.values[0].clone()).collect();
    assert_eq!(ids, vec![DbValue::Int(2), DbValue::Int(1), DbValue::Int(3)]);
}

fn fts_docs_db() -> (Arc<Database>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());
    let mut schema = Schema::new(vec![int_col("id", true, false), string_col("body", true)]);
    schema.indexes.push(IndexDefinition {
        name: "idx_body".to_string(),
        columns: vec!["body".to_string()],
        index_type: dtdb_relational::IndexType::FullText,
        tokenizer: Some("simple".to_string()),
    });
    db.create_table("docs", schema).unwrap();
    (db, temp_dir)
}

fn put_doc(tx: &Transaction, id: i64, body: DbValue) {
    tx.put(
        "docs",
        DbKey::Int(id),
        Row::new(vec![DbValue::Int(id), body]),
    )
    .unwrap();
}

fn scan_ids(tx: &Transaction, query: &str) -> Vec<i64> {
    let mut ids: Vec<i64> = tx
        .fulltext_scan("docs", "idx_body", query, None)
        .unwrap()
        .iter()
        .map(|r| match r.values[0] {
            DbValue::Int(v) => v,
            _ => panic!("unexpected pk type"),
        })
        .collect();
    ids.sort();
    ids
}

/// Multi-token phrase queries against committed data must use the inverted
/// index's positional postings: a phrase matches only when its tokens occur at
/// consecutive positions, so a document that contains all the tokens but not
/// adjacently is rejected.
#[test]
fn test_fulltext_phrase_positional_matching_committed() {
    let (db, _temp_dir) = fts_docs_db();

    let tx1 = Transaction::new(1, db.clone());
    // doc 1: "alpha beta" are adjacent. doc 2: "alpha" and "beta" both present
    // but separated by "gamma".
    put_doc(&tx1, 1, DbValue::string("alpha beta delta"));
    put_doc(&tx1, 2, DbValue::string("alpha gamma beta"));
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    // Adjacent in doc 1 only.
    assert_eq!(scan_ids(&tx2, "\"alpha beta\""), vec![1]);
    // Present in both docs but never adjacent -> no phrase match anywhere.
    assert_eq!(scan_ids(&tx2, "\"beta alpha\""), Vec::<i64>::new());
    // Single-token queries still resolve via the index for both docs.
    assert_eq!(scan_ids(&tx2, "alpha"), vec![1, 2]);
}

/// `fulltext_scan` must evaluate the query against the transaction's own
/// uncommitted write-buffer rows (read-your-own-writes), covering token, phrase,
/// too-short-phrase, and non-string-column cases.
#[test]
fn test_fulltext_scan_evaluates_uncommitted_write_buffer() {
    let (db, _temp_dir) = fts_docs_db();

    let tx = Transaction::new(1, db.clone());
    put_doc(&tx, 10, DbValue::string("red green blue"));
    put_doc(&tx, 11, DbValue::Null); // non-string indexed column
    put_doc(&tx, 12, DbValue::string("green"));

    // Token match across two buffered rows; the NULL-bodied row is skipped.
    assert_eq!(scan_ids(&tx, "green"), vec![10, 12]);
    // Adjacent phrase matches doc 10.
    assert_eq!(scan_ids(&tx, "\"red green\""), vec![10]);
    // Tokens present but not adjacent -> no match.
    assert_eq!(scan_ids(&tx, "\"red blue\""), Vec::<i64>::new());
    // Phrase longer than any buffered document's token count -> no match.
    assert_eq!(scan_ids(&tx, "\"red green blue extra\""), Vec::<i64>::new());
}

/// When the transaction has a non-empty write buffer, `fulltext_scan` re-checks
/// index-resolved committed rows against that buffer: an in-place update of a
/// matching row must surface the buffered version rather than the stored one.
#[test]
fn test_fulltext_scan_retains_buffered_committed_rows() {
    let (db, _temp_dir) = fts_docs_db();

    let tx1 = Transaction::new(1, db.clone());
    put_doc(&tx1, 1, DbValue::string("rust safe"));
    put_doc(&tx1, 2, DbValue::string("rust fast"));
    tx1.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    // Update doc 1 in place (still contains "rust"); this populates the write
    // buffer so the retain pass runs over the index-resolved committed rows.
    put_doc(&tx2, 1, DbValue::string("rust safe and sound"));

    let res = tx2.fulltext_scan("docs", "idx_body", "rust", None).unwrap();
    let mut by_id: Vec<(i64, String)> = res
        .iter()
        .map(|r| {
            let id = match r.values[0] {
                DbValue::Int(v) => v,
                _ => panic!(),
            };
            let body = match &r.values[1] {
                DbValue::String(s) => s.to_string(),
                _ => panic!(),
            };
            (id, body)
        })
        .collect();
    by_id.sort();
    assert_eq!(
        by_id,
        vec![
            (1, "rust safe and sound".to_string()),
            (2, "rust fast".to_string()),
        ]
    );
}

/// `create_index` must build a full-text inverted index from rows that already
/// exist in the table, tokenizing each indexed string column and emitting one
/// posting per token. After creation the index is immediately queryable.
#[test]
fn test_create_fulltext_index_populates_from_existing_rows() {
    use dtdb_relational::IndexType;

    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());

    let schema = Schema::new(vec![int_col("id", true, false), string_col("body", true)]);
    db.create_table("docs", schema).unwrap();

    let tx = Transaction::new(1, db.clone());
    tx.put(
        "docs",
        DbKey::Int(1),
        Row::new(vec![
            DbValue::Int(1),
            DbValue::string("the quick brown fox"),
        ]),
    )
    .unwrap();
    tx.put(
        "docs",
        DbKey::Int(2),
        Row::new(vec![DbValue::Int(2), DbValue::string("the lazy brown dog")]),
    )
    .unwrap();
    // A NULL-bodied row must be tolerated (no String value to tokenize).
    tx.put(
        "docs",
        DbKey::Int(3),
        Row::new(vec![DbValue::Int(3), DbValue::Null]),
    )
    .unwrap();
    tx.commit().unwrap();
    drop(tx);

    db.create_index(
        "docs",
        "idx_body",
        vec!["body".to_string()],
        IndexType::FullText,
        Some("simple".to_string()),
    )
    .unwrap();

    let tx2 = Transaction::new(2, db.clone());
    // "brown" appears in both documents.
    assert_eq!(
        tx2.fulltext_scan("docs", "idx_body", "brown", None)
            .unwrap()
            .len(),
        2
    );
    // "quick" is unique to doc 1.
    let only = tx2
        .fulltext_scan("docs", "idx_body", "quick", None)
        .unwrap();
    assert_eq!(only.len(), 1);
    assert_eq!(only[0].values[0], DbValue::Int(1));
    // Phrase query exercises positional postings built during population.
    assert_eq!(
        tx2.fulltext_scan("docs", "idx_body", "\"lazy brown\"", None)
            .unwrap()
            .len(),
        1
    );
}

/// `start_background_flush_if_needed` must spawn a periodic task that flushes
/// table memtables to disk, and must be idempotent (a second call is a no-op
/// while one is already running).
#[test]
fn test_background_flush_persists_memtables() {
    fn has_sst(dir: &std::path::Path) -> bool {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return false;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if has_sst(&path) {
                    return true;
                }
            } else if path.extension().and_then(|e| e.to_str()) == Some("sst") {
                return true;
            }
        }
        false
    }

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let options = dtdb_relational::DatabaseOptions {
        compression: dtdb_storage::CompressionType::Lz4,
        memtable_size_limit: 1024 * 1024,
        block_size_limit: 4096,
        wal_size_limit: 32 * 1024 * 1024,
        flush_interval_ms: Some(10),
        l0_compaction_threshold: None,
        sstable_target_size: None,
        base_level_size_limit: None,
        level_size_multiplier: None,
        max_level: None,
        block_cache_capacity: Some(1000),
        analyze_frequency_ms: None,
        wal_sync_interval_ms: None,
        memory_budget: None,
        fsync_method: dtdb_storage::FsyncMethod::default(),
    };

    let db = Arc::new(Database::open_with_options(db_path.clone(), options).unwrap());
    db.start_background_flush_if_needed(&db);
    // Second call is a no-op: a flush task is already registered.
    db.start_background_flush_if_needed(&db);

    let schema = Schema::new(vec![int_col("id", true, false), int_col("v", false, true)]);
    db.create_table("t", schema).unwrap();
    let tx = Transaction::new(1, db.clone());
    tx.put(
        "t",
        DbKey::Int(1),
        Row::new(vec![DbValue::Int(1), DbValue::Int(10)]),
    )
    .unwrap();
    tx.commit().unwrap();

    // Poll until the periodic flush task has pushed the memtable to an on-disk
    // SST rather than relying on a fixed sleep.
    let mut flushed = false;
    for _ in 0..500 {
        if has_sst(&db_path) {
            flushed = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(
        flushed,
        "background flush did not persist the memtable to an SST"
    );
}

/// A table split across multiple locality groups must reopen correctly: each
/// group engine is reloaded from its own subdirectory and the rows merge back
/// into the original column layout.
#[test]
fn test_multi_locality_group_table_survives_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let mut name_col = string_col("name", true);
    name_col.locality_group = Some("lg_name".to_string());
    let mut score_col = int_col("score", false, true);
    score_col.locality_group = Some("lg_score".to_string());
    let schema = Schema::new(vec![int_col("id", true, false), name_col, score_col]);

    {
        let db = Arc::new(Database::open(&db_path).unwrap());
        db.create_table("users", schema).unwrap();
        let tx = Transaction::new(1, db.clone());
        tx.put(
            "users",
            DbKey::Int(1),
            Row::new(vec![
                DbValue::Int(1),
                DbValue::string("alice"),
                DbValue::Int(95),
            ]),
        )
        .unwrap();
        tx.commit().unwrap();
    }

    // Reopen: the multi-engine load path must reconstruct every locality group.
    let db = Arc::new(Database::open(&db_path).unwrap());
    let tx = Transaction::new(2, db.clone());
    let row = tx.get("users", &DbKey::Int(1)).unwrap().unwrap();
    assert_eq!(row.values[0], DbValue::Int(1));
    assert_eq!(row.values[1], DbValue::string("alice"));
    assert_eq!(row.values[2], DbValue::Int(95));
}

/// `analyze_table` over an integer-typed secondary index must scan the index's
/// composite-key range (using integer column bounds) and report the correct
/// entry count, distinct-value count, and most-common-value frequencies.
#[test]
fn test_analyze_integer_index_statistics() {
    use dtdb_relational::IndexType;

    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());

    let schema = Schema::new(vec![
        int_col("id", true, false),
        int_col("kind", false, true),
    ]);
    db.create_table("events", schema).unwrap();
    db.create_index(
        "events",
        "events_kind_idx",
        vec!["kind".to_string()],
        IndexType::BTree,
        None,
    )
    .unwrap();

    // Skewed "kind" distribution: 3x 0, 2x 1, 1x 2.
    let tx = Transaction::new(1, db.clone());
    for (i, kind) in [0i64, 0, 0, 1, 1, 2].into_iter().enumerate() {
        tx.put(
            "events",
            DbKey::Int(i as i64),
            Row::new(vec![DbValue::Int(i as i64), DbValue::Int(kind)]),
        )
        .unwrap();
    }
    tx.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());
    db.analyze_table("events", &tx2).unwrap();
    drop(tx2);

    let stats = db.get_table_statistics("events").unwrap();
    let idx = stats.index_stats.get("events_kind_idx").unwrap();
    assert_eq!(idx.entry_count, 6);
    assert_eq!(idx.unique_values, 3);
    // Most frequent indexed value (kind=0) appears three times.
    assert_eq!(idx.posting_len_upper_bound(&[DbKey::Int(0)]), 3);
    assert_eq!(idx.posting_len_upper_bound(&[DbKey::Int(2)]), 1);
}

/// Projected point reads (`get_projected` / `multi_get_projected`) must touch
/// only the locality-group engines that hold the requested columns, and fall
/// back to the default group when a requested column name does not exist.
#[test]
fn test_projected_point_reads_prune_locality_groups() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());

    let mut name_col = string_col("name", true);
    name_col.locality_group = Some("lg_name".to_string());
    let score_col = Column {
        id: 0,
        name: "score".to_string(),
        data_type: DataType::Float,
        is_primary_key: false,
        is_nullable: true,
        locality_group: Some("lg_score".to_string()),
        default_value: None,
        is_auto_increment: false,
    };
    let schema = Schema::new(vec![int_col("id", true, false), name_col, score_col]);
    db.create_table("users", schema).unwrap();

    let tx = Transaction::new(1, db.clone());
    tx.put(
        "users",
        DbKey::Int(1),
        Row::new(vec![
            DbValue::Int(1),
            DbValue::string("alice"),
            DbValue::Float(9.5),
        ]),
    )
    .unwrap();
    tx.put(
        "users",
        DbKey::Int(2),
        Row::new(vec![
            DbValue::Int(2),
            DbValue::string("bob"),
            DbValue::Float(8.0),
        ]),
    )
    .unwrap();
    tx.commit().unwrap();

    let tx2 = Transaction::new(2, db.clone());

    // Project only "name" -> only the lg_name group is read.
    let name_only = tx2
        .get_projected("users", &DbKey::Int(1), Some(&["name".to_string()]))
        .unwrap()
        .unwrap();
    assert_eq!(name_only.values[1], DbValue::string("alice"));

    // multi_get projecting only "score" -> only the lg_score group is read.
    let scores = tx2
        .multi_get_projected(
            "users",
            &[DbKey::Int(1), DbKey::Int(2)],
            Some(&["score".to_string()]),
        )
        .unwrap();
    assert_eq!(scores[0].as_ref().unwrap().values[2], DbValue::Float(9.5));
    assert_eq!(scores[1].as_ref().unwrap().values[2], DbValue::Float(8.0));

    // A requested column that does not exist falls back to the default group,
    // which still yields the primary key.
    let fallback = tx2
        .get_projected("users", &DbKey::Int(2), Some(&["missing".to_string()]))
        .unwrap()
        .unwrap();
    assert_eq!(fallback.values[0], DbValue::Int(2));

    // A miss returns None even under projection.
    assert!(
        tx2.get_projected("users", &DbKey::Int(99), Some(&["name".to_string()]))
            .unwrap()
            .is_none()
    );
}

/// Direct coverage of the auto-increment sequence helpers: `update_sequence_value`
/// seeds and advances the floor, and `next_sequence_value` on a table without an
/// auto-increment column falls through to its scan-based initialization path.
#[test]
fn test_sequence_value_helpers() {
    let temp_dir = TempDir::new().unwrap();
    let db = Arc::new(Database::open(temp_dir.path()).unwrap());

    // No auto-increment column: create_table does not seed the sequence, so the
    // first next_sequence_value() hits the lazy-initialization branch (no
    // auto-increment column found, so it starts at 1).
    let schema = Schema::new(vec![int_col("id", true, false), int_col("v", false, true)]);
    db.create_table("t", schema).unwrap();

    assert_eq!(db.next_sequence_value("t").unwrap(), 1);
    assert_eq!(db.next_sequence_value("t").unwrap(), 2);

    // A lower floor must not pull the sequence back.
    db.update_sequence_value("t", 1).unwrap();
    assert_eq!(db.next_sequence_value("t").unwrap(), 3);

    // A higher floor advances it.
    db.update_sequence_value("t", 100).unwrap();
    assert_eq!(db.next_sequence_value("t").unwrap(), 101);

    // update_sequence_value on a table the sequence map has never seen seeds it
    // directly (its own lazy-initialization branch).
    let schema2 = Schema::new(vec![int_col("id", true, false), int_col("v", false, true)]);
    db.create_table("t2", schema2).unwrap();
    db.update_sequence_value("t2", 41).unwrap();
    assert_eq!(db.next_sequence_value("t2").unwrap(), 42);
}
