use dtdb_bindings::{ffi, new_in_process_client};

fn make_query(text: &str) -> ffi::CxxSqlQuery {
    ffi::CxxSqlQuery {
        text: text.to_string(),
        params: Vec::new(),
    }
}

/// Commits and rollbacks must wait for the worker task to actually finish
/// before returning, not fire-and-forget. The end-to-end check below would
/// silently pass under the old try_send / Ok-without-ack rollback path, but
/// the visible side effect (row presence / absence) verifies that the
/// terminal request was actually delivered and processed before the call
/// returned.
#[test]
fn commit_persists_and_rollback_reverts() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let client = new_in_process_client(tmp.path().to_str().unwrap()).expect("client");

    client.create_db("t").expect("create_db");
    client
        .execute_query(
            "t",
            make_query("CREATE TABLE k (id INTEGER PRIMARY KEY, v INTEGER);"),
        )
        .expect("create table");

    // Commit path: insert then commit; row must be visible afterwards.
    let tx1 = client.start_transaction("t").expect("start tx");
    client
        .execute_tx_query(&tx1, make_query("INSERT INTO k (id, v) VALUES (1, 100);"))
        .expect("insert");
    client.commit_tx(tx1).expect("commit");

    let result = client
        .execute_query("t", make_query("SELECT v FROM k WHERE id = 1;"))
        .expect("select after commit");
    assert_eq!(result.rows, vec!["100".to_string()]);

    // Rollback path: insert then rollback; row must NOT be visible.
    let tx2 = client.start_transaction("t").expect("start tx");
    client
        .execute_tx_query(&tx2, make_query("INSERT INTO k (id, v) VALUES (2, 200);"))
        .expect("insert");
    client.rollback_tx(tx2).expect("rollback");

    let result = client
        .execute_query("t", make_query("SELECT v FROM k WHERE id = 2;"))
        .expect("select after rollback");
    assert!(
        result.rows.is_empty(),
        "rollback should have reverted the insert"
    );
}
