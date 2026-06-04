//! End-to-end tests for the `dtdb_sql_cli` REPL binary. These drive the real
//! binary over stdin/stdout so the argument handling, the read-eval loop, and
//! every `display_result` branch are exercised.

use std::io::Write;
use std::process::{Command, Stdio};
use tempfile::TempDir;

/// Runs the CLI binary against a fresh database directory, feeding `script` on
/// stdin, and returns `(stdout, stderr, success)`.
fn run_cli(script: &str) -> (String, String, bool) {
    let tmp = TempDir::new().unwrap();
    run_cli_with_args(&[tmp.path().to_str().unwrap()], script)
}

fn run_cli_with_args(args: &[&str], script: &str) -> (String, String, bool) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_dtdb_sql_cli"))
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn CLI");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(script.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
        out.status.success(),
    )
}

#[test]
fn cli_missing_argument_prints_usage() {
    let child = Command::new(env!("CARGO_BIN_EXE_dtdb_sql_cli"))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("Usage:"), "stderr was: {stderr}");
}

#[test]
fn cli_open_failure_reports_error() {
    // A path that exists as a *file* (not a directory) makes Database::open fail.
    let tmp = TempDir::new().unwrap();
    let file_path = tmp.path().join("not_a_dir");
    std::fs::write(&file_path, b"x").unwrap();
    let (_stdout, stderr, success) = run_cli_with_args(&[file_path.to_str().unwrap()], "exit\n");
    assert!(!success);
    assert!(
        stderr.contains("Failed to open database"),
        "stderr was: {stderr}"
    );
}

#[test]
fn cli_exit_command_quits_cleanly() {
    let (stdout, _stderr, success) = run_cli("exit\n");
    assert!(success);
    assert!(stdout.contains("Welcome to DuctTapeDB SQL CLI!"));
    assert!(stdout.contains("Goodbye!"));
}

#[test]
fn cli_eof_quits() {
    // Closing stdin with no input drives the EOF break path.
    let (stdout, _stderr, success) = run_cli("");
    assert!(success);
    assert!(stdout.contains("Welcome"));
}

#[test]
fn cli_ddl_dml_select_and_display_branches() {
    // Multi-line continuation, every DDL/DML result message, and the SELECT
    // table renderer across all value types plus the empty-set path.
    let script = "\
CREATE TABLE t (
  id INT PRIMARY KEY,
  name STRING,
  score FLOAT,
  active BOOL,
  d DATE,
  tm TIME,
  ts TIMESTAMP,
  dec DECIMAL,
  note STRING
);
INSERT INTO t (id, name, score, active, d, tm, ts, dec, note) VALUES
  (1, 'Alice', 9.5, true, DATE '2026-06-04', TIME '12:00:00', TIMESTAMP '2026-06-04 12:00:00', DECIMAL '3.14', 'hi');
SELECT * FROM t;
SELECT * FROM t WHERE id = 999;
UPDATE t SET score = 1.0 WHERE id = 1;
DELETE FROM t WHERE id = 1;
SELECT bogus_fn();
DROP TABLE t;
quit
";
    let (stdout, _stderr, success) = run_cli(script);
    assert!(success, "cli should exit cleanly; stdout:\n{stdout}");

    assert!(stdout.contains("Table created successfully."));
    assert!(stdout.contains("Inserted 1 row(s)."));
    // SELECT * rendered as a bordered table with the row present.
    assert!(stdout.contains("Alice"));
    assert!(stdout.contains("2026-06-04"));
    assert!(stdout.contains("3.14"));
    assert!(stdout.contains("1 row(s) in set"));
    // Empty result set path.
    assert!(stdout.contains("Empty set."));
    assert!(stdout.contains("Updated 1 row(s)."));
    assert!(stdout.contains("Deleted 1 row(s)."));
    // An execution error is reported and does not abort the REPL.
    assert!(stdout.contains("Error:"));
    assert!(stdout.contains("Table dropped successfully."));
    assert!(stdout.contains("Goodbye!"));
}

#[test]
fn cli_blank_statement_is_skipped() {
    // A lone semicolon clears the buffer without executing anything.
    let (stdout, _stderr, success) = run_cli(";\nexit\n");
    assert!(success);
    assert!(stdout.contains("Goodbye!"));
}
