//! End-to-end tests for the `dtdb_storage_cli` REPL binary. We spawn the real
//! compiled binary (located via the `CARGO_BIN_EXE_*` env var Cargo sets for
//! integration tests) and drive it through stdin, asserting on its stdout.

use std::io::Write;
use std::process::{Command, Stdio};
use tempfile::TempDir;

/// Runs the CLI against a fresh database directory, feeding `script` on stdin,
/// and returns combined stdout.
fn run_cli(db_dir: &std::path::Path, script: &str) -> String {
    let bin = env!("CARGO_BIN_EXE_dtdb_storage_cli");
    let mut child = Command::new(bin)
        .arg(db_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn dtdb_storage_cli");

    child
        .stdin
        .take()
        .unwrap()
        .write_all(script.as_bytes())
        .unwrap();

    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "CLI exited with failure");
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn test_cli_put_get_delete_and_scan() {
    let dir = TempDir::new().unwrap();
    let script = "\
put 1 100
put 2 hello
put 3 2.5
get 1
get 2
get 3
get 99
scan 1 3
flush
get 1
delete 2
get 2
scan
exit
";
    let out = run_cli(dir.path(), script);

    // put → OK acknowledgements (one per successful mutation).
    assert!(out.contains("OK"), "expected put/delete acks, got:\n{out}");
    // Numeric value parsed as Int, string as String, decimal as Float.
    assert!(out.contains("Int(100)"), "missing Int value:\n{out}");
    assert!(
        out.contains("String(\"hello\")"),
        "missing String value:\n{out}"
    );
    assert!(out.contains("Float(2.5)"), "missing Float value:\n{out}");
    // Missing key path.
    assert!(out.contains("Key not found"), "missing not-found:\n{out}");
    // flush command.
    assert!(
        out.contains("Flush complete."),
        "missing flush output:\n{out}"
    );
    // Range scan prints the key/value pairs.
    assert!(out.contains("Int(1)"), "range scan missing key 1:\n{out}");
    // Clean exit.
    assert!(out.contains("Goodbye!"), "missing goodbye:\n{out}");

    // Data persists: reopen and read key 1 back (key 2 was deleted).
    let out2 = run_cli(dir.path(), "get 1\nget 2\nexit\n");
    assert!(out2.contains("Int(100)"), "value did not persist:\n{out2}");
    assert!(
        out2.contains("Key not found"),
        "delete did not persist:\n{out2}"
    );
}

#[test]
fn test_cli_usage_errors_and_unknown_command() {
    let dir = TempDir::new().unwrap();
    let script = "\
put 1
get
delete

boguscmd
scan a b c
exit
";
    let out = run_cli(dir.path(), script);

    assert!(
        out.contains("Usage: put <key> <value>"),
        "missing put usage:\n{out}"
    );
    assert!(
        out.contains("Usage: get <key>"),
        "missing get usage:\n{out}"
    );
    assert!(
        out.contains("Usage: delete <key>"),
        "missing delete usage:\n{out}"
    );
    assert!(
        out.contains("Unknown command: 'boguscmd'"),
        "missing unknown cmd:\n{out}"
    );
    assert!(out.contains("Usage: scan"), "missing scan usage:\n{out}");
}

#[test]
fn test_cli_scan_range_empty() {
    let dir = TempDir::new().unwrap();
    let out = run_cli(dir.path(), "scan 100 200\nexit\n");
    assert!(
        out.contains("No keys found in range"),
        "expected empty-range message, got:\n{out}"
    );
}

#[test]
fn test_cli_requires_database_argument() {
    let bin = env!("CARGO_BIN_EXE_dtdb_storage_cli");
    let output = Command::new(bin).output().unwrap();
    assert!(
        !output.status.success(),
        "CLI should fail without a db dir arg"
    );
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("Usage:"),
        "missing usage message:\n{stderr}"
    );
}
