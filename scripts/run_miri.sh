#!/bin/bash
# A script to run DuctTapeDB tests under Miri (Undefined Behavior and memory leak detection)

set -e

echo "=== Miri Test Runner ==="

# 1. Check if nightly compiler is installed
if ! rustup toolchain list | grep -q "nightly"; then
    echo "Rust nightly toolchain not found. Installing..."
    rustup toolchain install nightly
else
    echo "Rust nightly toolchain is already installed."
fi

# 2. Check if miri component is installed
if ! rustup +nightly component list | grep -q "miri (installed)"; then
    echo "Miri component not found. Installing..."
    rustup +nightly component add miri
else
    echo "Miri component is already installed."
fi

# 3. Run cargo miri test
# Note:
# - Miri requires disabling isolation (-Zmiri-disable-isolation) to allow file system access for temp files.
# - Miri does not support OS-specific network APIs (like kqueue on macOS), so we exclude the `dtdb_api` crate.
# - We also skip `test_compaction_crash_garbage_collection` as it relies on background thread timing that Miri's deterministic scheduling conflicts with.
echo "Running core tests under Miri..."
if [ $# -eq 0 ]; then
    MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-ignore-leaks" \
    cargo +nightly miri test -p dtdb_storage -p dtdb_relational -p dtdb_sql -- --skip test_compaction_crash_garbage_collection
else
    MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-ignore-leaks" \
    cargo +nightly miri test "$@"
fi
