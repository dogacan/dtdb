#!/bin/bash
# A script to run DuctTapeDB tests under ThreadSanitizer (TSan)

set -e

# 1. Detect the host target architecture
TARGET=$(rustc -vV | grep host | cut -d ' ' -f 2)

echo "=== ThreadSanitizer (TSan) Test Runner ==="
echo "Host target: $TARGET"

# 2. Check if nightly compiler is installed
if ! rustup toolchain list | grep -q "nightly"; then
    echo "Rust nightly toolchain not found. Installing..."
    rustup toolchain install nightly
else
    echo "Rust nightly toolchain is already installed."
fi

# 3. Run cargo test with TSan enabled
echo "Compiling and running tests with ThreadSanitizer..."

# Point TSan at our suppressions file (benign tokio-runtime false positives).
# An absolute path is required since test binaries run from various cwds.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export TSAN_OPTIONS="suppressions=${SCRIPT_DIR}/tsan_suppressions.txt${TSAN_OPTIONS:+ ${TSAN_OPTIONS}}"

RUSTFLAGS="-Z sanitizer=thread -Cunsafe-allow-abi-mismatch=sanitizer" \
RUSTDOCFLAGS="-Z sanitizer=thread -Cunsafe-allow-abi-mismatch=sanitizer" \
cargo +nightly test -Zbuild-std --target "$TARGET" "$@"
