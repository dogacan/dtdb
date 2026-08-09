use std::path::{Path, PathBuf};
use std::process::Command;

/// Given `current_exe` path of a test binary, walk up to find the cargo profile directory
/// (e.g., `target/debug`, `target/release`, `target/x86_64-unknown-linux-gnu/debug`).
fn find_profile_dir(current_exe: &Path) -> PathBuf {
    let mut curr = current_exe.to_path_buf();
    while let Some(parent) = curr.parent().map(|p| p.to_path_buf()) {
        if matches!(
            curr.file_name().and_then(|n| n.to_str()),
            Some("deps" | "build" | "examples")
        ) {
            return parent;
        }
        curr = parent;
    }
    current_exe
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| current_exe.to_path_buf())
}

fn ensure_static_lib(profile_dir: &Path, workspace_root: &Path) {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let mut cmd = Command::new(cargo);
    cmd.arg("build").arg("-p").arg("dtdb_bindings");

    // Detect profile (debug vs release)
    let is_release = profile_dir
        .file_name()
        .map(|n| n.to_str() == Some("release"))
        .unwrap_or(false)
        || profile_dir.ends_with("release");
    if is_release {
        cmd.arg("--release");
    }

    // Detect target triple or custom target-dir
    if let Some(parent) = profile_dir.parent() {
        let parent_name = parent.file_name().and_then(|n| n.to_str());
        if parent_name == Some("llvm-cov-target") {
            cmd.arg("--target-dir").arg(parent);
        } else if let Some(target_triple) = parent_name.filter(|&n| n != "target") {
            if let Some(grandparent) = parent
                .parent()
                .filter(|gp| gp.file_name().and_then(|n| n.to_str()) == Some("llvm-cov-target"))
            {
                cmd.arg("--target-dir").arg(grandparent);
            }
            cmd.arg("--target").arg(target_triple);
        }
    }

    // Detect nightly or sanitizer build-std options
    if std::env::var("RUSTFLAGS")
        .map(|f| f.contains("sanitizer=thread"))
        .unwrap_or(false)
    {
        cmd.arg("-Zbuild-std");
    }

    let status = cmd
        .current_dir(workspace_root)
        .status()
        .expect("Failed to run cargo build to generate static library");
    assert!(
        status.success(),
        "Failed to compile libdtdb_bindings.a static library for FFI smoke test"
    );
}

#[test]
fn test_cpp_bridge_smoke() {
    // Skip if MSan is enabled via RUSTFLAGS (in case cfg(sanitize = "memory") is not set by the compiler version/target configuration)
    if std::env::var("RUSTFLAGS")
        .map(|f| f.contains("sanitizer=memory"))
        .unwrap_or(false)
    {
        println!(
            "Skipping C++ smoke test under MemorySanitizer (MSan) to avoid uninstrumented C++ stdlib false positives."
        );
        return;
    }

    // 1. Locate the workspace root
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir.parent().expect("workspace root");

    // 2. Locate the target profile directory containing libdtdb_bindings.a
    let current_exe = std::env::current_exe().expect("current exe path");
    let profile_dir = find_profile_dir(&current_exe);

    // Ensure the static library is built for this profile/target
    ensure_static_lib(&profile_dir, workspace_root);

    let lib_path = profile_dir.join("libdtdb_bindings.a");
    assert!(
        lib_path.exists(),
        "Could not find libdtdb_bindings.a at {}",
        lib_path.display()
    );

    // 3. Locate the cxxbridge directory
    // We try workspace_root/target/cxxbridge or search upward from profile_dir
    let mut cxxbridge_dir = workspace_root.join("target/cxxbridge");
    if !cxxbridge_dir.exists() {
        let mut curr = profile_dir.to_path_buf();
        loop {
            let candidate = curr.join("cxxbridge");
            if candidate.exists() {
                cxxbridge_dir = candidate;
                break;
            }
            if let Some(parent) = curr.parent() {
                curr = parent.to_path_buf();
            } else {
                break;
            }
        }
    }

    assert!(
        cxxbridge_dir.exists(),
        "Could not find cxxbridge directory at {}",
        cxxbridge_dir.display()
    );

    // 4. Find C++ compiler
    let compiler = if Command::new("clang++").arg("--version").output().is_ok() {
        "clang++"
    } else if Command::new("g++").arg("--version").output().is_ok() {
        "g++"
    } else if Command::new("c++").arg("--version").output().is_ok() {
        "c++"
    } else {
        println!("No C++ compiler found. Skipping C++ smoke test.");
        return;
    };

    // 5. Setup temp directory for the test executable
    let tmp_dir = tempfile::tempdir().expect("create temp dir");
    let output_exe = tmp_dir.path().join("cpp_demo");

    // 6. Compile the C++ demo
    let mut compile_cmd = Command::new(compiler);
    compile_cmd
        .arg("-std=c++17")
        .arg(workspace_root.join("dtdb_bindings/examples/cpp_demo.cc"))
        .arg("-I")
        .arg(workspace_root)
        .arg("-I")
        .arg(&cxxbridge_dir)
        .arg("-L")
        .arg(profile_dir)
        .arg("-ldtdb_bindings")
        .arg("-lpthread")
        .arg("-ldl")
        .arg("-o")
        .arg(&output_exe);

    if cfg!(target_os = "macos") {
        compile_cmd.arg("-lresolv");
        compile_cmd.arg("-framework").arg("CoreFoundation");
    }

    // Detect if we need to pass sanitizer flags to the C++ compiler/linker
    if let Ok(rustflags) = std::env::var("RUSTFLAGS") {
        if rustflags.contains("sanitizer=thread") {
            compile_cmd.arg("-fsanitize=thread");
        } else if rustflags.contains("sanitizer=address") {
            compile_cmd.arg("-fsanitize=address");
        } else if rustflags.contains("sanitizer=leak") {
            compile_cmd.arg("-fsanitize=leak");
        }
    }

    let compile_status = compile_cmd
        .status()
        .expect("failed to execute compiler command");

    assert!(
        compile_status.success(),
        "C++ compilation failed for FFI smoke test"
    );

    // 7. Run the compiled demo. We change Cwd to the temp directory so it doesn't pollute the workspace.
    let run_status = Command::new(&output_exe)
        .current_dir(tmp_dir.path())
        .status()
        .expect("failed to run compiled C++ smoke test");

    assert!(
        run_status.success(),
        "Compiled C++ smoke test failed to execute successfully"
    );
}
