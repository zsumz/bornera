//! Executable evidence for Bornera's architecture boundaries.

use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

const CORE_FORBIDDEN: &[&str] = &[
    "calandria_mio",
    "mio::",
    "tokio::",
    "std::net",
    "std::process",
    "std::sync::Mutex",
    "std::sync::RwLock",
    "std::thread",
    "kafka",
    "cassandra",
    "postgres",
    "redis",
];

#[test]
fn core_dependencies_point_inward_and_simulation_reuses_production_slot()
-> Result<(), Box<dyn Error>> {
    let root = repository_root()?;
    let core = fs::read_to_string(root.join("crates/bornera-core/Cargo.toml"))?;
    let simulation = fs::read_to_string(root.join("crates/bornera-sim/Cargo.toml"))?;

    if core.contains("bornera =") || core.contains("bornera-sim =") {
        return Err(std::io::Error::other("bornera-core depends on an adapter package").into());
    }
    if !simulation.contains("bornera.workspace = true") {
        return Err(std::io::Error::other(
            "bornera-sim does not exercise the production connection slot",
        )
        .into());
    }
    Ok(())
}

#[test]
fn deterministic_core_remains_sans_io_and_protocol_neutral() -> Result<(), Box<dyn Error>> {
    let root = repository_root()?;
    for path in rust_files(&root.join("crates/bornera-core/src"))? {
        let source = fs::read_to_string(&path)?;
        let lowered = source.to_lowercase();
        for forbidden in CORE_FORBIDDEN {
            if lowered.contains(&forbidden.to_lowercase()) {
                return Err(std::io::Error::other(
                    "core contains forbidden protocol vocabulary or I/O capability",
                )
                .into());
            }
        }
    }
    Ok(())
}

#[test]
fn connection_core_is_the_only_public_mutation_owner() -> Result<(), Box<dyn Error>> {
    let root = repository_root()?;
    let facade = fs::read_to_string(root.join("crates/bornera-core/src/lib.rs"))?;
    let machine = fs::read_to_string(root.join("crates/bornera-core/src/connection/machine.rs"))?;
    let drive = fs::read_to_string(root.join("crates/bornera-core/src/connection/drive.rs"))?;
    let writes = fs::read_to_string(root.join("crates/bornera-core/src/write/queue.rs"))?;
    let effects = fs::read_to_string(root.join("crates/bornera-core/src/connection/effect.rs"))?;

    assert!(facade.contains("ConnectionCore"));
    assert!(!machine.contains("    pub fn new("));
    assert!(!machine.contains("    pub fn with_identity_seeds("));
    assert!(!drive.contains("    pub fn apply("));
    assert!(!writes.contains("    pub fn admit("));
    assert!(!writes.contains("    pub fn advance("));
    assert!(!writes.contains("    pub fn discard("));
    assert!(!effects.contains("EnqueueWrite"));
    Ok(())
}

#[test]
fn simulation_crate_remains_unpublished() -> Result<(), Box<dyn Error>> {
    let root = repository_root()?;
    let simulation = fs::read_to_string(root.join("crates/bornera-sim/Cargo.toml"))?;
    assert!(simulation.contains("publish = false"));
    Ok(())
}

#[test]
fn source_shape_is_bounded_and_tests_remain_separate() -> Result<(), Box<dyn Error>> {
    let root = repository_root()?;
    for path in rust_files(&root.join("crates"))? {
        let source = fs::read_to_string(&path)?;
        let lines = source.lines().count();
        if lines > 300 {
            return Err(std::io::Error::other("Rust file exceeds the 300-line hard limit").into());
        }
        let implementation = strip_test_module_edges(&source);
        if !is_test_file(&path)
            && (implementation.contains("#[test]") || implementation.contains("#[cfg(test)]"))
        {
            return Err(std::io::Error::other(
                "implementation file contains tests outside a sibling test file",
            )
            .into());
        }
    }
    Ok(())
}

fn strip_test_module_edges(source: &str) -> String {
    let mut lines = source.lines().peekable();
    let mut retained = Vec::new();
    while let Some(line) = lines.next() {
        if line.trim() == "#[cfg(test)]"
            && lines
                .peek()
                .is_some_and(|next| next.trim().starts_with("mod ") && next.trim().ends_with(';'))
        {
            let _module = lines.next();
        } else {
            retained.push(line);
        }
    }
    retained.join("\n")
}

fn repository_root() -> Result<PathBuf, Box<dyn Error>> {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let Some(root) = manifest.parent().and_then(Path::parent) else {
        return Err(std::io::Error::other("crate manifest has no workspace root").into());
    };
    Ok(root.to_path_buf())
}

fn rust_files(root: &Path) -> Result<Vec<PathBuf>, Box<dyn Error>> {
    let mut pending = Vec::from([root.to_path_buf()]);
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

fn is_test_file(path: &Path) -> bool {
    path.components().any(|part| part.as_os_str() == "tests")
        || path.file_name().is_some_and(|name| {
            let name = name.to_string_lossy();
            name == "tests.rs" || name.ends_with("_test.rs") || name.ends_with("_tests.rs")
        })
}
