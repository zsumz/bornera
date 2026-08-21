//! Executable evidence for the production engine's Calandria boundary.

use std::{error::Error, fs, path::PathBuf};

use bornera::{ConnectionEngine, ConnectionWaiter, EngineError, InboundClassifier};
use bornera_core::FrameDecoder;
use calandria::{Duty, Retained, Waiter};

#[test]
fn engine_and_waiter_use_calandria_host_contracts() {
    fn assert_duty<D, C>()
    where
        D: FrameDecoder,
        D::Frame: Retained,
        C: InboundClassifier<D::Frame>,
        ConnectionEngine<D, C>: Duty<Error = EngineError>,
        ConnectionWaiter: Waiter<ConnectionEngine<D, C>, Error = EngineError>,
    {
    }

    assert_duty::<ArchitectureDecoder, ArchitectureClassifier>();
}

#[test]
fn mio_tcp_remains_a_private_native_capability() -> Result<(), Box<dyn Error>> {
    let root = repository_root()?;
    let manifest = fs::read_to_string(root.join("crates/bornera/Cargo.toml"))?;
    let facade = fs::read_to_string(root.join("crates/bornera/src/lib.rs"))?;
    let engine = fs::read_to_string(root.join("crates/bornera/src/engine.rs"))?;

    assert!(manifest.contains("calandria-mio.workspace = true"));
    assert!(manifest.contains("mio.workspace = true"));
    assert!(!facade.contains("pub use mio"));
    assert!(!engine.contains("std::thread"));
    assert!(!engine.contains("std::time::Instant"));
    for entry in fs::read_dir(root.join("crates/bornera/src"))? {
        let path = entry?.path();
        if path.extension().is_some_and(|extension| extension == "rs") {
            let source = fs::read_to_string(path)?;
            assert!(!source.contains("Fn("));
            assert!(!source.contains("FnMut("));
            assert!(!source.contains("FnOnce("));
        }
    }
    Ok(())
}

fn repository_root() -> Result<PathBuf, Box<dyn Error>> {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let Some(root) = manifest.parent().and_then(std::path::Path::parent) else {
        return Err(std::io::Error::other("crate manifest has no workspace root").into());
    };
    Ok(root.to_path_buf())
}

#[derive(Debug)]
struct ArchitectureDecoder;

impl FrameDecoder for ArchitectureDecoder {
    type Frame = ();
    type Error = std::convert::Infallible;

    fn feed(&mut self, _bytes: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }

    fn next_frame(&mut self) -> Result<Option<Self::Frame>, Self::Error> {
        Ok(None)
    }

    fn retained_bytes(&self) -> bornera_core::RetainedBytes {
        bornera_core::RetainedBytes::ZERO
    }
}

#[derive(Debug)]
struct ArchitectureClassifier;

impl InboundClassifier<()> for ArchitectureClassifier {
    type Error = std::convert::Infallible;

    fn reply_key(&mut self, _frame: &()) -> Result<bornera_core::MatchKey, Self::Error> {
        Ok(bornera_core::MatchKey::new(0))
    }
}
