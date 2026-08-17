//! Complete opaque frame fixtures shared by deterministic core tests.

use bornera_core::{RetainedBytes, WriteFrame};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TestFrame(pub(crate) Vec<u8>);

impl WriteFrame for TestFrame {
    fn bytes(&self) -> &[u8] {
        &self.0
    }

    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::new(u64::try_from(self.0.len()).unwrap_or(u64::MAX))
    }
}
