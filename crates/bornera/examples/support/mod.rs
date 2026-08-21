//! Declarative exports for the hosting examples' opaque adapter.

mod adapter;

pub(crate) use adapter::{BoxError, ExampleFrame, FRAME_BYTES, prepared_engine};
