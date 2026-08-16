//! Bounded driving of protocol-owned incremental frame decoders.

mod driver;
mod error;
mod port;

pub use driver::FrameDriver;
pub use error::FrameDecodeError;
pub use port::FrameDecoder;
