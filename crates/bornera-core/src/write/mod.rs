//! Bounded FIFO ownership and exact progress for complete outbound frames.

mod admission;
mod error;
mod frame;
mod integrity;
mod limits;
mod queue;
mod state;

pub use error::{
    WriteAdmissionError, WriteAdmissionFailure, WriteIdentityKind, WriteProgressError,
};
pub use frame::WriteFrame;
pub use limits::WriteQueueLimits;
pub use queue::WriteQueue;
pub use state::{
    DiscardedWrite, DiscardedWrites, WriteAccepted, WriteBoundary, WriteProgress, WriteSlice,
};
