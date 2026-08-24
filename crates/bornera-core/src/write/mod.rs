//! Bounded FIFO ownership and exact progress for complete outbound frames.

mod admission;
mod error;
mod frame;
mod integrity;
mod limits;
mod queue;
mod queued;
mod state;

pub use error::{WriteAdmissionFailure, WriteIdentityKind, WriteProgressError};
pub use frame::{FrameContractViolation, FrameMeasure, WriteFrame};
pub use state::{DiscardedWrite, DiscardedWrites, WriteSlice};

pub(crate) use error::WriteAdmissionError;
pub(crate) use limits::WriteQueueLimits;
pub(crate) use queue::WriteQueue;
pub(crate) use state::{WriteBoundary, WriteProgress};
