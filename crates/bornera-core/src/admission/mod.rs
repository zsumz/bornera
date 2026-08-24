//! Atomic admission and affine operation permits.

mod error;
mod gate;
mod key_set;
mod ledger;
mod options;
mod permit;

pub use error::{CommitErrorKind, FrameCommitError, FrameCommitFailure, ReserveError};
pub use gate::{AdmissionClass, AdmissionGate};
pub use options::{CompletionMode, OperationOptions};
pub use permit::OperationPermit;

pub(crate) use ledger::{Reservation, ReservationLedger};
