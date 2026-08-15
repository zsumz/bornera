//! Atomic admission and affine operation permits.

mod error;
mod gate;
mod ledger;
mod options;
mod permit;

pub use error::{CommitErrorKind, FrameCommitError, FrameCommitFailure, ReserveError};
pub use gate::{AdmissionClass, AdmissionGate};
pub use options::OperationOptions;
pub use permit::OperationPermit;

pub(crate) use ledger::{Reservation, ReservationLedger};
