//! Operation phases and terminal mechanical outcomes.

mod outcome;
mod record;

pub use outcome::{OperationFailure, OperationOutcome, OperationPhase};

pub(crate) use record::OperationRecord;
