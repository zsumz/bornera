//! Absolute-deadline and retained-capacity operation options.

use calandria::{Deadline, RetainedBytes};

use super::AdmissionClass;

/// Mechanical condition that releases an accepted operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletionMode {
    /// Retain the operation after writing until a matching reply arrives.
    ReplyExpected,
    /// Complete once the full application frame leaves Bornera write ownership.
    ///
    /// A buffering transport may still own encoded output that has not reached the
    /// operating system.
    WriteComplete,
}

/// Mechanical resources and timing attached to one operation reservation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationOptions {
    deadline: Deadline,
    class: AdmissionClass,
    retained_bytes: RetainedBytes,
    write_retained_bytes: RetainedBytes,
    completion_mode: CompletionMode,
}

impl OperationOptions {
    /// Creates regular-work options with an absolute deadline.
    pub const fn until(deadline: Deadline) -> Self {
        Self {
            deadline,
            class: AdmissionClass::Regular,
            retained_bytes: RetainedBytes::ZERO,
            write_retained_bytes: RetainedBytes::ZERO,
            completion_mode: CompletionMode::ReplyExpected,
        }
    }

    /// Marks this as session-establishment work.
    #[must_use]
    pub const fn session(mut self) -> Self {
        self.class = AdmissionClass::Session;
        self
    }

    /// Sets semantic memory retained while the operation remains owned.
    #[must_use]
    pub const fn retained_bytes(mut self, retained_bytes: RetainedBytes) -> Self {
        self.retained_bytes = retained_bytes;
        self
    }

    /// Reserves the maximum memory retained by the complete outbound frame.
    #[must_use]
    pub const fn write_retained_bytes(mut self, retained_bytes: RetainedBytes) -> Self {
        self.write_retained_bytes = retained_bytes;
        self
    }

    /// Selects whether this operation waits for a reply or completes on write.
    #[must_use]
    pub const fn completion_mode(mut self, completion_mode: CompletionMode) -> Self {
        self.completion_mode = completion_mode;
        self
    }

    /// Returns the absolute deadline.
    pub const fn deadline(self) -> Deadline {
        self.deadline
    }

    /// Returns the admission class.
    pub const fn class(self) -> AdmissionClass {
        self.class
    }

    /// Returns semantic retained bytes.
    pub const fn retained(self) -> RetainedBytes {
        self.retained_bytes
    }

    /// Returns reserved complete-frame retained memory.
    pub const fn write_retained(self) -> RetainedBytes {
        self.write_retained_bytes
    }

    /// Returns the mechanical completion condition.
    pub const fn completion(self) -> CompletionMode {
        self.completion_mode
    }
}
