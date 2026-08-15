//! Absolute-deadline and retained-capacity operation options.

use calandria::{Deadline, RetainedBytes};

use super::AdmissionClass;

/// Mechanical resources and timing attached to one operation reservation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationOptions {
    deadline: Deadline,
    class: AdmissionClass,
    retained_bytes: RetainedBytes,
    write_bytes: RetainedBytes,
}

impl OperationOptions {
    /// Creates regular-work options with an absolute deadline.
    pub const fn until(deadline: Deadline) -> Self {
        Self {
            deadline,
            class: AdmissionClass::Regular,
            retained_bytes: RetainedBytes::ZERO,
            write_bytes: RetainedBytes::ZERO,
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

    /// Reserves the maximum complete encoded frame size.
    #[must_use]
    pub const fn write_bytes(mut self, write_bytes: RetainedBytes) -> Self {
        self.write_bytes = write_bytes;
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

    /// Returns reserved encoded-frame bytes.
    pub const fn write(self) -> RetainedBytes {
        self.write_bytes
    }
}
