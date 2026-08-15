//! Mechanical admission gates for session establishment and shutdown.

/// The class of work requesting admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionClass {
    /// Establishes a protocol-defined session before ordinary work is admitted.
    Session,
    /// Ordinary protocol work.
    Regular,
}

/// Which classes of work one connection epoch currently accepts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionGate {
    /// Only session-establishment work is accepted.
    SessionOnly,
    /// Session and ordinary work are accepted.
    Open,
    /// No new work is accepted while owned work drains.
    Draining,
    /// No new work is accepted because the epoch is closing or closed.
    Closed,
}

impl AdmissionGate {
    pub(crate) const fn admits(self, class: AdmissionClass) -> bool {
        matches!(
            (self, class),
            (Self::SessionOnly, AdmissionClass::Session)
                | (
                    Self::Open,
                    AdmissionClass::Session | AdmissionClass::Regular
                )
        )
    }
}
