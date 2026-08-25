//! Auditable variable-memory pressure reported by one transport adapter.

use core::fmt;

use calandria::RetainedBytes;

/// Accounted per-connection variable-memory charge for one transport.
///
/// Adapters report observable allocation capacities and conservative configured
/// charges for opaque transport-library state. Shared configuration and operating-
/// system socket buffers are excluded and must be bounded by their respective owners.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransportPressure {
    inbound: RetainedBytes,
    outbound: RetainedBytes,
    plaintext: RetainedBytes,
    protocol: RetainedBytes,
    total: RetainedBytes,
}

impl TransportPressure {
    /// No adapter-owned variable memory.
    pub const ZERO: Self = Self {
        inbound: RetainedBytes::ZERO,
        outbound: RetainedBytes::ZERO,
        plaintext: RetainedBytes::ZERO,
        protocol: RetainedBytes::ZERO,
        total: RetainedBytes::ZERO,
    };

    /// Maximum representable accounted pressure after a platform conversion overflow.
    pub const MAX: Self = Self {
        inbound: RetainedBytes::ZERO,
        outbound: RetainedBytes::ZERO,
        plaintext: RetainedBytes::ZERO,
        protocol: RetainedBytes::new(u64::MAX),
        total: RetainedBytes::new(u64::MAX),
    };

    /// Creates checked observable capacities and conservative protocol charges.
    pub const fn new(
        inbound: RetainedBytes,
        outbound: RetainedBytes,
        plaintext: RetainedBytes,
        protocol: RetainedBytes,
    ) -> Result<Self, TransportPressureError> {
        let Some(total) = inbound.checked_add(outbound) else {
            return Err(TransportPressureError);
        };
        let Some(total) = total.checked_add(plaintext) else {
            return Err(TransportPressureError);
        };
        let Some(total) = total.checked_add(protocol) else {
            return Err(TransportPressureError);
        };
        Ok(Self {
            inbound,
            outbound,
            plaintext,
            protocol,
            total,
        })
    }

    /// Returns encoded input capacity retained by the adapter.
    pub const fn inbound(self) -> RetainedBytes {
        self.inbound
    }

    /// Returns encoded output capacity retained by the adapter.
    pub const fn outbound(self) -> RetainedBytes {
        self.outbound
    }

    /// Returns retained application-plaintext capacity.
    pub const fn plaintext(self) -> RetainedBytes {
        self.plaintext
    }

    /// Returns the conservative charge for other per-connection protocol state.
    pub const fn protocol(self) -> RetainedBytes {
        self.protocol
    }

    /// Returns the checked aggregate pressure.
    pub const fn total(self) -> RetainedBytes {
        self.total
    }
}

/// Transport pressure exceeded the fixed-width accounting domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct TransportPressureError;

impl fmt::Display for TransportPressureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("transport retained-byte pressure overflowed")
    }
}

impl core::error::Error for TransportPressureError {}
