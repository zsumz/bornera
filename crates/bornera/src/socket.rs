//! Explicit post-connect TCP socket policy.

use calandria::Span;

/// Whether established TCP streams disable Nagle's algorithm.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TcpNoDelay {
    /// Set `TCP_NODELAY` after connection establishment.
    Enabled,
    /// Leave Nagle's algorithm enabled.
    Disabled,
}

/// Portable keepalive subset applied to an established TCP stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpKeepalivePolicy {
    idle: Span,
}

impl TcpKeepalivePolicy {
    /// Creates a keepalive policy with a nonzero idle duration.
    pub const fn new(idle: Span) -> Result<Self, SocketPolicyError> {
        if idle.as_nanos() == 0 {
            Err(SocketPolicyError::ZeroKeepaliveIdle)
        } else {
            Ok(Self { idle })
        }
    }

    /// Returns the portable idle duration before keepalive probes begin.
    pub const fn idle(self) -> Span {
        self.idle
    }
}

/// TCP behavior applied only after a nonblocking connect succeeds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpSocketPolicy {
    no_delay: TcpNoDelay,
    keepalive: Option<TcpKeepalivePolicy>,
}

impl TcpSocketPolicy {
    /// Low-latency default with keepalive disabled.
    pub const DEFAULT: Self = Self {
        no_delay: TcpNoDelay::Enabled,
        keepalive: None,
    };

    /// Creates explicit Nagle behavior with keepalive disabled.
    pub const fn new(no_delay: TcpNoDelay) -> Self {
        Self {
            no_delay,
            keepalive: None,
        }
    }

    /// Enables the portable keepalive idle-time policy.
    #[must_use]
    pub const fn keepalive(mut self, keepalive: TcpKeepalivePolicy) -> Self {
        self.keepalive = Some(keepalive);
        self
    }

    /// Returns the configured Nagle policy.
    pub const fn no_delay(self) -> TcpNoDelay {
        self.no_delay
    }

    /// Returns the configured portable keepalive policy, if enabled.
    pub const fn keepalive_policy(self) -> Option<TcpKeepalivePolicy> {
        self.keepalive
    }
}

impl Default for TcpSocketPolicy {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Invalid portable TCP socket policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SocketPolicyError {
    /// Keepalive idle time must be positive.
    ZeroKeepaliveIdle,
}

impl core::fmt::Display for SocketPolicyError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("TCP keepalive idle time must be positive")
    }
}

impl core::error::Error for SocketPolicyError {}
