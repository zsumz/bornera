//! Selector-free graceful-shutdown transport fixture.

use std::io;

use bornera::{
    SlotTransport, TcpSocketPolicy, TransportBudget, TransportDiagnostic, TransportError,
    TransportFailureKind, TransportFailurePhase, TransportLimits, TransportPressure,
    TransportProgress,
};
use bornera_core::RetainedBytes;
use calandria::Interest;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProgressMode {
    Operation,
    Idle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Connecting,
    Open,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShutdownPhase {
    NotStarted,
    Started,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BeginBehavior {
    Succeed,
    PrematureComplete,
    Fail,
}

#[derive(Debug)]
pub(crate) struct ShutdownTransport {
    phase: Phase,
    write_ready: bool,
    shutdown: ShutdownPhase,
    begin_calls: usize,
    drive_calls: usize,
    remaining: usize,
    runnable: bool,
    behavior: BeginBehavior,
    progress_mode: ProgressMode,
    pressure: TransportPressure,
    pressure_after_begin: Option<TransportPressure>,
    limits: TransportLimits,
}

impl ShutdownTransport {
    pub(crate) const fn complete_before_begin() -> Self {
        Self::new(0, true)
    }

    pub(crate) const fn steps(remaining: usize) -> Self {
        Self::new(remaining, false)
    }

    pub(crate) const fn waiting() -> Self {
        let mut transport = Self::new(1, false);
        transport.runnable = false;
        transport
    }

    pub(crate) const fn begin_failure() -> Self {
        let mut transport = Self::new(0, false);
        transport.behavior = BeginBehavior::Fail;
        transport
    }

    pub(crate) const fn idle_progress() -> Self {
        let mut transport = Self::new(1, false);
        transport.progress_mode = ProgressMode::Idle;
        transport
    }

    pub(crate) const fn pressure_after_begin(
        pressure: TransportPressure,
        limit: RetainedBytes,
    ) -> Self {
        let mut transport = Self::new(0, false);
        transport.pressure_after_begin = Some(pressure);
        transport.limits = TransportLimits::new(limit);
        transport
    }

    pub(crate) const fn writable(mut self) -> Self {
        self.write_ready = true;
        self
    }

    pub(crate) const fn idle_control_work(mut self) -> Self {
        self.shutdown = ShutdownPhase::Started;
        self.remaining = 1;
        self.progress_mode = ProgressMode::Idle;
        self
    }

    pub(crate) fn allow_transport_work(&mut self) {
        self.runnable = true;
    }

    pub(crate) const fn begin_calls(&self) -> usize {
        self.begin_calls
    }

    pub(crate) const fn drive_calls(&self) -> usize {
        self.drive_calls
    }

    const fn new(remaining: usize, complete_before_begin: bool) -> Self {
        Self {
            phase: Phase::Connecting,
            write_ready: false,
            shutdown: ShutdownPhase::NotStarted,
            begin_calls: 0,
            drive_calls: 0,
            remaining,
            runnable: true,
            behavior: if complete_before_begin {
                BeginBehavior::PrematureComplete
            } else {
                BeginBehavior::Succeed
            },
            progress_mode: ProgressMode::Operation,
            pressure: TransportPressure::ZERO,
            pressure_after_begin: None,
            limits: TransportLimits::new(RetainedBytes::new(64)),
        }
    }
}

impl io::Read for ShutdownTransport {
    fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
        Err(io::ErrorKind::WouldBlock.into())
    }
}

impl io::Write for ShutdownTransport {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self.phase != Phase::Open || !self.write_ready {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        self.write_ready = false;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl SlotTransport for ShutdownTransport {
    fn drive_establishment(
        &mut self,
        _policy: TcpSocketPolicy,
        _budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        self.phase = Phase::Open;
        Ok(TransportProgress::operation())
    }

    fn drive_transport(
        &mut self,
        _budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        self.drive_calls = self.drive_calls.saturating_add(1);
        match self.progress_mode {
            ProgressMode::Operation => {
                self.remaining = self.remaining.saturating_sub(1);
                Ok(TransportProgress::operation())
            }
            ProgressMode::Idle => Ok(TransportProgress::IDLE),
        }
    }

    fn begin_shutdown(
        &mut self,
        _budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        self.begin_calls = self.begin_calls.saturating_add(1);
        self.shutdown = ShutdownPhase::Started;
        if let Some(pressure) = self.pressure_after_begin.take() {
            self.pressure = pressure;
        }
        if self.behavior == BeginBehavior::Fail {
            return Err(shutdown_error());
        }
        Ok(match self.progress_mode {
            ProgressMode::Operation => TransportProgress::operation(),
            ProgressMode::Idle => TransportProgress::IDLE,
        })
    }

    fn can_establish(&self) -> bool {
        self.phase == Phase::Connecting
    }

    fn has_transport_work(&self) -> bool {
        self.shutdown == ShutdownPhase::Started && self.remaining != 0 && self.runnable
    }

    fn is_shutdown_complete(&self) -> bool {
        self.behavior == BeginBehavior::PrematureComplete
            || (self.shutdown == ShutdownPhase::Started && self.remaining == 0)
    }

    fn is_open(&self) -> bool {
        self.phase == Phase::Open
    }

    fn can_read(&self) -> bool {
        false
    }

    fn can_write(&self) -> bool {
        self.phase == Phase::Open && self.write_ready
    }

    fn desired_interest(&self, has_writes: bool) -> Interest {
        if has_writes || self.has_transport_work() {
            Interest::READ_WRITE
        } else {
            Interest::READABLE
        }
    }

    fn pressure(&self) -> TransportPressure {
        self.pressure
    }

    fn pressure_limit(&self) -> TransportLimits {
        self.limits
    }

    fn clear_read(&mut self) {}

    fn clear_write(&mut self) {
        self.write_ready = false;
    }
}

fn shutdown_error() -> TransportError {
    TransportError::new(TransportDiagnostic::new(
        TransportFailurePhase::TransportWrite,
        TransportFailureKind::Protocol,
        io::ErrorKind::InvalidData,
        Some(7),
    ))
}
