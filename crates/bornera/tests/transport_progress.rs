//! Bounded transport-local progression and scheduler contract regressions.

use std::{convert::Infallible, error::Error, io, num::NonZeroUsize};

use bornera::{
    ConnectionIdentity, ConnectionSlot, ConnectionSlotConfig, ConnectionSlotLimits, DecoderLimits,
    EngineError, EngineInvariant, InboundClassifier, IoLimits, OwnerFailure, PublicationLimits,
    SlotTransport, TcpSocketPolicy, TransportBudget, TransportError, TransportProgress,
};
use bornera_core::{
    CompletionMode, ConnectionEpoch, ConnectionId, ConnectionLimits, Deadline, EndpointId,
    FrameDecoder, LaneId, MatchKey, MatchKeySpace, Moment, OperationOptions, RetainedBytes,
};
use calandria::{Interest, TimerOwnerId};

#[test]
fn deadline_budget_retains_immediate_transport_work() -> Result<(), Box<dyn Error>> {
    let mut slot = slot(1)?;
    let mut transport = ProgressTransport::new(TransportProgress::operation());
    let opened = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;
    assert!(opened.saturated());
    slot.open_admission()?;
    let options = OperationOptions::until(Deadline::at(Moment::from_nanos(1)))
        .completion_mode(CompletionMode::ReplyExpected)
        .retained_bytes(RetainedBytes::new(1))
        .write_retained_bytes(RetainedBytes::new(1));
    let permit = slot.reserve(Moment::ORIGIN, options)?;
    let _operation = slot.commit(permit, bornera::OutboundFrame::copy_from_slice(&[1])?)?;

    let deadline = slot.drive_quantum(Moment::from_nanos(1), Some(&mut transport))?;
    assert_eq!(deadline.work(), 1);
    assert!(deadline.saturated());

    let control = slot.drive_quantum(Moment::from_nanos(1), Some(&mut transport))?;
    assert_eq!(control.work(), 1);
    assert!(!control.saturated());
    Ok(())
}

#[test]
fn advertised_transport_work_must_report_progress() -> Result<(), Box<dyn Error>> {
    let mut slot = slot(1)?;
    let mut transport = ProgressTransport::new(TransportProgress::IDLE);
    let _opened = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;

    let error = slot
        .drive_quantum(Moment::ORIGIN, Some(&mut transport))
        .err()
        .ok_or_else(|| io::Error::other("idle transport work was accepted"))?;
    assert!(matches!(
        error,
        EngineError::Invariant(EngineInvariant::TransportNoProgress)
    ));
    assert_eq!(
        slot.snapshot().owner_failure,
        Some(OwnerFailure::OwnerInvariant)
    );
    Ok(())
}

#[test]
fn transport_progress_cannot_exceed_hard_bounds() -> Result<(), Box<dyn Error>> {
    for reported in [
        TransportProgress::new(nonzero(2)?, 0, 0),
        TransportProgress::new(NonZeroUsize::MIN, 9, 0),
    ] {
        let mut slot = slot(4)?;
        let mut transport = ProgressTransport::new(reported);
        let error = slot
            .drive_quantum(Moment::ORIGIN, Some(&mut transport))
            .err()
            .ok_or_else(|| io::Error::other("over-budget transport progress was accepted"))?;
        assert!(matches!(
            error,
            EngineError::Invariant(EngineInvariant::TransportProgressContract {
                reported: observed,
                ..
            }) if observed == reported
        ));
    }
    Ok(())
}

#[test]
fn preopened_transport_is_rejected_before_policy_bypass() -> Result<(), Box<dyn Error>> {
    let mut slot = slot(1)?;
    let mut transport = ProgressTransport::preopened();
    let error = slot
        .drive_quantum(Moment::ORIGIN, Some(&mut transport))
        .err()
        .ok_or_else(|| io::Error::other("preopened transport bypassed establishment policy"))?;
    assert!(matches!(
        error,
        EngineError::Invariant(EngineInvariant::TransportOpenedBeforeEstablishment)
    ));
    Ok(())
}

#[test]
fn transport_work_remains_after_write_complete_releases_the_frame() -> Result<(), Box<dyn Error>> {
    let mut slot = slot(1)?;
    let mut transport = ProgressTransport::buffered_write();
    let _opened = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;
    slot.open_admission()?;
    let options = OperationOptions::until(Deadline::at(Moment::from_nanos(50)))
        .completion_mode(CompletionMode::WriteComplete)
        .retained_bytes(RetainedBytes::new(1))
        .write_retained_bytes(RetainedBytes::new(1));
    let permit = slot.reserve(Moment::ORIGIN, options)?;
    let _operation = slot.commit(permit, bornera::OutboundFrame::copy_from_slice(&[7])?)?;

    let accepted = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;
    assert_eq!(slot.snapshot().queued_write_frames, 0);
    assert_eq!(slot.snapshot().pending_outcomes, 1);
    assert!(accepted.saturated());

    let drained = slot.drive_quantum(Moment::ORIGIN, Some(&mut transport))?;
    assert_eq!(drained.work(), 1);
    assert!(!drained.saturated());
    Ok(())
}

fn slot(operations: usize) -> Result<ConnectionSlot<Decoder, Classifier>, Box<dyn Error>> {
    let core = ConnectionLimits::new(
        4,
        RetainedBytes::new(64),
        4,
        RetainedBytes::new(64),
        MatchKeySpace::new(0, 3)?,
    )?;
    let limits = ConnectionSlotLimits::new(
        core,
        DecoderLimits::new(RetainedBytes::new(8), RetainedBytes::new(8)),
        IoLimits::new(nonzero(operations)?, nonzero(8)?),
        PublicationLimits::new(nonzero(8)?),
    )?;
    let identity = ConnectionIdentity::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        ConnectionEpoch::new(4),
    );
    Ok(ConnectionSlot::new(
        ConnectionSlotConfig::new(
            identity,
            Deadline::at(Moment::from_nanos(100)),
            TimerOwnerId::new(5),
        ),
        limits,
        Decoder,
        Classifier,
    )?)
}

fn nonzero(value: usize) -> Result<NonZeroUsize, Box<dyn Error>> {
    NonZeroUsize::new(value).ok_or_else(|| io::Error::other("test bound must be nonzero").into())
}

#[derive(Debug)]
struct Decoder;

impl FrameDecoder for Decoder {
    type Frame = ();
    type Error = Infallible;

    fn feed(&mut self, _bytes: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }

    fn next_frame(&mut self) -> Result<Option<Self::Frame>, Self::Error> {
        Ok(None)
    }

    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::ZERO
    }
}

#[derive(Debug)]
struct Classifier;

impl InboundClassifier<()> for Classifier {
    type Error = Infallible;

    fn reply_key(&mut self, _frame: &()) -> Result<MatchKey, Self::Error> {
        Ok(MatchKey::new(0))
    }
}

#[derive(Debug)]
struct ProgressTransport {
    open: bool,
    progress: Option<TransportProgress>,
    write_ready: bool,
}

impl ProgressTransport {
    const fn new(progress: TransportProgress) -> Self {
        Self {
            open: false,
            progress: Some(progress),
            write_ready: false,
        }
    }

    const fn buffered_write() -> Self {
        Self {
            open: false,
            progress: None,
            write_ready: true,
        }
    }

    const fn preopened() -> Self {
        Self {
            open: true,
            progress: None,
            write_ready: false,
        }
    }
}

impl io::Read for ProgressTransport {
    fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::from(io::ErrorKind::WouldBlock))
    }
}

impl io::Write for ProgressTransport {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if !self.write_ready {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        self.write_ready = false;
        self.progress = Some(TransportProgress::operation());
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl SlotTransport for ProgressTransport {
    fn drive_establishment(
        &mut self,
        _policy: TcpSocketPolicy,
        _budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        self.open = true;
        Ok(TransportProgress::operation())
    }

    fn drive_transport(
        &mut self,
        _budget: TransportBudget,
    ) -> Result<TransportProgress, TransportError> {
        Ok(self.progress.take().unwrap_or(TransportProgress::IDLE))
    }

    fn can_establish(&self) -> bool {
        !self.open
    }

    fn has_transport_work(&self) -> bool {
        self.progress.is_some()
    }

    fn is_open(&self) -> bool {
        self.open
    }

    fn can_read(&self) -> bool {
        false
    }

    fn can_write(&self) -> bool {
        self.open && self.write_ready
    }

    fn desired_interest(&self, _has_writes: bool) -> Interest {
        Interest::READABLE
    }

    fn clear_read(&mut self) {}

    fn clear_write(&mut self) {}
}
