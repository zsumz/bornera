//! Focused evidence for generic bounded complete-frame aggregate ownership.

use std::{error::Error, num::NonZeroUsize};

use bornera_core::{
    CloseReason, CommitErrorKind, ConnectionCore, ConnectionCoreError, ConnectionEffect,
    ConnectionEpoch, ConnectionId, ConnectionInput, ConnectionLimits, Deadline, Delivery, EffectId,
    EndpointId, FrameCommitFailure, LaneId, MatchKeySpace, Moment, OperationOptions,
    OperationOutcome, RetainedBytes, WriteFrame, WriteProgressError,
};

const EPOCH: ConnectionEpoch = ConnectionEpoch::new(7);

#[derive(Clone, Debug, Eq, PartialEq)]
struct TestFrame {
    bytes: Vec<u8>,
    retained: RetainedBytes,
}

impl TestFrame {
    fn new(bytes: &[u8]) -> Self {
        Self {
            bytes: bytes.to_vec(),
            retained: RetainedBytes::new(u64::try_from(bytes.len()).unwrap_or(u64::MAX)),
        }
    }

    fn retaining(bytes: &[u8], retained: u64) -> Self {
        Self {
            bytes: bytes.to_vec(),
            retained: RetainedBytes::new(retained),
        }
    }
}

impl WriteFrame for TestFrame {
    fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    fn retained_bytes(&self) -> RetainedBytes {
        self.retained
    }
}

fn owner(
    max_frames: usize,
    max_retained: u64,
) -> Result<ConnectionCore<TestFrame>, Box<dyn Error>> {
    Ok(ConnectionCore::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        EPOCH,
        ConnectionLimits::new(
            max_frames,
            RetainedBytes::new(max_retained),
            max_frames,
            RetainedBytes::new(max_retained),
            MatchKeySpace::new(0, u32::try_from(max_frames.saturating_sub(1))?)?,
        )?,
    ))
}

fn commit(
    owner: &mut ConnectionCore<TestFrame>,
    frame: TestFrame,
) -> Result<EffectId, Box<dyn Error>> {
    let retained = frame.retained_bytes();
    let permit = owner.reserve(
        Moment::ORIGIN,
        OperationOptions::until(Deadline::at(Moment::from_nanos(10)))
            .session()
            .retained_bytes(retained)
            .write_bytes(retained),
    )?;
    owner.commit(permit, frame)?;
    owner
        .front_write(NonZeroUsize::MAX)
        .map(|front| front.effect)
        .ok_or_else(|| std::io::Error::other("aggregate retained no write front").into())
}

#[test]
fn arbitrary_and_empty_complete_frames_have_no_protocol_minimum() -> Result<(), Box<dyn Error>> {
    let mut owner = owner(2, 8)?;
    let first = commit(&mut owner, TestFrame::new(&[9]))?;
    commit(&mut owner, TestFrame::new(&[]))?;
    assert_eq!(
        owner
            .front_write(NonZeroUsize::MAX)
            .map(|front| front.bytes),
        Some(&[9][..])
    );
    owner.advance_write(EPOCH, first, 1)?;
    let empty = owner
        .front_write(NonZeroUsize::MAX)
        .ok_or_else(|| std::io::Error::other("empty frame was not retained"))?;
    assert!(empty.bytes.is_empty());
    owner.advance_write(EPOCH, empty.effect, 0)?;
    assert_eq!(owner.queued_write_frames(), 0);
    Ok(())
}

#[test]
fn partial_progress_crosses_delivery_once_and_preserves_fifo() -> Result<(), Box<dyn Error>> {
    let mut owner = owner(2, 16)?;
    let first = commit(&mut owner, TestFrame::new(&[1, 2, 3, 4]))?;
    commit(&mut owner, TestFrame::new(&[5]))?;
    assert_eq!(
        owner
            .front_write(NonZeroUsize::new(2).ok_or_else(|| std::io::Error::other("zero"))?)
            .map(|front| front.bytes),
        Some(&[1, 2][..])
    );
    owner.advance_write(EPOCH, first, 2)?;
    assert_eq!(
        owner
            .front_write(NonZeroUsize::MAX)
            .map(|front| front.bytes),
        Some(&[3, 4][..])
    );
    owner.advance_write(EPOCH, first, 2)?;
    assert_eq!(
        owner
            .front_write(NonZeroUsize::MAX)
            .map(|front| front.bytes),
        Some(&[5][..])
    );
    Ok(())
}

#[test]
fn invalid_epoch_effect_and_progress_leave_the_fifo_front_unchanged() -> Result<(), Box<dyn Error>>
{
    let mut owner = owner(2, 16)?;
    let effect = commit(&mut owner, TestFrame::new(&[1, 2, 3]))?;
    assert!(matches!(
        owner.advance_write(ConnectionEpoch::new(8), effect, 1),
        Err(ConnectionCoreError::Write(
            WriteProgressError::StaleEpoch { .. }
        ))
    ));
    assert!(matches!(
        owner.advance_write(EPOCH, EffectId::new(effect.get() + 1), 1),
        Err(ConnectionCoreError::Write(
            WriteProgressError::OutOfOrderEffect { .. }
        ))
    ));
    assert!(matches!(
        owner.advance_write(EPOCH, effect, 4),
        Err(ConnectionCoreError::Write(
            WriteProgressError::ExceedsRemaining { .. }
        ))
    ));
    assert_eq!(
        owner
            .front_write(NonZeroUsize::MAX)
            .map(|front| front.bytes),
        Some(&[1, 2, 3][..])
    );
    Ok(())
}

#[test]
fn byte_rejection_returns_the_exact_unsent_frame() -> Result<(), Box<dyn Error>> {
    let mut owner = owner(1, 3)?;
    let permit = owner.reserve(
        Moment::ORIGIN,
        OperationOptions::until(Deadline::at(Moment::from_nanos(10)))
            .session()
            .write_bytes(RetainedBytes::new(3)),
    )?;
    let frame = TestFrame::retaining(&[1], 4);
    let error = owner
        .commit(permit, frame.clone())
        .err()
        .ok_or_else(|| std::io::Error::other("oversized frame was accepted"))?;
    assert_eq!(
        error.failure(),
        FrameCommitFailure::Policy(CommitErrorKind::FrameTooLarge)
    );
    assert_eq!(error.delivery(), Delivery::NotSent);
    let (permit, returned) = error.into_parts();
    assert_eq!(returned, frame);
    drop(permit);
    assert_eq!(owner.queued_write_frames(), 0);
    Ok(())
}

#[test]
fn close_cleanup_releases_all_frames_with_conservative_delivery() -> Result<(), Box<dyn Error>> {
    let mut owner = owner(2, 16)?;
    let first = commit(&mut owner, TestFrame::new(&[1, 2, 3]))?;
    commit(&mut owner, TestFrame::new(&[4, 5]))?;
    owner.advance_write(EPOCH, first, 2)?;

    let closed = owner.apply(ConnectionInput::CloseRequested {
        epoch: EPOCH,
        reason: CloseReason::TransportLost,
    })?;
    let deliveries: Vec<_> = closed
        .effects()
        .iter()
        .filter_map(|effect| match effect {
            ConnectionEffect::PublishOutcome {
                outcome: OperationOutcome::Failed { delivery, .. },
                ..
            } => Some(*delivery),
            _ => None,
        })
        .collect();
    assert_eq!(deliveries, [Delivery::PossiblySent, Delivery::NotSent]);
    assert_eq!(owner.queued_write_frames(), 0);
    assert_eq!(owner.buffered_write_bytes(), RetainedBytes::ZERO);
    Ok(())
}
