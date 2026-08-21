//! Private capability cleanup and readiness fault evidence.

use std::{error::Error, net::TcpListener, num::NonZeroUsize};

use bornera_core::{
    ConnectionEpoch, ConnectionId, ConnectionLimits, ConnectionPhase, Deadline, EndpointId,
    FrameDecoder, LaneId, MatchKey, MatchKeySpace, Moment, OperationOptions, ReserveError,
};
use calandria::{Readiness, ResourceOwnerId, Retained, RetainedBytes, TimerOwnerId};

use crate::{
    ConnectionEngine, DecoderLimits, EngineCommitError, EngineConfig, EngineLimits,
    InboundClassifier, OutboundFrame, OwnerFailure, PublicationLimits, TurnLimits,
};

#[derive(Debug)]
struct Decoder;

impl FrameDecoder for Decoder {
    type Frame = Frame;
    type Error = std::convert::Infallible;

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
struct Frame;

impl Retained for Frame {
    fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes::ZERO
    }
}

#[derive(Debug)]
struct Classifier;

impl InboundClassifier<Frame> for Classifier {
    type Error = std::convert::Infallible;

    fn reply_key(&mut self, _frame: &Frame) -> Result<MatchKey, Self::Error> {
        Ok(MatchKey::new(0))
    }
}

#[test]
fn cleanup_failure_keeps_policy_closing_and_never_publishes_closed() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let mut engine = engine(listener.local_addr()?)?;
    let token = engine
        .transport
        .ok_or_else(|| std::io::Error::other("engine owns no transport token"))?;
    let _transport = engine
        .resources
        .remove(token)
        .map_err(|_| std::io::Error::other("transport fault injection failed"))?;

    assert!(engine.close().is_err());
    assert_eq!(engine.core.snapshot().phase, ConnectionPhase::Closing);
    assert!(
        engine
            .drain_events()
            .all(|event| !matches!(event, crate::ConnectionEvent::Closed { .. }))
    );
    Ok(())
}

#[test]
fn pending_connect_clears_cached_completion_readiness() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let mut transport = crate::PlaintextTransport::connect(listener.local_addr()?)?;
    transport.observe(Readiness::WRITABLE.union(Readiness::ERROR));
    assert!(transport.can_finish_connect());
    transport.clear_connect();
    assert!(!transport.can_finish_connect());
    Ok(())
}

#[test]
fn reserve_observing_core_poison_latches_the_engine() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let mut engine = engine(listener.local_addr()?)?;
    let epoch = engine.core.epoch();
    let port = engine.port();
    let _fault = engine.core.recover();

    assert!(matches!(
        engine.reserve(Moment::ORIGIN, options()),
        Err(ReserveError::OwnerPoisoned)
    ));
    assert_eq!(engine.snapshot().owner_failure, Some(OwnerFailure::Core));
    assert!(port.close(epoch).is_err());
    let report = engine
        .try_recover()
        .map_err(|_| std::io::Error::other("poisoned owner rejected recovery"))?;
    assert_eq!(report.reason, OwnerFailure::Core);
    Ok(())
}

#[test]
fn commit_observing_core_poison_returns_affine_ownership_and_latches() -> Result<(), Box<dyn Error>>
{
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let mut engine = engine(listener.local_addr()?)?;
    let epoch = engine.core.epoch();
    let port = engine.port();
    let permit = engine.reserve(Moment::ORIGIN, options())?;
    let operation = permit.operation_id();
    let frame = OutboundFrame::copy_from_slice(&[7])?;
    let _fault = engine.core.recover();

    let Err(error) = engine.commit(permit, frame) else {
        return Err(std::io::Error::other("poisoned core accepted a frame").into());
    };
    let (permit, frame) = match error {
        EngineCommitError::OwnerFailed {
            reason,
            permit,
            frame,
        } => {
            assert_eq!(reason, OwnerFailure::Core);
            (permit, frame)
        }
        other => return Err(std::io::Error::other(other.to_string()).into()),
    };
    assert_eq!(permit.operation_id(), operation);
    assert_eq!(frame.as_bytes(), &[7]);
    assert_eq!(engine.snapshot().owner_failure, Some(OwnerFailure::Core));
    assert!(port.close(epoch).is_err());
    drop(permit);

    let report = engine
        .try_recover()
        .map_err(|_| std::io::Error::other("poisoned owner rejected recovery"))?;
    assert_eq!(report.reason, OwnerFailure::Core);
    Ok(())
}

#[test]
fn outcome_capacity_rejection_rolls_back_the_new_core_permit() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let mut engine = engine(listener.local_addr()?)?;
    let permit = engine.reserve(Moment::ORIGIN, options())?;
    let operation = engine.commit(permit, OutboundFrame::copy_from_slice(&[7])?)?;
    engine.cancel(operation)?;

    let retained = engine.reserve(Moment::ORIGIN, options())?;
    assert!(matches!(
        engine.reserve(Moment::ORIGIN, options()),
        Err(ReserveError::OperationCapacity)
    ));
    let snapshot = engine.core.snapshot();
    assert_eq!(snapshot.reserved_permits, 1);
    assert_eq!(snapshot.owned_operations, 1);
    drop(retained);
    let snapshot = engine.core.snapshot();
    assert_eq!(snapshot.reserved_permits, 0);
    assert_eq!(snapshot.owned_operations, 0);
    Ok(())
}

fn options() -> OperationOptions {
    OperationOptions::until(Deadline::at(Moment::from_nanos(20)))
        .session()
        .retained_bytes(RetainedBytes::new(1))
        .write_bytes(RetainedBytes::new(1))
}

fn engine(
    address: std::net::SocketAddr,
) -> Result<ConnectionEngine<Decoder, Classifier>, Box<dyn Error>> {
    let connection = ConnectionLimits::new(
        2,
        RetainedBytes::new(16),
        2,
        RetainedBytes::new(16),
        MatchKeySpace::new(0, 1)?,
    )?;
    let two = NonZeroUsize::new(2)
        .ok_or_else(|| std::io::Error::other("fixture limit must be nonzero"))?;
    let limits = EngineLimits::new(
        connection,
        DecoderLimits::new(RetainedBytes::new(16), RetainedBytes::new(16)),
        TurnLimits::new(two, two, two, two),
        PublicationLimits::new(two),
    )?;
    Ok(ConnectionEngine::connect(
        EngineConfig {
            endpoint: EndpointId::new(1),
            lane: LaneId::new(2),
            connection: ConnectionId::new(3),
            epoch: ConnectionEpoch::new(4),
            address,
            resource_owner: ResourceOwnerId::new(5),
            timer_owner: TimerOwnerId::new(6),
        },
        limits,
        Decoder,
        Classifier,
    )?)
}
