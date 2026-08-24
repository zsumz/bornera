//! Private capability cleanup and readiness fault evidence.

use std::{error::Error, net::TcpListener, num::NonZeroUsize};

use bornera_core::{
    ConnectionEpoch, ConnectionId, ConnectionLimits, ConnectionPhase, Deadline, EndpointId,
    FrameDecoder, LaneId, MatchKey, MatchKeySpace, Moment, OperationOptions, ReserveError,
};
use calandria::{Readiness, ResourceOwnerId, Retained, RetainedBytes, TimerOwnerId};

use crate::{
    ConnectionConfig, ConnectionIdentity, ConnectionSetConfig, ConnectionSlotLimits, DecoderLimits,
    EngineCommitError, EngineError, InboundClassifier, IoLimits, OutboundFrame, OwnerFailure,
    PublicationLimits, StandaloneConnection, StandaloneConnectionConfig,
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
    let mut connection = connection(listener.local_addr()?)?;
    let resource = connection.connection.resource();
    let (poller, resources) = (&mut connection.set.poller, &mut connection.set.resources);
    let (_, entry) = resources
        .get_mut(resource)
        .map_err(|_| std::io::Error::other("connection token disappeared"))?;
    let transport = entry
        .transport
        .as_mut()
        .ok_or_else(|| std::io::Error::other("connection owns no transport"))?;
    poller.deregister(transport, resource)?;

    assert!(matches!(
        connection.finalize(bornera_core::CloseReason::Requested),
        Err(EngineError::Mio(
            calandria_mio::MioError::NotRegistered { .. }
        ))
    ));
    let entry = connection.set.entry(connection.connection)?;
    assert_eq!(entry.slot.core.snapshot().phase, ConnectionPhase::Closing);
    assert!(
        connection
            .drain_events()?
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
fn reserve_observing_core_poison_latches_the_connection() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let mut connection = connection(listener.local_addr()?)?;
    let port = connection.port();
    let _fault = connection
        .set
        .entry_mut(connection.connection)?
        .slot
        .core
        .recover();

    assert!(matches!(
        connection.reserve(Moment::ORIGIN, options()),
        Err(ReserveError::OwnerPoisoned)
    ));
    assert_eq!(
        connection.snapshot()?.owner_failure,
        Some(OwnerFailure::Core)
    );
    port.close()?;
    let report = connection
        .try_recover()
        .map_err(|_| std::io::Error::other("poisoned owner rejected recovery"))?;
    assert_eq!(report.reason, OwnerFailure::Core);
    Ok(())
}

#[test]
fn commit_observing_core_poison_returns_affine_ownership() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let mut connection = connection(listener.local_addr()?)?;
    let permit = connection.reserve(Moment::ORIGIN, options())?;
    let operation = permit.operation_id();
    let frame = OutboundFrame::copy_from_slice(&[7])?;
    let _fault = connection
        .set
        .entry_mut(connection.connection)?
        .slot
        .core
        .recover();

    let Err(error) = connection.commit(permit, frame) else {
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
    assert_eq!(
        connection.snapshot()?.owner_failure,
        Some(OwnerFailure::Core)
    );
    drop(permit);

    let report = connection
        .try_recover()
        .map_err(|_| std::io::Error::other("poisoned owner rejected recovery"))?;
    assert_eq!(report.reason, OwnerFailure::Core);
    Ok(())
}

#[test]
fn outcome_capacity_rejection_rolls_back_the_new_core_permit() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let mut connection = connection(listener.local_addr()?)?;
    let permit = connection.reserve(Moment::ORIGIN, options())?;
    let operation = connection.commit(permit, OutboundFrame::copy_from_slice(&[7])?)?;
    connection.cancel(operation)?;

    let retained = connection.reserve(Moment::ORIGIN, options())?;
    assert!(matches!(
        connection.reserve(Moment::ORIGIN, options()),
        Err(ReserveError::OperationCapacity)
    ));
    let snapshot = connection.snapshot()?.connection;
    assert_eq!(snapshot.reserved_permits, 1);
    assert_eq!(snapshot.owned_operations, 1);
    drop(retained);
    let snapshot = connection.snapshot()?.connection;
    assert_eq!(snapshot.reserved_permits, 0);
    assert_eq!(snapshot.owned_operations, 0);
    Ok(())
}

fn options() -> OperationOptions {
    OperationOptions::until(Deadline::at(Moment::from_nanos(20)))
        .session()
        .retained_bytes(RetainedBytes::new(1))
        .write_retained_bytes(RetainedBytes::new(1))
}

fn connection(
    address: std::net::SocketAddr,
) -> Result<StandaloneConnection<Decoder, Classifier>, Box<dyn Error>> {
    let core = ConnectionLimits::new(
        2,
        RetainedBytes::new(16),
        2,
        RetainedBytes::new(16),
        MatchKeySpace::new(0, 1)?,
    )?;
    let two = NonZeroUsize::new(2)
        .ok_or_else(|| std::io::Error::other("fixture limit must be nonzero"))?;
    let limits = ConnectionSlotLimits::new(
        core,
        DecoderLimits::new(RetainedBytes::new(16), RetainedBytes::new(16)),
        IoLimits::new(two, two),
        PublicationLimits::new(two),
    )?;
    let identity = ConnectionIdentity::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        ConnectionEpoch::new(4),
    );
    let exact = ConnectionConfig::new(
        identity,
        address,
        Deadline::at(Moment::from_nanos(u64::MAX)),
        TimerOwnerId::new(6),
    );
    Ok(StandaloneConnection::connect(
        StandaloneConnectionConfig::new(ConnectionSetConfig::new(ResourceOwnerId::new(5)), exact),
        limits,
        Decoder,
        Classifier,
    )?)
}
