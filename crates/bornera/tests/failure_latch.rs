//! Fatal production errors permanently fence normal owner mutation.

use std::{
    error::Error,
    fmt,
    net::{Shutdown, TcpListener},
    num::NonZeroUsize,
    sync::mpsc,
    thread,
};

use bornera::{
    ConnectionConfig, ConnectionIdentity, ConnectionSetConfig, ConnectionSlotLimits, DecoderLimits,
    EngineCommitError, EngineError, EngineInvariant, InboundClassifier, IoLimits, OutboundFrame,
    OwnerFailure, PublicationLimits, StandaloneConnection, StandaloneConnectionConfig,
    TransportLimits,
};
use bornera_core::{
    CloseReason, ConnectionEpoch, ConnectionId, ConnectionLimits, Deadline, EndpointId,
    FrameDecoder, LaneId, MatchKey, MatchKeySpace, Moment, OperationId, OperationOptions,
    ReserveError, RetainedBytes,
};
use calandria::{Next, ResourceOwnerId, Retained, Span, TimerOwnerId};

#[test]
fn fatal_publication_failure_fences_every_normal_owner_api() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let (release, hold) = mpsc::channel();
    let server = thread::spawn(move || -> std::io::Result<()> {
        let _stream = listener.accept()?.0;
        hold.recv()
            .map_err(|_| std::io::Error::other("failure-latch release was dropped"))
    });
    let lifecycle = NonZeroUsize::new(2)
        .ok_or_else(|| std::io::Error::other("lifecycle capacity must be nonzero"))?;
    let (config, limits) = engine_parts(address, lifecycle)?;
    let mut engine = StandaloneConnection::connect(config, limits, Decoder, Classifier)?;
    run_until_open(&mut engine)?;
    engine.open_admission()?;
    let port = engine.port();
    let permit = engine.reserve(Moment::ORIGIN, options())?;
    let operation = permit.operation_id();
    let frame = OutboundFrame::copy_from_slice(&[7])?;

    let fatal = engine
        .finalize(CloseReason::Requested)
        .err()
        .ok_or_else(|| std::io::Error::other("full lifecycle stream accepted closure"))?;
    assert!(matches!(
        fatal,
        EngineError::Invariant(EngineInvariant::LifecyclePublication(_))
    ));
    assert_eq!(
        engine.snapshot()?.owner_failure,
        Some(OwnerFailure::OwnerInvariant)
    );
    assert!(matches!(
        engine.reserve(Moment::ORIGIN, options()),
        Err(ReserveError::OwnerPoisoned)
    ));

    let Err(rejected) = engine.commit(permit, frame) else {
        return Err(std::io::Error::other("failed owner accepted a prepared frame").into());
    };
    let (permit, frame) = match rejected {
        EngineCommitError::OwnerFailed {
            reason,
            permit,
            frame,
        } => {
            assert_eq!(reason, OwnerFailure::OwnerInvariant);
            (permit, frame)
        }
        other => return Err(std::io::Error::other(other.to_string()).into()),
    };
    assert_eq!(permit.operation_id(), operation);
    assert_eq!(frame.as_bytes(), &[7]);
    drop(permit);

    assert_owner_failed(&engine.open_admission())?;
    assert_owner_failed(&engine.cancel(OperationId::new(99)))?;
    assert_owner_failed(&engine.begin_drain())?;
    assert_owner_failed(&engine.finalize(CloseReason::Requested))?;
    assert_owner_failed(&engine.poll_io(Span::ZERO))?;
    assert_owner_failed(&engine.turn_component(Moment::ORIGIN))?;
    port.close()?;

    let report = engine
        .try_recover()
        .map_err(|_| std::io::Error::other("failed owner rejected recovery"))?;
    assert_eq!(report.reason, OwnerFailure::OwnerInvariant);
    assert!(report.unmatched_writes.is_empty());
    release.send(())?;
    join(server)?;
    Ok(())
}

#[test]
fn fatal_failure_during_a_turn_cannot_look_like_clean_stop() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let server = thread::spawn(move || -> std::io::Result<()> {
        let (stream, _) = listener.accept()?;
        stream.shutdown(Shutdown::Both)
    });
    let (config, limits) = engine_parts(address, NonZeroUsize::MIN)?;
    let mut engine = StandaloneConnection::connect(config, limits, Decoder, Classifier)?;
    let mut observed_failure = false;
    for _ in 0..128 {
        match engine.turn_component(Moment::ORIGIN) {
            Err(EngineError::OwnerFailed(OwnerFailure::OwnerInvariant)) => {
                observed_failure = true;
                break;
            }
            Err(error) => return Err(error.into()),
            Ok(turn) if turn.next() != Next::Now => {
                engine.poll_io(Span::from_nanos(10_000_000))?;
            }
            Ok(_) => {}
        }
    }
    assert!(observed_failure);
    assert_eq!(
        engine.snapshot()?.owner_failure,
        Some(OwnerFailure::OwnerInvariant)
    );
    let report = engine
        .try_recover()
        .map_err(|_| std::io::Error::other("turn failure rejected recovery"))?;
    assert_eq!(report.reason, OwnerFailure::OwnerInvariant);
    assert_eq!(report.events.len(), 3);
    join(server)?;
    Ok(())
}

fn options() -> OperationOptions {
    OperationOptions::until(Deadline::at(Moment::from_nanos(u64::MAX)))
        .session()
        .retained_bytes(RetainedBytes::new(1))
        .write_retained_bytes(RetainedBytes::new(1))
}

fn assert_owner_failed<T>(result: &Result<T, EngineError>) -> Result<(), Box<dyn Error>> {
    if matches!(
        result,
        Err(EngineError::OwnerFailed(OwnerFailure::OwnerInvariant))
    ) {
        Ok(())
    } else {
        Err(std::io::Error::other("normal API did not return the latched failure").into())
    }
}

fn run_until_open(engine: &mut TestEngine) -> Result<(), Box<dyn Error>> {
    for _ in 0..128 {
        let turn = engine.turn_component(Moment::ORIGIN)?;
        if engine.is_transport_open()? {
            return Ok(());
        }
        if turn.next() != Next::Now {
            engine.poll_io(Span::from_nanos(10_000_000))?;
        }
    }
    Err(std::io::Error::other("transport did not open within bounded turns").into())
}

type TestEngine = StandaloneConnection<Decoder, Classifier>;

#[derive(Debug)]
struct Decoder;

impl FrameDecoder for Decoder {
    type Frame = Frame;
    type Error = DecodeError;

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
    type Error = DecodeError;

    fn reply_key(&mut self, _frame: &Frame) -> Result<MatchKey, Self::Error> {
        Ok(MatchKey::new(0))
    }
}

#[derive(Debug)]
struct DecodeError;

impl fmt::Display for DecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("failure-latch decoder failed")
    }
}

impl Error for DecodeError {}

fn engine_parts(
    address: std::net::SocketAddr,
    lifecycle: NonZeroUsize,
) -> Result<(StandaloneConnectionConfig, ConnectionSlotLimits), Box<dyn Error>> {
    let connection = ConnectionLimits::new(
        4,
        RetainedBytes::new(64),
        4,
        RetainedBytes::new(64),
        MatchKeySpace::new(0, 3)?,
    )?;
    let four = NonZeroUsize::new(4)
        .ok_or_else(|| std::io::Error::other("fixture limit must be nonzero"))?;
    let limits = ConnectionSlotLimits::new(
        connection,
        DecoderLimits::new(RetainedBytes::new(16), RetainedBytes::new(16)),
        IoLimits::new(four, four),
        TransportLimits::new(RetainedBytes::ZERO),
        PublicationLimits::new(lifecycle),
    )?;
    let identity = ConnectionIdentity::new(
        EndpointId::new(1),
        LaneId::new(2),
        ConnectionId::new(3),
        ConnectionEpoch::new(4),
    );
    let connection = ConnectionConfig::new(
        identity,
        address,
        Deadline::at(Moment::from_nanos(u64::MAX)),
        TimerOwnerId::new(6),
    );
    Ok((
        StandaloneConnectionConfig::new(
            ConnectionSetConfig::new(ResourceOwnerId::new(5)),
            connection,
        ),
        limits,
    ))
}

fn join(handle: thread::JoinHandle<std::io::Result<()>>) -> Result<(), Box<dyn Error>> {
    Ok(handle
        .join()
        .map_err(|_| std::io::Error::other("test server panicked"))??)
}
