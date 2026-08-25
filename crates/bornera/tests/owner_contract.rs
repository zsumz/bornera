//! Lifecycle publication, explicit recovery, and decoder-failure evidence.

mod support;

use std::{
    error::Error,
    io::{Read, Write},
    net::TcpListener,
    num::NonZeroUsize,
    sync::mpsc,
    thread,
};

use bornera::{
    ConnectionEvent, EngineInvariant, OutboundFrame, OwnerFailure, StandaloneConnection,
    TransportState,
};
use bornera_core::{
    CloseReason, Delivery, FrameDecoder, OperationOptions, RetainedBytes as CoreRetainedBytes,
};
use calandria::{Deadline, Moment, Next, RetainedBytes, Span};

use support::framing::{DecodeError, FRAME_BYTES, KeyClassifier, TestFrame, request};
use support::{TestEngine, engine, engine_parts, engine_parts_with_events};
#[test]
fn lifecycle_edges_are_separate_sequenced_and_snapshot_backed() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let (release, hold) = mpsc::channel();
    let server = thread::spawn(move || -> std::io::Result<()> {
        let _stream = listener.accept()?.0;
        hold.recv()
            .map_err(|_| std::io::Error::other("lifecycle release was dropped"))
    });
    let mut engine = engine(address)?;
    run_until_open(&mut engine)?;
    engine.open_admission()?;
    engine.finalize(CloseReason::Requested)?;
    let events: Vec<_> = engine.drain_events()?.collect();
    assert!(matches!(
        events.as_slice(),
        [
            ConnectionEvent::TransportOpened { sequence: 1, .. },
            ConnectionEvent::AdmissionOpened { sequence: 2, .. },
            ConnectionEvent::Closing {
                sequence: 3,
                reason: CloseReason::Requested,
                ..
            },
            ConnectionEvent::Closed {
                sequence: 4,
                reason: CloseReason::Requested,
                ..
            },
        ]
    ));
    let snapshot = engine.snapshot()?;
    assert_eq!(snapshot.event_sequence, 4);
    assert_eq!(snapshot.pending_events, 0);
    assert_eq!(
        snapshot.connection.close_reason,
        Some(CloseReason::Requested)
    );
    assert_eq!(snapshot.transport, TransportState::Closed);
    release.send(())?;
    join(server)?;
    Ok(())
}

#[test]
fn recovery_returns_an_exact_not_sent_frame() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let mut engine = engine(listener.local_addr()?)?;
    let permit = engine.reserve(Moment::ORIGIN, options())?;
    let bytes = request(permit.match_key(), 7);
    let operation = engine.commit(permit, OutboundFrame::copy_from_slice(&bytes)?)?;
    let report = engine.abandon(OwnerFailure::OwnerInvariant);
    assert_eq!(report.operations.len(), 1);
    assert_eq!(report.operations[0].operation, operation);
    assert_eq!(report.operations[0].delivery, Delivery::NotSent);
    assert_eq!(
        report.operations[0]
            .frame
            .as_ref()
            .map(OutboundFrame::as_bytes),
        Some(&bytes[..])
    );
    assert!(!report.ownership_diverged);
    Ok(())
}

#[test]
fn recovery_retains_terminal_outcomes_and_rejected_lifecycle_edges() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let (release, hold) = mpsc::channel();
    let server = thread::spawn(move || -> std::io::Result<()> {
        let _stream = listener.accept()?.0;
        hold.recv()
            .map_err(|_| std::io::Error::other("recovery release was dropped"))
    });
    let capacity = NonZeroUsize::new(2).ok_or(std::io::Error::other("zero event capacity"))?;
    let (config, limits) = engine_parts_with_events(address, capacity)?;
    let mut engine = StandaloneConnection::connect(
        config,
        limits,
        support::framing::FixedDecoder { bytes: Vec::new() },
        KeyClassifier,
    )?;
    run_until_open(&mut engine)?;
    engine.open_admission()?;
    let permit = engine.reserve(Moment::ORIGIN, options())?;
    let bytes = request(permit.match_key(), 11);
    let operation = engine.commit(permit, OutboundFrame::copy_from_slice(&bytes)?)?;
    let error = engine
        .finalize(CloseReason::Requested)
        .err()
        .ok_or(std::io::Error::other(
            "full lifecycle stream accepted close",
        ))?;
    assert!(matches!(
        error,
        bornera::EngineError::Invariant(EngineInvariant::LifecyclePublication(_))
    ));
    let report = engine
        .try_recover()
        .map_err(|_| std::io::Error::other("failed owner rejected recovery"))?;
    assert!(report.operations.is_empty());
    assert_eq!(report.outcomes.len(), 1);
    assert_eq!(report.outcomes[0].operation(), operation);
    assert!(matches!(
        report.events.as_slice(),
        [
            ConnectionEvent::TransportOpened { sequence: 1, .. },
            ConnectionEvent::AdmissionOpened { sequence: 2, .. },
            ConnectionEvent::Closing { sequence: 3, .. },
        ]
    ));
    release.send(())?;
    join(server)?;
    Ok(())
}

#[test]
fn recovery_after_write_completion_is_conservatively_possible_send() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let (release, hold) = mpsc::channel();
    let server = thread::spawn(move || -> std::io::Result<()> {
        let (mut stream, _) = listener.accept()?;
        let mut frame = [0_u8; FRAME_BYTES];
        stream.read_exact(&mut frame)?;
        hold.recv()
            .map_err(|_| std::io::Error::other("recovery release was dropped"))
    });
    let mut engine = engine(address)?;
    let permit = engine.reserve(Moment::ORIGIN, options())?;
    let bytes = request(permit.match_key(), 9);
    let operation = engine.commit(permit, OutboundFrame::copy_from_slice(&bytes)?)?;
    run_until_written(&mut engine)?;
    let report = engine.abandon(OwnerFailure::Core);
    assert_eq!(report.operations.len(), 1);
    assert_eq!(report.operations[0].operation, operation);
    assert_eq!(report.operations[0].delivery, Delivery::PossiblySent);
    assert!(report.operations[0].frame.is_none());
    assert!(!report.ownership_diverged);
    release.send(())?;
    join(server)?;
    Ok(())
}

#[test]
fn next_frame_errors_use_mechanical_close_reasons() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let server = thread::spawn(move || listener.accept()?.0.write_all(&[1]));
    let (config, limits) = engine_parts(address)?;
    let mut engine = StandaloneConnection::connect(
        config,
        limits,
        ViolatingDecoder {
            violated: false,
            fail: false,
        },
        KeyClassifier,
    )?;
    run_until_stopped(&mut engine)?;
    assert_eq!(
        engine.snapshot()?.connection.close_reason,
        Some(CloseReason::InboundRetainedCapacity)
    );
    join(server)?;
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let server = thread::spawn(move || listener.accept()?.0.write_all(&[1]));
    let (config, limits) = engine_parts(address)?;
    let mut engine = StandaloneConnection::connect(
        config,
        limits,
        ViolatingDecoder {
            violated: false,
            fail: true,
        },
        KeyClassifier,
    )?;
    run_until_stopped(&mut engine)?;
    assert_eq!(
        engine.snapshot()?.connection.close_reason,
        Some(CloseReason::MalformedReply)
    );
    join(server)?;
    Ok(())
}

#[derive(Debug)]
struct ViolatingDecoder {
    violated: bool,
    fail: bool,
}

impl FrameDecoder for ViolatingDecoder {
    type Frame = TestFrame;
    type Error = DecodeError;

    fn feed(&mut self, _bytes: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }

    fn next_frame(&mut self) -> Result<Option<Self::Frame>, Self::Error> {
        if self.fail {
            return Err(DecodeError);
        }
        self.violated = true;
        Ok(None)
    }

    fn retained_bytes(&self) -> CoreRetainedBytes {
        if self.violated {
            CoreRetainedBytes::new(65)
        } else {
            CoreRetainedBytes::ZERO
        }
    }
}

fn options() -> OperationOptions {
    OperationOptions::until(Deadline::at(Moment::from_nanos(u64::MAX)))
        .session()
        .retained_bytes(RetainedBytes::new(FRAME_BYTES as u64))
        .write_retained_bytes(RetainedBytes::new(FRAME_BYTES as u64))
}

fn run_until_open(engine: &mut TestEngine) -> Result<(), Box<dyn Error>> {
    for _ in 0..128 {
        engine.turn_component(Moment::ORIGIN)?;
        if engine.is_transport_open()? {
            return Ok(());
        }
        engine.poll_io(Span::from_nanos(10_000_000))?;
    }
    Err(std::io::Error::other("transport did not open within bounded turns").into())
}

fn run_until_written(engine: &mut TestEngine) -> Result<(), Box<dyn Error>> {
    for _ in 0..128 {
        engine.turn_component(Moment::ORIGIN)?;
        if engine.snapshot()?.queued_write_frames == 0 {
            return Ok(());
        }
        engine.poll_io(Span::from_nanos(10_000_000))?;
    }
    Err(std::io::Error::other("write did not complete within bounded turns").into())
}

fn run_until_stopped<D>(
    engine: &mut StandaloneConnection<D, KeyClassifier>,
) -> Result<(), Box<dyn Error>>
where
    D: FrameDecoder<Frame = TestFrame, Error = DecodeError>,
{
    for _ in 0..128 {
        let turn = engine.turn_component(Moment::ORIGIN)?;
        if turn.next() == Next::Stop {
            return Ok(());
        }
        if turn.next() != Next::Now {
            engine.poll_io(Span::from_nanos(10_000_000))?;
        }
    }
    Err(std::io::Error::other("engine did not stop within bounded turns").into())
}

fn join(handle: thread::JoinHandle<std::io::Result<()>>) -> Result<(), Box<dyn Error>> {
    Ok(handle
        .join()
        .map_err(|_| std::io::Error::other("test server panicked"))??)
}
