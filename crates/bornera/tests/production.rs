//! Focused production-owner evidence over native loopback TCP.

mod support;

use std::{
    error::Error,
    io::{Read, Write},
    net::{Shutdown, TcpListener},
    sync::mpsc,
    thread,
};

use bornera::{EngineCommand, OutboundFrame, TransportState};
use bornera_core::{
    CloseReason, ConnectionEpoch, Delivery, OperationFailure, OperationId, OperationOptions,
    OperationOutcome, RetainedBytes,
};
use calandria::{Deadline, Lane, Moment, Next, Span};

use support::{FRAME_BYTES, TestEngine, engine, request};

#[test]
fn plaintext_duty_matches_a_fragmented_reply_and_stops_on_peer_close() -> Result<(), Box<dyn Error>>
{
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let server = thread::spawn(move || -> std::io::Result<()> {
        let (mut stream, _) = listener.accept()?;
        let mut frame = [0_u8; FRAME_BYTES];
        stream.read_exact(&mut frame)?;
        stream.write_all(&frame[..3])?;
        stream.write_all(&frame[3..])?;
        stream.shutdown(Shutdown::Both)
    });

    let mut engine = engine(address)?;
    let permit = engine.reserve(Moment::ORIGIN, options(8).session())?;
    let expected = request(permit.match_key(), 41);
    let operation = engine.commit(permit, OutboundFrame::copy_from_slice(&expected)?)?;
    run_until_stopped(&mut engine)?;

    let outcomes: Vec<_> = engine.drain_outcomes().collect();
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].operation(), operation);
    assert!(matches!(
        outcomes[0].outcome(),
        OperationOutcome::Reply(frame) if frame.0 == expected
    ));
    join(server)?;
    Ok(())
}

#[test]
fn wrong_reply_key_closes_the_epoch_without_reassignment() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let server = thread::spawn(move || -> std::io::Result<()> {
        let (mut stream, _) = listener.accept()?;
        let mut frame = [0_u8; FRAME_BYTES];
        stream.read_exact(&mut frame)?;
        frame[..4].copy_from_slice(&31_u32.to_be_bytes());
        stream.write_all(&frame)
    });

    let mut engine = engine(address)?;
    let permit = engine.reserve(Moment::ORIGIN, options(8).session())?;
    let expected_key = permit.match_key();
    let frame = request(expected_key, 9);
    engine.commit(permit, OutboundFrame::copy_from_slice(&frame)?)?;
    run_until_stopped(&mut engine)?;

    let outcomes: Vec<_> = engine.drain_outcomes().collect();
    assert!(matches!(
        outcomes.as_slice(),
        [outcome]
            if matches!(
                outcome.outcome(),
                OperationOutcome::Failed {
                    failure: OperationFailure::MatchKeyMismatch { expected, received },
                    delivery: Delivery::PossiblySent,
                } if *expected == expected_key && received.get() == 31
            )
    ));
    join(server)?;
    Ok(())
}

#[test]
fn absolute_deadline_after_write_progress_closes_with_possible_send() -> Result<(), Box<dyn Error>>
{
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let (release, hold) = mpsc::channel();
    let server = thread::spawn(move || -> std::io::Result<()> {
        let (mut stream, _) = listener.accept()?;
        let mut frame = [0_u8; FRAME_BYTES];
        stream.read_exact(&mut frame)?;
        hold.recv()
            .map_err(|_| std::io::Error::other("deadline test release was dropped"))?;
        Ok(())
    });

    let mut engine = engine(address)?;
    let deadline = Deadline::at(Moment::from_nanos(100));
    let options = OperationOptions::until(deadline)
        .retained_bytes(RetainedBytes::new(8))
        .write_bytes(RetainedBytes::new(8))
        .session();
    let permit = engine.reserve(Moment::ORIGIN, options)?;
    let frame = request(permit.match_key(), 7);
    engine.commit(permit, OutboundFrame::copy_from_slice(&frame)?)?;
    run_until_written(&mut engine)?;

    let turn = engine.turn_component(deadline.moment())?;
    assert_eq!(turn.next(), Next::Stop);
    let outcomes: Vec<_> = engine.drain_outcomes().collect();
    assert!(matches!(
        outcomes.as_slice(),
        [outcome]
            if matches!(
                outcome.outcome(),
                OperationOutcome::Failed {
                    failure: OperationFailure::ConnectionClosed(
                        CloseReason::DeadlineAfterPossibleSend
                    ),
                    delivery: Delivery::PossiblySent,
                }
            )
    ));
    release.send(())?;
    join(server)?;
    Ok(())
}

#[test]
fn command_mailbox_rejects_at_its_exact_count_bound() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let engine = engine(listener.local_addr()?)?;
    let port = engine.port();
    let epoch = engine.snapshot().connection.epoch;
    for operation in 0..8 {
        port.cancel(epoch, OperationId::new(operation))?;
    }
    let rejected = EngineCommand::Cancel {
        epoch,
        operation: OperationId::new(8),
    };
    let error = port
        .cancel(epoch, OperationId::new(8))
        .err()
        .ok_or_else(|| std::io::Error::other("full command lane accepted another command"))?;
    assert_eq!(error.into_item(), rejected);
    let work = engine.snapshot().commands.lane(Lane::Work);
    assert_eq!(work.queued_messages(), 8);
    assert_eq!(work.message_rejections(), 1);
    Ok(())
}

#[test]
fn stale_epoch_command_cannot_close_the_live_transport() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let mut engine = engine(listener.local_addr()?)?;
    let port = engine.port();
    let epoch = engine.snapshot().connection.epoch;
    port.close(ConnectionEpoch::new(epoch.get() + 1))?;
    engine.turn_component(Moment::ORIGIN)?;
    assert!(engine.snapshot().transport != TransportState::Closed);
    assert_eq!(engine.snapshot().stale_commands, 1);

    port.close(epoch)?;
    let turn = engine.turn_component(Moment::ORIGIN)?;
    assert_eq!(turn.next(), Next::Stop);
    assert_eq!(engine.snapshot().transport, TransportState::Closed);
    let rejected = EngineCommand::Close { epoch };
    let error = port
        .close(epoch)
        .err()
        .ok_or_else(|| std::io::Error::other("closed engine accepted another command"))?;
    assert_eq!(error.into_item(), rejected);
    Ok(())
}

fn options(write_bytes: u64) -> OperationOptions {
    OperationOptions::until(Deadline::at(Moment::from_nanos(u64::MAX)))
        .retained_bytes(RetainedBytes::new(write_bytes))
        .write_bytes(RetainedBytes::new(write_bytes))
}

fn run_until_written(engine: &mut TestEngine) -> Result<(), Box<dyn Error>> {
    for _ in 0..128 {
        engine.turn_component(Moment::ORIGIN)?;
        if engine.snapshot().queued_write_frames == 0 {
            return Ok(());
        }
        engine.poll_io(Span::from_nanos(10_000_000))?;
    }
    Err(std::io::Error::other("write did not complete within bounded test turns").into())
}

fn run_until_stopped(engine: &mut TestEngine) -> Result<(), Box<dyn Error>> {
    for _ in 0..128 {
        let turn = engine.turn_component(Moment::ORIGIN)?;
        if turn.next() == Next::Stop {
            return Ok(());
        }
        if turn.next() != Next::Now {
            engine.poll_io(Span::from_nanos(10_000_000))?;
        }
    }
    Err(std::io::Error::other("engine did not stop within bounded test turns").into())
}

fn join(handle: thread::JoinHandle<std::io::Result<()>>) -> Result<(), Box<dyn Error>> {
    handle
        .join()
        .map_err(|_| std::io::Error::other("test server panicked"))??;
    Ok(())
}
