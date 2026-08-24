//! Runs a connection engine under caller-driven Calandria hosting.

mod support;

use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener},
    thread,
};

use calandria::{EmbeddedHost, HostAction, HostConfig, MonotonicClock};

use support::{BoxError, FRAME_BYTES, prepared_engine};

fn main() -> Result<(), BoxError> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let peer = thread::spawn(move || -> std::io::Result<()> {
        let (mut stream, _) = listener.accept()?;
        let mut frame = [0_u8; FRAME_BYTES];
        stream.read_exact(&mut frame)?;
        stream.write_all(&frame)?;
        stream.shutdown(Shutdown::Both)
    });

    let (engine, expected) = prepared_engine(address)?;
    let mut host = EmbeddedHost::new(engine, MonotonicClock::new(), HostConfig::default());
    loop {
        match host.step()?.action() {
            HostAction::Continue => {}
            HostAction::Wait(maximum) => {
                host.duty_mut().poll_io(maximum)?;
            }
            HostAction::Stop => break,
        }
    }

    let mut engine = host.into_duty();
    let reply = engine
        .drain_outcomes()?
        .next()
        .ok_or_else(|| std::io::Error::other("embedded example received no outcome"))?;
    if reply.into_outcome()
        != bornera_core::OperationOutcome::Reply(support::ExampleFrame(expected.to_vec()))
    {
        return Err(std::io::Error::other("embedded reply did not match").into());
    }
    peer.join()
        .map_err(|_| std::io::Error::other("embedded peer panicked"))??;
    Ok(())
}
