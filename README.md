<p align="center">
  <img src="https://raw.githubusercontent.com/zsumz/bornera/main/bornera-logo.svg" alt="bornera" width="720">
</p>

<p align="center">
  <strong>Deterministic connection ownership for native protocol clients.</strong>
</p>

<p align="center">
  Bornera owns bounded connection lifecycles between protocol codecs and
  sockets without taking over protocol semantics, retry policy, or public APIs.
</p>

<p align="center">
  <a href="#model">Model</a>
  <span> · </span>
  <a href="#crates">Crates</a>
  <span> · </span>
  <a href="#start">Start</a>
  <span> · </span>
  <a href="#qualification">Qualification</a>
</p>

<br />

## Model

```text
bornera       production connection ownership hosted by Calandria
bornera-core  deterministic sans-I/O connection policy
bornera-rustls bounded rustls client transport for production owners
bornera-sim   deterministic bounded trace replay (currently unpublished)
```

Operations follow `reserve -> prepare -> commit`. Bornera reserves bounded
capacity before an adapter encodes a frame, then takes ownership only when the
complete frame is committed. Every accepted operation belongs to one connection
epoch and ends exactly once while its owner is driven to completion or consumed
through explicit fatal-owner recovery. Arbitrary Rust `drop`, process failure,
or a panic in adapter code can discard observations that were not drained.

Production ownership is split deliberately:

```text
ConnectionSlot         selector-free state for one exact connection epoch
ConnectionSet          one selector and bounded fair progression for many slots
StandaloneConnection   capacity-one convenience wrapper around ConnectionSet
```

`bornera-sim` drives that same selector-free `ConnectionSlot`—including its
decoder, classifier, deadlines, publications, and recovery path—through
Calandria virtual time and a bounded simulated transport. It remains an
unpublished qualification crate rather than a peer production capability.

Delivery certainty is deliberately limited to `NotSent` and `PossiblySent`.
Delivery becomes `PossiblySent` when application bytes cross the irreversible
transport-write ownership boundary. With a buffering transport, complete frames
can leave Bornera before encoded output reaches the operating system. Neither
boundary proves remote receipt or processing. Protocol crates retain codecs,
routing, session semantics, topology, errors, and retry policy.

Each registered transport reports an auditable per-connection memory charge:
observable allocation capacities plus conservative configured charges for opaque
transport-library state. Bornera checks it around selector registration and after
every readiness, transport, or application-I/O step, retains the last observation
in snapshots and recovery, and fails closed if the configured limit is crossed.
Shared configuration and operating-system socket buffers remain explicitly
outside this measure and require bounds from their respective owners.

Ordered draining takes one caller-established absolute deadline spanning both
accepted operations and bounded transport-local graceful shutdown. Once core
policy has drained, Bornera progresses the adapter until all retained egress and
its local close signal leave adapter ownership. Reaching the deadline or calling
forced finalization releases the physical capability without waiting for a peer.
For `bornera-rustls`, the connect deadline spans TCP, socket policy, and the TLS
handshake; `TransportOpened` is published only after the application channel is
ready and the final handshake flight has left rustls ownership.

## Crates

| Crate | Purpose |
| --- | --- |
| `bornera` | Shared-selector production ownership for registered native transports under Calandria hosting |
| `bornera-core` | Bounded admission, framing, matching, deadlines, delivery certainty, and recovery policy |
| `bornera-rustls` | Bounded TLS client establishment, encrypted I/O, diagnostics, and graceful close over rustls |
| `bornera-sim` | Unpublished bounded trace capture, exact replay, and generated policy properties |

The crates begin at the same version and remain lockstepped during pre-alpha.
Use only the layer you need.

## Start

Add only the layers you need:

```toml
[dependencies]
bornera = "=0.0.1-rc.3"
bornera-core = "=0.0.1-rc.3"
bornera-rustls = "=0.0.1-rc.3" # when TLS is required
calandria = { version = "=0.0.1-rc.2", features = ["std"] }
rustls = { version = "=0.23.43", default-features = false, features = ["ring", "std", "tls12"] }
```

Run either production hosting model from a checkout:

```sh
cargo run -p bornera --example embedded --locked --offline
cargo run -p bornera --example dedicated --locked --offline
```

Public configuration and host contracts use Bornera-Core and Calandria value
types, so production consumers should declare all three layers explicitly.
The optional TLS adapter accepts rustls `ClientConfig` values, so TLS consumers
also declare compatible `bornera-rustls` and `rustls` dependencies.
Mailbox success means a command is queued, not applied. The sequenced
`AdmissionOpened` lifecycle event is the authoritative confirmation that
regular admission opened.

The owner exposes bounded, generation- and epoch-fenced mechanical control. It
does not expose an async runtime, protocol-semantic work, or automatic retry
decisions.

## Qualification

```sh
zcheck
```

The zcheck graph is the complete local gate for formatting, tests, examples,
Clippy, rustdoc, source shape, zrail architecture, clean diffs, package
contents, packaged-crate smoke compilation, and publish ordering.

Bornera requires Rust 1.88 or newer. Checked-in CI additionally qualifies the
latest stable toolchain, macOS, Windows, cargo-deny, selected Miri tests, and a
workspace coverage report. The production loopback persona covers session
establishment, correlated request/reply, Kafka-style no-reply writes, partial
writes at deadline, cancellation on both sides of first write progress, peer
loss, and explicit owner recovery. The rustls qualification adds handshake
read/write alternation, buffered ciphertext, local decrypted plaintext, SNI and
certificate failures, truncation, graceful close, logical buffer ceilings, and
connection-local TLS failure isolation. DNS ownership, address selection, and
reconnect policy remain caller work. A prerelease version in source is not
release proof, and package publication must occur in dependency order:
`bornera-core` before `bornera` before `bornera-rustls`.

## License

Apache-2.0. See [LICENSE](LICENSE).
