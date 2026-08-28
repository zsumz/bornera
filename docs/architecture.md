# Architecture and ownership

Bornera owns the mechanical lifetime of native protocol connections. Protocol
crates keep codecs, routing, session semantics, topology, public errors, and
retry policy.

## Crate roles

| Crate | Role |
| --- | --- |
| `bornera` | Production connection ownership hosted by Calandria |
| `bornera-core` | Deterministic sans-I/O connection policy |
| `bornera-rustls` | Bounded rustls client transport and socket-free server session |
| `bornera-sim` | Unpublished deterministic bounded trace replay |

The crates begin at the same version and remain lockstepped during pre-alpha.

## Ownership lifecycle

Operations follow `reserve -> prepare -> commit`. Bornera reserves bounded
capacity before an adapter encodes a frame, then takes ownership only when the
complete frame is committed.

Every accepted operation belongs to one connection epoch and ends exactly once
while its owner is driven to completion or consumed through explicit fatal-owner
recovery. Arbitrary Rust `drop`, process failure, or a panic in adapter code can
discard observations that were not drained.

Mailbox success means a command is queued, not applied. The sequenced
`AdmissionOpened` lifecycle event is the authoritative confirmation that normal
admission opened.

## Production owners

```text
ConnectionSlot         selector-free state for one exact connection epoch
ConnectionSet          one selector and bounded fair progression for many slots
StandaloneConnection   capacity-one convenience wrapper around ConnectionSet
```

`bornera-sim` drives the same selector-free `ConnectionSlot`, including its
decoder, classifier, deadlines, publications, and recovery path, through
Calandria virtual time and a bounded simulated transport. It is a qualification
crate rather than a peer production capability.

## Delivery certainty

Delivery certainty is deliberately limited to `NotSent` and `PossiblySent`.
Delivery becomes `PossiblySent` when application bytes cross the irreversible
transport-write ownership boundary.

With a buffering transport, complete frames can leave Bornera before encoded
output reaches the operating system. Neither boundary proves remote receipt or
processing.

## Memory accounting

Each registered transport reports an auditable per-connection memory charge.
The charge includes observable allocation capacities and conservative configured
charges for opaque transport-library state.

Bornera checks the charge around selector registration and after readiness,
transport, and application-I/O steps. It retains the latest observation in
snapshots and recovery, and fails closed if the configured limit is crossed.
Shared configuration and operating-system socket buffers remain outside this
measure and require bounds from their respective owners.

## Shutdown and TLS

Ordered draining uses one caller-established absolute deadline for accepted
operations and bounded transport-local graceful shutdown. After core policy has
drained, Bornera progresses the adapter until retained egress and its local close
signal leave adapter ownership. At the deadline, or during forced finalization,
the physical capability is released without waiting for a peer.

For `bornera-rustls`, the connect deadline spans TCP, socket policy, and the TLS
handshake. `TransportOpened` is published only after the application channel is
ready and the final handshake flight has left rustls ownership.

The server-session surface owns only `rustls::ServerConnection` state. Its caller
owns accepted sockets, absolute handshake deadlines, tasks, readiness, and every
buffer between drained TLS egress and the network. Rustls handshake completion and
session opening remain distinct: opening latches only after required handshake
egress has left rustls, and the outer owner must additionally prove its own egress
has reached the transport before publishing an open connection.

This boundary is socket-free in source, ownership, and operation, but not yet in
the resolved crate graph: `bornera-rustls` still depends on the production client
stack. A server-only dependency feature requires a future contract-layer split.

TLS consumers add the adapter and compatible rustls release:

```toml
[dependencies]
bornera-rustls = "=0.0.1-rc.3"
rustls = { version = "=0.23.43", default-features = false, features = ["ring", "std", "tls12"] }
```

## Integration boundary

Bornera exposes bounded, generation- and epoch-fenced mechanical control. It
does not provide an async runtime, protocol-semantic work, or automatic retry
decisions. DNS ownership, address selection, and reconnect policy remain caller
responsibilities.

Public production configuration and host contracts currently use
`bornera-core` and Calandria value types. Consumers should therefore declare
those direct dependencies alongside `bornera` until facade coverage changes.
