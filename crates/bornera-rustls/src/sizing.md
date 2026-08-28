
## Sizing a Kafka-oriented connection

There is deliberately no universal TLS profile. The protocol owner chooses frame
and decoder limits; the transport profile separately bounds per-turn work and
accounts for rustls-owned memory. Revalidate the profile when the exact rustls
version, crypto provider, certificate chain, client-auth configuration, or frame
limits change.

The limits relate as follows:

- `IoLimits::chunk_bytes` bounds application and raw TLS bytes progressed by one
  slot operation. It is a fairness and latency bound, not a frame-size limit.
- `application_write_buffer_bytes` is rustls's application-write buffer limit.
  A positive application write is Bornera's irreversible transport-ownership edge.
- `max_tls_egress_bytes` bounds observable ciphertext waiting inside rustls.
- `max_plaintext_bytes` bounds decrypted plaintext waiting to be read by Bornera.
- `TransportPressure::outbound` must cover `max_tls_egress_bytes`.
- `TransportPressure::plaintext` must cover both the application-write and readable-
  plaintext bounds. The inbound and protocol categories are caller-audited charges
  for opaque rustls and provider state; they are accounting reservations, not
  allocator controls.
- `RustlsServerSession::ingest_tls` consumes at most the caller's slice and the
  configured inbound charge in one call. The session never retains that caller
  slice; rustls's private encoded-input storage remains covered by the audited
  inbound charge.

For example, a Kafka driver might start qualification with 16 KiB I/O chunks,
64 KiB of application-write buffering, 128 KiB each of observable TLS egress and
readable plaintext, and audited 128 KiB charges for opaque inbound and protocol
state. This charges 576 KiB per connection:

```rust
use std::{io, num::NonZeroUsize};

use bornera::{IoLimits, TransportPressure};
use bornera_core::RetainedBytes;
use bornera_rustls::RustlsTransportLimits;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
fn nonzero(value: usize) -> Result<NonZeroUsize, io::Error> {
    NonZeroUsize::new(value).ok_or_else(|| io::Error::other("limit must be nonzero"))
}
let kib = |value: u64| RetainedBytes::new(value * 1_024);

let _io = IoLimits::new(nonzero(8)?, nonzero(16 * 1_024)?);
let pressure = TransportPressure::new(kib(128), kib(128), kib(192), kib(128))?;
let tls = RustlsTransportLimits::new(
    nonzero(64 * 1_024)?,
    nonzero(128 * 1_024)?,
    nonzero(128 * 1_024)?,
    pressure,
)?;

assert_eq!(tls.transport_limits().retained_bytes(), kib(576));
# Ok(())
# }
```

This is a worked starting point, not a production recommendation. The Kafka
driver should own the selected values, exercise maximum request and response
frames, handshake and reauthentication, certificate chains, and client
certificates where enabled, then record the measured/audited profile.

## Rustls feature contract

This release pins rustls 0.23.43 with `ring`, `std`, and `tls12`. TLS 1.2 support
is intentional for Kafka broker compatibility rather than an accidental result
of feature unification. The supplied [`rustls::ClientConfig`] determines client
versions, while [`rustls::ServerConfig`] determines server-session versions,
identity, client authentication, and ALPN policy. Downstream consumers should
record any decision to narrow or change either protocol-version set.
