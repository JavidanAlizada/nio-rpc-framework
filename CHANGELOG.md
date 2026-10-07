# Changelog

All notable changes to this project are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/); entries are grouped
per milestone, not per commit.

## [Unreleased]

### Milestone 2 — NIO transport (in progress)

- `dev.rpc.transport` API: `Transport` (bind, connect), `Server`,
  `Connection` (write from any thread, `isWritable`, idempotent `close`),
  and `ConnectionHandler`, whose events all run on the connection's
  event-loop thread, with `onClosed` exactly once.
- `TransportConfig` with validated defaults (none tuned from measurements):
  - CPU-count event loops and 1,000 max connections;
  - 256 KiB / 1 MiB watermarks and a 16 MiB write-queue limit;
  - a 30 s idle timeout, with a PING after 15 s;
  - a 5 s connect timeout and 100 ms to 10 s reconnect backoff;
  - TCP_NODELAY on.
- Config validation includes: the watermarks must be ordered, and the
  write-queue limit must hold at least one largest legal frame
  (`ProtocolLimits.maxFrameSize()`, new).
- Checkstyle `ImportControl`: `transport` may not import `serialization`.
- Version is `0.2.0-SNAPSHOT`.
- `EventLoop`: one platform thread per reactor, owning a Selector, a
  lock-free task queue (with `wakeup()` on every submit, so no lost
  wakeups) and a loop-confined timer queue. Each iteration selects (with
  a timeout from the next timer, rounded up so it never becomes "forever"),
  dispatches ready keys, runs tasks, then runs due timers.
  - Channels can only be registered on the loop thread.
  - A failing task, timer or handler is logged and the loop continues.
  - A task submitted while the loop shuts down either runs or is rejected,
    never lost (checked by a race test).
  - Timers are ordered by `nanoTime` difference, which is safe across
    overflow, with FIFO order for ties.
- `NioTransport`: the event-loop group plus `bind`/`connect`.
  - **Server:** an acceptor thread per server, blocking in `accept()`,
    hands channels to the loops round robin.
  - **`maxConnections`:** an extra connection is accepted and closed at
    once, because TCP can't refuse it earlier.
  - **`acceptBacklog`** (new setting, default 1,024, clamped by the OS):
    Java's default of 50 reset connections during a 200-connection burst.
  - **Client:** non-blocking connect with a connect-timeout timer. The
    connect future completes off the event loop, so a caller's `thenApply`
    never runs on a loop thread.
- `NioConnection`:
  - **Read path:** the loop's shared read buffer feeds `FrameDecoder` and
    then the handler, with at most 16 reads per wakeup.
  - **Write queue:** a lock-free queue. A partly written frame is resumed
    on the next flush, and OP_WRITE is on only while data is left over.
    A flush is scheduled by whichever writer flips a `flushPending` flag;
    the loop clears it before draining. This replaces #9's "schedule on
    pendingBytes 0 → non-zero", which could strand a frame when the loop
    drained a buffer before its writer had counted it.
  - **Backpressure:** above the high watermark `isWritable()` turns false
    and `onWritabilityChanged(false)` fires; below the low watermark both
    revert. Events fire only on the loop thread. Above the hard limit the
    connection closes with a "write queue overflow" cause, and the write
    that crossed it throws `ConnectionClosedException`.
  - **Pausing reads** while unwritable applies to accepted (server)
    connections only. On a client it deadlocked: both ends stopped reading
    and neither queue could drain.
  - **Liveness:** PING is answered with PONG by the transport, and PONG
    never reaches the handler.
  - **Idle detection:** after idleTimeout/2 with nothing read, the
    connection sends a PING carrying `System.nanoTime()`; after idleTimeout
    it closes with a `SocketTimeoutException("idle timeout …")` cause. Any
    inbound byte resets it, on clients and servers alike. One timer per
    connection re-arms itself; reads only update a timestamp.
  - While a server connection has paused reads for backpressure, bytes the
    peer's socket accepts from our queue count as life too, since no read
    can. How quickly that progress shows depends on kernel buffer sizes:
    with the 30 s default, a peer reading faster than roughly 20 KB/s
    counts as alive.
  - **Closing:** EOF, an I/O error, a protocol error or a throwing handler
    closes the connection. `onClosed` fires exactly once, and only for
    connections that opened; a failed connect fails its future instead.
- `Transport.close()` stops the servers, closes every connection, stops the
  loops, then sweeps any connection a racing `connect()` left behind.
- Close-cause contract: once a close was requested locally (`close()` or
  `Transport.close()`), `onClosed` reports a `null` cause even if the
  peer's FIN is processed first. `Transport.close()` marks every connection
  before closing any, so two ends in one transport both report a local
  close. Found by CI on Linux, where the FIN sometimes won the race; a
  50-round regression test covers it.

## [0.1.0] — 2026-10-04

### Milestone 1 — Wire protocol + codec

- Gradle build: Java 21 toolchain, Checkstyle, SpotBugs, JaCoCo, PR CI.
- `dev.rpc.protocol` frame types: sealed `Frame` with `Request`, `Response`,
  `Cancel`, `Ping`, `Pong` records (payload arrays owned, not copied), and
  `Status`, which keeps unknown codes instead of failing on them.
- `FrameEncoder`: 12-byte header (version first), fixed-width big-endian
  fields, body cap from `ProtocolLimits` (default 4 MiB) checked before
  allocating, strict UTF-8 (a lone surrogate is an error, not a silent `?`).
- Golden-byte tests pinning the wire format of every frame type.
- `FrameDecoder`: two-state machine (header, body) plus a terminal FAILED
  state; fed chunks of any size from 1 byte up. Version checked on the first
  byte, body cap checked from the header before allocating, the fixed body
  sizes of CANCEL/PING/PONG checked from the header, strict UTF-8, and a
  `ProtocolException` for every malformed case. Unknown frame types are
  skipped (and counted), unknown status codes kept.
- Decoder tests: every single and every pair of split points, 1-byte
  chunks, 1,000 seeded random chunkings, hostile headers and bodies, and the
  tolerance rules.
- `dev.rpc.serialization`: `Serializer` SPI and the built-in
  `RecordSerializer`. Positional binary encoding of primitives and boxes,
  String, byte[], enums, List, Map and records (recursive ones included),
  with a 1-byte null marker on every reference-typed value. The codec tree
  for a type is built once, eagerly (an unsupported type anywhere fails up
  front and names its path, e.g. `NestedBad.bad.value`), and cached in a
  `ClassValue`.
- Hostile-input limits: collection counts checked against the bytes left
  before allocating, nesting depth capped (default 64), strict UTF-8, strict
  booleans and presence markers, duplicate map keys rejected.
- Checkstyle `ImportControl`: `protocol` and `serialization` may not import
  each other.
- `SchemaFingerprint.of(Type)`: the first 8 bytes of SHA-256 over a
  canonical description of a type's wire structure (component names, types
  and order, enum constant names, collection type arguments, boxed vs.
  primitive). Record and enum type names are deliberately left out, so
  renaming or moving a type stays compatible. Recursive and repeated records
  become `ref N`. Unsupported types are rejected with the codec's own error.
- README: layers, protocol and compatibility rules, serialization,
  concurrency, design patterns, security posture, and the trade-offs behind
  each decision.

### Scope notes

- **No benchmarks.** No JMH and no load generator, by explicit decision.
  The project therefore makes no performance claims, and the optimizations
  deferred to Phase 2 (buffer pooling, zero-copy, batching) stay unjustified
  until a benchmarking phase exists.
- **No mutation testing** in Phase 1.
- **No Dockerfile:** a library with nothing to run on its own.
- **Phase 1 is plaintext and unauthenticated.** TLS is Phase 2.
