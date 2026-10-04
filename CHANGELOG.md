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
