# nio-rpc-framework

An RPC framework for the JVM, built from first principles: a client calls a
method on a plain Java interface, the call travels over TCP in a custom
binary protocol on a hand-rolled Java NIO event loop, and the server runs
it on a virtual thread and sends the result back.

The point isn't to rebuild gRPC. It's to build the parts that make RPC
hard (framing, multiplexing, correlation, the event loop, deadlines and
cancellation across a network, backpressure) without hiding them behind
Netty, and to write the reasoning down.

**Status:** Milestones 1 and 2 of 6 are done. Milestone 1, the wire
protocol and payload codec, was released as 0.1.0. Milestone 2, the NIO
transport, is released as 0.2.0. Frames now travel over real TCP
connections, but there are no remote calls yet: the RPC layer (proxies,
request ids, dispatch) comes in Milestone 3. See
[CHANGELOG.md](CHANGELOG.md) for what has landed.

**Performance:** this framework has not been measured. No benchmarking is
done in this project, as a deliberate scope decision, so this README makes
no performance claims. Where the design talks about copies or allocations,
that's reasoning about the code, not a measured result.

## Layers

```
 service interface proxy ─┐                   ┌─ service implementation
 serialization            │  (Milestone 3+)   │  serialization
 protocol: frames         │ ◀── TCP / NIO ──▶ │  protocol: frames
 transport: event loops   ┘                   └─ transport: event loops
```

Three rules shape the code so far:

* **The protocol knows nothing about Java objects.** Frames carry a method
  id, headers and opaque payload bytes. Framing can therefore be tested
  without a network or a serializer.
* **Serialization knows nothing about frames.** The two packages meet only
  in the RPC core. A Checkstyle `ImportControl` rule fails the build if
  either one imports the other.
* **The transport knows frames, never payloads.** The same rule stops
  `transport` from importing `serialization`.

## Protocol

Every frame is a fixed 12-byte header followed by a body. All integers are
big-endian and fixed width.

```
 0        1        2        3        4                 8                 12
 ┌────────┬────────┬────────┬────────┬─────────────────┬─────────────────┬──────────┐
 │version │ type   │ flags  │reserved│ requestId (u32) │ bodyLength (u32)│ body ... │
 └────────┴────────┴────────┴────────┴─────────────────┴─────────────────┴──────────┘
```

| Type | Body |
|---|---|
| REQUEST (1) | methodId i32 · timeoutNanos i64 · headerCount u16 · headers · payload |
| RESPONSE (2) | status u8 · then the payload if OK, otherwise errorType and message strings |
| CANCEL (3) | empty |
| PING (4) / PONG (5) | 8 opaque bytes; PONG echoes the PING |

* **requestId 0** is reserved for connection-level frames (PING/PONG).
  Every call frame uses a nonzero id.
* **timeoutNanos** is the time remaining, not a timestamp, because two
  machines' clocks can't be compared. 0 means no deadline.
* **Strings** are a u16 byte length followed by UTF-8. **Headers** are key
  and value strings; a duplicate key is an error.
* **Exceptions** cross the wire as a type name plus a message, never as
  serialized objects.
* **Frame size:** bodies are capped at 4 MiB by default (`ProtocolLimits`).
  The decoder checks the cap from the header, before it allocates
  anything.

### Compatibility rules

Every incompatibility must be detected and reported, never silently
misread.

| Situation | Behavior |
|---|---|
| Unknown version | Connection-fatal: nothing after byte 0 can be trusted |
| Unknown flag bit | Connection-fatal: a flag can change how the body is read |
| Unknown frame type | Skipped (the length is known) and counted |
| Unknown header | Passed through, for interceptors to use or ignore |
| Unknown status code | Kept as `Status.of(n)` with its number |
| Malformed body of a known type | Connection-fatal |

In short: tolerant at the points built for extension (frame types,
headers, status codes) and strict everywhere else. Any change that doesn't
fit one of those points bumps the version.

### Why it looks like this

* **Version first, no magic number.** The decoder rejects a different
  version on the very first byte, before it interprets a length. A magic
  number would mainly catch "something that isn't this protocol
  connected", and the version byte, type check and frame cap already catch
  that. An HTTP client's `GET ` fails on the `G`.
* **Fixed-width integers, no varints.** Varints save a few bytes per frame
  but add a decoding loop and edge cases (overlong encodings). With
  nothing measured, that complexity has no evidence to justify it.
* **32-bit request ids.** An id only repeats after 2^32 calls on one
  connection. 64-bit ids would add 4 bytes to every frame to guard against
  a case that's practically impossible.
* **A two-state decoder with exact-size bodies.** The decoder is a small
  state machine: read a header, then read a body. Once the header is
  valid, the body array is allocated at its exact size and filled across
  as many reads as it takes. The alternative, Netty-style, keeps appending
  input to a growing buffer and re-parses it. That copies bytes more than
  once and hides the partial-read problem this project is about. After a
  bad header the decoder fails for good, because the position in the byte
  stream can't be trusted any more and the only recovery is to close the
  connection.
* **An oversized frame closes the connection.** The alternative is to
  reply with an error and discard the body. That would mean reading up to
  4 GiB of garbage from a hostile peer just to keep the connection open.

## Serialization

`Serializer` is the plug-in point. `RecordSerializer` is the built-in
implementation:

* **Supported types:** primitives and their boxes, `String`, `byte[]`,
  enums (sent as their ordinal), `List`, `Map`, and records made of those.
  Recursive records such as `record Node(int v, Node next)` work.
* **Positional encoding.** No field names or type tags go on the wire;
  both sides take the types from the shared service interface.
  Reference-typed values carry a 1-byte null marker, and primitives don't.
* **Unsupported types** (`Object`, interfaces, `Set`, generic records,
  arrays other than `byte[]`) fail the first time the type is used, with
  the path to the bad component, e.g. `NestedBad.bad.value`. That holds
  even when the value at that path is null.
* **Hostile input:**
  * A collection's count is checked against the bytes left before
    anything is allocated.
  * Nesting is capped (64 levels by default), so deep input gets a
    `SerializationException` instead of a `StackOverflowError`.
  * UTF-8, booleans and null markers are checked strictly.

```java
Serializer serializer = new RecordSerializer();
record Point(int x, int y) { }

byte[] bytes = serializer.serialize(new Type[] {Point.class, String.class},
        new Object[] {new Point(1, 2), "hello"});
Object[] values = serializer.deserialize(new Type[] {Point.class, String.class}, bytes);
```

**The trade-off:** positional encoding is simple, but if one side adds a
record component, the other side misreads the bytes. Rather than making
the encoding tolerant (Protobuf-style tagged fields, planned with a
Protobuf adapter in Phase 2), the framework makes a mismatch impossible to
miss. `SchemaFingerprint.of(type)` hashes:
* component names, types and order;
* enum constant names;
* collection type arguments;
* whether a value is boxed or primitive.

Milestone 3 folds the fingerprint into method ids, so a mismatched peer
gets `UNKNOWN_METHOD`. Type names are deliberately left out of the hash,
so renaming or moving a record stays compatible.

The nesting cap has a cost too: a legitimately deep structure, such as a
`Node` chain longer than the cap, is rejected. `new RecordSerializer(n)`
raises the cap.

## Transport

`dev.rpc.transport` moves frames between processes over TCP. It knows
frames, never payloads. The RPC layer (Milestone 3) sees only this:

```java
try (var transport = new NioTransport()) {
    Server server = transport.bind(new InetSocketAddress(9000), serverHandler);
    Connection c = transport.connect(server.localAddress(), clientHandler).get();
    c.write(request);                 // any thread; encoded on the caller
}
```

`ConnectionHandler` gets `onFrame`, `onWritabilityChanged` and
`onClosed`. `onClosed` runs exactly once per connection that opened, with
a `null` cause when the close was local. All three run on the
connection's event-loop thread, so a handler must not block.

### Threads and ownership

```
 acceptor thread (blocking accept) ──round robin──▶ ┌ event loop 0 ┐ selector, task queue, timers
                                                    ├ event loop 1 ┤   each connection is owned by
 client connect() ──────────────────round robin──▶  └ event loop N ┘   exactly one loop, for life
                                                          │
 any thread: connection.write(frame) ── encode on the caller ──▶ write queue ──▶ owning loop writes
```

* **Event loops:** each is one platform thread with a Selector, a task
  queue and a timer queue. Every iteration it selects (the timeout comes
  from the next timer), handles ready keys, runs queued tasks, then runs
  due timers. These are platform threads because a loop blocks in
  `select()` for its whole life, which virtual threads handle badly. The
  default is one loop per CPU. That's a starting point, not a tuned
  number.
* **Confinement is the core rule.** A connection's channel, selection
  key, decoder and read state are touched only by its owning loop. Other
  threads reach it in two ways only: the write queue, and tasks submitted
  to the loop. Channels are registered by the loop itself, never by the
  caller's thread. A key registered from another thread only takes effect
  at the selector's next selection. It would also be created on one
  thread and used on another.
* **Accepting:** each server has one thread blocking in `accept()`, which
  hands channels to the loops round robin. A Netty-style "boss" selector
  would be one more event loop to get right. `SO_REUSEPORT` with a
  listener per loop behaves differently on Linux and macOS. A blocking
  acceptor is the simplest correct option, and an accept storm can't take
  time from I/O. Over `maxConnections`, an extra connection is accepted and
  closed at once, because TCP has no way to refuse it earlier.
* **Reads** go into one 64 KiB buffer per loop, shared by its connections.
  This is safe because the decoder copies bytes out during the same read.
  A connection gets at most 16 reads per loop iteration, so one busy peer
  can't starve the others.

Compared with a textbook single-selector reactor, this design fixes four
things. It runs N loops instead of one. Registration happens on the loop
instead of from the caller's thread. The write queue is bounded instead of
unbounded. And a write that the socket only partly accepts is resumed
later instead of being dropped.

### Write path and backpressure

`write(frame)` encodes the frame on the caller's thread and adds the
buffer to a lock-free queue. Many threads can write; only the owning loop
drains it. Each frame is one buffer, and buffers go out in queue order.
So concurrent writers interleave whole frames, never bytes inside a frame,
and each thread's frames arrive in the order it wrote them.

* **When a flush runs:** the writer that flips a `flushPending` flag from
  false to true schedules it. The loop clears the flag before draining, so
  a buffer added during a drain is either seen by that drain or triggers
  the next one.
* **Partial writes:** a buffer the socket only partly takes stays at the
  head of the queue, with its position kept. OP_WRITE is on only while
  data is left over.

Queued bytes are compared with three thresholds:

| Threshold | Default | When crossed |
|---|---|---|
| high watermark | 1 MiB | `isWritable()` turns false and `onWritabilityChanged(false)` fires |
| low watermark | 256 KiB | Back below it: writable again, and the event fires |
| hard limit | 16 MiB | The connection closes with a "write queue overflow" cause |

On a **server** connection, the loop also stops reading from that peer
while it's above the high watermark. Every request read produces a
response to the same peer. If that peer isn't reading our responses,
reading more of its requests only grows the queue. When reads pause, our
receive buffer fills and TCP flow control stops the peer's sends, so the
slow reader is the one that waits. No new protocol messages are needed.

**Client** connections keep reading. A client's backlog is new calls, not
answers. In an early version both ends paused, and two connected peers
could deadlock with neither queue able to drain.

The hard limit exists because responses to calls already running are
still queued while reads are paused. Dropping them would silently lose
results. Past the limit, the peer is treated as stuck and closed.

### Liveness: PING/PONG, not TCP keepalive

A peer can be gone while TCP hasn't noticed. Examples: a pulled cable, a
frozen process whose kernel still ACKs, or an expired NAT entry. Calls on
that connection would then wait out their full deadlines.

* **Pinging:** after `idleTimeout / 2` (15 s by default) with nothing
  read, the connection sends a PING. After `idleTimeout` (30 s) with
  nothing read, it closes with an "idle timeout" cause.
* **Any inbound byte counts as life,** not just a PONG. So a busy
  connection is never pinged.
* **The transport answers PINGs itself.** PING and PONG never reach the
  handler, and both ends run the check.
* **While a server has paused reads,** it can't hear anything from the
  peer. Instead, our writes being accepted by the peer's socket count as
  life. A frozen peer's receive window stays full, so it is still closed.
  The catch is resolution. On macOS loopback, kernel buffers hid a slow
  reader from this signal for over 400 ms. With the 30 s default, any peer
  reading faster than about 20 KB/s counts as alive.

TCP keepalive was the alternative. Its OS default is two hours, and the
per-socket settings aren't supported everywhere. Above all, it only proves
the peer's *kernel* is alive: a process stuck in a deadlock still ACKs
keepalives but never answers a PING. The cost of the app-level check is a
20-byte PING and a 20-byte PONG per idle interval.

### Reconnecting

`transport.reconnecting(address, handler)` returns a
`ReconnectingConnection`, which keeps one connection open to an endpoint.

* **Its state** is CONNECTING, CONNECTED, BACKING_OFF or CLOSED, read with
  `state()`.
* **Backoff:** after a close or a failed connect, it waits with
  exponential backoff and full jitter (100 ms up to 10 s), then tries
  again. Full jitter spreads out clients that lost the same server at the
  same moment.
* **Retry timers** run on the transport's own loops. `Transport.close()`
  closes these connections too.
* **No queueing while disconnected:** `write()` throws
  `ConnectionClosedException` at once, so a dead endpoint can't build up a
  backlog.
* **Known limitation:** the attempt count resets on every successful
  connect. An endpoint that accepts and then drops connections at once is
  retried within 100 ms each time.

The backoff is about 20 lines written locally. Project 02's backoff
replaces it in Milestone 5, when that project becomes a dependency
anyway.

### Closing

* **`close()` is immediate.** It works from any thread, is idempotent, and
  discards queued writes. A graceful drain needs a GOAWAY frame, which the
  protocol doesn't have yet.
* **`onClosed` fires exactly once.** Only the loop thread moves a
  connection to CLOSED, and it checks the state first. That holds however
  many of these race: concurrent `close()` calls, the peer's EOF or reset,
  a protocol error, an idle timeout, or a write-queue overflow.
* **A local close reports a `null` cause,** even if the peer's FIN is
  processed first, since that FIN is usually the peer reacting to our
  close. CI on Linux found this race; the Mac runs never hit it.
* **`Transport.close()`** stops the servers and closes every connection.
  It then joins the acceptors and loops, so no `rpc-` thread is left
  running when it returns.

### Memory bound

A connection in the middle of a large frame holds a body array of up to
`maxBodySize` (4 MiB). It can also have up to the hard limit queued for
writing. So the worst case is `maxConnections × (maxBodySize + hard
limit)`, which with the defaults is 1,000 × 20 MiB = **20 GiB**. In
practice frames are small and queues stay under the watermarks, but this
bound is what an operator has to size for. A shared memory budget across
connections is Phase 2 work.

### Defaults

`TransportConfig.builder()` validates every setting. For example, the
watermarks must be ordered, and the hard limit must hold at least one
largest legal frame. None of the defaults are tuned from measurements:

| Setting | Default | Why |
|---|---|---|
| `ioThreads` | CPU count | one loop per core is a reasoned start |
| `maxConnections` | 1,000 | sets the memory bound above |
| `acceptBacklog` | 1,024 (the OS clamps it) | Java's default of 50 reset connections in a 200-connection test burst |
| watermarks / hard limit | 256 KiB / 1 MiB / 16 MiB | see backpressure |
| `idleTimeout` | 30 s | PING after 15 s |
| `connectTimeout` | 5 s | |
| reconnect delays | 100 ms to 10 s | |
| `tcpNoDelay` | on | see below |

**TCP_NODELAY** is on because RPC sends small request and response frames.
That's the classic case where Nagle's algorithm, combined with delayed
ACKs, holds a small write back by one ACK delay. This is reasoning about
well-known TCP behavior, not a measurement.

## Concurrency

* `FrameDecoder` is **not** thread-safe. Each connection gets its own, and
  only that connection's event-loop thread uses it, so it needs no
  locking.
* `FrameEncoder`, `RecordSerializer` and `SchemaFingerprint` are
  thread-safe.
* Codecs are cached per class in a `ClassValue`. Two threads may build the
  same codec at once. That's harmless, because the build has no side
  effects and one result simply wins.
* Frames take ownership of their payload array, and nobody may change it
  afterwards. Because records have final fields, Java guarantees the array
  contents are visible to any thread that sees the frame, even without
  explicit synchronization.
* **Transport hand-offs between threads:**

  | Hand-off | Mechanism |
  |---|---|
  | Caller → loop: a frame to write | `ConcurrentLinkedQueue` offer → poll |
  | Caller → loop: a task (flush, close, register) | the same, plus `selector.wakeup()` |
  | Loop → caller: `isWritable()` | a volatile flag, written only by the loop |
  | Any thread → loop: `close()` | a volatile `closing` flag, then a close task |
  | Loop / connect future / caller → `ReconnectingConnection` | one lock, and a per-attempt object that discards stale callbacks |

  Wakeups are never lost: the Selector contract says a `wakeup()` that
  lands before `select()` makes the next select return at once. A task
  submitted while a loop shuts down either runs or is rejected, never
  silently dropped.

## Design patterns

Each pattern below has a real caller in the code.

* **Strategy:** `Serializer`, with `RecordSerializer` as the built-in.
* **Composite:** the codec tree. Record codecs hold their components'
  codecs; list and map codecs hold their element codecs.
* **Value Object:** the frame records and `Status`.
* **Tolerant Reader:** unknown frame types, headers and status codes,
  limited to those points.
* **Registry:** the `ClassValue` codec cache.
* **Reactor:** the event loop, with selector-based demultiplexing and
  ready keys dispatched to the owning connection.
* **Producer-Consumer:** the per-connection write queue.
* **Backpressure:** the watermarks, paused reads and the hard limit.
* **Health Check:** PING/PONG liveness and `ReconnectingConnection`'s
  state.
* **Builder:** `TransportConfig`, with cross-field validation.
* **Facade:** `NioTransport` keeps selectors, keys, loops, acceptors and
  timers behind `bind` and `connect`.
* **Half-Sync/Half-Async,** started here and finished in Milestone 3. The
  loops are the async half, and `ConnectionHandler` is the boundary that
  Milestone 3's virtual threads sit behind.

Considered and rejected:
* **State objects for the decoder and the connection lifecycles.** A few
  states are clearer as an enum and a switch.
* **Observer for `ConnectionHandler`.** There's one handler per
  connection, chosen when it opens. That's a callback, not a subscription
  list.
* **Strategy for the transport or the backoff.** Each has one
  implementation. The `Transport` interface exists so that a Netty
  transport could be swapped in for comparison later.
* **Visitor over frames.** `Frame` is a sealed interface, so a
  pattern-matching `switch` is checked by the compiler and needs no
  accept/visit boilerplate.

## Security

* **Phase 1 is plaintext with no authentication. Use it only on trusted
  networks.** TLS is Phase 2, because on raw NIO it means driving
  `SSLEngine` by hand. Interceptors (Milestone 4) are where authentication
  would plug in.
* The decoders are the attack surface, and both treat input as hostile:
  * frame cap and count checks run before allocating;
  * nesting depth is capped;
  * text and markers are parsed strictly;
  * there's no Java deserialization anywhere.
* The transport bounds what one peer can cost: `maxConnections`, a
  per-connection write-queue limit, and idle closes for peers that go
  silent. The worst-case memory bound is written out under Transport.
  Nothing yet limits how fast a client can open connections or send
  requests. That's rate limiting, which arrives with Project 02 in
  Milestone 5.
* CI runs GitHub's dependency review on every pull request. The only
  runtime dependency planned is `resilience-traffic-control`, a pinned
  tag, from Milestone 5.

## Build & test

Requires JDK 21.

```bash
./gradlew check     # compile, tests, Checkstyle, SpotBugs, JaCoCo report
```

The tests cover:
* golden bytes for every frame type and for the codec;
* the decoder fed every single split point and every pair of split points,
  1-byte chunks, and 1,000 seeded random chunkings;
* hostile and malformed input;
* fingerprint stability, with the golden values computed outside Java;
* a 16-thread codec concurrency test;
* the transport on real localhost sockets:
  * echo, 200 connections across all loops, partial writes of frames near
    the size cap, and 64 concurrent writers to one connection;
  * backpressure against a peer that stops reading, and the hard limit;
  * PING/PONG liveness and idle closes on both ends;
  * reconnects across a server restart on the same port;
  * mid-frame FIN and RST, garbage input, throwing handlers, and the
    exactly-once close race with 32 threads;
  * shutdown that leaves no transport thread behind.

The transport tests wait on latches and polling helpers with timeouts,
never on fixed sleeps. Some backpressure timing depends on the OS's
kernel buffer sizes, so those tests only check lower bounds.

## CI

`.github/workflows/pr.yml` runs on every push and pull request to `main`:
* compile and run the tests;
* Checkstyle and SpotBugs;
* a JaCoCo coverage report;
* packaging (`jar`/`sourcesJar`);
* GitHub dependency review.

Every change goes through an issue, a branch and a squash-merged pull
request.

## Containers

There's no Dockerfile, on purpose. This is a library with nothing to run
on its own yet. The Milestone 6 demo runs with `./gradlew run`, and an
image for a demo would add container and SBOM upkeep without anything real
to deploy.

## License

MIT, see [LICENSE](LICENSE).
