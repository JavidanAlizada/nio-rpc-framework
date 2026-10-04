# nio-rpc-framework

An RPC framework for the JVM, built from first principles: a client calls a
method on a plain Java interface, the call travels over TCP in a custom
binary protocol on a hand-rolled Java NIO event loop, and the server runs
it on a virtual thread and sends the result back.

The point isn't to rebuild gRPC. It's to build the parts that make RPC
hard (framing, multiplexing, correlation, the event loop, deadlines and
cancellation across a network, backpressure) without hiding them behind
Netty, and to write the reasoning down.

**Status:** Milestone 1 of 6 (wire protocol and payload codec) is done and
released as 0.1.0. There is no network code yet: the transport comes in
Milestone 2 and the RPC layer in Milestone 3. See
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
 transport: event loop    ┘    (Milestone 2)  └─ transport: event loop
```

Two rules shape the code so far:

* **The protocol knows nothing about Java objects.** Frames carry a method
  id, headers and opaque payload bytes. Framing can therefore be tested
  without a network or a serializer.
* **Serialization knows nothing about frames.** The two packages meet only
  in the RPC core. A Checkstyle `ImportControl` rule fails the build if
  either one imports the other.

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

## Concurrency

* `FrameDecoder` is **not** thread-safe. Each connection gets its own, and
  only that connection's event-loop thread uses it (Milestone 2), so it
  needs no locking.
* `FrameEncoder`, `RecordSerializer` and `SchemaFingerprint` are
  thread-safe.
* Codecs are cached per class in a `ClassValue`. Two threads may build the
  same codec at once. That's harmless, because the build has no side
  effects and one result simply wins.
* Frames take ownership of their payload array, and nobody may change it
  afterwards. Because records have final fields, Java guarantees the array
  contents are visible to any thread that sees the frame, even without
  explicit synchronization.

## Design patterns

Each pattern below has a real caller in the code.

* **Strategy:** `Serializer`, with `RecordSerializer` as the built-in.
* **Composite:** the codec tree. Record codecs hold their components'
  codecs; list and map codecs hold their element codecs.
* **Value Object:** the frame records and `Status`.
* **Tolerant Reader:** unknown frame types, headers and status codes,
  limited to those points.
* **Registry:** the `ClassValue` codec cache.

Considered and rejected:
* **State objects for the decoder.** Two states and a failed state are
  clearer as an enum and a switch.
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
* a 16-thread codec concurrency test.

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
