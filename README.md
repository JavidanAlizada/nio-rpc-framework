# nio-rpc-framework

An RPC framework for the JVM, built from first principles: a client calls a
method on a plain Java interface, the call travels over TCP in a custom
binary protocol on a hand-rolled Java NIO event loop, and the server runs
it on a virtual thread and sends the result back.

The point isn't to rebuild gRPC. It's to build the parts that make RPC
hard (framing, multiplexing, correlation, the event loop, deadlines and
cancellation across a network, backpressure) without hiding them behind
Netty, and to write the reasoning down.

**Status:** early. Milestone 1 (wire protocol and payload codec) is in
progress. See [CHANGELOG.md](CHANGELOG.md) for what has actually landed.

**Performance:** this framework has not been measured. No benchmarking is
done in this project, as a deliberate scope decision, so the README makes
no performance claims.

## Build & test

Requires JDK 21.

```bash
./gradlew check     # compile, tests, Checkstyle, SpotBugs, JaCoCo report
```

## CI

`.github/workflows/pr.yml` runs on every push and PR to `main`: compile,
tests, Checkstyle and SpotBugs, a JaCoCo coverage report, packaging
(`jar`/`sourcesJar`), and a GitHub dependency-vulnerability review.

## License

MIT, see [LICENSE](LICENSE).
