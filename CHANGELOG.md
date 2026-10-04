# Changelog

All notable changes to this project are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/); entries are grouped
per milestone, not per commit.

## [Unreleased]

### Milestone 1 — Wire protocol + codec (in progress)

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
