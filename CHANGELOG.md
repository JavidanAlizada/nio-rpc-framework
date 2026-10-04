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
