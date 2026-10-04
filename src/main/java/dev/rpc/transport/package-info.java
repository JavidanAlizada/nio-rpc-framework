/**
 * Moves frames between processes over TCP: event loops, connections, the write path with backpressure, liveness
 * checks and reconnects. Knows frames, never payloads; serialization stays out of this package.
 */
package dev.rpc.transport;
