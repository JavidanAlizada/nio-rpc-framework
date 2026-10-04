package dev.rpc.transport;

import dev.rpc.protocol.ProtocolLimits;
import java.time.Duration;
import java.util.Objects;

/**
 * Every limit and timeout of the transport. None of the defaults are tuned from measurements; they're reasoned
 * starting points, documented in the README.
 *
 * A PING is sent after half of idleTimeout without inbound bytes, so there is no separate ping setting.
 */
public record TransportConfig(
        int ioThreads,
        int maxConnections,
        int acceptBacklog,
        int lowWatermark,
        int highWatermark,
        int writeQueueLimit,
        Duration idleTimeout,
        Duration connectTimeout,
        Duration reconnectBaseDelay,
        Duration reconnectMaxDelay,
        boolean tcpNoDelay,
        ProtocolLimits protocolLimits) {

    public TransportConfig {
        requirePositive("ioThreads", ioThreads);
        requirePositive("maxConnections", maxConnections);
        requirePositive("acceptBacklog", acceptBacklog);
        requirePositive("lowWatermark", lowWatermark);
        if (!(lowWatermark < highWatermark && highWatermark < writeQueueLimit)) {
            throw new IllegalArgumentException("need lowWatermark < highWatermark < writeQueueLimit, got "
                    + lowWatermark + ", " + highWatermark + ", " + writeQueueLimit);
        }
        Objects.requireNonNull(protocolLimits, "protocolLimits");
        // Otherwise a single legal frame would overflow the queue and close the connection.
        if (writeQueueLimit < protocolLimits.maxFrameSize()) {
            throw new IllegalArgumentException("writeQueueLimit " + writeQueueLimit
                    + " is smaller than the largest legal frame (" + protocolLimits.maxFrameSize() + " bytes)");
        }
        requirePositive("idleTimeout", idleTimeout);
        requirePositive("connectTimeout", connectTimeout);
        requirePositive("reconnectBaseDelay", reconnectBaseDelay);
        requirePositive("reconnectMaxDelay", reconnectMaxDelay);
        if (reconnectBaseDelay.compareTo(reconnectMaxDelay) > 0) {
            throw new IllegalArgumentException("reconnectBaseDelay " + reconnectBaseDelay
                    + " is larger than reconnectMaxDelay " + reconnectMaxDelay);
        }
    }

    /** All defaults. */
    public static TransportConfig defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    /** How long without inbound bytes before a PING is sent: half the idle timeout. */
    public Duration pingAfter() {
        return idleTimeout.dividedBy(2);
    }

    private static void requirePositive(String name, int value) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive: " + value);
        }
    }

    private static void requirePositive(String name, Duration value) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive: " + value);
        }
    }

    /** Starts from the defaults; validation happens in build(). */
    public static final class Builder {

        private int ioThreads = Runtime.getRuntime().availableProcessors();
        private int maxConnections = 1_000;
        // The OS clamps this to its own limit (kern.ipc.somaxconn is 128 on macOS; net.core.somaxconn is often
        // 4096 on Linux). Java's own default of 50 resets connections under a modest connect burst.
        private int acceptBacklog = 1_024;
        private int lowWatermark = 256 * 1024;
        private int highWatermark = 1024 * 1024;
        private int writeQueueLimit = 16 * 1024 * 1024;
        private Duration idleTimeout = Duration.ofSeconds(30);
        private Duration connectTimeout = Duration.ofSeconds(5);
        private Duration reconnectBaseDelay = Duration.ofMillis(100);
        private Duration reconnectMaxDelay = Duration.ofSeconds(10);
        private boolean tcpNoDelay = true;
        private ProtocolLimits protocolLimits = ProtocolLimits.defaults();

        private Builder() {
        }

        public Builder ioThreads(int ioThreads) {
            this.ioThreads = ioThreads;
            return this;
        }

        public Builder maxConnections(int maxConnections) {
            this.maxConnections = maxConnections;
            return this;
        }

        /** Connections the kernel may queue before accept(); the OS caps it at its own maximum. */
        public Builder acceptBacklog(int acceptBacklog) {
            this.acceptBacklog = acceptBacklog;
            return this;
        }

        /** Below low the connection is writable again; above high it isn't; above limit it is closed. */
        public Builder watermarks(int low, int high, int limit) {
            this.lowWatermark = low;
            this.highWatermark = high;
            this.writeQueueLimit = limit;
            return this;
        }

        public Builder idleTimeout(Duration idleTimeout) {
            this.idleTimeout = idleTimeout;
            return this;
        }

        public Builder connectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder reconnectDelays(Duration base, Duration max) {
            this.reconnectBaseDelay = base;
            this.reconnectMaxDelay = max;
            return this;
        }

        public Builder tcpNoDelay(boolean tcpNoDelay) {
            this.tcpNoDelay = tcpNoDelay;
            return this;
        }

        public Builder protocolLimits(ProtocolLimits protocolLimits) {
            this.protocolLimits = protocolLimits;
            return this;
        }

        public TransportConfig build() {
            return new TransportConfig(ioThreads, maxConnections, acceptBacklog, lowWatermark, highWatermark,
                    writeQueueLimit, idleTimeout, connectTimeout, reconnectBaseDelay, reconnectMaxDelay, tcpNoDelay,
                    protocolLimits);
        }
    }
}
