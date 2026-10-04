package dev.rpc.protocol;

/**
 * Size limits shared by the encoder and the decoder. The body cap is checked against the header before any body
 * memory is allocated.
 */
public record ProtocolLimits(int maxBodySize) {

    public static final int DEFAULT_MAX_BODY_SIZE = 4 * 1024 * 1024;

    public ProtocolLimits {
        if (maxBodySize <= 0 || maxBodySize > Integer.MAX_VALUE - FrameFormat.HEADER_SIZE) {
            throw new IllegalArgumentException("maxBodySize out of range: " + maxBodySize);
        }
    }

    /** A 4 MiB body cap. */
    public static ProtocolLimits defaults() {
        return new ProtocolLimits(DEFAULT_MAX_BODY_SIZE);
    }
}
