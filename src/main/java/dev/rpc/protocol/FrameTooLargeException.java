package dev.rpc.protocol;

/** Thrown when encoding a frame whose body exceeds the cap. Only that call fails; the connection is unaffected. */
public final class FrameTooLargeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final long bodyLength;
    private final int maxBodySize;

    public FrameTooLargeException(long bodyLength, int maxBodySize) {
        super("frame body is " + bodyLength + " bytes, the limit is " + maxBodySize);
        this.bodyLength = bodyLength;
        this.maxBodySize = maxBodySize;
    }

    public long bodyLength() {
        return bodyLength;
    }

    public int maxBodySize() {
        return maxBodySize;
    }
}
