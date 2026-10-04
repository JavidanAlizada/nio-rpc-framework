package dev.rpc.serialization;

/** Values couldn't be encoded, or bytes couldn't be decoded into the expected types. */
public final class SerializationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SerializationException(String message) {
        super(message);
    }

    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
