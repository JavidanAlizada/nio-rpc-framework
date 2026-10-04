package dev.rpc.serialization;

import java.lang.reflect.Type;

/**
 * Encodes a method's argument list (or a one-element result) as payload bytes. Both sides know the types from the
 * shared service interface, so they aren't written to the wire. RecordSerializer is the built-in implementation.
 */
public interface Serializer {

    /** Encodes values, one per type, in order. */
    byte[] serialize(Type[] types, Object[] values);

    /** Decodes exactly types.length values; leftover bytes are an error. */
    Object[] deserialize(Type[] types, byte[] data);
}
