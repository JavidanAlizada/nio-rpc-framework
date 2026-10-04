package dev.rpc.serialization;

import java.lang.reflect.Type;
import java.util.Objects;

/**
 * The built-in Serializer: positional binary encoding of primitives and their boxes, String, byte[], enums, List,
 * Map and records made of those. Reference-typed values carry a 1-byte null marker; nothing else describes the
 * schema, so both sides must agree on the types. Thread-safe.
 */
public final class RecordSerializer implements Serializer {

    public static final int DEFAULT_MAX_DEPTH = 64;

    private final int maxDepth;

    public RecordSerializer() {
        this(DEFAULT_MAX_DEPTH);
    }

    /** maxDepth bounds record and collection nesting, so hostile input can't overflow the stack. */
    public RecordSerializer(int maxDepth) {
        if (maxDepth < 1) {
            throw new IllegalArgumentException("maxDepth must be >= 1: " + maxDepth);
        }
        this.maxDepth = maxDepth;
    }

    @Override
    public byte[] serialize(Type[] types, Object[] values) {
        checkLengths(types, values.length);
        Output out = new Output(maxDepth);
        for (int i = 0; i < types.length; i++) {
            if (values[i] == null && types[i] instanceof Class<?> c && c.isPrimitive()) {
                throw new SerializationException("value " + i + " is null but its type is " + c);
            }
            try {
                TypeCodecs.forType(types[i]).write(out, values[i], 0);
            } catch (ClassCastException e) {
                throw new SerializationException("value " + i + " doesn't match its declared type "
                        + types[i].getTypeName(), e);
            }
        }
        return out.toByteArray();
    }

    @Override
    public Object[] deserialize(Type[] types, byte[] data) {
        checkLengths(types, -1);
        Objects.requireNonNull(data, "data");
        Input in = new Input(data, maxDepth);
        Object[] values = new Object[types.length];
        for (int i = 0; i < types.length; i++) {
            values[i] = TypeCodecs.forType(types[i]).read(in, 0);
        }
        in.requireEnd();
        return values;
    }

    private static void checkLengths(Type[] types, int valueCount) {
        Objects.requireNonNull(types, "types");
        if (valueCount >= 0 && valueCount != types.length) {
            throw new IllegalArgumentException(types.length + " types but " + valueCount + " values");
        }
    }
}
