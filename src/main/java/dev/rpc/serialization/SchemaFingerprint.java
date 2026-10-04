package dev.rpc.serialization;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A 64-bit hash of everything about a type that decides how RecordSerializer reads and writes it. Two peers whose
 * fingerprints match will decode each other's bytes as intended; the RPC core folds it into method ids, so a
 * mismatch surfaces as an unknown method rather than as garbage.
 *
 * Covered: component names, types and order; enum constant names (enums travel as ordinals); collection type
 * arguments; boxed vs. primitive (the null marker). Not covered: record and enum type names, which never reach the
 * wire, so renaming or moving a type stays compatible.
 */
public final class SchemaFingerprint {

    private SchemaFingerprint() {
    }

    /** The fingerprint of a supported type; unsupported types are rejected exactly as the codec rejects them. */
    public static long of(Type type) {
        TypeCodecs.forType(type);
        try {
            byte[] text = describe(type).getBytes(StandardCharsets.UTF_8);
            return ByteBuffer.wrap(MessageDigest.getInstance("SHA-256").digest(text)).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("every JVM must provide SHA-256", e);
        }
    }

    /**
     * The canonical text the fingerprint hashes, e.g. "record(x:int,y:int)". Java identifiers can't contain the
     * punctuation used here, so two different structures can't produce the same text.
     */
    static String describe(Type type) {
        var sb = new StringBuilder();
        append(sb, type, new HashMap<>());
        return sb.toString();
    }

    // seen numbers records in the order they're first reached, so a repeat or a cycle becomes "ref N".
    private static void append(StringBuilder sb, Type type, Map<Class<?>, Integer> seen) {
        if (type instanceof ParameterizedType p) {
            sb.append(p.getRawType() == List.class ? "list<" : "map<");
            Type[] args = p.getActualTypeArguments();
            for (int i = 0; i < args.length; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                append(sb, args[i], seen);
            }
            sb.append('>');
            return;
        }
        Class<?> c = (Class<?>) type;
        if (c == byte[].class) {
            sb.append("bytes");
        } else if (c.isEnum()) {
            sb.append("enum{")
                    .append(Arrays.stream(c.getEnumConstants())
                            .map(e -> ((Enum<?>) e).name())
                            .collect(Collectors.joining(",")))
                    .append('}');
        } else if (c.isRecord()) {
            Integer ref = seen.get(c);
            if (ref != null) {
                sb.append("ref ").append(ref);
                return;
            }
            seen.put(c, seen.size());
            sb.append("record(");
            RecordComponent[] components = c.getRecordComponents();
            for (int i = 0; i < components.length; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(components[i].getName()).append(':');
                append(sb, components[i].getGenericType(), seen);
            }
            sb.append(')');
        } else {
            // Primitives, boxes and String: the class name says everything about their encoding.
            sb.append(c.getName());
        }
    }
}
