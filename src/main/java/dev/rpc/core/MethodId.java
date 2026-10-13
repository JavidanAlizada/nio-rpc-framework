package dev.rpc.core;

import dev.rpc.serialization.SchemaFingerprint;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Method ids: the first 4 bytes of SHA-256 over a canonical signature such as
 * "com.acme.Greeter#greet(5a1f…)->5a1f…". Each type appears as its schema fingerprint, so a peer whose records
 * changed shape gets UNKNOWN_METHOD instead of misreading the payload. The service is the interface being exposed,
 * not the one that happens to declare the method.
 */
final class MethodId {

    private MethodId() {
    }

    /** resultType is the unwrapped result (T of CompletableFuture&lt;T&gt;), or null for void. */
    static String canonical(Class<?> service, String methodName, Type[] parameterTypes, Type resultType) {
        var sb = new StringBuilder(service.getName()).append('#').append(methodName).append('(');
        for (int i = 0; i < parameterTypes.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(hex(SchemaFingerprint.of(parameterTypes[i])));
        }
        sb.append(")->").append(resultType == null ? "void" : hex(SchemaFingerprint.of(resultType)));
        return sb.toString();
    }

    static int of(String canonical) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(digest).getInt();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("every JVM must provide SHA-256", e);
        }
    }

    private static String hex(long fingerprint) {
        return String.format("%016x", fingerprint);
    }
}
