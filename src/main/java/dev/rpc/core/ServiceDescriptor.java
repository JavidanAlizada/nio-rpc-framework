package dev.rpc.core;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The remote methods of one service interface, checked up front: an interface that can't be served or called fails
 * here, when a stub is created or a service registered, not on its first call. Immutable.
 *
 * Remote methods are the abstract ones. Static and default methods stay local, and so do redeclared Object methods
 * (toString and friends), which a proxy answers itself.
 */
final class ServiceDescriptor {

    private final Class<?> type;
    private final Map<Method, MethodDescriptor> byMethod;
    private final Map<Integer, MethodDescriptor> byId;

    private ServiceDescriptor(Class<?> type, Map<Method, MethodDescriptor> byMethod,
            Map<Integer, MethodDescriptor> byId) {
        this.type = type;
        this.byMethod = byMethod;
        this.byId = byId;
    }

    static ServiceDescriptor of(Class<?> type) {
        if (!type.isInterface() || type.isAnnotation()) {
            throw new IllegalArgumentException(type.getName() + " is not an interface");
        }
        if (type.getTypeParameters().length > 0) {
            throw new IllegalArgumentException("generic service interfaces aren't supported: " + type.getName());
        }
        var byMethod = new HashMap<Method, MethodDescriptor>();
        var byId = new LinkedHashMap<Integer, MethodDescriptor>();
        for (Method m : type.getMethods()) {
            if (Modifier.isStatic(m.getModifiers()) || m.isDefault() || isObjectMethod(m)) {
                continue;
            }
            MethodDescriptor d = describe(type, m);
            MethodDescriptor existing = byId.putIfAbsent(d.id(), d);
            if (existing == null) {
                byMethod.put(m, d);
            } else if (sameJavaSignature(existing.method(), m)) {
                // The same signature inherited through two superinterfaces is one remote method.
                byMethod.put(m, existing);
            } else {
                throw new IllegalArgumentException("method id " + d.id() + " is shared by " + existing.method()
                        + " and " + m + "; overloads need parameter types of different shape, otherwise rename one");
            }
        }
        return new ServiceDescriptor(type, Collections.unmodifiableMap(byMethod), Collections.unmodifiableMap(byId));
    }

    Class<?> type() {
        return type;
    }

    /** The descriptor for a method of this interface, or null if the method is local. */
    MethodDescriptor forMethod(Method method) {
        return byMethod.get(method);
    }

    /** The descriptor for a method id, or null if this service has no such method. */
    MethodDescriptor forId(int id) {
        return byId.get(id);
    }

    Collection<MethodDescriptor> methods() {
        return byId.values();
    }

    private static MethodDescriptor describe(Class<?> service, Method m) {
        String where = service.getName() + "." + m.getName();
        if (m.getTypeParameters().length > 0) {
            throw new IllegalArgumentException("generic methods aren't supported: " + where);
        }
        Type returnType = m.getGenericReturnType();
        boolean async = m.getReturnType() == CompletableFuture.class;
        Type result = async ? futureResult(returnType, where) : returnType;
        if (result == void.class || result == Void.class) {
            result = null;
        }
        Type[] params = m.getGenericParameterTypes();
        String canonical;
        try {
            canonical = MethodId.canonical(service, m.getName(), params, result);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(where + ": " + e.getMessage(), e);
        }
        Type[] results = result == null ? new Type[0] : new Type[] {result};
        return new MethodDescriptor(MethodId.of(canonical), m, params, results, async);
    }

    private static Type futureResult(Type returnType, String where) {
        if (returnType instanceof ParameterizedType p) {
            Type arg = p.getActualTypeArguments()[0];
            if (arg instanceof Class<?> || arg instanceof ParameterizedType) {
                return arg;
            }
        }
        throw new IllegalArgumentException(where + ": declare the CompletableFuture's result type, e.g."
                + " CompletableFuture<String>; raw types and wildcards aren't supported");
    }

    private static boolean sameJavaSignature(Method a, Method b) {
        return a.getName().equals(b.getName()) && Arrays.equals(a.getParameterTypes(), b.getParameterTypes());
    }

    private static boolean isObjectMethod(Method m) {
        try {
            Object.class.getMethod(m.getName(), m.getParameterTypes());
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }
}
