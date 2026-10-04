package dev.rpc.serialization;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.RecordComponent;
import java.util.Set;

/**
 * A record as its components in declaration order, with no names or tags on the wire. Built once per record class;
 * the canonical constructor and accessors are resolved to method handles at build time.
 */
final class RecordCodec implements TypeCodec {

    private final Class<?> type;
    private final TypeCodec[] components;
    private final MethodHandle[] accessors;
    private final MethodHandle constructor;

    private RecordCodec(Class<?> type, TypeCodec[] components, MethodHandle[] accessors, MethodHandle constructor) {
        this.type = type;
        this.components = components;
        this.accessors = accessors;
        this.constructor = constructor;
    }

    static RecordCodec build(Class<?> type, Set<Class<?>> building, String path) {
        RecordComponent[] rc = type.getRecordComponents();
        TypeCodec[] components = new TypeCodec[rc.length];
        MethodHandle[] accessors = new MethodHandle[rc.length];
        Class<?>[] parameterTypes = new Class<?>[rc.length];
        MethodHandles.Lookup lookup = lookupFor(type);
        try {
            for (int i = 0; i < rc.length; i++) {
                components[i] = TypeCodecs.build(rc[i].getGenericType(), building, path + "." + rc[i].getName());
                accessors[i] = lookup.unreflect(rc[i].getAccessor())
                        .asType(MethodType.methodType(Object.class, Object.class));
                parameterTypes[i] = rc[i].getType();
            }
            MethodHandle constructor = lookup.findConstructor(type, MethodType.methodType(void.class, parameterTypes))
                    .asType(MethodType.genericMethodType(rc.length))
                    .asSpreader(Object[].class, rc.length);
            return new RecordCodec(type, components, accessors, constructor);
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("can't access record " + type.getName(), e);
        }
    }

    // Records are often package-private or nested; a private lookup reaches them as long as their package is open
    // to this module, which is always true on the class path.
    private static MethodHandles.Lookup lookupFor(Class<?> type) {
        try {
            return MethodHandles.privateLookupIn(type, MethodHandles.lookup());
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("record " + type.getName() + " isn't accessible; open its package to "
                    + RecordCodec.class.getModule().getName(), e);
        }
    }

    @Override
    public void write(Output out, Object value, int depth) {
        TypeCodec.checkDepth(depth, out.maxDepth);
        Object record = type.cast(value);
        for (int i = 0; i < components.length; i++) {
            Object component;
            try {
                component = (Object) accessors[i].invokeExact(record);
            } catch (RuntimeException | Error e) {
                throw e;
            } catch (Throwable t) {
                throw new SerializationException("accessor of " + type.getName() + " failed", t);
            }
            components[i].write(out, component, depth + 1);
        }
    }

    @Override
    public Object read(Input in, int depth) {
        TypeCodec.checkDepth(depth, in.maxDepth);
        Object[] args = new Object[components.length];
        for (int i = 0; i < args.length; i++) {
            args[i] = components[i].read(in, depth + 1);
        }
        try {
            return (Object) constructor.invokeExact(args);
        } catch (Error e) {
            throw e;
        } catch (Throwable t) {
            // Usually a compact constructor rejecting the decoded values: bad data, not a bug here.
            throw new SerializationException(type.getName() + " rejected the decoded values: " + t.getMessage(), t);
        }
    }
}
