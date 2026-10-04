package dev.rpc.serialization;

import dev.rpc.serialization.ContainerCodecs.ListCodec;
import dev.rpc.serialization.ContainerCodecs.MapCodec;
import dev.rpc.serialization.ScalarCodecs.EnumCodec;
import dev.rpc.serialization.ScalarCodecs.Nullable;
import dev.rpc.serialization.ScalarCodecs.Primitive;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds the codec tree for a declared type, and caches it per class.
 *
 * A whole tree is built eagerly, so an unsupported type anywhere inside it fails when the codec is first built,
 * not halfway through encoding a value. The one exception is a record that refers back to itself (directly or
 * through other records): that reference becomes a lazy lookup in the cache, which is what lets the build finish.
 */
final class TypeCodecs {

    private static final ClassValue<TypeCodec> REGISTRY = new ClassValue<>() {
        @Override
        protected TypeCodec computeValue(Class<?> type) {
            return build(type, new HashSet<>(), type.getSimpleName());
        }
    };

    private TypeCodecs() {
    }

    /** The codec for a declared type: cached for plain classes, built fresh for parameterized ones. */
    static TypeCodec forType(Type type) {
        return type instanceof Class<?> c ? REGISTRY.get(c) : build(type, new HashSet<>(), type.getTypeName());
    }

    static TypeCodec build(Type type, Set<Class<?>> building, String path) {
        if (type instanceof Class<?> c) {
            return buildClass(c, building, path);
        }
        if (type instanceof ParameterizedType p) {
            Type[] args = p.getActualTypeArguments();
            if (p.getRawType() == List.class) {
                return new Nullable(new ListCodec(build(args[0], building, path + "[]")));
            }
            if (p.getRawType() == Map.class) {
                return new Nullable(new MapCodec(build(args[0], building, path + "{key}"),
                        build(args[1], building, path + "{value}")));
            }
        }
        throw unsupported(type, path);
    }

    private static TypeCodec buildClass(Class<?> c, Set<Class<?>> building, String path) {
        Primitive primitive = Primitive.of(c);
        if (primitive != null) {
            return c.isPrimitive() ? primitive : new Nullable(primitive);
        }
        if (c == String.class) {
            return new Nullable(ScalarCodecs.STRING);
        }
        if (c == byte[].class) {
            return new Nullable(ScalarCodecs.BYTES);
        }
        if (c.isEnum()) {
            return new Nullable(new EnumCodec(c));
        }
        if (c.isRecord()) {
            if (c.getTypeParameters().length > 0) {
                throw unsupported(c, path + " (generic records aren't supported)");
            }
            if (!building.add(c)) {
                return new LazyRecordCodec(c);
            }
            try {
                return new Nullable(RecordCodec.build(c, building, path));
            } finally {
                building.remove(c);
            }
        }
        if (c == List.class || c == Map.class) {
            throw unsupported(c, path + " (declare its type arguments)");
        }
        throw unsupported(c, path);
    }

    private static IllegalArgumentException unsupported(Type type, String path) {
        return new IllegalArgumentException("unsupported type " + type.getTypeName() + " at " + path
                + "; supported: primitives and boxes, String, byte[], enums, List, Map and records of those");
    }

    /** A record's reference to itself; resolved from the registry on each use, presence marker included. */
    private static final class LazyRecordCodec implements TypeCodec {

        private final Class<?> type;

        LazyRecordCodec(Class<?> type) {
            this.type = type;
        }

        @Override
        public void write(Output out, Object value, int depth) {
            REGISTRY.get(type).write(out, value, depth);
        }

        @Override
        public Object read(Input in, int depth) {
            return REGISTRY.get(type).read(in, depth);
        }
    }
}
