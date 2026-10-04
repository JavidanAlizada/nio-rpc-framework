package dev.rpc.serialization;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** List and Map codecs. Decoded collections are unmodifiable; null elements, keys and values are allowed. */
final class ContainerCodecs {

    private ContainerCodecs() {
    }

    static final class ListCodec implements TypeCodec {

        private final TypeCodec element;

        ListCodec(TypeCodec element) {
            this.element = element;
        }

        @Override
        public void write(Output out, Object value, int depth) {
            TypeCodec.checkDepth(depth, out.maxDepth);
            List<?> list = (List<?>) value;
            out.writeInt(list.size());
            for (Object e : list) {
                element.write(out, e, depth + 1);
            }
        }

        @Override
        public Object read(Input in, int depth) {
            TypeCodec.checkDepth(depth, in.maxDepth);
            // Elements are always reference-typed, so each carries at least its presence byte.
            int count = in.readCount(1);
            List<Object> list = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                list.add(element.read(in, depth + 1));
            }
            return Collections.unmodifiableList(list);
        }
    }

    static final class MapCodec implements TypeCodec {

        private final TypeCodec key;
        private final TypeCodec value;

        MapCodec(TypeCodec key, TypeCodec value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public void write(Output out, Object v, int depth) {
            TypeCodec.checkDepth(depth, out.maxDepth);
            Map<?, ?> map = (Map<?, ?>) v;
            out.writeInt(map.size());
            for (Map.Entry<?, ?> e : map.entrySet()) {
                key.write(out, e.getKey(), depth + 1);
                value.write(out, e.getValue(), depth + 1);
            }
        }

        @Override
        public Object read(Input in, int depth) {
            TypeCodec.checkDepth(depth, in.maxDepth);
            int count = in.readCount(2);
            Map<Object, Object> map = new LinkedHashMap<>(Math.max(16, count * 2));
            for (int i = 0; i < count; i++) {
                Object k = key.read(in, depth + 1);
                if (map.containsKey(k)) {
                    throw new SerializationException("duplicate map key " + k);
                }
                map.put(k, value.read(in, depth + 1));
            }
            return Collections.unmodifiableMap(map);
        }
    }
}
