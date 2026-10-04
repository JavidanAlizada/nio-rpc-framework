package dev.rpc.serialization;

import java.util.Map;

/** Leaf codecs: primitives (also used for their boxes), String, byte[] and enums. */
final class ScalarCodecs {

    private ScalarCodecs() {
    }

    enum Primitive implements TypeCodec {
        BOOLEAN {
            @Override
            public void write(Output out, Object value, int depth) {
                out.writeByte((Boolean) value ? 1 : 0);
            }

            @Override
            public Object read(Input in, int depth) {
                byte b = in.readByte();
                if (b != 0 && b != 1) {
                    throw new SerializationException("invalid boolean byte " + b);
                }
                return b == 1;
            }
        },
        BYTE {
            @Override
            public void write(Output out, Object value, int depth) {
                out.writeByte((Byte) value);
            }

            @Override
            public Object read(Input in, int depth) {
                return in.readByte();
            }
        },
        SHORT {
            @Override
            public void write(Output out, Object value, int depth) {
                out.writeShort((Short) value);
            }

            @Override
            public Object read(Input in, int depth) {
                return in.readShort();
            }
        },
        CHAR {
            @Override
            public void write(Output out, Object value, int depth) {
                out.writeShort((Character) value);
            }

            @Override
            public Object read(Input in, int depth) {
                return in.readChar();
            }
        },
        INT {
            @Override
            public void write(Output out, Object value, int depth) {
                out.writeInt((Integer) value);
            }

            @Override
            public Object read(Input in, int depth) {
                return in.readInt();
            }
        },
        LONG {
            @Override
            public void write(Output out, Object value, int depth) {
                out.writeLong((Long) value);
            }

            @Override
            public Object read(Input in, int depth) {
                return in.readLong();
            }
        },
        // Raw bits, so every NaN payload and -0.0 survive the round trip unchanged.
        FLOAT {
            @Override
            public void write(Output out, Object value, int depth) {
                out.writeInt(Float.floatToRawIntBits((Float) value));
            }

            @Override
            public Object read(Input in, int depth) {
                return Float.intBitsToFloat(in.readInt());
            }
        },
        DOUBLE {
            @Override
            public void write(Output out, Object value, int depth) {
                out.writeLong(Double.doubleToRawLongBits((Double) value));
            }

            @Override
            public Object read(Input in, int depth) {
                return Double.longBitsToDouble(in.readLong());
            }
        };

        private static final Map<Class<?>, Primitive> BY_CLASS = Map.ofEntries(
                Map.entry(boolean.class, BOOLEAN), Map.entry(Boolean.class, BOOLEAN),
                Map.entry(byte.class, BYTE), Map.entry(Byte.class, BYTE),
                Map.entry(short.class, SHORT), Map.entry(Short.class, SHORT),
                Map.entry(char.class, CHAR), Map.entry(Character.class, CHAR),
                Map.entry(int.class, INT), Map.entry(Integer.class, INT),
                Map.entry(long.class, LONG), Map.entry(Long.class, LONG),
                Map.entry(float.class, FLOAT), Map.entry(Float.class, FLOAT),
                Map.entry(double.class, DOUBLE), Map.entry(Double.class, DOUBLE));

        /** The codec for a primitive or boxed class, or null if c is neither. */
        static Primitive of(Class<?> c) {
            return BY_CLASS.get(c);
        }
    }

    static final TypeCodec STRING = new TypeCodec() {
        @Override
        public void write(Output out, Object value, int depth) {
            out.writeString((String) value);
        }

        @Override
        public Object read(Input in, int depth) {
            return in.readString();
        }
    };

    static final TypeCodec BYTES = new TypeCodec() {
        @Override
        public void write(Output out, Object value, int depth) {
            byte[] bytes = (byte[]) value;
            out.writeInt(bytes.length);
            out.writeBytes(bytes);
        }

        @Override
        public Object read(Input in, int depth) {
            return in.readBytes(in.readCount(1));
        }
    };

    /** Written as the ordinal. Reordering constants changes the meaning, which the schema fingerprint catches. */
    static final class EnumCodec implements TypeCodec {

        private final Class<?> type;
        private final Object[] constants;

        EnumCodec(Class<?> type) {
            this.type = type;
            this.constants = type.getEnumConstants();
        }

        @Override
        public void write(Output out, Object value, int depth) {
            out.writeInt(((Enum<?>) type.cast(value)).ordinal());
        }

        @Override
        public Object read(Input in, int depth) {
            int ordinal = in.readInt();
            if (ordinal < 0 || ordinal >= constants.length) {
                throw new SerializationException("ordinal " + ordinal + " out of range for " + type.getName());
            }
            return constants[ordinal];
        }
    }

    /** Puts the 1-byte presence marker in front of a reference-typed value. */
    static final class Nullable implements TypeCodec {

        private final TypeCodec inner;

        Nullable(TypeCodec inner) {
            this.inner = inner;
        }

        @Override
        public void write(Output out, Object value, int depth) {
            if (value == null) {
                out.writeByte(0);
            } else {
                out.writeByte(1);
                inner.write(out, value, depth);
            }
        }

        @Override
        public Object read(Input in, int depth) {
            return in.readPresence() ? inner.read(in, depth) : null;
        }
    }
}
