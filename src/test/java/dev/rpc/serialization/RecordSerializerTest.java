package dev.rpc.serialization;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RecordSerializerTest {

    private static final HexFormat HEX = HexFormat.of();

    private final RecordSerializer serializer = new RecordSerializer();

    // --- types used by the tests; the interface methods stand in for service signatures ---

    enum Color { RED, GREEN, BLUE }

    record Point(int x, int y) { }

    record Shape(String name, Color color, List<Point> points, byte[] tag, Integer weight) { }

    record Node(int value, Node next) { }

    record Tree(String label, List<Tree> children) { }

    record Positive(int n) {
        Positive {
            if (n <= 0) {
                throw new IllegalArgumentException("n must be positive");
            }
        }
    }

    record Box<T>(T value) { }

    record HasObject(Object value) { }

    record HasSet(Set<String> values) { }

    record HasRawList(@SuppressWarnings("rawtypes") List values) { }

    record HasIntArray(int[] values) { }

    record NestedBad(Point ok, HasObject bad) { }

    interface Signatures {
        void primitives(boolean a, byte b, short c, char d, int e, long f, float g, double h);

        void boxes(Boolean a, Byte b, Short c, Character d, Integer e, Long f, Float g, Double h);

        void collections(List<String> strings, Map<String, List<Point>> nested, List<List<Integer>> matrix);

        void wildcard(List<? extends Point> points);
    }

    private static Type[] params(String method) {
        return Arrays.stream(Signatures.class.getMethods())
                .filter(m -> m.getName().equals(method))
                .findFirst()
                .map(Method::getGenericParameterTypes)
                .orElseThrow();
    }

    private Object[] roundTrip(Type[] types, Object... values) {
        return serializer.deserialize(types, serializer.serialize(types, values));
    }

    private Object roundTrip(Type type, Object value) {
        return roundTrip(new Type[] {type}, value)[0];
    }

    // --- wire format ---

    @Test
    void goldenBytes() {
        // Point: presence, x, y. String: presence, i32 length, UTF-8. A primitive int has no presence byte.
        byte[] bytes = serializer.serialize(new Type[] {Point.class, String.class, int.class},
                new Object[] {new Point(1, -1), "é", 7});
        assertEquals("01" + "00000001" + "ffffffff" + "01" + "00000002" + "c3a9" + "00000007", HEX.formatHex(bytes));
    }

    @Test
    void nullIsOneByte() {
        assertEquals("00", HEX.formatHex(serializer.serialize(new Type[] {Shape.class}, new Object[] {null})));
    }

    // --- round trips ---

    @Test
    void primitivesAtTheirLimits() {
        Type[] types = params("primitives");
        Object[] min = {false, Byte.MIN_VALUE, Short.MIN_VALUE, Character.MIN_VALUE, Integer.MIN_VALUE,
            Long.MIN_VALUE, Float.MIN_VALUE, Double.MIN_VALUE};
        Object[] max = {true, Byte.MAX_VALUE, Short.MAX_VALUE, Character.MAX_VALUE, Integer.MAX_VALUE,
            Long.MAX_VALUE, Float.MAX_VALUE, Double.MAX_VALUE};
        assertArrayEquals(min, roundTrip(types, min));
        assertArrayEquals(max, roundTrip(types, max));
    }

    @Test
    void floatingPointBitsSurvive() {
        float oddNan = Float.intBitsToFloat(0x7FC00123);
        double oddDoubleNan = Double.longBitsToDouble(0x7FF8000000000123L);
        Object[] out = roundTrip(params("primitives"), false, (byte) 0, (short) 0, 'x', 0, 0L, oddNan, -0.0);
        assertEquals(Float.floatToRawIntBits(oddNan), Float.floatToRawIntBits((Float) out[6]));
        assertEquals(Double.doubleToRawLongBits(-0.0), Double.doubleToRawLongBits((Double) out[7]));
        Object nan = roundTrip(double.class, oddDoubleNan);
        assertEquals(Double.doubleToRawLongBits(oddDoubleNan), Double.doubleToRawLongBits((Double) nan));
    }

    @Test
    void boxesMayBeNull() {
        Object[] nulls = new Object[8];
        assertArrayEquals(nulls, roundTrip(params("boxes"), nulls));
    }

    @Test
    void strings() {
        for (String s : List.of("", "plain", "é € 漢字", "emoji 😀", "nul \0 inside")) {
            assertEquals(s, roundTrip(String.class, s));
        }
        assertNull(roundTrip(String.class, null));
    }

    @Test
    void recordWithEveryKindOfComponent() {
        var shape = new Shape("tri", Color.BLUE, List.of(new Point(0, 0), new Point(3, 4)), new byte[] {1, 2}, 9);
        Shape out = (Shape) roundTrip(Shape.class, shape);
        assertEquals(shape.name(), out.name());
        assertEquals(shape.color(), out.color());
        assertEquals(shape.points(), out.points());
        assertArrayEquals(shape.tag(), out.tag());
        assertEquals(shape.weight(), out.weight());
    }

    @Test
    void nullAtEveryReferencePosition() {
        var shape = new Shape(null, null, null, null, null);
        Shape out = (Shape) roundTrip(Shape.class, shape);
        assertEquals(shape, out);
        assertNull(roundTrip(Shape.class, null));
    }

    @Test
    void collections() {
        var strings = new ArrayList<String>(List.of("a", "b"));
        strings.add(null);
        var nested = new LinkedHashMap<String, List<Point>>();
        nested.put("one", List.of(new Point(1, 1)));
        nested.put("empty", List.of());
        nested.put(null, null);
        var matrix = List.of(List.of(1, 2), List.<Integer>of(), List.of(3));
        Object[] out = roundTrip(params("collections"), strings, nested, matrix);
        assertEquals(strings, out[0]);
        assertEquals(nested, out[1]);
        assertEquals(new ArrayList<>(nested.keySet()), new ArrayList<>(((Map<?, ?>) out[1]).keySet()));
        assertEquals(matrix, out[2]);
    }

    @Test
    void decodedCollectionsAreUnmodifiable() {
        Object[] out = roundTrip(params("collections"), List.of("a"), Map.of(), List.of());
        assertThrows(UnsupportedOperationException.class, () -> ((List<Object>) out[0]).add("b"));
        assertThrows(UnsupportedOperationException.class, () -> ((Map<Object, Object>) out[1]).put("k", null));
    }

    @Test
    void recursiveRecords() {
        var list = new Node(1, new Node(2, new Node(3, null)));
        assertEquals(list, roundTrip(Node.class, list));

        var tree = new Tree("root", List.of(new Tree("a", List.of()), new Tree("b", List.of(new Tree("c", null)))));
        assertEquals(tree, roundTrip(Tree.class, tree));
    }

    @Test
    void severalValuesAndNone() {
        Type[] types = {Point.class, String.class, long.class};
        assertArrayEquals(new Object[] {new Point(5, 6), "x", 42L}, roundTrip(types, new Point(5, 6), "x", 42L));
        assertEquals(0, serializer.serialize(new Type[0], new Object[0]).length);
        assertEquals(0, serializer.deserialize(new Type[0], new byte[0]).length);
    }

    // --- unsupported types fail when the codec is built, naming where ---

    @Test
    void unsupportedTypesNameTheComponent() {
        assertMessage(HasObject.class, "HasObject.value");
        assertMessage(HasSet.class, "HasSet.values");
        assertMessage(HasRawList.class, "declare its type arguments");
        assertMessage(HasIntArray.class, "HasIntArray.values");
        assertMessage(Box.class, "generic records");
        assertMessage(Runnable.class, "Runnable");
        assertMessage(params("wildcard")[0], "[]");
    }

    @Test
    void unsupportedTypeDeepInsideFailsEvenWhenTheValueIsNull() {
        // The bad component is never reached by this value; the build still rejects the type up front.
        var e = assertThrows(IllegalArgumentException.class,
                () -> serializer.serialize(new Type[] {NestedBad.class}, new Object[] {new NestedBad(null, null)}));
        assertTrue(e.getMessage().contains("NestedBad.bad.value"), e.getMessage());
    }

    private void assertMessage(Type type, String expected) {
        var e = assertThrows(IllegalArgumentException.class,
                () -> serializer.serialize(new Type[] {type}, new Object[] {null}));
        assertTrue(e.getMessage().contains(expected), e.getMessage());
    }

    // --- caller mistakes ---

    @Test
    void lengthMismatch() {
        assertThrows(IllegalArgumentException.class,
                () -> serializer.serialize(new Type[] {int.class}, new Object[] {1, 2}));
    }

    @Test
    void nullForAPrimitive() {
        assertThrows(SerializationException.class,
                () -> serializer.serialize(new Type[] {int.class}, new Object[] {null}));
    }

    @Test
    void valueOfTheWrongType() {
        assertThrows(SerializationException.class,
                () -> serializer.serialize(new Type[] {Point.class}, new Object[] {"not a point"}));
    }

    @Test
    void loneSurrogateIsRejected() {
        assertThrows(SerializationException.class,
                () -> serializer.serialize(new Type[] {String.class}, new Object[] {"bad \uD800"}));
    }

    @Test
    void cyclicObjectGraphHitsTheDepthLimit() {
        // A tree that is its own child: without the depth limit this would be a StackOverflowError.
        var children = new ArrayList<Tree>();
        var tree = new Tree("loop", children);
        children.add(tree);
        assertThrows(SerializationException.class,
                () -> serializer.serialize(new Type[] {Tree.class}, new Object[] {tree}));
    }

    @Test
    void deepButFiniteStructureHitsTheDepthLimitOnWrite() {
        Node head = null;
        for (int i = 0; i < 100; i++) {
            head = new Node(i, head);
        }
        Node deep = head;
        assertThrows(SerializationException.class, () -> roundTrip(Node.class, deep));
        assertEquals(deep, new RecordSerializer(200).deserialize(new Type[] {Node.class},
                new RecordSerializer(200).serialize(new Type[] {Node.class}, new Object[] {deep}))[0]);
    }

    // --- hostile input ---

    @Test
    void hostileCountsAreRejectedBeforeAllocating() {
        Type list = params("collections")[0];
        Type map = params("collections")[1];
        assertDecodeFails(list, "01 7FFFFFFF");
        assertDecodeFails(list, "01 FFFFFFFF");
        assertDecodeFails(list, "01 00000003 0100");         // 3 elements need at least 3 bytes, 2 left
        assertDecodeFails(map, "01 00000002 00 00 00");       // 2 entries need at least 4 bytes, 3 left
        assertDecodeFails(byte[].class, "01 00000005 0102");
        assertDecodeFails(String.class, "01 80000000");
    }

    @Test
    void deeplyNestedInputHitsTheDepthLimitOnRead() {
        // A Node chain 100 deep, written by hand: presence 01, value 0, then the next node.
        String hex = "01 00000000".repeat(100) + "00";
        assertDecodeFails(Node.class, hex);
    }

    @Test
    void malformedValues() {
        assertDecodeFails(boolean.class, "02");
        assertDecodeFails(Point.class, "02 00000001 00000002");    // bad presence marker
        assertDecodeFails(Color.class, "01 00000003");             // ordinal out of range
        assertDecodeFails(Color.class, "01 FFFFFFFF");
        assertDecodeFails(String.class, "01 00000002 C328");       // invalid UTF-8
        assertDecodeFails(String.class, "01 00000002 C0AF");       // overlong encoding
        assertDecodeFails(mapOfStrings(), "01 00000002 01 00000001 61 00 01 00000001 61 00"); // duplicate key
    }

    @Test
    void truncatedAndTrailingBytes() {
        assertDecodeFails(Point.class, "01 00000001 0000");
        assertDecodeFails(long.class, "00000000");
        assertDecodeFails(int.class, "00000001 FF");
        assertDecodeFails(Point.class, "");
    }

    @Test
    void compactConstructorRejectingDecodedValues() {
        var e = assertThrows(SerializationException.class,
                () -> serializer.deserialize(new Type[] {Positive.class}, HEX.parseHex("01FFFFFFFF")));
        assertTrue(e.getMessage().contains("n must be positive"), e.getMessage());
    }

    @Test
    void maxDepthIsValidated() {
        assertThrows(IllegalArgumentException.class, () -> new RecordSerializer(0));
    }

    private static Type mapOfStrings() {
        record Holder(Map<String, String> map) { }
        return Holder.class.getRecordComponents()[0].getGenericType();
    }

    private void assertDecodeFails(Type type, String hex) {
        byte[] bytes = HEX.parseHex(hex.replace(" ", ""));
        assertThrows(SerializationException.class, () -> serializer.deserialize(new Type[] {type}, bytes), hex);
    }
}
