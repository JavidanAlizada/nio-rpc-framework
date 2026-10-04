package dev.rpc.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SchemaFingerprintTest {

    // Each pair below is the "same" record before and after one change, in separate scopes so only that change
    // differs. Type names aren't part of the fingerprint, which the first pair relies on.

    static final class Base {
        record Point(int x, int y) { }

        enum Color { RED, GREEN }

        record Shape(Point origin, List<Point> points, Color color) { }
    }

    static final class Renamed {
        record Location(int x, int y) { }
    }

    static final class Added {
        record Point(int x, int y, int z) { }
    }

    static final class ComponentRenamed {
        record Point(int x, int height) { }
    }

    static final class Reordered {
        record Point(int y, int x) { }
    }

    static final class Retyped {
        record Point(int x, long y) { }
    }

    static final class Boxed {
        record Point(int x, Integer y) { }
    }

    static final class EnumAdded {
        enum Color { RED, GREEN, BLUE }
    }

    static final class EnumReordered {
        enum Color { GREEN, RED }
    }

    static final class DeepChange {
        record Point(int x, int y, int z) { }

        enum Color { RED, GREEN }

        record Shape(Point origin, List<Point> points, Color color) { }
    }

    record Node(int value, Node next) { }

    record Tree(String label, List<Tree> children) { }

    record Twice(Base.Point a, Base.Point b) { }

    record Unsupported(Object value) { }

    record Holder(List<String> strings, Map<String, List<Base.Point>> nested, List<Integer> boxes) { }

    private static Type component(Class<?> record, int index) {
        return record.getRecordComponents()[index].getGenericType();
    }

    // --- canonical text ---

    @Test
    void describesEachKindOfType() {
        assertEquals("int", SchemaFingerprint.describe(int.class));
        assertEquals("java.lang.Integer", SchemaFingerprint.describe(Integer.class));
        assertEquals("java.lang.String", SchemaFingerprint.describe(String.class));
        assertEquals("bytes", SchemaFingerprint.describe(byte[].class));
        assertEquals("enum{RED,GREEN}", SchemaFingerprint.describe(Base.Color.class));
        assertEquals("record(x:int,y:int)", SchemaFingerprint.describe(Base.Point.class));
        assertEquals("list<java.lang.String>", SchemaFingerprint.describe(component(Holder.class, 0)));
        assertEquals("map<java.lang.String,list<record(x:int,y:int)>>",
                SchemaFingerprint.describe(component(Holder.class, 1)));
        assertEquals("record(origin:record(x:int,y:int),points:list<ref 1>,color:enum{RED,GREEN})",
                SchemaFingerprint.describe(Base.Shape.class));
    }

    @Test
    void recursiveTypesTerminate() {
        assertEquals("record(value:int,next:ref 0)", SchemaFingerprint.describe(Node.class));
        assertEquals("record(label:java.lang.String,children:list<ref 0>)", SchemaFingerprint.describe(Tree.class));
        SchemaFingerprint.of(Node.class);
        SchemaFingerprint.of(Tree.class);
    }

    @Test
    void aRepeatedRecordIsWrittenOutOnce() {
        assertEquals("record(a:record(x:int,y:int),b:ref 1)", SchemaFingerprint.describe(Twice.class));
    }

    // --- stability: these values must never change, or every deployed method id changes with them ---

    // Computed outside Java: the first 8 bytes, big-endian, of SHA-256 over the canonical text above.
    @Test
    void goldenValues() {
        assertEquals(0x6DA88C34BA124C41L, SchemaFingerprint.of(int.class));
        assertEquals(0x73CC7CE309D9BE85L, SchemaFingerprint.of(String.class));
        assertEquals(0xD2A16094C8887D20L, SchemaFingerprint.of(Base.Point.class));
        assertEquals(0xDE05B4A2D9E92263L, SchemaFingerprint.of(Node.class));
    }

    @Test
    void sameTypeSameFingerprint() {
        assertEquals(SchemaFingerprint.of(Base.Shape.class), SchemaFingerprint.of(Base.Shape.class));
    }

    // --- what changes it, and what doesn't ---

    @Test
    void renamingTheTypeKeepsTheFingerprint() {
        assertEquals(SchemaFingerprint.of(Base.Point.class), SchemaFingerprint.of(Renamed.Location.class));
    }

    @Test
    void everyWireRelevantChangeChangesTheFingerprint() {
        long base = SchemaFingerprint.of(Base.Point.class);
        assertNotEquals(base, SchemaFingerprint.of(Added.Point.class), "component added");
        assertNotEquals(base, SchemaFingerprint.of(ComponentRenamed.Point.class), "component renamed");
        assertNotEquals(base, SchemaFingerprint.of(Reordered.Point.class), "components reordered");
        assertNotEquals(base, SchemaFingerprint.of(Retyped.Point.class), "component retyped");
        assertNotEquals(base, SchemaFingerprint.of(Boxed.Point.class), "primitive boxed");
    }

    @Test
    void enumChangesChangeTheFingerprint() {
        long base = SchemaFingerprint.of(Base.Color.class);
        assertNotEquals(base, SchemaFingerprint.of(EnumAdded.Color.class), "constant added");
        assertNotEquals(base, SchemaFingerprint.of(EnumReordered.Color.class), "constants reordered");
    }

    @Test
    void aChangeDeepInsideReachesTheTop() {
        assertNotEquals(SchemaFingerprint.of(Base.Shape.class), SchemaFingerprint.of(DeepChange.Shape.class));
    }

    @Test
    void collectionTypeArgumentsCount() {
        assertNotEquals(SchemaFingerprint.of(component(Holder.class, 0)),
                SchemaFingerprint.of(component(Holder.class, 2)));
    }

    @Test
    void unsupportedTypesAreRejectedLikeTheCodecRejectsThem() {
        var e = assertThrows(IllegalArgumentException.class, () -> SchemaFingerprint.of(Unsupported.class));
        assertTrue(e.getMessage().contains("Unsupported.value"), e.getMessage());
    }
}
