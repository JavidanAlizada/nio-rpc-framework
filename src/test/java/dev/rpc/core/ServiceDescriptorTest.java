package dev.rpc.core;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class ServiceDescriptorTest {

    // Greeter.greet's id, pinned when it was first computed. If this changes, every deployed peer stops
    // understanding the other, so a failure here means the id rules changed, not that the test needs updating.
    private static final int PINNED_GREET_ID = 1858103884;

    record Point(int x, int y) { }

    record Size(int x, int y) { }

    interface Greeter {
        String greet(String name);

        CompletableFuture<List<Point>> path(Point from, Point to);

        void ping();

        CompletableFuture<Void> reset();

        default String greetTwice(String name) {
            return greet(name) + greet(name);
        }

        static Greeter none() {
            return null;
        }

        @Override
        String toString();
    }

    // Each scope holds the "same" record before and after one change, so only that change differs.
    static final class Before {
        record Point(int x, int y) { }
    }

    static final class TypeRenamed {
        record Location(int x, int y) { }
    }

    static final class ComponentRenamed {
        record Point(int x, int height) { }
    }

    @Test
    void remoteMethodsAreTheAbstractOnes() {
        var d = ServiceDescriptor.of(Greeter.class);

        assertEquals(List.of("greet", "path", "ping", "reset"),
                d.methods().stream().map(m -> m.method().getName()).sorted().toList());
    }

    @Test
    void localMethodsHaveNoDescriptor() throws Exception {
        var d = ServiceDescriptor.of(Greeter.class);

        assertNull(d.forMethod(Greeter.class.getMethod("greetTwice", String.class)));
        assertNull(d.forMethod(Greeter.class.getMethod("toString")));
        assertNull(d.forMethod(Object.class.getMethod("hashCode")));
    }

    @Test
    void lookupByMethodAndByIdAgree() throws Exception {
        var d = ServiceDescriptor.of(Greeter.class);
        MethodDescriptor greet = d.forMethod(Greeter.class.getMethod("greet", String.class));

        assertSame(greet, d.forId(greet.id()));
        assertFalse(greet.async());
        assertArrayEquals(new Type[] {String.class}, greet.parameterTypes());
        assertArrayEquals(new Type[] {String.class}, greet.resultTypes());
    }

    @Test
    void canonicalSignatureNamesServiceMethodAndFingerprints() {
        String canonical = MethodId.canonical(Greeter.class, "greet", new Type[] {String.class}, String.class);

        assertTrue(canonical.matches("dev\\.rpc\\.core\\.ServiceDescriptorTest\\$Greeter#greet\\([0-9a-f]{16}\\)"
                + "->[0-9a-f]{16}"), canonical);
        assertTrue(MethodId.canonical(Greeter.class, "ping", new Type[0], null).endsWith("#ping()->void"));
    }

    @Test
    void idsAreStableAcrossRunsAndVersions() {
        String canonical = MethodId.canonical(Greeter.class, "greet", new Type[] {String.class}, String.class);

        assertEquals(PINNED_GREET_ID, MethodId.of(canonical));
    }

    @Test
    void asyncAndBlockingFormsShareAnId() throws Exception {
        var d = ServiceDescriptor.of(Greeter.class);
        MethodDescriptor path = d.forMethod(Greeter.class.getMethod("path", Point.class, Point.class));
        MethodDescriptor reset = d.forMethod(Greeter.class.getMethod("reset"));

        Type pathResult = Greeter.class.getMethod("path", Point.class, Point.class).getGenericReturnType();
        Type listOfPoints = ((java.lang.reflect.ParameterizedType) pathResult).getActualTypeArguments()[0];
        assertTrue(path.async());
        assertEquals(MethodId.of(MethodId.canonical(Greeter.class, "path",
                new Type[] {Point.class, Point.class}, listOfPoints)), path.id());
        assertArrayEquals(new Type[] {listOfPoints}, path.resultTypes());

        assertTrue(reset.async());
        assertEquals(MethodId.of(MethodId.canonical(Greeter.class, "reset", new Type[0], null)), reset.id());
        assertEquals(0, reset.resultTypes().length);
    }

    @Test
    void recordShapeChangesTheIdButItsNameDoesNot() {
        int before = idWithParameter(Before.Point.class);

        assertEquals(before, idWithParameter(TypeRenamed.Location.class));
        assertNotEquals(before, idWithParameter(ComponentRenamed.Point.class));
    }

    @Test
    void serviceAndMethodNamesAreInTheId() {
        Type[] params = {String.class};

        assertNotEquals(MethodId.of(MethodId.canonical(Greeter.class, "greet", params, String.class)),
                MethodId.of(MethodId.canonical(Greeter.class, "hello", params, String.class)));
        assertNotEquals(MethodId.of(MethodId.canonical(Greeter.class, "greet", params, String.class)),
                MethodId.of(MethodId.canonical(Overloads.class, "greet", params, String.class)));
    }

    interface Overloads {
        String greet(String name);

        String greet(int times);

        String greet(Map<String, Integer> counts);
    }

    @Test
    void overloadsOfDifferentShapeGetDifferentIds() {
        assertEquals(3, ServiceDescriptor.of(Overloads.class).methods().size());
    }

    interface SameShapeOverloads {
        int area(Point p);

        int area(Size s);
    }

    @Test
    void overloadsOfTheSameShapeAreRejected() {
        var e = assertThrows(IllegalArgumentException.class, () -> ServiceDescriptor.of(SameShapeOverloads.class));

        assertTrue(e.getMessage().contains("area(dev.rpc.core.ServiceDescriptorTest$Point)"), e.getMessage());
        assertTrue(e.getMessage().contains("area(dev.rpc.core.ServiceDescriptorTest$Size)"), e.getMessage());
    }

    interface Left {
        String name(int id);
    }

    interface Right {
        String name(int id);
    }

    interface Both extends Left, Right {
    }

    @Test
    void aSignatureInheritedTwiceIsOneMethod() throws Exception {
        var d = ServiceDescriptor.of(Both.class);

        assertEquals(1, d.methods().size());
        assertSame(d.forMethod(Left.class.getMethod("name", int.class)),
                d.forMethod(Right.class.getMethod("name", int.class)));
    }

    @Test
    void theExposedInterfaceNamesTheServiceNotTheDeclaringOne() throws Exception {
        MethodDescriptor viaBoth = ServiceDescriptor.of(Both.class).forMethod(Left.class.getMethod("name", int.class));
        MethodDescriptor viaLeft = ServiceDescriptor.of(Left.class).forMethod(Left.class.getMethod("name", int.class));

        assertNotEquals(viaLeft.id(), viaBoth.id());
    }

    interface TakesObject {
        void put(Object value);
    }

    interface GenericMethod {
        <T> T echo(T value);
    }

    interface Generic<T> {
        T get();
    }

    interface RawFuture {
        @SuppressWarnings("rawtypes")
        CompletableFuture raw();
    }

    interface WildcardFuture {
        CompletableFuture<?> any();
    }

    static final class NotAnInterface {
    }

    @Test
    void unusableInterfacesFailUpFront() {
        assertRejected(TakesObject.class, "TakesObject.put");
        assertRejected(GenericMethod.class, "generic methods");
        assertRejected(Generic.class, "generic service interfaces");
        assertRejected(RawFuture.class, "declare the CompletableFuture's result type");
        assertRejected(WildcardFuture.class, "declare the CompletableFuture's result type");
        assertRejected(NotAnInterface.class, "not an interface");
        assertRejected(Override.class, "not an interface");
    }

    private static void assertRejected(Class<?> type, String messagePart) {
        var e = assertThrows(IllegalArgumentException.class, () -> ServiceDescriptor.of(type));
        assertTrue(e.getMessage().contains(messagePart), e.getMessage());
    }

    private static int idWithParameter(Class<?> parameter) {
        return MethodId.of(MethodId.canonical(Greeter.class, "area", new Type[] {parameter}, int.class));
    }
}
