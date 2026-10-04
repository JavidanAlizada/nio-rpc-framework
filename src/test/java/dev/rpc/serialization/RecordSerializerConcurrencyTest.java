package dev.rpc.serialization;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

// Codecs are built lazily and cached in a ClassValue, so the first uses of a class race to build its codec.
// These record types are used by no other test, so every thread here really does hit an empty cache.
class RecordSerializerConcurrencyTest {

    record A(int n, String s) { }

    record B(A a, List<A> as) { }

    record C(B b, C next) { }

    @Test
    void manyThreadsBuildAndUseCodecsAtOnce() throws Exception {
        int threads = 16;
        var serializer = new RecordSerializer();
        var start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> results = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int id = t;
                results.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < 500; i++) {
                        var a = new A(id, "t" + i);
                        var c = new C(new B(a, List.of(a, a)), new C(null, null));
                        Type type = switch ((id + i) % 3) {
                            case 0 -> A.class;
                            case 1 -> B.class;
                            default -> C.class;
                        };
                        Object value = type == A.class ? a : type == B.class ? c.b() : c;
                        byte[] bytes = serializer.serialize(new Type[] {type}, new Object[] {value});
                        assertEquals(value, serializer.deserialize(new Type[] {type}, bytes)[0]);
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : results) {
                f.get();
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
