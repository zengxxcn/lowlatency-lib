package jzeng.lowlatency;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Throughput benchmarks for {@link ConflatedValue} (1 producer × N consumers
 * last-value register) vs {@link AtomicReference} and plain-volatile-holder
 * conflation baselines.
 *
 * <p>Consumer-side numbers count polls (mostly "no new data" -1s); the publish rate
 * is the comparable figure. The baseline allocates an immutable holder per publish —
 * its nature; zero-alloc is part of what is measured.
 */
public class ConflatedBenchmarks {

    private static final int PAYLOAD = 32;

    /**
     * Fixed filler for bytes 5..31, identical to the old per-byte loops. Bytes
     * 0..4 stay sequence-derived per publish. Bulk copy instead of hand loop:
     * same bytes, less loop overhead.
     */
    private static final byte[] TEMPLATE = new byte[PAYLOAD];

    static {
        for (int i = 0; i < PAYLOAD; i++) {
            TEMPLATE[i] = (byte) i;
        }
    }

    // ---------------- Conflated 1P x 1C / 1P x 3C ----------------

    @State(Scope.Group)
    public static class ConflatedState {
        ConflatedValue value = new ConflatedValue();
    }

    @State(Scope.Thread)
    public static class ConflatedCursorState {
        ConflatedValue.ConflatedCursor cursor = new ConflatedValue.ConflatedCursor();
        byte[] payload = new byte[PAYLOAD]; // reused: zero per-op allocation
        int seq;
    }

    private static void conflatedPublish(ConflatedValue v, ConflatedCursorState s) {
        final int id = s.seq++;
        byte[] p = s.payload;
        p[0] = (byte) (id >>> 24);
        p[1] = (byte) (id >>> 16);
        p[2] = (byte) (id >>> 8);
        p[3] = (byte) id;
        p[4] = (byte) (id * 31 + 7);
        System.arraycopy(TEMPLATE, 5, p, 5, p.length - 5);
        v.publish(p);
    }

    @Benchmark
    @Group("conflated1p1c")
    @GroupThreads(1)
    public void conflatedProducer(ConflatedState g, ConflatedCursorState s) {
        conflatedPublish(g.value, s);
    }

    @Benchmark
    @Group("conflated1p1c")
    @GroupThreads(1)
    public int conflatedConsumer(ConflatedState g, ConflatedCursorState s, Scratch scratch) {
        return g.value.poll(s.cursor, scratch.buf);
    }

    @Benchmark
    @Group("conflated1p3c")
    @GroupThreads(1)
    public void conflatedProducer3(ConflatedState g, ConflatedCursorState s) {
        conflatedPublish(g.value, s);
    }

    @Benchmark
    @Group("conflated1p3c")
    @GroupThreads(3)
    public int conflatedConsumer3(ConflatedState g, ConflatedCursorState s, Scratch scratch) {
        return g.value.poll(s.cursor, scratch.buf);
    }

    // ---------------- AtomicReference baselines ----------------

    /** Immutable holder; the producer allocates per publish (its nature). */
    static final class VersionedBytes {
        final int seq;
        final byte[] data;

        VersionedBytes(int seq, byte[] data) {
            this.seq = seq;
            this.data = data;
        }
    }

    @State(Scope.Group)
    public static class AtomicRefState {
        AtomicReference<VersionedBytes> ref =
                new AtomicReference<>(new VersionedBytes(-1, new byte[0]));
    }

    @State(Scope.Thread)
    public static class AtomicRefCursor {
        int seq;
        int lastSeen = -2;
    }

    private static byte[] versionedPayload(int id) {
        byte[] data = new byte[PAYLOAD];
        data[0] = (byte) (id >>> 24);
        data[1] = (byte) (id >>> 16);
        data[2] = (byte) (id >>> 8);
        data[3] = (byte) id;
        data[4] = (byte) (id * 31 + 7);
        System.arraycopy(TEMPLATE, 5, data, 5, data.length - 5);
        return data;
    }

    @Benchmark
    @Group("atomicref1p1c")
    @GroupThreads(1)
    public void atomicrefProducer(AtomicRefState g, AtomicRefCursor s) {
        int id = s.seq++;
        g.ref.set(new VersionedBytes(id, versionedPayload(id)));
    }

    @Benchmark
    @Group("atomicref1p1c")
    @GroupThreads(1)
    public int atomicrefConsumer(AtomicRefState g, AtomicRefCursor s) {
        VersionedBytes v = g.ref.get();
        if (v.seq == s.lastSeen) {
            return -1;
        }
        s.lastSeen = v.seq;
        return v.data.length;
    }

    @Benchmark
    @Group("atomicref1p3c")
    @GroupThreads(1)
    public void atomicrefProducer3(AtomicRefState g, AtomicRefCursor s) {
        int id = s.seq++;
        g.ref.set(new VersionedBytes(id, versionedPayload(id)));
    }

    @Benchmark
    @Group("atomicref1p3c")
    @GroupThreads(3)
    public int atomicrefConsumer3(AtomicRefState g, AtomicRefCursor s) {
        VersionedBytes v = g.ref.get();
        if (v.seq == s.lastSeen) {
            return -1;
        }
        s.lastSeen = v.seq;
        return v.data.length;
    }

    // ---------------- helpers ----------------

    // ---------------- volatile-holder baseline ----------------

    @State(Scope.Group)
    public static class VolatileState {
        volatile VersionedBytes current = new VersionedBytes(-1, new byte[0]);
    }

    @State(Scope.Thread)
    public static class VolatileCursor {
        int seq;
        int lastSeen = -2;
    }

    @Benchmark
    @Group("volatile1p1c")
    @GroupThreads(1)
    public void volatileProducer(VolatileState g, VolatileCursor s) {
        int id = s.seq++;
        g.current = new VersionedBytes(id, versionedPayload(id));
    }

    @Benchmark
    @Group("volatile1p1c")
    @GroupThreads(1)
    public int volatileConsumer(VolatileState g, VolatileCursor s) {
        VersionedBytes v = g.current;
        if (v.seq == s.lastSeen) {
            return -1;
        }
        s.lastSeen = v.seq;
        return v.data.length;
    }

    @Benchmark
    @Group("volatile1p3c")
    @GroupThreads(1)
    public void volatileProducer3(VolatileState g, VolatileCursor s) {
        int id = s.seq++;
        g.current = new VersionedBytes(id, versionedPayload(id));
    }

    @Benchmark
    @Group("volatile1p3c")
    @GroupThreads(3)
    public int volatileConsumer3(VolatileState g, VolatileCursor s) {
        VersionedBytes v = g.current;
        if (v.seq == s.lastSeen) {
            return -1;
        }
        s.lastSeen = v.seq;
        return v.data.length;
    }

    @State(Scope.Thread)
    public static class Scratch {
        byte[] buf = new byte[64];
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(ConflatedBenchmarks.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(3)
                .timeUnit(TimeUnit.SECONDS)
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
