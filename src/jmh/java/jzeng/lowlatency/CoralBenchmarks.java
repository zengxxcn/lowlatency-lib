package jzeng.lowlatency;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.coralblocks.coralqueue.broadcaster.AtomicBroadcaster;
import com.coralblocks.coralqueue.queue.AtomicQueue;
import com.coralblocks.coralqueue.raw.ByteBufferRawQueue;
import com.coralblocks.coralqueue.raw.RawBytes;
import com.coralblocks.coralqueue.util.Builder;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CoralQueue baselines vs our rings: same test shape (single producer, 32 B
 * payload, capacity 1024 slots), each Coral structure at batch=1 and batch=8.
 * Batching is Coral's load-bearing optimisation (one flush() publishes the whole
 * batch), so batch=1 flatters us and batch=8 flatters them — both are reported.
 *
 * <p>Groups: coralSpsc1p1c (AtomicQueue) vs our spsc1p1c; coralSpmc1p3c
 * (AtomicBroadcaster, multicast like our SPMC) vs our spmc1p3c; coralRaw1p1c
 * (ByteBufferRawQueue, direct buffer) vs our spsc1p1c.
 *
 * <p>Every spin carries the same ~10 ms starvation bail as RingBenchmarks: at
 * iteration end the producer may exit while a consumer waits for data that will
 * never arrive, which would hang JMH's teardown barrier forever.
 */
public class CoralBenchmarks {

    private static final int CAPACITY = 1024;
    private static final int PAYLOAD = 32;
    private static final int BATCH = 8;

    /**
     * Fixed fill pattern, identical bytes to the old per-byte loops. Fills go
     * through System.arraycopy and checksums observe two bytes (anti-DCE only):
     * same per-message work, less loop overhead.
     */
    private static final byte[] TEMPLATE = new byte[PAYLOAD];

    static {
        for (int i = 0; i < PAYLOAD; i++) {
            TEMPLATE[i] = (byte) i;
        }
    }

    /** Preallocated transfer object — filled in place, never reallocated. */
    public static final class Msg {
        final byte[] body = new byte[PAYLOAD];
    }

    private static Builder<Msg> msgBuilder() {
        // Explicit Builder (not reflection): created once per group state,
        // never on the hot path.
        return new Builder<Msg>() {
            @Override
            public Msg newInstance() {
                return new Msg();
            }
        };
    }

    @State(Scope.Thread)
    public static class Scratch {
        final byte[] buf = new byte[PAYLOAD];
        long sink; // checksum accumulator (anti-DCE for batch copies)
    }

    /** 10 ms starvation bail shared by all spins; lazy-start masked check. */
    private static boolean starved(long spins, long start) {
        return ((spins & 0x3FF) == 0) && (System.nanoTime() - start) > 10_000_000L;
    }

    private static void fill(byte[] body) {
        System.arraycopy(TEMPLATE, 0, body, 0, PAYLOAD);
    }

    private static void copyTo(Msg m, byte[] dst, Scratch s) {
        System.arraycopy(m.body, 0, dst, 0, PAYLOAD);
        s.sink += dst[0] + dst[PAYLOAD - 1];
    }

    // ---------------- AtomicQueue SPSC 1P x 1C, batch=1 ----------------

    @State(Scope.Group)
    public static class CoralSpsc1 {
        final AtomicQueue<Msg> queue = new AtomicQueue<>(CAPACITY, msgBuilder());
    }

    @Benchmark
    @Group("coralSpsc1p1c")
    @GroupThreads(1)
    public void coralSpsc1Producer(CoralSpsc1 g) {
        Msg m = g.queue.nextToDispatch();
        if (m == null) {
            long spins = 0;
            long start = System.nanoTime();
            while ((m = g.queue.nextToDispatch()) == null) {
                if (starved(++spins, start)) {
                    return; // iteration over; consumer gone
                }
            }
        }
        fill(m.body);
        g.queue.flush();
    }

    @Benchmark
    @Group("coralSpsc1p1c")
    @GroupThreads(1)
    public int coralSpsc1Consumer(CoralSpsc1 g, Scratch s) {
        if (g.queue.availableToFetch() == 0) {
            long spins = 0;
            long start = System.nanoTime();
            while (g.queue.availableToFetch() == 0) {
                if (starved(++spins, start)) {
                    return -1; // iteration over: one phantom op per thread per phase
                }
            }
        }
        copyTo(g.queue.fetch(), s.buf, s);
        g.queue.doneFetching();
        return PAYLOAD;
    }

    // ---------------- AtomicQueue SPSC 1P x 1C, batch=8 ----------------
    // Producer claims up to 8, fills each, publishes with ONE flush().
    // Consumer drains up to 8 per availableToFetch, one doneFetching().

    @State(Scope.Group)
    public static class CoralSpsc8 {
        final AtomicQueue<Msg> queue = new AtomicQueue<>(CAPACITY, msgBuilder());
    }

    @Benchmark
    @Group("coralSpsc1p1cB8")
    @GroupThreads(1)
    public void coralSpsc8Producer(CoralSpsc8 g) {
        int k = 0;
        for (; k < BATCH; k++) {
            Msg m = g.queue.nextToDispatch();
            if (m == null) {
                long spins = 0;
                long start = System.nanoTime();
                while ((m = g.queue.nextToDispatch()) == null) {
                    if (starved(++spins, start)) {
                        if (k > 0) {
                            g.queue.flush(); // publish the partial batch
                        }
                        return; // iteration over; consumer gone
                    }
                }
            }
            fill(m.body);
        }
        g.queue.flush();
    }

    @Benchmark
    @Group("coralSpsc1p1cB8")
    @GroupThreads(1)
    public int coralSpsc8Consumer(CoralSpsc8 g, Scratch s) {
        long avail = g.queue.availableToFetch();
        if (avail == 0) {
            long spins = 0;
            long start = System.nanoTime();
            while ((avail = g.queue.availableToFetch()) == 0) {
                if (starved(++spins, start)) {
                    return -1; // iteration over: one phantom op per thread per phase
                }
            }
        }
        long n = Math.min(avail, BATCH);
        for (long k = 0; k < n; k++) {
            copyTo(g.queue.fetch(), s.buf, s);
        }
        g.queue.doneFetching();
        return (int) n;
    }

    // ---------------- AtomicBroadcaster SPMC 1P x 3C, batch=1 ----------------
    // Multicast like our SPMC: every consumer receives every message. Producer
    // gates on the slowest cursor (lossless backpressure), so it needs the same
    // bail as Disruptor's producer when a consumer exits at iteration end.

    @State(Scope.Group)
    public static class CoralSpmc1 {
        final AtomicBroadcaster<Msg> queue = new AtomicBroadcaster<>(CAPACITY, msgBuilder(), 3);
        final AtomicInteger nextConsumer = new AtomicInteger();
    }

    @State(Scope.Thread)
    public static class CoralCursor {
        int index = -1;
    }

    private static int claimIndex(AtomicInteger next, CoralCursor c) {
        if (c.index < 0) {
            c.index = next.getAndIncrement();
        }
        return c.index;
    }

    @Benchmark
    @Group("coralSpmc1p3c")
    @GroupThreads(1)
    public void coralSpmc1Producer(CoralSpmc1 g) {
        Msg m = g.queue.nextToDispatch();
        if (m == null) {
            long spins = 0;
            long start = System.nanoTime();
            while ((m = g.queue.nextToDispatch()) == null) {
                if (starved(++spins, start)) {
                    return; // iteration over; a consumer bailed, min-cursor frozen
                }
            }
        }
        fill(m.body);
        g.queue.flush();
    }

    @Benchmark
    @Group("coralSpmc1p3c")
    @GroupThreads(3)
    public int coralSpmc1Consumer(CoralSpmc1 g, CoralCursor c, Scratch s) {
        int idx = claimIndex(g.nextConsumer, c);
        if (g.queue.availableToFetch(idx) == 0) {
            long spins = 0;
            long start = System.nanoTime();
            while (g.queue.availableToFetch(idx) == 0) {
                if (starved(++spins, start)) {
                    return -1; // iteration over: one phantom op per thread per phase
                }
            }
        }
        copyTo(g.queue.fetch(idx), s.buf, s);
        g.queue.doneFetching(idx);
        return PAYLOAD;
    }

    // ---------------- AtomicBroadcaster SPMC 1P x 3C, batch=8 ----------------

    @State(Scope.Group)
    public static class CoralSpmc8 {
        final AtomicBroadcaster<Msg> queue = new AtomicBroadcaster<>(CAPACITY, msgBuilder(), 3);
        final AtomicInteger nextConsumer = new AtomicInteger();
    }

    @Benchmark
    @Group("coralSpmc1p3cB8")
    @GroupThreads(1)
    public void coralSpmc8Producer(CoralSpmc8 g) {
        int k = 0;
        for (; k < BATCH; k++) {
            Msg m = g.queue.nextToDispatch();
            if (m == null) {
                long spins = 0;
                long start = System.nanoTime();
                while ((m = g.queue.nextToDispatch()) == null) {
                    if (starved(++spins, start)) {
                        if (k > 0) {
                            g.queue.flush(); // publish the partial batch
                        }
                        return; // iteration over; a consumer bailed
                    }
                }
            }
            fill(m.body);
        }
        g.queue.flush();
    }

    @Benchmark
    @Group("coralSpmc1p3cB8")
    @GroupThreads(3)
    public int coralSpmc8Consumer(CoralSpmc8 g, CoralCursor c, Scratch s) {
        int idx = claimIndex(g.nextConsumer, c);
        long avail = g.queue.availableToFetch(idx);
        if (avail == 0) {
            long spins = 0;
            long start = System.nanoTime();
            while ((avail = g.queue.availableToFetch(idx)) == 0) {
                if (starved(++spins, start)) {
                    return -1; // iteration over: one phantom op per thread per phase
                }
            }
        }
        long n = Math.min(avail, BATCH);
        for (long k = 0; k < n; k++) {
            copyTo(g.queue.fetch(idx), s.buf, s);
        }
        g.queue.doneFetching(idx);
        return (int) n;
    }

    // ---------------- ByteBufferRawQueue SPSC 1P x 1C, batch=1 ----------------
    // Raw bytes over a DIRECT buffer (matches our off-heap rings). Capacity is
    // in bytes and must be pow2: 65536 bytes = 2048 message-depth vs our 1024
    // slots — documented depth difference, same per-message work.

    @State(Scope.Group)
    public static class CoralRaw1 {
        static final int RAW_CAPACITY = 65536; // pow2 bytes
        final ByteBufferRawQueue queue = new ByteBufferRawQueue(RAW_CAPACITY, true);
        final byte[] src = new byte[PAYLOAD];

        public CoralRaw1() {
            fill(src);
        }
    }

    @Benchmark
    @Group("coralRaw1p1c")
    @GroupThreads(1)
    public void coralRaw1Producer(CoralRaw1 g) {
        if (g.queue.availableToWrite() < PAYLOAD) {
            long spins = 0;
            long start = System.nanoTime();
            while (g.queue.availableToWrite() < PAYLOAD) {
                if (starved(++spins, start)) {
                    return; // iteration over; consumer gone
                }
            }
        }
        RawBytes w = g.queue.getProducer();
        w.putByteArray(g.src, 0, PAYLOAD);
        g.queue.flush();
    }

    @Benchmark
    @Group("coralRaw1p1c")
    @GroupThreads(1)
    public int coralRaw1Consumer(CoralRaw1 g, Scratch s) {
        if (g.queue.availableToRead() < PAYLOAD) {
            long spins = 0;
            long start = System.nanoTime();
            while (g.queue.availableToRead() < PAYLOAD) {
                if (starved(++spins, start)) {
                    return -1; // iteration over: one phantom op per thread per phase
                }
            }
        }
        RawBytes r = g.queue.getConsumer();
        r.getByteArray(s.buf, 0, PAYLOAD);
        s.sink += s.buf[0] + s.buf[PAYLOAD - 1];
        g.queue.doneReading();
        return PAYLOAD;
    }

    // ---------------- ByteBufferRawQueue SPSC 1P x 1C, batch=8 ----------------
    // One getProducer/flush pair per 8 messages; consumer drains up to 8
    // (bounded by getRemaining, the exact analogue of min(avail, BATCH)).

    @State(Scope.Group)
    public static class CoralRaw8 {
        final ByteBufferRawQueue queue = new ByteBufferRawQueue(CoralRaw1.RAW_CAPACITY, true);
        final byte[] src = new byte[PAYLOAD];

        public CoralRaw8() {
            fill(src);
        }
    }

    @Benchmark
    @Group("coralRaw1p1cB8")
    @GroupThreads(1)
    public void coralRaw8Producer(CoralRaw8 g) {
        final int need = BATCH * PAYLOAD;
        if (g.queue.availableToWrite() < need) {
            long spins = 0;
            long start = System.nanoTime();
            while (g.queue.availableToWrite() < need) {
                if (starved(++spins, start)) {
                    return; // iteration over; consumer gone
                }
            }
        }
        RawBytes w = g.queue.getProducer();
        for (int k = 0; k < BATCH; k++) {
            w.putByteArray(g.src, 0, PAYLOAD);
        }
        g.queue.flush();
    }

    @Benchmark
    @Group("coralRaw1p1cB8")
    @GroupThreads(1)
    public int coralRaw8Consumer(CoralRaw8 g, Scratch s) {
        if (g.queue.availableToRead() < PAYLOAD) {
            long spins = 0;
            long start = System.nanoTime();
            while (g.queue.availableToRead() < PAYLOAD) {
                if (starved(++spins, start)) {
                    return -1; // iteration over: one phantom op per thread per phase
                }
            }
        }
        RawBytes r = g.queue.getConsumer();
        int n = 0;
        while (n < BATCH && r.getRemaining() >= PAYLOAD) {
            r.getByteArray(s.buf, 0, PAYLOAD);
            s.sink += s.buf[0] + s.buf[PAYLOAD - 1];
            n++;
        }
        g.queue.doneReading();
        return n;
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(CoralBenchmarks.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(3)
                .timeUnit(TimeUnit.SECONDS)
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
