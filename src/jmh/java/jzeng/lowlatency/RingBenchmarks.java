package jzeng.lowlatency;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.YieldingWaitStrategy;

import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;

import java.nio.ByteBuffer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Throughput benchmarks: 1 producer × N consumers, total consumer-side messages/s.
 * total consumer-side messages/s. Each consumer holds its own cursor, so the SPMC
 * ring fan-outs (multicast) while the baselines split work (competing consumers) —
 * see README fairness note; compare relatively, like the original histogram.
 *
 * <p>All consumers (and the SPSC/Disruptor producers' backpressure paths) carry a
 * ~10 ms starvation bail: at iteration end the producer may exit while a consumer
 * waits for data that will never arrive, which would hang JMH's teardown barrier
 * forever. A live producer delivers every ~30 ns, so the bail never triggers
 * mid-iteration; the fed path is a single check.
 */
public class RingBenchmarks {

    private static final int CAPACITY = 1024;
    private static final int PAYLOAD = 32;

    // ---------------- SPMC 1P x 1C ----------------

    @State(Scope.Group)
    public static class Spmc1p1c {
        SpmcOffHeapRing ring = new SpmcOffHeapRing(CAPACITY);
    }

    @State(Scope.Thread)
    public static class Cursor {
        long cursor;
    }

    @Benchmark
    @Group("spmc1p1c")
    @GroupThreads(1)
    public void spmc1p1cProducer(Spmc1p1c g) {
        g.ring.write(PAYLOAD, (buf, off, n) -> {
            for (int i = 0; i < n; i++) {
                buf.put(off + i, (byte) i);
            }
        });
    }

    @Benchmark
    @Group("spmc1p1c")
    @GroupThreads(1)
    public int spmc1p1cConsumer(Spmc1p1c g, Cursor c, Scratch s) {
        return spinRead(g.ring, c, s.buf);
    }

    // ---------------- SPMC 1P x 3C ----------------

    @State(Scope.Group)
    public static class Spmc1p3c {
        SpmcOffHeapRing ring = new SpmcOffHeapRing(CAPACITY);
    }

    @Benchmark
    @Group("spmc1p3c")
    @GroupThreads(1)
    public void spmc1p3cProducer(Spmc1p3c g) {
        g.ring.write(PAYLOAD, (buf, off, n) -> {
            for (int i = 0; i < n; i++) {
                buf.put(off + i, (byte) i);
            }
        });
    }

    @Benchmark
    @Group("spmc1p3c")
    @GroupThreads(3)
    public int spmc1p3cConsumer(Spmc1p3c g, Cursor c, Scratch s) {
        return spinRead(g.ring, c, s.buf);
    }

    // ---------------- SPMC catch-up 1P x 1C (forced permanent overrun) ----------------
    // Consumer burns ~2 us/msg checksumming, pinning backlog above CAPACITY, so
    // clampToOldestAlive fires on (almost) every read. Measures sustained consumer
    // throughput on the clamp path. Jump counts are asserted in SpmcJumpAheadTest;
    // JMH cannot print aux counters, so this group proves hang-free throughput only.
    // Clamp-only by design: jumping to newest every invocation would keep backlog
    // small and the clamp would never fire — the policies would mask each other.

    @State(Scope.Group)
    public static class SpmcCatchup {
        SpmcOffHeapRing ring = new SpmcOffHeapRing(CAPACITY);
    }

    @State(Scope.Thread)
    public static class CatchupStats {
        long jumps; // times the clamp advanced the cursor
        long sink;  // checksum accumulator (anti-DCE for the burn loop)
    }

    @Benchmark
    @Group("spmcCatchup1p1c")
    @GroupThreads(1)
    public void spmcCatchupProducer(SpmcCatchup g) {
        g.ring.write(PAYLOAD, (buf, off, n) -> {
            for (int i = 0; i < n; i++) {
                buf.put(off + i, (byte) i);
            }
        });
    }

    @Benchmark
    @Group("spmcCatchup1p1c")
    @GroupThreads(1)
    public int spmcCatchupConsumer(SpmcCatchup g, Cursor c, CatchupStats st, Scratch s) {
        long adjusted = g.ring.clampToOldestAlive(c.cursor);
        if (adjusted != c.cursor) {
            st.jumps++;
            c.cursor = adjusted;
        }
        int n = spinRead(g.ring, c, s.buf);
        for (int r = 0; r < 64; r++) { // ~2 us burn: 64 x 32 byte-adds
            for (int i = 0; i < PAYLOAD; i++) {
                st.sink += s.buf[i];
            }
        }
        return n;
    }

    // ---------------- SPSC 1P x 1C ----------------

    @State(Scope.Group)
    public static class SpscState {
        SpscOffHeapRing ring = new SpscOffHeapRing(CAPACITY);
        // External flow control belt-and-braces on top of the ring's own
        // backpressure (write returns ERROR on a full ring without consuming
        // a sequence). The producer ignores the write result and advances its
        // cursor unconditionally, so without this gate an ERROR would skip a
        // sequence and stall the consumer.
        java.util.concurrent.atomic.AtomicLong consumed = new java.util.concurrent.atomic.AtomicLong(-1);
    }

    @Benchmark
    @Group("spsc1p1c")
    @GroupThreads(1)
    public void spscProducer(SpscState g, Cursor c) {
        if (c.cursor - g.consumed.get() >= CAPACITY) {
            // Backpressure with escape hatch: the consumer may have bailed (see below),
            // leaving consumed frozen. A live consumer drains every ~30 ns, so a 10 ms
            // stall means the iteration is over.
            long spins = 0;
            long start = System.nanoTime();
            while (c.cursor - g.consumed.get() >= CAPACITY) {
                if (((++spins & 0x3FF) == 0) && (System.nanoTime() - start) > 10_000_000L) {
                    return; // iteration over; consumer gone
                }
            }
        }
        g.ring.write(PAYLOAD, (buf, off, n) -> {
            for (int i = 0; i < n; i++) {
                buf.put(off + i, (byte) i);
            }
        });
        c.cursor++;
    }

    @Benchmark
    @Group("spsc1p1c")
    @GroupThreads(1)
    public int spscConsumer(SpscState g, Cursor c, Scratch s) {
        int n;
        if ((n = g.ring.read(c.cursor, s.buf)) < 0) {
            // Starved: spin with escape hatch (see spinRead). Bail WITHOUT advancing
            // cursor/consumed: the sequence was never consumed.
            long spins = 0;
            long start = System.nanoTime();
            while ((n = g.ring.read(c.cursor, s.buf)) < 0) {
                if (((++spins & 0x3FF) == 0) && (System.nanoTime() - start) > 10_000_000L) {
                    return -1; // iteration over: one phantom op per thread per phase
                }
            }
        }
        c.cursor++;
        g.consumed.set(c.cursor - 1);
        return n;
    }

    // ---------------- Agrona OneToOneRingBuffer 1P x 1C ----------------
    // Same test shape as spsc1p1c (single producer, single consumer, 32 B
    // payload) backed by Agrona's OneToOneRingBuffer over off-heap memory.
    // Framing differs: 8 B header + payload (8-aligned), so a 32 B message is
    // one 40 B record. Capacity must be pow2 bytes: 65536 usable bytes hold
    // 1638 records vs our 1024 slots — documented depth difference, same
    // per-message work (fill 32 B, copy 32 B on consume).

    @State(Scope.Group)
    public static class AgronaSpsc {
        static final int AGRONA_CAPACITY = 65536; // pow2 usable bytes
        final OneToOneRingBuffer rb;
        final UnsafeBuffer src;

        public AgronaSpsc() {
            rb = new OneToOneRingBuffer(new UnsafeBuffer(
                    ByteBuffer.allocateDirect(AGRONA_CAPACITY + RingBufferDescriptor.TRAILER_LENGTH)));
            src = new UnsafeBuffer(new byte[PAYLOAD]);
        }
    }

    @State(Scope.Thread)
    public static class AgronaConsumer {
        final byte[] buf = new byte[PAYLOAD];
        int lastSize;
        // Allocated once per thread (field init); the benchmark itself never allocates.
        final MessageHandler handler = (msgTypeId, buffer, index, length) -> {
            buffer.getBytes(index, buf, 0, length);
            lastSize = length;
        };
    }

    @Benchmark
    @Group("agronaSpsc1p1c")
    @GroupThreads(1)
    public void agronaSpscProducer(AgronaSpsc g) {
        for (int i = 0; i < PAYLOAD; i++) {
            g.src.putByte(i, (byte) i);
        }
        if (!g.rb.write(1, g.src, 0, PAYLOAD)) {
            // Backpressure with escape hatch (same shape as spsc1p1c): Agrona
            // reports full instead of consuming a sequence, so retry; a live
            // consumer drains every ~30 ns, so 10 ms means the iteration is over.
            long spins = 0;
            long start = System.nanoTime();
            while (!g.rb.write(1, g.src, 0, PAYLOAD)) {
                if (((++spins & 0x3FF) == 0) && (System.nanoTime() - start) > 10_000_000L) {
                    return;
                }
            }
        }
    }

    @Benchmark
    @Group("agronaSpsc1p1c")
    @GroupThreads(1)
    public int agronaSpscConsumer(AgronaSpsc g, AgronaConsumer c) {
        if (g.rb.read(c.handler, 1) == 0) {
            // Starved: spin with escape hatch (see spinRead).
            long spins = 0;
            long start = System.nanoTime();
            while (g.rb.read(c.handler, 1) == 0) {
                if (((++spins & 0x3FF) == 0) && (System.nanoTime() - start) > 10_000_000L) {
                    return -1; // iteration over: one phantom op per thread per phase
                }
            }
        }
        return c.lastSize;
    }

    // ---------------- Disruptor 1P x 1C / 1P x 3C ----------------
    // Same test shape as the SPMC groups (single producer, multicast consumers,
    // 32 B payload, capacity 1024) but backed by LMAX Disruptor's single-producer
    // RingBuffer used manually: the producer claims via next()/publish(), each
    // consumer tracks its own cursor and advances a gating Sequence for
    // backpressure. Fan-out semantics match SPMC, so group totals are comparable.

    /** Preallocated slot payload — filled in place, like the rings' byte[] path. */
    public static final class ValueEvent {
        private final byte[] data = new byte[PAYLOAD];

        public static final EventFactory<ValueEvent> FACTORY = ValueEvent::new;
    }

    @State(Scope.Group)
    public static class Disruptor1p1c {
        final RingBuffer<ValueEvent> rb = RingBuffer.createSingleProducer(
                ValueEvent.FACTORY, CAPACITY, new YieldingWaitStrategy());
        final Sequence[] gates = {new Sequence(-1)};
        final AtomicInteger nextGate = new AtomicInteger();

        public Disruptor1p1c() {
            rb.addGatingSequences(gates);
        }
    }

    @State(Scope.Thread)
    public static class DisruptorCursor1 {
        long cursor;
        Sequence gate;
    }

    @Benchmark
    @Group("disruptor1p1c")
    @GroupThreads(1)
    public void disruptorProducer1(Disruptor1p1c g) {
        // Backpressure with escape hatch: next() parks while gated, and a consumer
        // that bailed (see below) would strand it forever. hasAvailableCapacity only
        // re-scans gates when nearly full, so the fed path costs ~2 ns.
        if (!g.rb.hasAvailableCapacity(1)) {
            long spins = 0;
            long start = System.nanoTime();
            while (!g.rb.hasAvailableCapacity(1)) {
                if (((++spins & 0x3FF) == 0) && (System.nanoTime() - start) > 10_000_000L) {
                    return; // iteration over; consumers gone
                }
            }
        }
        // Immediate: single producer, and gates only advance from here.
        long seq = g.rb.next();
        try {
            byte[] d = g.rb.get(seq).data;
            for (int i = 0; i < PAYLOAD; i++) {
                d[i] = (byte) i;
            }
        } finally {
            g.rb.publish(seq);
        }
    }

    @Benchmark
    @Group("disruptor1p1c")
    @GroupThreads(1)
    public int disruptorConsumer1(Disruptor1p1c g, DisruptorCursor1 c, Scratch s) {
        if (c.gate == null) {
            c.gate = g.gates[g.nextGate.getAndIncrement()];
        }
        long await = c.cursor;
        // Starved: spin with escape hatch. Once the producer exits (iteration end)
        // the sequence never arrives and JMH's preTearDown barrier would hang
        // forever. A live producer publishes every ~30 ns, so a 10 ms gap means it
        // is gone; bail out. The fed path (single getCursor check) is unaffected,
        // and the time check runs every 1024 spins to keep reaction latency tight.
        if (g.rb.getCursor() < await) {
            long spins = 0;
            long start = System.nanoTime();
            while (g.rb.getCursor() < await) {
                if (((++spins & 0x3FF) == 0) && (System.nanoTime() - start) > 10_000_000L) {
                    return -1; // iteration over: one phantom op per thread per phase
                }
            }
        }
        byte[] d = g.rb.get(await).data;
        for (int i = 0; i < PAYLOAD; i++) {
            s.buf[i] = d[i];
        }
        c.gate.set(await);
        c.cursor++;
        return PAYLOAD;
    }

    @State(Scope.Group)
    public static class Disruptor1p3c {
        final RingBuffer<ValueEvent> rb = RingBuffer.createSingleProducer(
                ValueEvent.FACTORY, CAPACITY, new YieldingWaitStrategy());
        final Sequence[] gates = {new Sequence(-1), new Sequence(-1), new Sequence(-1)};
        final AtomicInteger nextGate = new AtomicInteger();

        public Disruptor1p3c() {
            rb.addGatingSequences(gates);
        }
    }

    @State(Scope.Thread)
    public static class DisruptorCursor3 {
        long cursor;
        Sequence gate;
    }

    @Benchmark
    @Group("disruptor1p3c")
    @GroupThreads(1)
    public void disruptorProducer3(Disruptor1p3c g) {
        // Backpressure with escape hatch (see disruptorProducer1).
        if (!g.rb.hasAvailableCapacity(1)) {
            long spins = 0;
            long start = System.nanoTime();
            while (!g.rb.hasAvailableCapacity(1)) {
                if (((++spins & 0x3FF) == 0) && (System.nanoTime() - start) > 10_000_000L) {
                    return; // iteration over; consumers gone
                }
            }
        }
        // Immediate: single producer, and gates only advance from here.
        long seq = g.rb.next();
        try {
            byte[] d = g.rb.get(seq).data;
            for (int i = 0; i < PAYLOAD; i++) {
                d[i] = (byte) i;
            }
        } finally {
            g.rb.publish(seq);
        }
    }

    @Benchmark
    @Group("disruptor1p3c")
    @GroupThreads(3)
    public int disruptorConsumer3(Disruptor1p3c g, DisruptorCursor3 c, Scratch s) {
        if (c.gate == null) {
            c.gate = g.gates[g.nextGate.getAndIncrement()];
        }
        long await = c.cursor;
        // Starved: spin with escape hatch (see disruptorConsumer1).
        if (g.rb.getCursor() < await) {
            long spins = 0;
            long start = System.nanoTime();
            while (g.rb.getCursor() < await) {
                if (((++spins & 0x3FF) == 0) && (System.nanoTime() - start) > 10_000_000L) {
                    return -1; // iteration over: one phantom op per thread per phase
                }
            }
        }
        byte[] d = g.rb.get(await).data;
        for (int i = 0; i < PAYLOAD; i++) {
            s.buf[i] = d[i];
        }
        c.gate.set(await);
        c.cursor++;
        return PAYLOAD;
    }

    // ---------------- Baselines ----------------

    @State(Scope.Group)
    public static class BlockingState {
        ArrayBlockingQueue<byte[]> queue = new ArrayBlockingQueue<>(CAPACITY);
    }

    @Benchmark
    @Group("blocking1p3c")
    @GroupThreads(1)
    public void blockingProducer(BlockingState g) throws InterruptedException {
        g.queue.put(new byte[PAYLOAD]);
    }

    @Benchmark
    @Group("blocking1p3c")
    @GroupThreads(3)
    public byte[] blockingConsumer(BlockingState g) throws InterruptedException {
        // poll(1 ms) instead of take(): identical fast path while items are available,
        // but a parked take() at iteration end (producer already exited, queue drained)
        // hangs JMH's preTearDown barrier forever — observed live. Nulls mean
        // starvation; a live producer keeps the queue full, so ~10 ms of nulls means
        // it is gone.
        byte[] m;
        int starved = 0;
        while ((m = g.queue.poll(1, TimeUnit.MILLISECONDS)) == null) {
            if (++starved >= 10) {
                return null; // iteration over: one phantom op per thread per phase
            }
        }
        return m;
    }

    @State(Scope.Group)
    public static class ClqState {
        ConcurrentLinkedQueue<byte[]> queue = new ConcurrentLinkedQueue<>();
    }

    @Benchmark
    @Group("clq1p3c")
    @GroupThreads(1)
    public void clqProducer(ClqState g) {
        g.queue.offer(new byte[PAYLOAD]);
    }

    @Benchmark
    @Group("clq1p3c")
    @GroupThreads(3)
    public int clqConsumer(ClqState g) {
        byte[] m;
        if ((m = g.queue.poll()) == null) {
            // Starved: spin with escape hatch (see spinRead).
            long spins = 0;
            long start = System.nanoTime();
            while ((m = g.queue.poll()) == null) {
                if (((++spins & 0x3FF) == 0) && (System.nanoTime() - start) > 10_000_000L) {
                    return -1; // iteration over: one phantom op per thread per phase
                }
            }
        }
        return m.length;
    }

    // ---------------- helpers ----------------

    @State(Scope.Thread)
    public static class Scratch {
        byte[] buf = new byte[64];
    }

    private static int spinRead(SpmcOffHeapRing ring, Cursor c, byte[] scratch) {
        int n;
        if ((n = ring.read(c.cursor, scratch)) < 0) {
            // Starved: spin with escape hatch. Once the producer exits (iteration
            // end) the cursor never advances and JMH's preTearDown barrier would hang
            // forever. A live producer publishes every ~30 ns, so a 10 ms gap means it
            // is gone; bail out. The fed path (single read) is unaffected, and the time
            // check runs every 1024 spins to keep reaction latency tight.
            long spins = 0;
            long start = System.nanoTime();
            while ((n = ring.read(c.cursor, scratch)) < 0) {
                if (((++spins & 0x3FF) == 0) && (System.nanoTime() - start) > 10_000_000L) {
                    return -1; // iteration over: one phantom op per thread per phase
                }
            }
        }
        c.cursor++;
        return n;
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(RingBenchmarks.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(3)
                .timeUnit(TimeUnit.SECONDS)
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
