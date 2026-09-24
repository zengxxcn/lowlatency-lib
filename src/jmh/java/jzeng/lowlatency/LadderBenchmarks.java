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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Typed-payload benchmarks: every transport carries a variable-depth FX price
 * ladder ({@link TestPriceLadder}, worst case 256 B, actual 88..256 B by depth).
 * Same 1 producer × N consumers shape as {@link RingBenchmarks}: SPMC and
 * Disruptor fan out (multicast, comparable totals), SPSC is 1×1, the heap
 * baselines split work (competing consumers).
 *
 * <p>Producer fill is a static method reference (non-capturing, zero-alloc);
 * symbol/market bytes are pre-encoded once — {@code String.getBytes} per publish
 * would be producer-side garbage in every group. Consumers checksum real fields
 * so the read cannot be dead-code eliminated. The heap baselines allocate a
 * snapshot per publish (their nature — zero-alloc is part of what is measured);
 * their consumers copy into a reused buffer, mirroring the rings' scratch copy.
 *
 * <p>Same ~10 ms starvation-bail protocol as {@link RingBenchmarks} on every
 * spin: at iteration end the producer may exit while a consumer waits for data
 * that will never arrive, which would hang JMH's teardown barrier forever.
 */
public class LadderBenchmarks {

    private static final int CAPACITY = 1024;
    private static final int MAX = TestPriceLadder.MAX_ENCODED; // 256

    private static final byte[] SYM = "EURUSD".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MKT = "LMAX".getBytes(StandardCharsets.US_ASCII);

    /** Deterministic fill: depth cycles 1..8, prices derive from the sequence. */
    static void fillLadder(TestPriceLadder l, long seq) {
        int depth = (int) (seq & 0x7) + 1;
        l.setDepth(depth);
        l.setSymbolBytes(SYM, SYM.length);
        l.setMarketBytes(MKT, MKT.length);
        l.setEventId(seq);
        l.setTimestampNs(seq);
        long base = 1_1000000L + (seq & 0xFF) * 10;
        for (int i = 0; i < depth; i++) {
            l.setLevel(i, base + i, base + i + 2, 1_000000L + i);
        }
    }

    /** Touches real slot memory so the consume cannot be eliminated. */
    private static int ladderChecksum(TestPriceLadder l, int size) {
        return (int) (l.eventId() ^ l.depth() ^ l.levelBid(0) ^ size);
    }

    @State(Scope.Thread)
    public static class Cursor {
        long cursor;
    }

    /** Per-thread reusable ladder view (producer or consumer side). */
    @State(Scope.Thread)
    public static class LadderView {
        final TestPriceLadder ladder = new TestPriceLadder();
    }

    /** Per-thread reusable heap copy buffer for baseline/disruptor consumers. */
    @State(Scope.Thread)
    public static class CopyScratch {
        final ByteBuffer heap = ByteBuffer.allocate(MAX);
        final TestPriceLadder ladder = new TestPriceLadder();
    }

    // ---------------- SPMC ladder 1P x 1C / 1P x 3C ----------------

    @State(Scope.Group)
    public static class SpmcLadder1p1c {
        final SpmcOffHeapRing ring = new SpmcOffHeapRing(CAPACITY, MAX);
    }

    @Benchmark
    @Group("spmcLadder1p1c")
    @GroupThreads(1)
    public void spmcLadderProducer1(SpmcLadder1p1c g, LadderView p) {
        g.ring.write(LadderBenchmarks::fillLadder, p.ladder);
    }

    @Benchmark
    @Group("spmcLadder1p1c")
    @GroupThreads(1)
    public int spmcLadderConsumer1(SpmcLadder1p1c g, Cursor c, LadderView v) {
        return spinTypedRead(g.ring, c, v.ladder);
    }

    @State(Scope.Group)
    public static class SpmcLadder1p3c {
        final SpmcOffHeapRing ring = new SpmcOffHeapRing(CAPACITY, MAX);
    }

    @Benchmark
    @Group("spmcLadder1p3c")
    @GroupThreads(1)
    public void spmcLadderProducer3(SpmcLadder1p3c g, LadderView p) {
        g.ring.write(LadderBenchmarks::fillLadder, p.ladder);
    }

    @Benchmark
    @Group("spmcLadder1p3c")
    @GroupThreads(3)
    public int spmcLadderConsumer3(SpmcLadder1p3c g, Cursor c, LadderView v) {
        return spinTypedRead(g.ring, c, v.ladder);
    }

    private static int spinTypedRead(SpmcOffHeapRing ring, Cursor c, TestPriceLadder reuse) {
        int n;
        if ((n = ring.read(c.cursor, reuse)) < 0) {
            // Starved: spin with escape hatch (same protocol as RingBenchmarks).
            long spins = 0;
            long start = System.nanoTime();
            while ((n = ring.read(c.cursor, reuse)) < 0) {
                if (((++spins & 0x3FF) == 0) && (System.nanoTime() - start) > 10_000_000L) {
                    return -1; // iteration over: one phantom op per thread per phase
                }
            }
        }
        int sum = ladderChecksum(reuse, n);
        c.cursor++;
        return sum;
    }

    // ---------------- SPSC ladder 1P x 1C ----------------

    @State(Scope.Group)
    public static class SpscLadderState {
        final SpscOffHeapRing ring = new SpscOffHeapRing(CAPACITY, MAX);
        // External flow control belt-and-braces on top of the ring's own
        // backpressure (ERROR on a full ring consumes no sequence, but the
        // producer advances its cursor unconditionally — without this gate a
        // skipped sequence would stall the consumer).
        final java.util.concurrent.atomic.AtomicLong consumed =
                new java.util.concurrent.atomic.AtomicLong(-1);
    }

    @Benchmark
    @Group("spscLadder1p1c")
    @GroupThreads(1)
    public void spscLadderProducer(SpscLadderState g, Cursor c, LadderView p) {
        if (c.cursor - g.consumed.get() >= CAPACITY) {
            long spins = 0;
            long start = System.nanoTime();
            while (c.cursor - g.consumed.get() >= CAPACITY) {
                if (((++spins & 0x3FF) == 0) && (System.nanoTime() - start) > 10_000_000L) {
                    return; // iteration over; consumer gone
                }
            }
        }
        g.ring.write(LadderBenchmarks::fillLadder, p.ladder);
        c.cursor++;
    }

    @Benchmark
    @Group("spscLadder1p1c")
    @GroupThreads(1)
    public int spscLadderConsumer(SpscLadderState g, Cursor c, LadderView v) {
        int n;
        if ((n = g.ring.read(c.cursor, v.ladder)) < 0) {
            long spins = 0;
            long start = System.nanoTime();
            while ((n = g.ring.read(c.cursor, v.ladder)) < 0) {
                if (((++spins & 0x3FF) == 0) && (System.nanoTime() - start) > 10_000_000L) {
                    return -1; // iteration over; cursor/consumed untouched
                }
            }
        }
        int sum = ladderChecksum(v.ladder, n);
        c.cursor++;
        g.consumed.set(c.cursor - 1);
        return sum;
    }

    // ---------------- Disruptor ladder 1P x 1C / 1P x 3C ----------------
    // Same shape, backed by LMAX Disruptor's single-producer RingBuffer used
    // manually. Each event preallocates a 256 B payload; the producer fills it
    // through a pre-wrapped per-slot view (one-time setup, zero per-op alloc);
    // consumers copy into their own buffer and parse, mirroring the scratch
    // copy in the raw-bytes benchmarks.

    public static final class LadderEvent {
        final byte[] data = new byte[MAX];
        int length;

        public static final EventFactory<LadderEvent> FACTORY = LadderEvent::new;
    }

    @State(Scope.Group)
    public static class DisruptorLadder1p1c {
        final RingBuffer<LadderEvent> rb = RingBuffer.createSingleProducer(
                LadderEvent.FACTORY, CAPACITY, new YieldingWaitStrategy());
        final Sequence[] gates = {new Sequence(-1)};
        final AtomicInteger nextGate = new AtomicInteger();
        final TestPriceLadder[] slotViews = new TestPriceLadder[CAPACITY];

        public DisruptorLadder1p1c() {
            rb.addGatingSequences(gates);
            for (int i = 0; i < CAPACITY; i++) {
                TestPriceLadder v = new TestPriceLadder();
                v.wrap(ByteBuffer.wrap(rb.get(i).data), 0, MAX);
                slotViews[i] = v;
            }
        }
    }

    @State(Scope.Thread)
    public static class DisruptorLadderCursor1 {
        long cursor;
        Sequence gate;
    }

    @Benchmark
    @Group("disruptorLadder1p1c")
    @GroupThreads(1)
    public void disruptorLadderProducer1(DisruptorLadder1p1c g) {
        if (!g.rb.hasAvailableCapacity(1)) {
            long spins = 0;
            long start = System.nanoTime();
            while (!g.rb.hasAvailableCapacity(1)) {
                if (((++spins & 0x3FF) == 0) && (System.nanoTime() - start) > 10_000_000L) {
                    return; // iteration over; consumers gone
                }
            }
        }
        long seq = g.rb.next();
        try {
            LadderEvent e = g.rb.get(seq);
            TestPriceLadder v = g.slotViews[(int) (seq & (CAPACITY - 1))];
            fillLadder(v, seq);
            e.length = v.encodedLength();
        } finally {
            g.rb.publish(seq);
        }
    }

    @Benchmark
    @Group("disruptorLadder1p1c")
    @GroupThreads(1)
    public int disruptorLadderConsumer1(
            DisruptorLadder1p1c g, DisruptorLadderCursor1 c, CopyScratch s) {
        if (c.gate == null) {
            c.gate = g.gates[g.nextGate.getAndIncrement()];
        }
        long await = c.cursor;
        if (g.rb.getCursor() < await) {
            long spins = 0;
            long start = System.nanoTime();
            while (g.rb.getCursor() < await) {
                if (((++spins & 0x3FF) == 0) && (System.nanoTime() - start) > 10_000_000L) {
                    return -1; // iteration over: one phantom op per thread per phase
                }
            }
        }
        LadderEvent e = g.rb.get(await);
        s.heap.clear();
        s.heap.put(e.data, 0, e.length);
        s.ladder.wrap(s.heap, 0, e.length);
        int sum = ladderChecksum(s.ladder, e.length);
        c.gate.set(await);
        c.cursor++;
        return sum;
    }

    @State(Scope.Group)
    public static class DisruptorLadder1p3c {
        final RingBuffer<LadderEvent> rb = RingBuffer.createSingleProducer(
                LadderEvent.FACTORY, CAPACITY, new YieldingWaitStrategy());
        final Sequence[] gates = {new Sequence(-1), new Sequence(-1), new Sequence(-1)};
        final AtomicInteger nextGate = new AtomicInteger();
        final TestPriceLadder[] slotViews = new TestPriceLadder[CAPACITY];

        public DisruptorLadder1p3c() {
            rb.addGatingSequences(gates);
            for (int i = 0; i < CAPACITY; i++) {
                TestPriceLadder v = new TestPriceLadder();
                v.wrap(ByteBuffer.wrap(rb.get(i).data), 0, MAX);
                slotViews[i] = v;
            }
        }
    }

    @State(Scope.Thread)
    public static class DisruptorLadderCursor3 {
        long cursor;
        Sequence gate;
    }

    @Benchmark
    @Group("disruptorLadder1p3c")
    @GroupThreads(1)
    public void disruptorLadderProducer3(DisruptorLadder1p3c g) {
        if (!g.rb.hasAvailableCapacity(1)) {
            long spins = 0;
            long start = System.nanoTime();
            while (!g.rb.hasAvailableCapacity(1)) {
                if (((++spins & 0x3FF) == 0) && (System.nanoTime() - start) > 10_000_000L) {
                    return; // iteration over; consumers gone
                }
            }
        }
        long seq = g.rb.next();
        try {
            LadderEvent e = g.rb.get(seq);
            TestPriceLadder v = g.slotViews[(int) (seq & (CAPACITY - 1))];
            fillLadder(v, seq);
            e.length = v.encodedLength();
        } finally {
            g.rb.publish(seq);
        }
    }

    @Benchmark
    @Group("disruptorLadder1p3c")
    @GroupThreads(3)
    public int disruptorLadderConsumer3(
            DisruptorLadder1p3c g, DisruptorLadderCursor3 c, CopyScratch s) {
        if (c.gate == null) {
            c.gate = g.gates[g.nextGate.getAndIncrement()];
        }
        long await = c.cursor;
        if (g.rb.getCursor() < await) {
            long spins = 0;
            long start = System.nanoTime();
            while (g.rb.getCursor() < await) {
                if (((++spins & 0x3FF) == 0) && (System.nanoTime() - start) > 10_000_000L) {
                    return -1; // iteration over: one phantom op per thread per phase
                }
            }
        }
        LadderEvent e = g.rb.get(await);
        s.heap.clear();
        s.heap.put(e.data, 0, e.length);
        s.ladder.wrap(s.heap, 0, e.length);
        int sum = ladderChecksum(s.ladder, e.length);
        c.gate.set(await);
        c.cursor++;
        return sum;
    }

    // ---------------- Heap baselines (competing consumers) ----------------
    // Immutable snapshot per publish (their nature). Producer fills a reusable
    // heap ladder, then copies the exact encoded bytes into the snapshot.

    static final class LadderSnapshot {
        final byte[] data;

        LadderSnapshot(byte[] data) {
            this.data = data;
        }
    }

    @State(Scope.Thread)
    public static class HeapProducerScratch {
        final ByteBuffer heap = ByteBuffer.allocate(MAX);
        final TestPriceLadder view = new TestPriceLadder();
        long seq;
    }

    private static LadderSnapshot takeSnapshot(HeapProducerScratch p) {
        p.view.wrap(p.heap, 0, MAX);
        fillLadder(p.view, p.seq++);
        return new LadderSnapshot(Arrays.copyOf(p.heap.array(), p.view.encodedLength()));
    }

    private static int consumeSnapshot(LadderSnapshot snap, CopyScratch s) {
        s.heap.clear();
        s.heap.put(snap.data);
        s.ladder.wrap(s.heap, 0, snap.data.length);
        return ladderChecksum(s.ladder, snap.data.length);
    }

    @State(Scope.Group)
    public static class BlockingLadderState {
        final ArrayBlockingQueue<LadderSnapshot> queue = new ArrayBlockingQueue<>(CAPACITY);
    }

    @Benchmark
    @Group("blockingLadder1p3c")
    @GroupThreads(1)
    public void blockingLadderProducer(BlockingLadderState g, HeapProducerScratch p)
            throws InterruptedException {
        g.queue.put(takeSnapshot(p));
    }

    @Benchmark
    @Group("blockingLadder1p3c")
    @GroupThreads(3)
    public int blockingLadderConsumer(BlockingLadderState g, CopyScratch s)
            throws InterruptedException {
        // poll(1 ms) instead of take(): identical fast path while items are
        // available, but a parked take() at iteration end hangs JMH teardown.
        LadderSnapshot m;
        int starved = 0;
        while ((m = g.queue.poll(1, TimeUnit.MILLISECONDS)) == null) {
            if (++starved >= 10) {
                return -1; // iteration over: one phantom op per thread per phase
            }
        }
        return consumeSnapshot(m, s);
    }

    @State(Scope.Group)
    public static class ClqLadderState {
        final ConcurrentLinkedQueue<LadderSnapshot> queue = new ConcurrentLinkedQueue<>();
    }

    @Benchmark
    @Group("clqLadder1p3c")
    @GroupThreads(1)
    public void clqLadderProducer(ClqLadderState g, HeapProducerScratch p) {
        g.queue.offer(takeSnapshot(p));
    }

    @Benchmark
    @Group("clqLadder1p3c")
    @GroupThreads(3)
    public int clqLadderConsumer(ClqLadderState g, CopyScratch s) {
        LadderSnapshot m;
        if ((m = g.queue.poll()) == null) {
            long spins = 0;
            long start = System.nanoTime();
            while ((m = g.queue.poll()) == null) {
                if (((++spins & 0x3FF) == 0) && (System.nanoTime() - start) > 10_000_000L) {
                    return -1; // iteration over: one phantom op per thread per phase
                }
            }
        }
        return consumeSnapshot(m, s);
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(LadderBenchmarks.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(3)
                .timeUnit(TimeUnit.SECONDS)
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
