package jzeng.lowlatency;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RED tests for typed SPSC exactly-once read/write via flyweights.
 */
class SpscFlyweightTest {

    @Test
    void typedRoundTripExactlyOnce() {
        try (SpscOffHeapRing ring = new SpscOffHeapRing(8)) {
            TestOrderEvent view = new TestOrderEvent();
            assertEquals(SpscWriteResult.SUCCESS,
                    ring.write((TestOrderEvent e, long seq) -> e.set(2002L, 9, 3100L), view));
            TestOrderEvent reuse = new TestOrderEvent();
            assertEquals(TestOrderEvent.ENCODED, ring.read(0, reuse));
            assertEquals(2002L, reuse.orderId());
            assertEquals(9, reuse.qty());
            assertEquals(3100L, reuse.price());
            // Consumed: second read of the same slot misses.
            assertEquals(-1, ring.read(0, reuse));
        }
    }

    @Test
    void oversizeTypeRejected() {
        try (SpscOffHeapRing ring = new SpscOffHeapRing(8)) {
            TestPriceLadder view = new TestPriceLadder();
            assertThrows(IllegalArgumentException.class,
                    () -> ring.write((TestPriceLadder e, long seq) -> e.setDepth(8), view));
        }
    }

    @Test
    void variableDepthLadderRoundTrip() {
        try (SpscOffHeapRing ring = new SpscOffHeapRing(8, TestPriceLadder.MAX_ENCODED)) {
            TestPriceLadder view = new TestPriceLadder();
            assertEquals(SpscWriteResult.SUCCESS, ring.write((TestPriceLadder e, long seq) -> {
                e.setSymbol("USDJPY");
                e.setMarket("EBS");
                e.setEventId(7002L);
                e.setTimestampNs(1_720_000_000_000_000_001L);
                e.setDepth(2);
                e.setLevel(0, 155400L, 155403L, 3_000_000L);
                e.setLevel(1, 155399L, 155404L, 3_500_000L);
            }, view));
            TestPriceLadder reuse = new TestPriceLadder();
            assertEquals(TestPriceLadder.HEADER + 2 * TestPriceLadder.ENTRY, ring.read(0, reuse));
            assertEquals("USDJPY", reuse.symbol());
            assertEquals("EBS", reuse.market());
            assertEquals(7002L, reuse.eventId());
            assertEquals(2, reuse.depth());
            assertEquals(155399L, reuse.levelBid(1));
            assertEquals(155404L, reuse.levelAsk(1));
            assertEquals(3_500_000L, reuse.levelQty(1));
        }
    }

    @Test
    void typedHotPathAllocatesNoHeapGarbage() {
        com.sun.management.ThreadMXBean mx =
                (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
        org.junit.jupiter.api.Assumptions.assumeTrue(mx.isThreadAllocatedMemorySupported());
        mx.setThreadAllocatedMemoryEnabled(true);
        try (SpscOffHeapRing ring = new SpscOffHeapRing(8)) {
            TestOrderEvent view = new TestOrderEvent();
            TestOrderEvent reuse = new TestOrderEvent();
            EventTranslator<TestOrderEvent> translator = (e, seq) -> e.set(seq, 1, 2L);
            long me = Thread.currentThread().getId();
            for (int w = 0; w < 10; w++) {
                long b = mx.getThreadAllocatedBytes(me);
                for (long i = 0; i < 100_000; i++) {
                    ring.write(translator, view);
                    ring.read(i, reuse);
                }
                if (mx.getThreadAllocatedBytes(me) - b == 0) {
                    return;
                }
            }
            fail("typed hot path keeps allocating heap garbage");
        }
    }
}
