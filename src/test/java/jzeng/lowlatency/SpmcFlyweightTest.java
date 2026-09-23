package jzeng.lowlatency;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RED tests for typed SPMC multicast read/write via flyweights.
 */
class SpmcFlyweightTest {

    @Test
    void typedRoundTrip() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            TestOrderEvent view = new TestOrderEvent();
            ring.write((TestOrderEvent e, long seq) -> e.set(1001L, 5, 9950L), view);
            TestOrderEvent reuse = new TestOrderEvent();
            assertEquals(TestOrderEvent.ENCODED, ring.read(0, reuse));
            assertEquals(1001L, reuse.orderId());
            assertEquals(5, reuse.qty());
            assertEquals(9950L, reuse.price());
        }
    }

    @Test
    void missLeavesReuseUntouched() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            TestOrderEvent reuse = new TestOrderEvent();
            assertEquals(-1, ring.read(0, reuse));
        }
    }

    @Test
    void oversizeTypeRejected() {
        // Ladder worst case (132B) does not fit the default 64B ring.
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            TestPriceLadder view = new TestPriceLadder();
            assertThrows(IllegalArgumentException.class,
                    () -> ring.write((TestPriceLadder e, long seq) -> e.setDepth(8), view));
        }
    }

    @Test
    void variableDepthLadderRoundTrip() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8, TestPriceLadder.MAX_ENCODED)) {
            TestPriceLadder view = new TestPriceLadder();
            ring.write((TestPriceLadder e, long seq) -> {
                e.setSymbol("EURUSD");
                e.setMarket("LMAX");
                e.setEventId(9001L);
                e.setTimestampNs(1_720_000_000_000_000_000L);
                e.setDepth(3);
                e.setLevel(0, 10810L, 10812L, 1_000_000L);
                e.setLevel(1, 10809L, 10813L, 1_500_000L);
                e.setLevel(2, 10808L, 10814L, 2_000_000L);
            }, view);
            TestPriceLadder reuse = new TestPriceLadder();
            assertEquals(TestPriceLadder.HEADER + 3 * TestPriceLadder.ENTRY, ring.read(0, reuse));
            assertEquals("EURUSD", reuse.symbol());
            assertEquals("LMAX", reuse.market());
            assertEquals(9001L, reuse.eventId());
            assertEquals(1_720_000_000_000_000_000L, reuse.timestampNs());
            assertEquals(3, reuse.depth());
            assertEquals(10809L, reuse.levelBid(1));
            assertEquals(10813L, reuse.levelAsk(1));
            assertEquals(1_500_000L, reuse.levelQty(1));
        }
    }

    @Test
    void multicastTypedFanOut() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            TestOrderEvent view = new TestOrderEvent();
            ring.write((TestOrderEvent e, long seq) -> e.set(7L, 1, 2L), view);
            for (int c = 0; c < 3; c++) {
                TestOrderEvent reuse = new TestOrderEvent();
                assertEquals(TestOrderEvent.ENCODED, ring.read(0, reuse));
                assertEquals(7L, reuse.orderId());
            }
        }
    }

    @Test
    void typedHotPathAllocatesNoHeapGarbage() {
        com.sun.management.ThreadMXBean mx =
                (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
        org.junit.jupiter.api.Assumptions.assumeTrue(mx.isThreadAllocatedMemorySupported());
        mx.setThreadAllocatedMemoryEnabled(true);
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
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
