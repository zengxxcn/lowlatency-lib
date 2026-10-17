package jzeng.lowlatency;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static jzeng.lowlatency.SpmcOffHeapRing.GAP_LAPPED;
import static jzeng.lowlatency.SpmcOffHeapRing.MISS;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Typed SPMC read/write via the restored Flyweight API. The typed read copies
 * into the view's own snapshot buffer and revalidates after the copy, so decode
 * cannot tear against an overwriting producer — the regression the slot-aliased
 * zero-copy read could not guard against — and multicast consumers cannot clobber
 * each other's decode target.
 */
class SpmcFlyweightApiTest {

    private static ByteBuffer sentinel(long orderId, int qty, long price) {
        ByteBuffer buf = ByteBuffer.allocate(TestOrderEvent.ENCODED);
        buf.putLong(orderId);
        buf.putInt(qty);
        buf.putLong(price);
        return buf;
    }

    @Test
    void typedWriteThenReadRoundTrip() {
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
    void typedVariableSizeRoundTrip() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8, TestPriceLadder.MAX_ENCODED)) {
            TestPriceLadder view = new TestPriceLadder();
            ring.write((TestPriceLadder e, long seq) -> {
                e.setSymbol("EURUSD");
                e.setMarket("LMAX");
                e.setEventId(9001L);
                e.setTimestampNs(1_720_000_000_000_000_000L);
                e.setDepth(2);
                e.setLevel(0, 10810L, 10812L, 1_000_000L);
                e.setLevel(1, 10809L, 10813L, 1_500_000L);
            }, view);
            TestPriceLadder reuse = new TestPriceLadder();
            int n = ring.read(0, reuse);
            assertEquals(TestPriceLadder.HEADER + 2 * TestPriceLadder.ENTRY, n);
            assertEquals("EURUSD", reuse.symbol());
            assertEquals(9001L, reuse.eventId());
            assertEquals(2, reuse.depth());
            assertEquals(10809L, reuse.levelBid(1));
            assertEquals(10813L, reuse.levelAsk(1));
            assertEquals(1_500_000L, reuse.levelQty(1));
        }
    }

    @Test
    void decodeSurvivesProducerLaps() {
        // The regression test for the removed slot-aliased read: the view wraps
        // the snapshot, not the slot, so later generations cannot bleed in.
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(4)) {
            TestOrderEvent view = new TestOrderEvent();
            ring.write((TestOrderEvent e, long seq) -> e.set(100L, 1, 1000L), view);
            TestOrderEvent reuse = new TestOrderEvent();
            assertEquals(TestOrderEvent.ENCODED, ring.read(0, reuse));
            for (int i = 0; i < 40; i++) {
                final int orderId = 900 + i;
                ring.write((TestOrderEvent e, long seq) -> e.set(orderId, 9, 9000L), view);
            }
            assertEquals(100L, reuse.orderId());
            assertEquals(1, reuse.qty());
            assertEquals(1000L, reuse.price());
        }
    }

    @Test
    void consecutiveTypedReadsEachStartAtSnapshotZero() {
        // The view's snapshot is reused, so every typed read must copy to
        // offset 0. A read that appends at the buffer's position would stack
        // copies (stale bytes decoded, then IndexOutOfBounds once the sum of
        // payload sizes passes maxPayload).
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8, TestPriceLadder.MAX_ENCODED)) {
            TestPriceLadder view = new TestPriceLadder();
            TestPriceLadder reuse = new TestPriceLadder();
            for (int i = 0; i < 6; i++) {
                final int depth = TestPriceLadder.MAX_DEPTH;
                final long eventId = 7000L + i;
                ring.write((TestPriceLadder e, long seq) -> {
                    e.setSymbol("EURUSD");
                    e.setMarket("LMAX");
                    e.setEventId(eventId);
                    e.setTimestampNs(1_720_000_000_000_000_000L);
                    e.setDepth(depth);
                    for (int lvl = 0; lvl < depth; lvl++) {
                        e.setLevel(lvl, 10_800L + lvl, 10_810L + lvl, 1_000_000L);
                    }
                }, view);
                int n = ring.read(i, reuse);
                assertEquals(TestPriceLadder.HEADER + depth * TestPriceLadder.ENTRY, n);
                assertEquals(eventId, reuse.eventId());
                assertEquals(depth, reuse.depth());
                assertEquals(10_800L + depth - 1, reuse.levelBid(depth - 1));
            }
        }
    }

    @Test
    void multicastViewsDecodeIndependently() {
        // Two consumers, two flyweights: each owns its own snapshot, so one
        // consumer's read cannot overwrite the bytes the other is decoding.
        // With a shared ring-owned snapshot this fails — view A would decode
        // consumer B's message.
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8, TestPriceLadder.MAX_ENCODED)) {
            TestPriceLadder producer = new TestPriceLadder();
            TestPriceLadder consumerA = new TestPriceLadder();
            TestPriceLadder consumerB = new TestPriceLadder();
            ring.write(ladderWithId(4001L), producer);
            ring.read(0, consumerA);
            assertEquals(4001L, consumerA.eventId());
            ring.write(ladderWithId(4002L), producer);
            ring.read(1, consumerB);
            assertEquals(4002L, consumerB.eventId());
            assertNotSame(consumerA.snapshotBuffer(), consumerB.snapshotBuffer());
            assertEquals(4001L, consumerA.eventId(), "consumer A's view was clobbered by consumer B");
        }
    }

    @Test
    void undersizedSnapshotBufferRejected() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            TestOrderEvent view = new TestOrderEvent();
            ring.write((TestOrderEvent e, long seq) -> e.set(1L, 1, 1L), view);
            TinySnapshotEvent reuse = new TinySnapshotEvent();
            IllegalStateException e = assertThrows(IllegalStateException.class, () -> ring.read(0, reuse));
            assertTrue(e.getMessage().contains("snapshot"), e.getMessage());
        }
    }

    private static EventTranslator<TestPriceLadder> ladderWithId(long eventId) {
        return (TestPriceLadder e, long seq) -> {
            e.setSymbol("EURUSD");
            e.setMarket("LMAX");
            e.setEventId(eventId);
            e.setTimestampNs(1_720_000_000_000_000_000L);
            e.setDepth(2);
            e.setLevel(0, 10810L, 10812L, 1_000_000L);
            e.setLevel(1, 10809L, 10813L, 1_500_000L);
        };
    }

    /** A flyweight whose snapshot buffer is too small for its own payload. */
    private static final class TinySnapshotEvent implements Flyweight {
        private final ByteBuffer snapshot = ByteBuffer.allocate(4);
        private ByteBuffer buffer;
        private int base;

        @Override
        public void wrap(ByteBuffer buffer, int offset, int length) {
            this.buffer = buffer;
            this.base = offset;
        }

        @Override
        public ByteBuffer snapshotBuffer() {
            return snapshot;
        }

        @Override
        public int maxEncodedLength() {
            return TestOrderEvent.ENCODED;
        }

        @Override
        public int encodedLength() {
            return TestOrderEvent.ENCODED;
        }

        public long orderId() {
            return buffer.getLong(base);
        }
    }

    @Test
    void missLeavesReuseUntouched() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            TestOrderEvent reuse = new TestOrderEvent();
            reuse.wrap(sentinel(111L, 2, 222L), 0, TestOrderEvent.ENCODED);
            assertEquals(MISS, ring.read(0, reuse));
            assertEquals(111L, reuse.orderId());
            assertEquals(2, reuse.qty());
            assertEquals(222L, reuse.price());
        }
    }

    @Test
    void gapLeavesReuseUntouched() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(2)) {
            TestOrderEvent view = new TestOrderEvent();
            for (int i = 0; i < 6; i++) {
                final int orderId = i;
                ring.write((TestOrderEvent e, long seq) -> e.set(orderId, 1, 1000L), view);
            }
            TestOrderEvent reuse = new TestOrderEvent();
            reuse.wrap(sentinel(111L, 2, 222L), 0, TestOrderEvent.ENCODED);
            assertEquals(GAP_LAPPED, ring.read(0, reuse));
            assertEquals(111L, reuse.orderId());
        }
    }

    @Test
    void oversizeTypeRejected() {
        // Ladder worst case (256B) does not fit the default 64B ring.
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            TestPriceLadder view = new TestPriceLadder();
            assertThrows(IllegalArgumentException.class,
                    () -> ring.write((TestPriceLadder e, long seq) -> e.setDepth(8), view));
        }
    }
}
