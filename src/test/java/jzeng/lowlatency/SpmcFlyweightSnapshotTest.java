package jzeng.lowlatency;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static jzeng.lowlatency.SpmcOffHeapRing.GAP_LAPPED;
import static jzeng.lowlatency.SpmcOffHeapRing.MISS;
import static org.junit.jupiter.api.Assertions.*;

/**
 * SPMC typed reads via the validated-snapshot pattern: copy into a private
 * dst buffer (read revalidates the version after the copy), then wrap the
 * flyweight over that snapshot and decode. The snapshot is immutable, so
 * decode can never tear against an overwriting producer — the guarantee the
 * removed zero-copy typed read (view aliased to the slot) could not offer.
 */
class SpmcFlyweightSnapshotTest {

    private static ByteBuffer snapshot(SpmcOffHeapRing ring, long cursor, int size) {
        byte[] dst = new byte[size];
        int n = ring.read(cursor, dst);
        assertEquals(size, n, "precondition: slot published");
        ByteBuffer snap = ByteBuffer.wrap(dst);
        snap.limit(n);
        return snap;
    }

    private static byte[] encode(long orderId, int qty, long price) {
        ByteBuffer buf = ByteBuffer.allocate(TestOrderEvent.ENCODED);
        buf.putLong(orderId);
        buf.putInt(qty);
        buf.putLong(price);
        return buf.array();
    }

    @Test
    void typedRoundTripFromSnapshot() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            ring.write(encode(1001L, 5, 9950L));
            TestOrderEvent reuse = new TestOrderEvent();
            reuse.wrap(snapshot(ring, 0, TestOrderEvent.ENCODED), 0, TestOrderEvent.ENCODED);
            assertEquals(1001L, reuse.orderId());
            assertEquals(5, reuse.qty());
            assertEquals(9950L, reuse.price());
        }
    }

    @Test
    void decodeAfterProducerLapCannotTear() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(4)) {
            ring.write(encode(100L, 1, 1000L));
            TestOrderEvent reuse = new TestOrderEvent();
            reuse.wrap(snapshot(ring, 0, TestOrderEvent.ENCODED), 0, TestOrderEvent.ENCODED);
            for (int i = 0; i < 40; i++) {
                ring.write(encode(900L + i, 9, 9000L));
            }
            assertEquals(100L, reuse.orderId());
            assertEquals(1, reuse.qty());
            assertEquals(1000L, reuse.price());
        }
    }

    @Test
    void consumerAheadMisses() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            assertEquals(MISS, ring.read(0, new byte[64]));
        }
    }

    @Test
    void lappedCursorSignalsGap() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(2)) {
            for (int i = 0; i < 6; i++) {
                ring.write(encode(i, 1, 1000L));
            }
            assertEquals(GAP_LAPPED, ring.read(0, new byte[64]),
                    "cursor 0 overwritten; caller must clamp, not decode stale slot");
        }
    }

    @Test
    void snapshotsAreConsumerPrivate() {
        // No ring-side snapshot buffer: each consumer decodes from its own dst.
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            ring.write(encode(7L, 1, 2L));
            TestOrderEvent a = new TestOrderEvent();
            TestOrderEvent b = new TestOrderEvent();
            a.wrap(snapshot(ring, 0, TestOrderEvent.ENCODED), 0, TestOrderEvent.ENCODED);
            b.wrap(snapshot(ring, 0, TestOrderEvent.ENCODED), 0, TestOrderEvent.ENCODED);
            a.set(42L, 7, 77L);
            assertEquals(7L, b.orderId(), "consumer B unaffected");
        }
    }
}
