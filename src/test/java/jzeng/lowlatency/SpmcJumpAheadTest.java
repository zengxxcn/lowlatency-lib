package jzeng.lowlatency;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RED tests for SPMC jump-ahead conflation.
 * Semantics under test: public producer sequence as backlog signal;
 * clamp-to-oldest-alive preserves live messages; jump-to-newest skips behind.
 */
class SpmcJumpAheadTest {

    @Test
    void producerSequenceStartsAtZeroAndTracksWrites() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            assertEquals(0, ring.producerSequence());
            ring.write("a".getBytes(StandardCharsets.UTF_8));
            ring.write("b".getBytes(StandardCharsets.UTF_8));
            ring.write("c".getBytes(StandardCharsets.UTF_8));
            assertEquals(3, ring.producerSequence());
        }
    }

    @Test
    void clampLeavesHealthyCursorAlone() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            for (int i = 0; i < 5; i++) ring.write(new byte[]{(byte) i});
            assertEquals(0, ring.clampToOldestAlive(0));
            assertEquals(3, ring.clampToOldestAlive(3));
        }
    }

    @Test
    void clampAdvancesOverrunCursorToOldestAlive() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(4)) {
            for (int i = 0; i < 10; i++) ring.write(new byte[]{(byte) i});
            assertEquals(6, ring.clampToOldestAlive(0)); // prod 10 - cap 4
            byte[] dst = new byte[64];
            assertTrue(ring.read(6, dst) > 0);
            assertEquals(6, dst[0]); // right generation, not silent skip
        }
    }

    @Test
    void clampNeverMovesCursorForward() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            assertEquals(0, ring.clampToOldestAlive(0)); // empty ring, prod 0
            ring.write(new byte[]{1});
            assertEquals(1, ring.clampToOldestAlive(1)); // cursor at frontier
        }
    }

    @Test
    void jumpLeavesCursorAloneWithinLag() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            for (int i = 0; i < 5; i++) ring.write(new byte[]{(byte) i});
            assertEquals(0, ring.jumpToNewest(0, 100));
        }
    }

    @Test
    void jumpFiresBeyondLagToNewestSettled() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            for (int i = 0; i < 10; i++) ring.write(new byte[]{(byte) i});
            assertEquals(8, ring.jumpToNewest(0, 4)); // prod 10 - 2, settled
            byte[] dst = new byte[64];
            assertTrue(ring.read(8, dst) >= 0); // hits first try, no spin
            assertEquals(8, dst[0]);
        }
    }

    @Test
    void jumpRejectsNegativeLag() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            assertThrows(IllegalArgumentException.class, () -> ring.jumpToNewest(0, -1));
        }
    }

    @Test
    void jumpNeverMovesCursorBackward() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            ring.write(new byte[]{1}); // prod 1, cursor 0, backlog 1 > maxLag 0
            assertEquals(0, ring.jumpToNewest(0, 0)); // target -1, stays put
        }
    }

    @Test
    void jumpingConsumerDoesNotDisturbOthers() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            for (int i = 0; i < 6; i++) ring.write(new byte[]{(byte) i});
            long jumper = ring.jumpToNewest(0, 2); // backlog 6 > 2 -> 4
            assertEquals(4, jumper);
            byte[] dst = new byte[64]; // steady consumer still sees every message
            for (long c = 0; c < 6; c++) {
                assertTrue(ring.read(c, dst) > 0, "steady consumer missed " + c);
                assertEquals((byte) c, dst[0]);
            }
        }
    }

    @Test
    void catchUpLoopReadsOnlyLiveDataUnderOverrun() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(4)) {
            for (int i = 0; i < 20; i++) ring.write(new byte[]{(byte) i});
            long cursor = 0;
            byte[] dst = new byte[64];
            int reads = 0;
            cursor = ring.clampToOldestAlive(cursor); // 16, skips overwritten 0..15
            assertEquals(16, cursor);
            while (cursor < ring.producerSequence() && reads < 10) {
                assertTrue(ring.read(cursor, dst) > 0, "clamped read must hit");
                assertEquals((byte) cursor, dst[0]); // generation matches cursor
                cursor++;
                reads++;
            }
            assertEquals(20, cursor); // drained to frontier, monotonic, no spins
        }
    }

    @Test
    void messagesLostIsZeroWhenHealthy() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(4)) {
            assertEquals(0, ring.messagesLost(0)); // empty ring
            for (int i = 0; i < 3; i++) ring.write(new byte[]{(byte) i});
            assertEquals(0, ring.messagesLost(0)); // backlog 3 < capacity 4
            assertEquals(0, ring.messagesLost(3)); // cursor at frontier
        }
    }

    @Test
    void messagesLostCountsOverwrittenMessages() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(4)) {
            for (int i = 0; i < 10; i++) ring.write(new byte[]{(byte) i});
            assertEquals(6, ring.messagesLost(0)); // prod 10 - cap 4 - cursor 0
            assertEquals(1, ring.messagesLost(5)); // only seq 5 gone
            assertEquals(0, ring.messagesLost(6)); // oldest live, nothing lost
        }
    }

    @Test
    void messagesLostAgreesWithClamp() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(4)) {
            for (int i = 0; i < 10; i++) ring.write(new byte[]{(byte) i});
            for (long c = 0; c < 10; c++) {
                assertEquals(ring.clampToOldestAlive(c) - c, ring.messagesLost(c));
            }
        }
    }
}
