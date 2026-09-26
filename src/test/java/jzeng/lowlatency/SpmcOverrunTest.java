package jzeng.lowlatency;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Seqlock generation fencing: a consumer ahead of the producer must miss,
 * never consume the previous lap's bytes as new messages.
 */
class SpmcOverrunTest {

    private static byte[] msg(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String readString(SpmcOffHeapRing ring, long cursor) {
        byte[] dst = new byte[64];
        int n = ring.read(cursor, dst);
        assertTrue(n > 0, "expected hit at cursor " + cursor);
        return new String(dst, 0, n, StandardCharsets.UTF_8);
    }

    @Test
    void consumerAheadOfProducerMissesInsteadOfReadingStaleLap() {
        // Capacity 4. Lap 1 fully published and consumed; lap-2 cursors
        // address the same slots but must miss until lap 2 is published.
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(4)) {
            for (int i = 0; i < 4; i++) {
                ring.write(msg("m" + i));
            }
            for (int i = 0; i < 4; i++) {
                assertEquals("m" + i, readString(ring, i));
            }
            byte[] dst = new byte[64];
            for (long c = 4; c < 8; c++) {
                assertEquals(-1, ring.read(c, dst),
                        "ahead read at cursor " + c + " must miss, not duplicate lap 1");
            }
            // Publish lap 2: the same cursors now hit with fresh data.
            for (int i = 4; i < 8; i++) {
                ring.write(msg("n" + i));
            }
            for (int i = 4; i < 8; i++) {
                assertEquals("n" + i, readString(ring, i));
            }
        }
    }

    @Test
    void consumerOverrunProducerMissesUntilPublish() {
        // Consumer ahead of the producer on a never-published slot: miss, then
        // the same cursor hits once the producer publishes — the spin-wait contract.
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            assertEquals(-1, ring.read(0, new byte[64]));
            ring.write(msg("late"));
            byte[] dst = new byte[64];
            assertEquals(4, ring.read(0, dst));
            assertEquals("late", new String(dst, 0, 4, StandardCharsets.UTF_8));
        }
    }

    @Test
    void producerOverrunSkipsMessages() {
        // Capacity 2, three writes: the slot for cursor 0 now holds generation 2
        // ("C"), a newer generation than cursor 0 expects. The lagging consumer
        // reads the newer data as if it were the old message — silent skip,
        // no corruption, no error signal.
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(2)) {
            ring.write(msg("A"));
            ring.write(msg("B"));
            ring.write(msg("C"));
            byte[] dst = new byte[64];
            int n = ring.read(0, dst);
            assertEquals(1, n);
            assertEquals("C", new String(dst, 0, n, StandardCharsets.UTF_8));
        }
    }

    @Test
    void partialPublishLeavesLaterCursorsMissing() {
        // Only seq 4 published: cursor 4 hits, cursors 5..7 still miss.
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(4)) {
            for (int i = 0; i < 4; i++) {
                ring.write(msg("m" + i));
            }
            for (int i = 0; i < 4; i++) {
                assertEquals("m" + i, readString(ring, i));
            }
            ring.write(msg("n4"));
            assertEquals("n4", readString(ring, 4));
            byte[] dst = new byte[64];
            for (long c = 5; c < 8; c++) {
                assertEquals(-1, ring.read(c, dst),
                        "cursor " + c + " not yet published, must miss");
            }
        }
    }
}
