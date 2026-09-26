package jzeng.lowlatency;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RED tests for the SPMC multicast off-heap ring.
 * Semantics under test: odd version = readable, reads are store-free (pure loads),
 * writer closes then publishes.
 */
class SpmcOffHeapRingTest {

    private static byte[] msg(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void roundtripSingleMessage() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            ring.write(msg("hello"));
            byte[] dst = new byte[64];
            int n = ring.read(0, dst);
            assertEquals(5, n);
            assertEquals("hello", new String(dst, 0, n, StandardCharsets.UTF_8));
        }
    }

    @Test
    void readMissOnEmptySlotReturnsMinusOne() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            assertEquals(-1, ring.read(0, new byte[64]));
        }
    }

    @Test
    void multicastEveryConsumerSeesEveryMessage() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            ring.write(msg("m1"));
            ring.write(msg("m2"));
            // Two independent cursors each read both messages (multicast, not competing).
            for (long cursor = 0; cursor < 2; cursor++) {
                byte[] dst = new byte[64];
                int n = ring.read(cursor, dst);
                assertTrue(n > 0, "consumer A missed slot " + cursor);
            }
            for (long cursor = 0; cursor < 2; cursor++) {
                byte[] dst = new byte[64];
                int n = ring.read(cursor, dst);
                assertTrue(n > 0, "consumer B missed slot " + cursor);
            }
        }
    }

    @Test
    void wrapAroundReusesSlots() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(4)) {
            for (int i = 0; i < 10; i++) {
                ring.write(msg("v" + i));
            }
            // Most recent write went to sequence 9 -> slot 1; it must be readable.
            byte[] dst = new byte[64];
            int n = ring.read(9, dst);
            assertEquals(2, n);
            assertEquals("v9", new String(dst, 0, n, StandardCharsets.UTF_8));
        }
    }

    @Test
    void rejectsOversizePayload() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            assertThrows(IllegalArgumentException.class, () -> ring.write(new byte[65]));
        }
    }

    @Test
    void rejectsZeroCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new SpmcOffHeapRing(0));
    }

    @Test
    void rejectsNonPowerOfTwoCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new SpmcOffHeapRing(3));
        assertThrows(IllegalArgumentException.class, () -> new SpmcOffHeapRing(1000));
        assertThrows(IllegalArgumentException.class, () -> new SpmcOffHeapRing(6, 128));
    }

    @Test
    void zeroCopyWriterCallback() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            byte[] payload = msg("cb");
            ring.write(payload.length, (buf, offset, size) -> {
                for (int i = 0; i < size; i++) {
                    buf.put(offset + i, payload[i]);
                }
            });
            byte[] dst = new byte[64];
            assertEquals(2, ring.read(0, dst));
        }
    }

    @Test
    void reReadingSameCursorReturnsDuplicate() {
        // Reads are non-destructive (no store to the version word): a consumer that
        // does not advance its cursor observes the same message again.
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            ring.write(msg("dup"));
            byte[] first = new byte[64];
            byte[] second = new byte[64];
            assertEquals(3, ring.read(0, first));
            assertEquals(3, ring.read(0, second));
            assertArrayEquals(first, second);
        }
    }

    @Test
    void consumerOverrunProducerMissesUntilPublish() {
        // Consumer ahead of the producer: miss, then the same cursor hits
        // once the producer publishes — the spin-wait contract.
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
        // Capacity 2, three writes: the slot for cursor 0 now holds "C".
        // The lagging consumer reads newer data as if it were the old message —
        // silent skip, no corruption, no error signal.
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
    void byteBufferOverloadsRoundtrip() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            ring.write(java.nio.ByteBuffer.wrap(msg("bb")));
            java.nio.ByteBuffer dst = java.nio.ByteBuffer.allocate(64);
            assertEquals(2, ring.read(0, dst));
            dst.flip();
            byte[] out = new byte[dst.remaining()];
            dst.get(out);
            assertEquals("bb", new String(out, StandardCharsets.UTF_8));
        }
    }

    @Test
    void closeReleasesCleanly() {
        SpmcOffHeapRing ring = new SpmcOffHeapRing(8);
        ring.write(msg("x"));
        assertDoesNotThrow(ring::close);
    }

    @Test
    void dstTooSmallThrows() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            ring.write(msg("hello"));
            assertThrows(IllegalArgumentException.class, () -> ring.read(0, new byte[2]));
        }
    }

    @Test
    void hotPathAllocatesNoHeapGarbage() {
        // write(byte[]) / read must not allocate (previously a capturing lambda per call).
        com.sun.management.ThreadMXBean mx =
                (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
        org.junit.jupiter.api.Assumptions.assumeTrue(mx.isThreadAllocatedMemorySupported());
        mx.setThreadAllocatedMemoryEnabled(true);
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            byte[] payload = msg("0123456789abcdef0123456789abcdef"); // 32 B
            byte[] dst = new byte[64];
            long me = Thread.currentThread().getId();
            // Settle until one full window allocates nothing (absorbs one-time
            // JIT/init costs — steady state is what the test pins).
            for (int w = 0; w < 10; w++) {
                long b = mx.getThreadAllocatedBytes(me);
                for (long i = 0; i < 100_000; i++) {
                    ring.write(payload);
                    ring.read(i, dst);
                }
                if (mx.getThreadAllocatedBytes(me) - b == 0) {
                    return; // steady state reached: hot path is allocation-free
                }
            }
            org.junit.jupiter.api.Assertions.fail("hot path keeps allocating heap garbage");
        }
    }
}
