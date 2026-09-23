package jzeng.lowlatency;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RED tests for the SPSC exactly-once off-heap ring.
 * Semantics under test: unread CAS gate + odd-version veto, Result SUCCESS/ERROR.
 */
class SpscOffHeapRingTest {

    private static byte[] msg(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void roundtripExactlyOnce() {
        try (SpscOffHeapRing ring = new SpscOffHeapRing(8)) {
            assertEquals(SpscWriteResult.SUCCESS, ring.write(msg("once")));
            byte[] dst = new byte[64];
            int n = ring.read(0, dst);
            assertEquals(4, n);
            assertEquals("once", new String(dst, 0, n, StandardCharsets.UTF_8));
            // Second read of the same slot must miss (exactly-once).
            assertEquals(-1, ring.read(0, new byte[64]));
        }
    }

    @Test
    void overrunOfUnreadSlotLosesMessage() {
        // Documents the ring contract "writer must not overwrite unread data":
        // with capacity 1 the second write steals the unread slot (CAS wins),
        // and `version += 1` flips it odd->even, so the slot looks empty.
        // Overrunning the consumer loses the message.
        try (SpscOffHeapRing ring = new SpscOffHeapRing(1)) {
            assertEquals(SpscWriteResult.SUCCESS, ring.write(msg("a")));
            assertEquals(SpscWriteResult.SUCCESS, ring.write(msg("b")));
            assertEquals(-1, ring.read(1, new byte[64]));
        }
    }

    @Test
    void rejectsOversizePayload() {
        try (SpscOffHeapRing ring = new SpscOffHeapRing(8)) {
            assertThrows(IllegalArgumentException.class, () -> ring.write(new byte[65]));
        }
    }

    @Test
    void readMissOnEmptySlot() {
        try (SpscOffHeapRing ring = new SpscOffHeapRing(8)) {
            assertEquals(-1, ring.read(0, new byte[64]));
        }
    }

    @Test
    void sequentialWritesAndReads() {
        try (SpscOffHeapRing ring = new SpscOffHeapRing(4)) {
            for (int i = 0; i < 8; i++) {
                String s = "k" + i;
                assertEquals(SpscWriteResult.SUCCESS, ring.write(msg(s)));
                byte[] dst = new byte[64];
                assertEquals(s.length(), ring.read(i, dst));
                assertEquals(s, new String(dst, 0, s.length(), StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void consumerOverrunProducerMissesUntilPublish() {
        try (SpscOffHeapRing ring = new SpscOffHeapRing(8)) {
            assertEquals(-1, ring.read(0, new byte[64]));
            assertEquals(SpscWriteResult.SUCCESS, ring.write(msg("late")));
            byte[] dst = new byte[64];
            assertEquals(4, ring.read(0, dst));
        }
    }

    @Test
    void byteBufferOverloadsRoundtrip() {
        try (SpscOffHeapRing ring = new SpscOffHeapRing(8)) {
            assertEquals(SpscWriteResult.SUCCESS,
                    ring.write(java.nio.ByteBuffer.wrap(msg("bb"))));
            java.nio.ByteBuffer dst = java.nio.ByteBuffer.allocate(64);
            assertEquals(2, ring.read(0, dst));
            // Exactly-once also holds on the ByteBuffer path.
            assertEquals(-1, ring.read(0, java.nio.ByteBuffer.allocate(64)));
        }
    }

    @Test
    void useAfterCloseThrows() {
        SpscOffHeapRing ring = new SpscOffHeapRing(8);
        ring.close();
        assertThrows(IllegalStateException.class, () -> ring.write(msg("x")));
        assertThrows(IllegalStateException.class, () -> ring.read(0, new byte[64]));
    }

    @Test
    void dstTooSmallThrows() {
        try (SpscOffHeapRing ring = new SpscOffHeapRing(8)) {
            assertEquals(SpscWriteResult.SUCCESS, ring.write(msg("hello")));
            assertThrows(IllegalArgumentException.class, () -> ring.read(0, new byte[2]));
        }
    }

    @Test
    @org.junit.jupiter.api.Timeout(30)
    void errorWhenOverwritingSlotMidRead() throws Exception {
        // The veto window (reader claimed `unread` but hasn't published yet) is
        // nanoseconds wide, so this hammers a capacity-1 ring from both sides:
        // every producer write targets the same slot the reader hammers.
        // No cursor tracking — slot is always 0 — so ERROR gaps are harmless here.
        try (SpscOffHeapRing ring = new SpscOffHeapRing(1)) {
            java.util.concurrent.atomic.AtomicBoolean running =
                    new java.util.concurrent.atomic.AtomicBoolean(true);
            java.util.concurrent.atomic.AtomicLong errors =
                    new java.util.concurrent.atomic.AtomicLong();
            java.util.concurrent.atomic.AtomicLong writes =
                    new java.util.concurrent.atomic.AtomicLong();
            byte[] payload = msg("v");
            Thread reader = new Thread(() -> {
                byte[] dst = new byte[64];
                while (running.get()) {
                    ring.read(0, dst);
                }
            });
            reader.start();
            try {
                long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
                while (errors.get() == 0 && System.nanoTime() < deadline) {
                    if (ring.write(payload) == SpscWriteResult.ERROR) {
                        errors.incrementAndGet();
                    }
                    writes.incrementAndGet();
                }
            } finally {
                running.set(false);
                reader.join(5_000);
            }
            assertTrue(errors.get() > 0,
                    "expected at least one ERROR veto in " + writes.get() + " contended writes");
        }
    }

    @Test
    void hotPathAllocatesNoHeapGarbage() {
        // write(byte[]) / read must not allocate (previously a capturing lambda per call).
        com.sun.management.ThreadMXBean mx =
                (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
        org.junit.jupiter.api.Assumptions.assumeTrue(mx.isThreadAllocatedMemorySupported());
        mx.setThreadAllocatedMemoryEnabled(true);
        try (SpscOffHeapRing ring = new SpscOffHeapRing(8)) {
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
