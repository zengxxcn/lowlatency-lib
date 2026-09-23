package jzeng.lowlatency;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Concurrent stress: 1 producer × 3 consumers with checksummed payloads.
 * Any torn read (partially overwritten message) fails the checksum.
 */
class RingStressTest {

    /** Payload: [seqId int][check byte][padding...]; check = (byte)(seqId * 31 + 7). */
    static byte[] payload(int seqId, int size) {
        byte[] p = new byte[size];
        p[0] = (byte) (seqId >>> 24);
        p[1] = (byte) (seqId >>> 16);
        p[2] = (byte) (seqId >>> 8);
        p[3] = (byte) seqId;
        p[4] = (byte) (seqId * 31 + 7);
        return p;
    }

    static void check(byte[] data, int n) {
        assertTrue(n >= 5, "payload too short: " + n);
        int seqId = ((data[0] & 0xFF) << 24) | ((data[1] & 0xFF) << 16)
                | ((data[2] & 0xFF) << 8) | (data[3] & 0xFF);
        assertEquals((byte) (seqId * 31 + 7), data[4],
                "torn read detected for seqId " + seqId);
    }

    @Test
    @Timeout(60)
    void spmcMulticastNoTornReads() throws Exception {
        int capacity = 1024;
        int total = 30_000;
        int consumers = 3;
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(capacity)) {
            CountDownLatch done = new CountDownLatch(consumers);
            AtomicLong errors = new AtomicLong();
            java.util.concurrent.atomic.AtomicReference<String> firstError =
                    new java.util.concurrent.atomic.AtomicReference<>();
            AtomicLong[] consumed = {new AtomicLong(-1), new AtomicLong(-1), new AtomicLong(-1)};
            // Published watermark: a consumer may only read cursor c once the
            // producer has published sequence c. Without this gate a consumer
            // ahead of the producer would hit stale (still-odd) data from the
            // previous lap and mistake it for the new message.
            AtomicLong published = new AtomicLong(-1);

            Thread producer = new Thread(() -> {
                for (int i = 0; i < total; i++) {
                    // Throttle to the slowest consumer: slots are never reused
                    // before every consumer has read them (no skips).
                    while ((long) i - Math.min(consumed[0].get(),
                            Math.min(consumed[1].get(), consumed[2].get())) >= capacity) {
                        Thread.yield();
                    }
                    ring.write(payload(i, 32));
                    published.set(i);
                }
            });
            producer.start();

            for (int c = 0; c < consumers; c++) {
                final int id = c;
                new Thread(() -> {
                    try {
                        byte[] dst = new byte[64];
                        int expect = 0;
                        while (expect < total) {
                            // Never read ahead of the producer (see above).
                            while (published.get() < expect) {
                                Thread.yield();
                            }
                            int n = ring.read(expect, dst);
                            if (n > 0) {
                                try {
                                    int seqId = ((dst[0] & 0xFF) << 24) | ((dst[1] & 0xFF) << 16)
                                            | ((dst[2] & 0xFF) << 8) | (dst[3] & 0xFF);
                                    assertEquals(expect, seqId,
                                            "consumer " + id + " skipped/duplicated");
                                    check(dst, n);
                                } catch (AssertionError e) {
                                    firstError.compareAndSet(null,
                                            "consumer " + id + " expect=" + expect + ": " + e.getMessage());
                                    errors.incrementAndGet();
                                    return;
                                }
                                expect++;
                                consumed[id].set(expect - 1);
                            } else {
                                Thread.yield();
                            }
                        }
                    } finally {
                        done.countDown();
                    }
                }).start();
            }

            assertTrue(done.await(60, TimeUnit.SECONDS), "consumers did not finish in time");
            producer.join(5_000);
            assertEquals(0, errors.get(),
                    "torn/skipped/duplicated reads observed: " + firstError.get());
        }
    }

    @Test
    @Timeout(60)
    void spscExactlyOnceNoLossNoDuplicates() throws Exception {
        int capacity = 1024;
        int messages = 50_000;
        try (SpscOffHeapRing ring = new SpscOffHeapRing(capacity)) {
            CountDownLatch done = new CountDownLatch(1);
            AtomicLong errors = new AtomicLong();
            java.util.concurrent.atomic.AtomicReference<Throwable> producerError =
                    new java.util.concurrent.atomic.AtomicReference<>();
            // External flow control: the ring itself has none (ring contract:
            // "writer must not overwrite unread data"), so the test models a
            // well-behaved producer that never laps the consumer by >= capacity.
            AtomicLong consumed = new AtomicLong(-1);

            Thread producer = new Thread(() -> {
                try {
                    for (int i = 0; i < messages; i++) {
                        while ((long) i - consumed.get() >= capacity) {
                            Thread.yield();
                        }
                        byte[] p = payload(i, 32);
                        // Sequence is consumed even on ERROR (claimed by fetch-add
                        // before the veto), so retrying would misalign the consumer
                        // cursor. ERROR means the producer lapped a mid-read slot;
                        // with capacity 1024 and a keeping-up consumer it must not happen.
                        if (ring.write(p) != SpscWriteResult.SUCCESS) {
                            producerError.set(new AssertionError(
                                    "producer overran consumer at message " + i));
                            return;
                        }
                    }
                } finally {
                    // If the producer dies early the consumer would stall;
                    // release it so the test fails fast instead of timing out.
                    if (producerError.get() != null) {
                        done.countDown();
                    }
                }
            });
            Thread consumer = new Thread(() -> {
                try {
                    byte[] dst = new byte[64];
                    for (int expect = 0; expect < messages; ) {
                        int n = ring.read(expect, dst);
                        if (n > 0) {
                            int seqId = ((dst[0] & 0xFF) << 24) | ((dst[1] & 0xFF) << 16)
                                    | ((dst[2] & 0xFF) << 8) | (dst[3] & 0xFF);
                            if (seqId != expect) {
                                errors.incrementAndGet();
                                return;
                            }
                            check(dst, n);
                            expect++;
                            consumed.set(expect - 1);
                        } else {
                            Thread.yield();
                        }
                    }
                } finally {
                    done.countDown();
                }
            });
            producer.start();
            consumer.start();
            assertTrue(done.await(60, TimeUnit.SECONDS), "consumer did not finish in time");
            producer.join(5_000);
            assertNull(producerError.get(), "producer failed");
            assertEquals(0, errors.get(), "lost/duplicated/out-of-order messages");
        }
    }

    @Test
    @Timeout(60)
    void conflatedNoTornReadsMonotonic() throws Exception {
        // Producer flat-out overwrites with ZERO coordination (that is the point
        // of conflation). Each consumer polls until it observes the final seqId,
        // asserting every observed value is checksummed and strictly newer than
        // the previous one (gaps allowed, duplicates/stale/torn reads forbidden).
        int total = 200_000;
        int consumers = 3;
        try (ConflatedValue value = new ConflatedValue()) {
            CountDownLatch done = new CountDownLatch(consumers);
            AtomicLong errors = new AtomicLong();
            java.util.concurrent.atomic.AtomicReference<String> firstError =
                    new java.util.concurrent.atomic.AtomicReference<>();

            Thread producer = new Thread(() -> {
                for (int i = 0; i < total; i++) {
                    value.publish(payload(i, 32));
                }
            });
            producer.start();

            for (int c = 0; c < consumers; c++) {
                final int id = c;
                new Thread(() -> {
                    try {
                        ConflatedValue.ConflatedCursor cursor =
                                new ConflatedValue.ConflatedCursor();
                        byte[] dst = new byte[64];
                        int last = -1;
                        int seen = 0;
                        while (last != total - 1) {
                            int n = value.poll(cursor, dst);
                            if (n > 0) {
                                int seqId = ((dst[0] & 0xFF) << 24) | ((dst[1] & 0xFF) << 16)
                                        | ((dst[2] & 0xFF) << 8) | (dst[3] & 0xFF);
                                try {
                                    assertTrue(seqId > last,
                                            "non-monotonic: " + seqId + " after " + last);
                                    check(dst, n);
                                } catch (AssertionError e) {
                                    firstError.compareAndSet(null,
                                            "consumer " + id + ": " + e.getMessage());
                                    errors.incrementAndGet();
                                    return;
                                }
                                last = seqId;
                                seen++;
                            } else {
                                Thread.yield();
                            }
                        }
                        if (seen == 0) {
                            firstError.compareAndSet(null, "consumer " + id + " saw nothing");
                            errors.incrementAndGet();
                        }
                    } finally {
                        done.countDown();
                    }
                }).start();
            }

            assertTrue(done.await(60, TimeUnit.SECONDS), "consumers did not finish in time");
            producer.join(5_000);
            assertEquals(0, errors.get(),
                    "torn/stale/duplicated reads observed: " + firstError.get());
        }
    }
}
