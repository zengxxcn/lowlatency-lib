package jzeng.lowlatency;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the single-producer / single-consumer conflating overwrite ring.
 * Semantics under test: ordered subsequence while the consumer keeps up; when
 * the consumer falls further behind than {@code maxLag} the read reports
 * {@link SpscConflatingRing#LAPPED} and the caller resumes at
 * {@code producerSequence() - 1}. No exactly-once guarantee: re-reading a live
 * cursor is allowed, and the producer never waits.
 */
class SpscConflatingRingTest {

    private static final int PAYLOAD = 32;

    private static byte[] msg(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String str(byte[] bytes, int len) {
        return new String(bytes, 0, len, StandardCharsets.UTF_8);
    }

    private static byte[] generation(long seq) {
        byte[] payload = new byte[PAYLOAD];
        fillGeneration(payload, seq);
        return payload;
    }

    private static void fillGeneration(byte[] payload, long seq) {
        ByteBuffer w = ByteBuffer.wrap(payload);
        w.putLong(0, seq);
        w.putLong(8, seq);
        w.putLong(16, ~seq);
        w.putLong(24, seq ^ 0x5A5A5A5A5A5A5A5AL);
    }

    private static boolean isGeneration(byte[] payload, long seq) {
        ByteBuffer r = ByteBuffer.wrap(payload);
        return r.getLong(0) == seq
                && r.getLong(8) == seq
                && r.getLong(16) == ~seq
                && r.getLong(24) == (seq ^ 0x5A5A5A5A5A5A5A5AL);
    }

    @Test
    void roundTripAndMiss() {
        try (SpscConflatingRing ring = new SpscConflatingRing(8)) {
            assertEquals(0, ring.producerSequence());
            assertEquals(SpscConflatingRing.MISS, ring.read(0, new byte[64]));

            ring.write(msg("hello"));
            assertEquals(1, ring.producerSequence());
            byte[] dst = new byte[64];
            assertEquals(5, ring.read(0, dst));
            assertEquals("hello", str(dst, 5));

            // Caught up at the next sequence: miss, not a duplicate.
            assertEquals(SpscConflatingRing.MISS, ring.read(1, dst));
            // Re-reading a still-live cursor is allowed (idempotent retry).
            assertEquals(5, ring.read(0, dst));
            assertEquals("hello", str(dst, 5));
        }
    }

    @Test
    void inStepReadsAreOrdered() {
        try (SpscConflatingRing ring = new SpscConflatingRing(8)) {
            for (int i = 0; i < 5; i++) {
                ring.write(msg("m" + i));
            }
            for (int i = 0; i < 5; i++) {
                byte[] dst = new byte[64];
                assertEquals(2, ring.read(i, dst));
                assertEquals("m" + i, str(dst, 2));
            }
            assertEquals(SpscConflatingRing.MISS, ring.read(5, new byte[8]));
        }
    }

    @Test
    void conflatesWhenBehindAndResumesAtNewest() {
        try (SpscConflatingRing ring = new SpscConflatingRing(4)) { // maxLag defaults to 3
            for (int i = 0; i < 6; i++) {
                ring.write(msg("m" + i));
            }
            byte[] dst = new byte[8];
            // Six pending, cursor 0: backlog 6 > maxLag 3 -> conflate.
            assertEquals(SpscConflatingRing.LAPPED, ring.read(0, dst));
            long newest = ring.producerSequence() - 1;
            assertEquals(5, newest);
            assertEquals(2, ring.read(newest, dst));
            assertEquals("m5", str(dst, 2));
            // The cursor jump is the loss signal.
            assertEquals(5, newest - 0);
        }
    }

    @Test
    void maxLagOneReturnsOnlyNewest() {
        try (SpscConflatingRing ring = new SpscConflatingRing(8, OffHeapRingSupport.MAX_PAYLOAD, 1)) {
            ring.write(msg("a"));
            ring.write(msg("b"));
            byte[] dst = new byte[8];
            assertEquals(SpscConflatingRing.LAPPED, ring.read(0, dst));
            assertEquals(1, ring.read(1, dst));
            assertEquals("b", str(dst, 1));

            ring.write(msg("c")); // p = 3
            // Lag 1 is readable: the newest message lives at p - 1.
            assertEquals(1, ring.read(2, dst));
            assertEquals("c", str(dst, 1));
            ring.write(msg("d"));
            ring.write(msg("e")); // p = 5, cursor 2 is now 3 behind
            assertEquals(SpscConflatingRing.LAPPED, ring.read(2, dst));
            assertEquals(1, ring.read(4, dst));
            assertEquals("e", str(dst, 1));
        }
    }

    @Test
    void rejectsInvalidGeometry() {
        assertThrows(IllegalArgumentException.class, () -> new SpscConflatingRing(1));
        assertThrows(IllegalArgumentException.class, () -> new SpscConflatingRing(3));
        assertThrows(IllegalArgumentException.class, () -> new SpscConflatingRing(8, 0));
        assertThrows(IllegalArgumentException.class, () -> new SpscConflatingRing(8, 64, 0));
        assertThrows(IllegalArgumentException.class, () -> new SpscConflatingRing(8, 64, 8));
        assertThrows(IllegalArgumentException.class, () -> new SpscConflatingRing(8, 64, -1));
    }

    @Test
    void rejectsOversizePayload() {
        try (SpscConflatingRing ring = new SpscConflatingRing(8, 16)) {
            assertThrows(IllegalArgumentException.class, () -> ring.write(new byte[17]));
        }
    }

    @Test
    void byteBufferWriteAndRead() {
        try (SpscConflatingRing ring = new SpscConflatingRing(8)) {
            ByteBuffer src = ByteBuffer.allocate(32);
            src.put("buffer".getBytes(StandardCharsets.UTF_8)).flip();
            ring.write(src);
            assertEquals(0, src.remaining());

            ByteBuffer dst = ByteBuffer.allocate(64);
            dst.position(4);
            int n = ring.read(0, dst);
            assertEquals(6, n);
            assertEquals(10, dst.position());
            assertEquals("buffer", new String(dst.array(), 4, n, StandardCharsets.UTF_8));
        }
    }

    @Test
    void directWriterWrite() {
        try (SpscConflatingRing ring = new SpscConflatingRing(8)) {
            byte[] payload = msg("direct");
            ring.write(payload.length, (buffer, offset, size) ->
                    buffer.put(offset, payload, 0, size));
            byte[] dst = new byte[16];
            assertEquals(6, ring.read(0, dst));
            assertEquals("direct", str(dst, 6));
        }
    }

    @Test
    void typedReadDecodesFromSnapshotThatSurvivesLaps() {
        try (SpscConflatingRing ring = new SpscConflatingRing(8, TestOrderEvent.ENCODED)) {
            TestOrderEvent writer = new TestOrderEvent();
            ring.write((event, sequence) -> event.set(sequence * 10, (int) sequence, sequence * 100), writer);

            TestOrderEvent reader = new TestOrderEvent();
            assertEquals(TestOrderEvent.ENCODED, ring.read(0, reader));
            assertEquals(0, reader.orderId());
            assertEquals(0, reader.qty());
            assertEquals(0, reader.price());

            // Lap the ring many times; the returned view decodes from the
            // consumer-owned snapshot, so it must still hold generation 0.
            for (int i = 1; i <= 40; i++) {
                ring.write((event, sequence) -> event.set(sequence * 10, (int) sequence, sequence * 100), writer);
            }
            assertEquals(0, reader.orderId());
            assertEquals(0, reader.qty());
            assertEquals(0, reader.price());

            TestOrderEvent latest = new TestOrderEvent();
            long newest = ring.producerSequence() - 1;
            assertEquals(TestOrderEvent.ENCODED, ring.read(newest, latest));
            assertEquals(newest * 10, latest.orderId());
            assertEquals((int) newest, latest.qty());
            assertEquals(newest * 100, latest.price());
        }
    }

    @Test
    void typedWriteRejectsTypeLargerThanRing() {
        try (SpscConflatingRing ring = new SpscConflatingRing(8, 64)) {
            TestPriceLadder ladder = new TestPriceLadder();
            assertThrows(IllegalArgumentException.class,
                    () -> ring.write((event, sequence) -> event.setDepth(1), ladder));
        }
    }

    @Test
    void tornFreeUnderLappingProducer() throws Exception {
        final int capacity = 8;
        final int total = 200_000;
        try (SpscConflatingRing ring = new SpscConflatingRing(capacity, PAYLOAD)) {
            AtomicReference<Throwable> producerFailure = new AtomicReference<>();
            Thread producer = new Thread(() -> {
                try {
                    byte[] payload = new byte[PAYLOAD];
                    for (int i = 0; i < total; i++) {
                        fillGeneration(payload, i);
                        ring.write(payload);
                    }
                } catch (Throwable t) {
                    producerFailure.set(t);
                }
            }, "conflating-producer");
            producer.start();

            byte[] dst = new byte[PAYLOAD];
            long cursor = 0;
            long lastSeq = -1;
            long hits = 0;
            long conflations = 0;
            long deadline = System.nanoTime() + 15_000_000_000L;
            while (true) {
                int n = ring.read(cursor, dst);
                if (n == SpscConflatingRing.MISS) {
                    if (ring.producerSequence() >= total) {
                        break;
                    }
                    Thread.onSpinWait();
                } else if (n == SpscConflatingRing.LAPPED) {
                    long published = ring.producerSequence();
                    assertTrue(published > cursor, "lapped but no newer message");
                    cursor = published - 1;
                    conflations++;
                } else {
                    assertEquals(PAYLOAD, n);
                    assertTrue(isGeneration(dst, cursor),
                            "torn or mislabelled read at cursor " + cursor);
                    lastSeq = cursor;
                    cursor++;
                    hits++;
                }
                if (System.nanoTime() > deadline) {
                    fail("consumer did not finish: cursor=" + cursor + " hits=" + hits);
                }
            }
            producer.join(15_000);
            assertFalse(producer.isAlive(), "producer did not finish");
            if (producerFailure.get() != null) {
                fail("producer failed", producerFailure.get());
            }
            assertEquals(total - 1, lastSeq, "consumer never saw the newest message");
            assertTrue(hits > 0, "consumer never hit a message");
            assertTrue(conflations > 0, "test did not exercise conflation");
        }
    }
}
