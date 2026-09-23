package jzeng.lowlatency;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

class ConflatedValueTest {

    private static byte[] msg(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void pollEmptyReturnsMinusOne() {
        try (ConflatedValue v = new ConflatedValue()) {
            assertEquals(-1, v.poll(new ConflatedValue.ConflatedCursor(), new byte[64]));
        }
    }

    @Test
    void publishThenPollReturnsData() {
        try (ConflatedValue v = new ConflatedValue()) {
            v.publish(msg("hi"));
            byte[] dst = new byte[64];
            assertEquals(2, v.poll(new ConflatedValue.ConflatedCursor(), dst));
            assertEquals("hi", new String(dst, 0, 2, StandardCharsets.UTF_8));
        }
    }

    @Test
    void secondPollWithoutPublishReturnsMinusOne() {
        try (ConflatedValue v = new ConflatedValue()) {
            v.publish(msg("hi"));
            ConflatedValue.ConflatedCursor cursor = new ConflatedValue.ConflatedCursor();
            assertEquals(2, v.poll(cursor, new byte[64]));
            assertEquals(-1, v.poll(cursor, new byte[64]));
        }
    }

    @Test
    void overwriteDropsIntermediate() {
        // Conflation: only the newest value is observable.
        try (ConflatedValue v = new ConflatedValue()) {
            v.publish(msg("stale"));
            v.publish(msg("fresh"));
            ConflatedValue.ConflatedCursor cursor = new ConflatedValue.ConflatedCursor();
            byte[] dst = new byte[64];
            assertEquals(5, v.poll(cursor, dst));
            assertEquals("fresh", new String(dst, 0, 5, StandardCharsets.UTF_8));
            assertEquals(-1, v.poll(cursor, new byte[64]));
        }
    }

    @Test
    void lateJoinerGetsLatest() {
        try (ConflatedValue v = new ConflatedValue()) {
            for (int i = 0; i < 100; i++) {
                v.publish(msg("v" + i));
            }
            byte[] dst = new byte[64];
            assertEquals(3, v.poll(new ConflatedValue.ConflatedCursor(), dst));
            assertEquals("v99", new String(dst, 0, 3, StandardCharsets.UTF_8));
        }
    }

    @Test
    void independentCursorsEachSeeLatest() {
        try (ConflatedValue v = new ConflatedValue()) {
            v.publish(msg("one"));
            ConflatedValue.ConflatedCursor a = new ConflatedValue.ConflatedCursor();
            ConflatedValue.ConflatedCursor b = new ConflatedValue.ConflatedCursor();
            byte[] da = new byte[64];
            byte[] db = new byte[64];
            assertEquals(3, v.poll(a, da));
            assertEquals(3, v.poll(b, db));
            assertArrayEquals(da, db);
            v.publish(msg("two"));
            assertEquals(3, v.poll(a, da));
            assertEquals(3, v.poll(b, db));
            assertEquals("two", new String(da, 0, 3, StandardCharsets.UTF_8));
        }
    }

    @Test
    void zeroCopyPublishCallback() {
        try (ConflatedValue v = new ConflatedValue()) {
            byte[] payload = msg("cb");
            v.publish(payload.length, (buf, offset, size) -> {
                for (int i = 0; i < size; i++) {
                    buf.put(offset + i, payload[i]);
                }
            });
            byte[] dst = new byte[64];
            assertEquals(2, v.poll(new ConflatedValue.ConflatedCursor(), dst));
        }
    }

    @Test
    void byteBufferOverloadsRoundtrip() {
        try (ConflatedValue v = new ConflatedValue()) {
            v.publish(ByteBuffer.wrap(msg("bb")));
            ConflatedValue.ConflatedCursor cursor = new ConflatedValue.ConflatedCursor();
            ByteBuffer dst = ByteBuffer.allocate(64);
            assertEquals(2, v.poll(cursor, dst));
            assertEquals(-1, v.poll(cursor, ByteBuffer.allocate(64)));
        }
    }

    @Test
    void rejectsOversizePayload() {
        try (ConflatedValue v = new ConflatedValue()) {
            assertThrows(IllegalArgumentException.class, () -> v.publish(new byte[65]));
            assertThrows(IllegalArgumentException.class, () -> v.publish(65, (buf, off, n) -> {
            }));
        }
    }

    @Test
    void typedPublishPollRoundTrip() {
        try (ConflatedValue v = new ConflatedValue(TestOrderEvent.ENCODED)) {
            assertEquals(TestOrderEvent.ENCODED, v.maxPayload());
            TestOrderEvent view = new TestOrderEvent();
            v.publish((TestOrderEvent e, long seq) -> e.set(2002L, 9, 3100L), view);
            TestOrderEvent reuse = new TestOrderEvent();
            ConflatedValue.ConflatedCursor cursor = new ConflatedValue.ConflatedCursor();
            assertEquals(TestOrderEvent.ENCODED, v.poll(cursor, reuse));
            assertEquals(2002L, reuse.orderId());
            assertEquals(9, reuse.qty());
            assertEquals(3100L, reuse.price());
            // No new publish: second poll with the same cursor misses.
            assertEquals(-1, v.poll(cursor, reuse));
        }
    }

    @Test
    void oversizeTypeRejected() {
        try (ConflatedValue v = new ConflatedValue()) {
            TestPriceLadder view = new TestPriceLadder();
            assertThrows(IllegalArgumentException.class,
                    () -> v.publish((TestPriceLadder e, long seq) -> e.setDepth(8), view));
        }
    }

    @Test
    void variableDepthLadderRoundTrip() {
        try (ConflatedValue v = new ConflatedValue(TestPriceLadder.MAX_ENCODED)) {
            TestPriceLadder view = new TestPriceLadder();
            v.publish((TestPriceLadder e, long seq) -> {
                e.setSymbol("EURUSD");
                e.setMarket("LMAX");
                e.setEventId(9001L);
                e.setTimestampNs(1_720_000_000_000_000_002L);
                e.setDepth(2);
                e.setLevel(0, 108100L, 108103L, 5_000_000L);
                e.setLevel(1, 108099L, 108104L, 6_000_000L);
            }, view);
            TestPriceLadder reuse = new TestPriceLadder();
            ConflatedValue.ConflatedCursor cursor = new ConflatedValue.ConflatedCursor();
            assertEquals(TestPriceLadder.HEADER + 2 * TestPriceLadder.ENTRY, v.poll(cursor, reuse));
            assertEquals("EURUSD", reuse.symbol());
            assertEquals("LMAX", reuse.market());
            assertEquals(9001L, reuse.eventId());
            assertEquals(2, reuse.depth());
            assertEquals(108099L, reuse.levelBid(1));
            assertEquals(108104L, reuse.levelAsk(1));
            assertEquals(6_000_000L, reuse.levelQty(1));
        }
    }

    @Test
    void typedHotPathAllocatesNoHeapGarbage() {
        com.sun.management.ThreadMXBean mx =
                (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
        org.junit.jupiter.api.Assumptions.assumeTrue(mx.isThreadAllocatedMemorySupported());
        mx.setThreadAllocatedMemoryEnabled(true);
        try (ConflatedValue v = new ConflatedValue(TestOrderEvent.ENCODED)) {
            TestOrderEvent view = new TestOrderEvent();
            TestOrderEvent reuse = new TestOrderEvent();
            ConflatedValue.ConflatedCursor cursor = new ConflatedValue.ConflatedCursor();
            EventTranslator<TestOrderEvent> translator = (e, seq) -> e.set(seq, 1, 2L);
            long me = Thread.currentThread().getId();
            for (int w = 0; w < 10; w++) {
                long b = mx.getThreadAllocatedBytes(me);
                for (long i = 0; i < 100_000; i++) {
                    v.publish(translator, view);
                    cursor.lastSeenVersion = -1; // force re-poll every iteration
                    v.poll(cursor, reuse);
                }
                if (mx.getThreadAllocatedBytes(me) - b == 0) {
                    return;
                }
            }
            fail("typed hot path keeps allocating heap garbage");
        }
    }

    @Test
    void closeReleasesCleanly() {
        ConflatedValue v = new ConflatedValue();
        v.publish(msg("x"));
        assertDoesNotThrow(v::close);
    }

    @Test
    void dstTooSmallThrows() {
        try (ConflatedValue v = new ConflatedValue()) {
            v.publish(msg("hello"));
            assertThrows(IllegalArgumentException.class,
                    () -> v.poll(new ConflatedValue.ConflatedCursor(), new byte[2]));
        }
    }

    @Test
    void hotPathAllocatesNoHeapGarbage() {
        // publish(byte[]) / poll must not allocate (previously a capturing
        // lambda per call). Measured via per-thread allocated bytes.
        com.sun.management.ThreadMXBean mx =
                (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
        org.junit.jupiter.api.Assumptions.assumeTrue(mx.isThreadAllocatedMemorySupported());
        mx.setThreadAllocatedMemoryEnabled(true);
        try (ConflatedValue v = new ConflatedValue()) {
            byte[] payload = msg("0123456789abcdef0123456789abcdef"); // 32 B
            byte[] dst = new byte[64];
            ConflatedValue.ConflatedCursor cursor = new ConflatedValue.ConflatedCursor();
            long me = Thread.currentThread().getId();
            // Settle until one full window allocates nothing (absorbs one-time
            // JIT/init costs — steady state is what the test pins).
            for (int w = 0; w < 10; w++) {
                long b = mx.getThreadAllocatedBytes(me);
                for (int i = 0; i < 100_000; i++) {
                    v.publish(payload);
                    v.poll(cursor, dst);
                }
                if (mx.getThreadAllocatedBytes(me) - b == 0) {
                    return; // steady state reached: hot path is allocation-free
                }
            }
            org.junit.jupiter.api.Assertions.fail("hot path keeps allocating heap garbage");
        }
    }
}
