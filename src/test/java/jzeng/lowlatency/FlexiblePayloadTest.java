package jzeng.lowlatency;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RED tests for per-instance max payload: slot stride is derived from the
 * ring's max payload instead of the fixed 64B global.
 */
class FlexiblePayloadTest {

    private static byte[] pattern(int size, int seed) {
        byte[] b = new byte[size];
        for (int i = 0; i < size; i++) {
            b[i] = (byte) (seed * 31 + i);
        }
        return b;
    }

    @Test
    void spmcDefaultConstructorStill64BCapped() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8)) {
            assertEquals(64, ring.maxPayload());
            assertThrows(IllegalArgumentException.class, () -> ring.write(pattern(65, 1)));
        }
    }

    @Test
    void spmc1024ByteRoundTrip() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8, 1024)) {
            assertEquals(1024, ring.maxPayload());
            byte[] payload = pattern(1024, 7);
            ring.write(payload);
            byte[] dst = new byte[1024];
            assertEquals(1024, ring.read(0, dst));
            assertArrayEquals(payload, dst);
        }
    }

    @Test
    void spmcOversizeRejectedAtInstanceCap() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(8, 1024)) {
            assertThrows(IllegalArgumentException.class, () -> ring.write(pattern(1025, 2)));
        }
    }

    @Test
    void spmcNeighborSlotIsolationAtWideStride() {
        try (SpmcOffHeapRing ring = new SpmcOffHeapRing(2, 1024)) {
            byte[] first = pattern(1024, 11);
            byte[] second = pattern(1024, 13);
            ring.write(first);
            ring.write(second);
            byte[] dst = new byte[1024];
            assertEquals(1024, ring.read(0, dst));
            assertArrayEquals(first, dst);
            assertEquals(1024, ring.read(1, dst));
            assertArrayEquals(second, dst);
        }
    }

    @Test
    void spscDefaultConstructorStill64BCapped() {
        try (SpscOffHeapRing ring = new SpscOffHeapRing(8)) {
            assertEquals(64, ring.maxPayload());
            assertThrows(IllegalArgumentException.class, () -> ring.write(pattern(65, 3)));
        }
    }

    @Test
    void spsc1024ByteRoundTrip() {
        try (SpscOffHeapRing ring = new SpscOffHeapRing(8, 1024)) {
            assertEquals(1024, ring.maxPayload());
            byte[] payload = pattern(1024, 5);
            assertEquals(SpscWriteResult.SUCCESS, ring.write(payload));
            byte[] dst = new byte[1024];
            assertEquals(1024, ring.read(0, dst));
            assertArrayEquals(payload, dst);
        }
    }

    @Test
    void spscOversizeRejectedAtInstanceCap() {
        try (SpscOffHeapRing ring = new SpscOffHeapRing(8, 1024)) {
            assertThrows(IllegalArgumentException.class, () -> ring.write(pattern(1025, 4)));
        }
    }
}
