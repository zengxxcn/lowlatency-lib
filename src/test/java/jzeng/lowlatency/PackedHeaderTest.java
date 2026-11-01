package jzeng.lowlatency;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * RED tests for the packed slot header: version (low 32 bits) and size
 * (high 32 bits) share one atomic 64-bit word at the slot base.
 *
 * <p>The encoding must round-trip every int version (including odd writer
 * marks and negative values reached by 32-bit wraparound) and every legal
 * payload size, and every slot base must stay 8-byte aligned so the long
 * VarHandle access is legal on a direct buffer.
 */
class PackedHeaderTest {

    @Test
    void packRoundTripsVersionAndSize() {
        long packed = OffHeapRingSupport.pack(7, 64);
        assertEquals(7, OffHeapRingSupport.versionOf(packed));
        assertEquals(64, OffHeapRingSupport.sizeOf(packed));
    }

    @Test
    void packRoundTripsEveryVersionShape() {
        int[] versions = {0, 1, 2, 3, -1, -2, Integer.MAX_VALUE, Integer.MIN_VALUE};
        for (int version : versions) {
            long packed = OffHeapRingSupport.pack(version, 17);
            assertEquals(version, OffHeapRingSupport.versionOf(packed),
                    "version " + version);
            assertEquals(17, OffHeapRingSupport.sizeOf(packed),
                    "size for version " + version);
        }
    }

    @Test
    void packRoundTripsBoundarySizes() {
        int[] sizes = {0, 1, 63, 64, 256, Integer.MAX_VALUE};
        for (int size : sizes) {
            long packed = OffHeapRingSupport.pack(5, size);
            assertEquals(5, OffHeapRingSupport.versionOf(packed));
            assertEquals(size, OffHeapRingSupport.sizeOf(packed), "size " + size);
        }
    }

    @Test
    void everySlotBaseIsLongAligned() {
        int[] capacities = {1, 2, 4, 1024};
        int[] payloads = {1, 64, 256, 1000};
        for (int capacity : capacities) {
            for (int maxPayload : payloads) {
                int stride = OffHeapRingSupport.slotStride(maxPayload);
                for (int slot = 0; slot < capacity; slot++) {
                    int base = OffHeapRingSupport.slotBase(slot, capacity, stride);
                    assertEquals(0, base & 7,
                            "slot " + slot + " base " + base + " must be 8-byte aligned");
                }
            }
        }
    }

    @Test
    void packedWordIsReadableAfterReleaseStore() {
        OffHeapRingSupport.Region region = OffHeapRingSupport.allocate(2, 64);
        try {
            int base = OffHeapRingSupport.slotBase(0, 2, OffHeapRingSupport.slotStride(64));
            OffHeapRingSupport.setPackedRelease(region.slice, base, 9, 32);
            long packed = OffHeapRingSupport.getPackedAcquire(region.slice, base);
            assertEquals(9, OffHeapRingSupport.versionOf(packed));
            assertEquals(32, OffHeapRingSupport.sizeOf(packed));
        } finally {
            OffHeapRingSupport.free(region.raw);
        }
    }
}
