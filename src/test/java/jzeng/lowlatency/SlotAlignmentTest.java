package jzeng.lowlatency;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RED tests for cache-line isolation of slots: the stride must be a multiple
 * of 64 so adjacent slots never share a cache line, and the allocated base
 * must be 64-aligned so offsets stay line-aligned in absolute terms.
 *
 * <p>Previously {@code slotStride} rounded to 8, so e.g. a 20B payload gave
 * stride 88 (88 % 64 == 24) and the producer's hottest word (next slot's
 * version) shared a line with the previous slot's payload tail.
 */
class SlotAlignmentTest {

    @Test
    void strideIsMultipleOf64AndFitsPayload() {
        int[] payloads = {1, 7, 8, 20, 64, 65, 236, 256, 1000};
        for (int maxPayload : payloads) {
            int stride = OffHeapRingSupport.slotStride(maxPayload);
            assertEquals(0, stride % 64,
                    "stride must be cache-line multiple, maxPayload=" + maxPayload);
            assertTrue(stride >= 64 + maxPayload,
                    "stride must fit header + payload, maxPayload=" + maxPayload);
        }
    }

    @Test
    void defaultAndLadderStridesUnchanged() {
        assertEquals(128, OffHeapRingSupport.slotStride(64));
        assertEquals(320, OffHeapRingSupport.slotStride(256));
    }

    @Test
    void alignPadMath() {
        assertEquals(0, OffHeapRingSupport.alignPad(64));
        assertEquals(0, OffHeapRingSupport.alignPad(0));
        assertEquals(56, OffHeapRingSupport.alignPad(8));
        assertEquals(63, OffHeapRingSupport.alignPad(65));
        assertEquals(1, OffHeapRingSupport.alignPad(127));
    }

    @Test
    void allocatedSliceBaseIsCacheLineAligned() {
        int[][] cases = {{1, 64}, {8, 20}, {1024, 64}, {1024, 256}, {16, 236}};
        for (int[] c : cases) {
            OffHeapRingSupport.Region region = OffHeapRingSupport.allocate(c[0], c[1]);
            try {
                long addr = OffHeapRingSupport.addressOf(region.slice);
                assertEquals(0, addr % 64,
                        "slice base must be 64-aligned, cap=" + c[0] + " max=" + c[1]);
                int stride = OffHeapRingSupport.slotStride(c[1]);
                assertEquals(64 + (long) c[0] * stride, region.slice.capacity());
            } finally {
                OffHeapRingSupport.free(region.raw);
            }
        }
    }
}
