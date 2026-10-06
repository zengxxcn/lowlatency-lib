package jzeng.lowlatency;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static jzeng.lowlatency.SpmcOffHeapRing.GAP_LAPPED;
import static jzeng.lowlatency.SpmcOffHeapRing.MISS;

/**
 * Int-version wraparound: published versions cycle mod 2^32
 * ({@code capacity * 2^30} messages to wrap), so gap-vs-miss
 * classification must use serial-arithmetic comparison, not signed {@code >}.
 */
class SpmcVersionWrapTest {

    @Test
    void normalGenerations() {
        assertEquals(GAP_LAPPED, SpmcOffHeapRing.classifyGap(6, 4));
        assertEquals(MISS, SpmcOffHeapRing.classifyGap(2, 4));
    }

    @Test
    void wrappedNewerGenerationIsGapNotMiss() {
        // Cursor expects the last pre-wrap version (even 2^31-2);
        // the slot already holds the next generation, wrapped to INT_MIN.
        // Signed '>' would report MISS and stall the lapped reader.
        assertEquals(GAP_LAPPED,
                SpmcOffHeapRing.classifyGap(Integer.MIN_VALUE, Integer.MAX_VALUE - 1));
    }

    @Test
    void stalePreWrapSlotIsMissNotGap() {
        // Cursor already past the wrap expects INT_MIN; the slot still
        // holds the pre-wrap generation (consumer ahead). Signed '>'
        // would report a false GAP.
        assertEquals(MISS,
                SpmcOffHeapRing.classifyGap(Integer.MAX_VALUE - 1, Integer.MIN_VALUE));
    }

    @Test
    void smallDistancesAcrossZeroBoundary() {
        assertEquals(GAP_LAPPED, SpmcOffHeapRing.classifyGap(-2147483646, 2147483646));
        assertEquals(MISS, SpmcOffHeapRing.classifyGap(2147483646, -2147483646));
    }
}
