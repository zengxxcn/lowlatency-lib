package jzeng.lowlatency;

import java.nio.ByteBuffer;

/**
 * Reusable view over an off-heap slot region (SBE/Agrona-style flyweight).
 *
 * <p>Implementations read/write fields straight through to the wrapped direct
 * buffer region — the setters are the encoder, the getters the decoder, so no
 * per-message allocation or copy ever occurs. Instances are caller-owned
 * reusables: the ring borrows them for the duration of a call only.
 *
 * <p>A wrapped view is valid only until the producer overwrites that slot.
 */
public interface Flyweight {

    /** Bind this view to {@code buffer[offset, offset+length)}. */
    void wrap(ByteBuffer buffer, int offset, int length);

    /**
     * Worst-case encoded size for this type. Sizes the ring, e.g.
     * {@code new SpmcOffHeapRing(capacity, MyEvent.MAX_ENCODED)}.
     */
    int maxEncodedLength();

    /**
     * Actual encoded size of the current contents, tracked by the mutating
     * setters during population. Fixed-size types return a constant.
     */
    int encodedLength();
}
