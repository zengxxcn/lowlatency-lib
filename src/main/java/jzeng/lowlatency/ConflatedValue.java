package jzeng.lowlatency;

import java.nio.ByteBuffer;

import static jzeng.lowlatency.OffHeapRingSupport.INT_HANDLE;
import static jzeng.lowlatency.OffHeapRingSupport.OFF_DATA;
import static jzeng.lowlatency.OffHeapRingSupport.OFF_SIZE;
import static jzeng.lowlatency.OffHeapRingSupport.OFF_VERSION;

/**
 * Single-producer / N-consumer conflated latest-value register, fully off-heap.
 *
 * <p>Depth-1 conflation: the producer unconditionally overwrites one slot, so only
 * the newest value is ever observable — intermediates are dropped by construction.
 * There are no cursors into a ring, no wrap-around, and no overrun failure modes;
 * overwrite <em>is</em> the semantics.
 *
 * <p>Seqlock protocol on a single slot (deliberately <em>not</em> the SPMC parity:
 * the producer may clobber a slot mid-copy, so readers re-validate): {@code version}
 * even = stable, odd = writer active; each publish adds +2.
 *
 * <p>Lifecycle is caller-owned: {@link #close()} releases the direct memory and
 * must be called exactly once; no operation may follow it. There is
 * intentionally no per-operation open check.
 */
public final class ConflatedValue implements AutoCloseable {

    /** Per-consumer-thread poll position. Thread-confined: one cursor per consumer. */
    public static final class ConflatedCursor {
        // Matches the zero-init (empty) slot version, so a pre-first-publish
        // poll misses while a late joiner immediately sees the latest value.
        int lastSeenVersion;
    }

    private static final int BASE = OffHeapRingSupport.HEADER_SIZE; // single slot 0

    private final ByteBuffer buffer;
    private final ByteBuffer rawOwner;

    public ConflatedValue() {
        OffHeapRingSupport.Region region = OffHeapRingSupport.allocate(1, OffHeapRingSupport.MAX_PAYLOAD);
        this.buffer = region.slice;
        this.rawOwner = region.raw;
    }

    /** Convenience copy of a heap payload (unconditional overwrite, allocation-free). */
    public void publish(byte[] payload) {
        OffHeapRingSupport.checkPayloadSize(payload.length);
        int v0 = beginPublish();
        OffHeapRingSupport.setSizeRelease(buffer, BASE, payload.length);
        OffHeapRingSupport.copyFrom(buffer, BASE + OFF_DATA, payload, 0, payload.length);
        endPublish(v0);
    }

    /** Convenience copy from a {@link ByteBuffer} (consumes {@code remaining()} bytes, allocation-free). */
    public void publish(ByteBuffer src) {
        int size = src.remaining();
        OffHeapRingSupport.checkPayloadSize(size);
        int v0 = beginPublish();
        OffHeapRingSupport.setSizeRelease(buffer, BASE, size);
        OffHeapRingSupport.copyFromBuffer(buffer, BASE + OFF_DATA, src, size);
        endPublish(v0);
    }

    /**
     * Zero-copy publish: {@code writer} fills {@code size} bytes at {@code dataOffset}
     * while the slot is closed for readers. Never blocks, never fails.
     *
     * <p>Pass a non-capturing lambda, method reference, or shared instance to stay
     * allocation-free; a capturing lambda allocates per call.
     */
    public void publish(int size, DirectWriter writer) {
        OffHeapRingSupport.checkPayloadSize(size);
        int v0 = beginPublish();
        OffHeapRingSupport.setSizeRelease(buffer, BASE, size);
        writer.writeTo(buffer, BASE + OFF_DATA, size);
        endPublish(v0);
    }

    /** Marks the slot writing (odd); returns the pre-publish version. */
    private int beginPublish() {
        int v0 = OffHeapRingSupport.getVersionAcquire(buffer, BASE);
        INT_HANDLE.setRelease(buffer, BASE + OFF_VERSION, v0 + 1);
        return v0;
    }

    /** Marks the slot stable (even). */
    private void endPublish(int v0) {
        INT_HANDLE.setRelease(buffer, BASE + OFF_VERSION, v0 + 2);
    }

    /**
     * Single poll attempt: copies the latest value into {@code dst} iff a newer
     * complete publish exists since {@code cursor}'s last success.
     *
     * @return payload size, or -1 if no new data, the writer is mid-publish,
     *         or the copy was torn (cursor not advanced in all -1 cases).
     */
    public int poll(ConflatedCursor cursor, byte[] dst) {
        return poll(cursor, dst, 0);
    }

    public int poll(ConflatedCursor cursor, byte[] dst, int dstPos) {
        int v0 = OffHeapRingSupport.getVersionAcquire(buffer, BASE);
        if ((v0 & 1) == 1 || v0 == cursor.lastSeenVersion) {
            return -1;
        }
        int size = OffHeapRingSupport.getSizeAcquire(buffer, BASE);
        if (dstPos < 0 || size < 0 || dstPos + size > dst.length) {
            throw new IllegalArgumentException("dst too small for payload of " + size);
        }
        OffHeapRingSupport.copyTo(buffer, BASE + OFF_DATA, dst, dstPos, size);
        if (OffHeapRingSupport.getVersionAcquire(buffer, BASE) != v0) {
            return -1; // torn by a concurrent publish; retry later
        }
        cursor.lastSeenVersion = v0;
        return size;
    }

    public int poll(ConflatedCursor cursor, ByteBuffer dst) {
        int v0 = OffHeapRingSupport.getVersionAcquire(buffer, BASE);
        if ((v0 & 1) == 1 || v0 == cursor.lastSeenVersion) {
            return -1;
        }
        int size = OffHeapRingSupport.getSizeAcquire(buffer, BASE);
        if (dst.remaining() < size) {
            throw new IllegalArgumentException("dst too small for payload of " + size);
        }
        OffHeapRingSupport.copyToBuffer(buffer, BASE + OFF_DATA, dst, size);
        if (OffHeapRingSupport.getVersionAcquire(buffer, BASE) != v0) {
            return -1;
        }
        cursor.lastSeenVersion = v0;
        return size;
    }

    /**
     * Releases the direct memory. Call exactly once when the value is no
     * longer needed. Lifecycle is caller-owned: no operation may be performed
     * on this instance afterwards (use-after-close is undefined and may crash
     * the JVM — there is intentionally no per-operation open check, to keep
     * the hot path free of volatile reads and branches).
     */
    @Override
    public void close() {
        OffHeapRingSupport.free(rawOwner);
    }
}
