package jzeng.lowlatency;

import java.nio.ByteBuffer;

import static jzeng.lowlatency.OffHeapRingSupport.INT_HANDLE;
import static jzeng.lowlatency.OffHeapRingSupport.OFF_DATA;
import static jzeng.lowlatency.OffHeapRingSupport.OFF_UNREAD;
import static jzeng.lowlatency.OffHeapRingSupport.OFF_VERSION;

/**
 * Single-producer / single-consumer exactly-once ring, fully off-heap.
 *
 * <p>Each slot carries an {@code unread} flag. The writer CASes {@code unread}
 * The writer CASes {@code unread} true→false to claim the slot; if the flag was
 * already false but the version is still odd, a read is in progress and the write
 * fails with {@link SpscWriteResult#ERROR}. The reader CASes {@code unread} to
 * consume each slot exactly once.
 */
public final class SpscOffHeapRing implements AutoCloseable {

    private final ByteBuffer buffer;
    private final int capacity;
    private final int maxPayload;
    private final int stride;
    private volatile boolean closed;

    public SpscOffHeapRing(int capacity) {
        this(capacity, OffHeapRingSupport.MAX_PAYLOAD);
    }

    public SpscOffHeapRing(int capacity, int maxPayload) {
        this.buffer = OffHeapRingSupport.allocate(capacity, maxPayload);
        this.capacity = capacity;
        this.maxPayload = maxPayload;
        this.stride = OffHeapRingSupport.slotStride(maxPayload);
    }

    public int capacity() {
        return capacity;
    }

    /** Maximum payload bytes per message for this ring (instance property). */
    public int maxPayload() {
        return maxPayload;
    }

    /** Convenience copy of a heap payload (allocation-free). */
    public SpscWriteResult write(byte[] payload) {
        ensureOpen();
        OffHeapRingSupport.checkPayloadSize(payload.length, maxPayload);
        long seq = claimSeqForWrite();
        if (seq < 0) {
            return SpscWriteResult.ERROR;
        }
        int base = OffHeapRingSupport.slotBase(seq, capacity, stride);
        OffHeapRingSupport.setSizeRelease(buffer, base, payload.length);
        OffHeapRingSupport.copyFrom(buffer, base + OFF_DATA, payload, 0, payload.length);
        publishSlot(base);
        return SpscWriteResult.SUCCESS;
    }

    /** Convenience copy from a {@link ByteBuffer} (consumes {@code remaining()} bytes, allocation-free). */
    public SpscWriteResult write(ByteBuffer src) {
        int size = src.remaining();
        OffHeapRingSupport.checkPayloadSize(size, maxPayload);
        ensureOpen();
        long seq = claimSeqForWrite();
        if (seq < 0) {
            return SpscWriteResult.ERROR;
        }
        int base = OffHeapRingSupport.slotBase(seq, capacity, stride);
        OffHeapRingSupport.setSizeRelease(buffer, base, size);
        OffHeapRingSupport.copyFromBuffer(buffer, base + OFF_DATA, src, size);
        publishSlot(base);
        return SpscWriteResult.SUCCESS;
    }

    /**
     * Zero-copy write with a {@link DirectWriter} callback.
     *
     * <p>Pass a non-capturing lambda, method reference, or shared instance to stay
     * allocation-free; a capturing lambda allocates per call.
     *
     * @return {@link SpscWriteResult#SUCCESS}, or {@link SpscWriteResult#ERROR} when a
     *         read is still in progress on the target slot.
     */
    public SpscWriteResult write(int size, DirectWriter writer) {
        ensureOpen();
        OffHeapRingSupport.checkPayloadSize(size, maxPayload);
        long seq = claimSeqForWrite();
        if (seq < 0) {
            return SpscWriteResult.ERROR;
        }
        int base = OffHeapRingSupport.slotBase(seq, capacity, stride);
        OffHeapRingSupport.setSizeRelease(buffer, base, size);
        try {
            writer.writeTo(buffer, base + OFF_DATA, size);
        } catch (RuntimeException | Error e) {
            abortSlot(base);
            throw e;
        }
        publishSlot(base);
        return SpscWriteResult.SUCCESS;
    }

    /**
     * Typed write mirroring the {@code DirectWriter} overload: claims the slot,
     * wraps {@code view} over it, translates, publishes.
     *
     * @return {@link SpscWriteResult#SUCCESS}, or {@link SpscWriteResult#ERROR} when a
     *         read is still in progress on the target slot.
     */
    public <E extends Flyweight> SpscWriteResult write(EventTranslator<E> translator, E view) {
        ensureOpen();
        OffHeapRingSupport.checkTypeFits(view, maxPayload);
        long seq = claimSeqForWrite();
        if (seq < 0) {
            return SpscWriteResult.ERROR;
        }
        int base = OffHeapRingSupport.slotBase(seq, capacity, stride);
        view.wrap(buffer, base + OFF_DATA, maxPayload);
        final int size;
        try {
            translator.translateTo(view, seq);
            size = OffHeapRingSupport.checkedEncodedSize(view, maxPayload);
        } catch (RuntimeException | Error e) {
            abortSlot(base);
            throw e;
        }
        OffHeapRingSupport.setSizeRelease(buffer, base, size);
        publishSlot(base);
        return SpscWriteResult.SUCCESS;
    }

    /**
     * Claims the next sequence (unread true→false on its slot); returns the
     * sequence, or -1 when a read is still in progress on the target slot
     * (version odd).
     */
    private long claimSeqForWrite() {
        long seq = OffHeapRingSupport.getAndAddSequence(buffer);
        int base = OffHeapRingSupport.slotBase(seq, capacity, stride);
        boolean claimed = (boolean) INT_HANDLE.compareAndSet(buffer, base + OFF_UNREAD, 1, 0);
        if (!claimed) {
            int version = OffHeapRingSupport.getVersionAcquire(buffer, base);
            if ((version & 1) == 1) {
                // Reader holds the slot (still odd) — must not overwrite.
                return -1;
            }
            // Version even: slot is free (never written, or already consumed).
        }
        return seq;
    }

    /** Marks the slot unread and publishes (even→odd) for the single reader. */
    private void publishSlot(int base) {
        INT_HANDLE.setRelease(buffer, base + OFF_UNREAD, 1);
        INT_HANDLE.getAndAdd(buffer, base + OFF_VERSION, 1);
    }

    /** Releases a claimed slot as unread without publishing (writer failed). */
    private void abortSlot(int base) {
        INT_HANDLE.setRelease(buffer, base + OFF_UNREAD, 0);
    }

    /**
     * Reads the slot at {@code blockIndex} (caller's cursor, wraps at capacity).
     *
     * @return payload size, or -1 on miss (not readable or already consumed).
     */
    public int read(long blockIndex, byte[] dst) {
        return read(blockIndex, dst, 0);
    }

    public int read(long blockIndex, byte[] dst, int dstPos) {
        ensureOpen();
        int base = OffHeapRingSupport.slotBase(blockIndex, capacity, stride);
        int version = OffHeapRingSupport.getVersionAcquire(buffer, base);
        if ((version & 1) == 0) {
            return -1;
        }
        boolean claimed = (boolean) INT_HANDLE.compareAndSet(buffer, base + OFF_UNREAD, 1, 0);
        if (!claimed) {
            return -1;
        }
        int size = OffHeapRingSupport.getSizeAcquire(buffer, base);
        if (dstPos < 0 || size < 0 || dstPos + size > dst.length) {
            INT_HANDLE.setRelease(buffer, base + OFF_UNREAD, 1);
            throw new IllegalArgumentException("dst too small for payload of " + size);
        }
        try {
            OffHeapRingSupport.copyTo(buffer, base + OFF_DATA, dst, dstPos, size);
        } catch (RuntimeException | Error e) {
            INT_HANDLE.setRelease(buffer, base + OFF_UNREAD, 1);
            throw e;
        }
        INT_HANDLE.setRelease(buffer, base + OFF_VERSION, version + 1);
        return size;
    }

    public int read(long blockIndex, ByteBuffer dst) {
        ensureOpen();
        int base = OffHeapRingSupport.slotBase(blockIndex, capacity, stride);
        int version = OffHeapRingSupport.getVersionAcquire(buffer, base);
        if ((version & 1) == 0) {
            return -1;
        }
        boolean claimed = (boolean) INT_HANDLE.compareAndSet(buffer, base + OFF_UNREAD, 1, 0);
        if (!claimed) {
            return -1;
        }
        int size = OffHeapRingSupport.getSizeAcquire(buffer, base);
        if (dst.remaining() < size) {
            INT_HANDLE.setRelease(buffer, base + OFF_UNREAD, 1);
            throw new IllegalArgumentException("dst too small for payload of " + size);
        }
        try {
            OffHeapRingSupport.copyToBuffer(buffer, base + OFF_DATA, dst, size);
        } catch (RuntimeException | Error e) {
            INT_HANDLE.setRelease(buffer, base + OFF_UNREAD, 1);
            throw e;
        }
        INT_HANDLE.setRelease(buffer, base + OFF_VERSION, version + 1);
        return size;
    }

    /**
     * Typed read: on hit wraps the caller-owned {@code reuse} over the slot,
     * consumes it exactly once, and returns the size; on miss returns -1 and
     * leaves {@code reuse} untouched. Zero-copy — no bytes moved, no objects created.
     *
     * <p>The wrapped view is valid only until the producer overwrites the slot.
     */
    public <E extends Flyweight> int read(long blockIndex, E reuse) {
        ensureOpen();
        int base = OffHeapRingSupport.slotBase(blockIndex, capacity, stride);
        int version = OffHeapRingSupport.getVersionAcquire(buffer, base);
        if ((version & 1) == 0) {
            return -1;
        }
        boolean claimed = (boolean) INT_HANDLE.compareAndSet(buffer, base + OFF_UNREAD, 1, 0);
        if (!claimed) {
            return -1;
        }
        int size = OffHeapRingSupport.getSizeAcquire(buffer, base);
        if (size < 0 || size > maxPayload) {
            INT_HANDLE.setRelease(buffer, base + OFF_UNREAD, 1);
            throw new IllegalStateException("corrupt slot size " + size);
        }
        reuse.wrap(buffer, base + OFF_DATA, size);
        INT_HANDLE.setRelease(buffer, base + OFF_VERSION, version + 1);
        return size;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("ring is closed");
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            OffHeapRingSupport.free(buffer);
        }
    }
}
