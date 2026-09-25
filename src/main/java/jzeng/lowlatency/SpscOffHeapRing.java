package jzeng.lowlatency;

import java.nio.ByteBuffer;

import static jzeng.lowlatency.OffHeapRingSupport.OFF_DATA;
import static jzeng.lowlatency.OffHeapRingSupport.OFF_VERSION;

/**
 * Single-producer / single-consumer exactly-once ring, fully off-heap.
 *
 * <p>No per-slot claim flag and no atomics on the hot path. Strict version
 * parity is the whole protocol: the producer touches only even slots and
 * publishes even→odd; the consumer touches only odd slots and releases
 * odd→even. A claim that observes an odd version means the consumer hasn't
 * released the slot (backlog == capacity), so the write fails with
 * {@link SpscWriteResult#ERROR} instead of overwriting — lossless
 * backpressure, no CAS, no fetch-add. The producer sequence is a plain
 * producer-confined field; exactly-once follows from single-reader cursor
 * order plus no-overwrite.
 *
 * <p>Lifecycle is caller-owned: {@link #close()} releases the direct memory and
 * must be called exactly once; no operation may follow it. There is
 * intentionally no per-operation open check.
 */
public final class SpscOffHeapRing implements AutoCloseable {

    private final ByteBuffer buffer;
    private final ByteBuffer rawOwner;
    private final int capacity;
    private final int maxPayload;
    private final int stride;
    /**
     * Next sequence to publish. Deliberately a separate object, not a field:
     * the consumer dereferences the ring (buffer/capacity/stride) on every
     * read, so a producer-mutated sequence on the ring would share its cache
     * line and bounce it every message. This holder is only ever touched by
     * the producer — its line stays producer-local.
     */
    private final ProducerSequence producer = new ProducerSequence();

    /** Producer-confined sequence (SPSC contract): no atomic needed. */
    private static final class ProducerSequence {
        long next;
    }

    public SpscOffHeapRing(int capacity) {
        this(capacity, OffHeapRingSupport.MAX_PAYLOAD);
    }

    public SpscOffHeapRing(int capacity, int maxPayload) {
        OffHeapRingSupport.Region region = OffHeapRingSupport.allocate(capacity, maxPayload);
        this.buffer = region.slice;
        this.rawOwner = region.raw;
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
        OffHeapRingSupport.checkPayloadSize(payload.length, maxPayload);
        // One slotBase + one version load per write: the observed even version
        // is carried in a local and published as version + 1.
        int base = OffHeapRingSupport.slotBase(producer.next, capacity, stride);
        int version = OffHeapRingSupport.getVersionAcquire(buffer, base);
        if ((version & 1) == 1) {
            return SpscWriteResult.ERROR;
        }
        OffHeapRingSupport.setSizeRelease(buffer, base, payload.length);
        OffHeapRingSupport.copyFrom(buffer, base + OFF_DATA, payload, 0, payload.length);
        OffHeapRingSupport.setVersionRelease(buffer, base, version + 1);
        producer.next++;
        return SpscWriteResult.SUCCESS;
    }

    /** Convenience copy from a {@link ByteBuffer} (consumes {@code remaining()} bytes, allocation-free). */
    public SpscWriteResult write(ByteBuffer src) {
        int size = src.remaining();
        OffHeapRingSupport.checkPayloadSize(size, maxPayload);
        int base = OffHeapRingSupport.slotBase(producer.next, capacity, stride);
        int version = OffHeapRingSupport.getVersionAcquire(buffer, base);
        if ((version & 1) == 1) {
            return SpscWriteResult.ERROR;
        }
        OffHeapRingSupport.setSizeRelease(buffer, base, size);
        OffHeapRingSupport.copyFromBuffer(buffer, base + OFF_DATA, src, size);
        OffHeapRingSupport.setVersionRelease(buffer, base, version + 1);
        producer.next++;
        return SpscWriteResult.SUCCESS;
    }

    /**
     * Zero-copy write with a {@link DirectWriter} callback.
     *
     * <p>Pass a non-capturing lambda, method reference, or shared instance to stay
     * allocation-free; a capturing lambda allocates per call.
     *
     * @return {@link SpscWriteResult#SUCCESS}, or {@link SpscWriteResult#ERROR} when the
     *         target slot has not been released by the consumer (ring full).
     */
    public SpscWriteResult write(int size, DirectWriter writer) {
        OffHeapRingSupport.checkPayloadSize(size, maxPayload);
        int base = OffHeapRingSupport.slotBase(producer.next, capacity, stride);
        int version = OffHeapRingSupport.getVersionAcquire(buffer, base);
        if ((version & 1) == 1) {
            return SpscWriteResult.ERROR;
        }
        OffHeapRingSupport.setSizeRelease(buffer, base, size);
        writer.writeTo(buffer, base + OFF_DATA, size);
        OffHeapRingSupport.setVersionRelease(buffer, base, version + 1);
        producer.next++;
        return SpscWriteResult.SUCCESS;
    }

    /**
     * Typed write mirroring the {@code DirectWriter} overload: claims the slot,
     * wraps {@code view} over it, translates, publishes.
     *
     * @return {@link SpscWriteResult#SUCCESS}, or {@link SpscWriteResult#ERROR} when the
     *         target slot has not been released by the consumer (ring full).
     */
    public <E extends Flyweight> SpscWriteResult write(EventTranslator<E> translator, E view) {
        OffHeapRingSupport.checkTypeFits(view, maxPayload);
        long seq = producer.next;
        int base = OffHeapRingSupport.slotBase(seq, capacity, stride);
        int version = OffHeapRingSupport.getVersionAcquire(buffer, base);
        if ((version & 1) == 1) {
            return SpscWriteResult.ERROR;
        }
        view.wrap(buffer, base + OFF_DATA, maxPayload);
        // Translator failure propagates with the version untouched: the slot
        // is still even (free) and `next` unadvanced, so no abort is needed.
        translator.translateTo(view, seq);
        int size = OffHeapRingSupport.checkedEncodedSize(view, maxPayload);
        OffHeapRingSupport.setSizeRelease(buffer, base, size);
        OffHeapRingSupport.setVersionRelease(buffer, base, version + 1);
        producer.next++;
        return SpscWriteResult.SUCCESS;
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
        int base = OffHeapRingSupport.slotBase(blockIndex, capacity, stride);
        int version = OffHeapRingSupport.getVersionAcquire(buffer, base);
        if ((version & 1) == 0) {
            return -1;
        }
        int size = OffHeapRingSupport.getSizeAcquire(buffer, base);
        if (dstPos < 0 || size < 0 || dstPos + size > dst.length) {
            // Slot stays odd (readable): caller can retry with a bigger dst.
            throw new IllegalArgumentException("dst too small for payload of " + size);
        }
        OffHeapRingSupport.copyTo(buffer, base + OFF_DATA, dst, dstPos, size);
        // Release (odd→even). No CAS: the producer never touches odd slots,
        // so this release is the only writer — the consumer owns the slot.
        OffHeapRingSupport.setVersionRelease(buffer, base, version + 1);
        return size;
    }

    public int read(long blockIndex, ByteBuffer dst) {
        int base = OffHeapRingSupport.slotBase(blockIndex, capacity, stride);
        int version = OffHeapRingSupport.getVersionAcquire(buffer, base);
        if ((version & 1) == 0) {
            return -1;
        }
        int size = OffHeapRingSupport.getSizeAcquire(buffer, base);
        if (dst.remaining() < size) {
            // Slot stays odd (readable): caller can retry with a bigger dst.
            throw new IllegalArgumentException("dst too small for payload of " + size);
        }
        OffHeapRingSupport.copyToBuffer(buffer, base + OFF_DATA, dst, size);
        OffHeapRingSupport.setVersionRelease(buffer, base, version + 1);
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
        int base = OffHeapRingSupport.slotBase(blockIndex, capacity, stride);
        int version = OffHeapRingSupport.getVersionAcquire(buffer, base);
        if ((version & 1) == 0) {
            return -1;
        }
        int size = OffHeapRingSupport.getSizeAcquire(buffer, base);
        if (size < 0 || size > maxPayload) {
            throw new IllegalStateException("corrupt slot size " + size);
        }
        reuse.wrap(buffer, base + OFF_DATA, size);
        OffHeapRingSupport.setVersionRelease(buffer, base, version + 1);
        return size;
    }

    /**
     * Releases the direct memory. Call exactly once when the ring is no
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
