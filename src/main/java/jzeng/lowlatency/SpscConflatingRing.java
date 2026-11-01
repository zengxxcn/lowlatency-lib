package jzeng.lowlatency;

import java.nio.ByteBuffer;

import static jzeng.lowlatency.OffHeapRingSupport.OFF_DATA;

/**
 * Single-producer / single-consumer conflating overwrite ring, fully off-heap.
 *
 * <p>One monotone {@code published} counter (the next sequence to publish) is
 * the whole protocol. The producer copies the payload into slot
 * {@code seq & (capacity - 1)}, release-stores the slot's packed size, then
 * release-stores {@code published = seq + 1}. The consumer never writes: it
 * acquire-loads {@code published}, reads the slot, then reloads it after the
 * copy to prove no overwrite could have raced the copy.
 *
 * <p>Unlike {@link SpscOffHeapRing} the producer never waits, and unlike
 * {@link SpmcOffHeapRing} there is no per-slot version handshake — the single
 * counter orders everything. A consumer that keeps up sees an ordered stream;
 * one that falls more than {@code maxLag} behind gets {@link #LAPPED} instead
 * of a torn or mislabelled message and resumes at
 * {@code producerSequence() - 1}, where the cursor jump is the loss signal.
 * The default {@code maxLag = capacity - 1} conflates only when the consumer
 * is about to be overwritten; {@code maxLag = 1} always yields the newest
 * message.
 *
 * <p>The memory-ordering argument assumes the same TSO/acquire-release model
 * as the other rings in this package: on x86 this is plain loads and stores;
 * weakly ordered CPUs would need a full fence in place of the release store.
 *
 * <p>Lifecycle is caller-owned: {@link #close()} releases the direct memory and
 * must be called exactly once; no operation may follow it. There is
 * intentionally no per-operation open check.
 */
public final class SpscConflatingRing implements AutoCloseable {

    /** Nothing new at the cursor (not yet published). */
    public static final int MISS = -1;

    /** The cursor is too far behind; resume at {@code producerSequence() - 1}. */
    public static final int LAPPED = -2;

    private final ByteBuffer buffer;
    private final ByteBuffer rawOwner;
    private final int capacity;
    private final int maxPayload;
    private final int maxLag;
    private final int stride;
    /**
     * Next sequence to publish, producer-confined: a separate object so the
     * consumer's dereferences of the ring never share a line with a field the
     * producer mutates on every message (same reasoning as
     * {@link SpscOffHeapRing}).
     */
    private final ProducerSequence producer = new ProducerSequence();

    private static final class ProducerSequence {
        long next;
    }

    public SpscConflatingRing(int capacity) {
        this(capacity, OffHeapRingSupport.MAX_PAYLOAD);
    }

    public SpscConflatingRing(int capacity, int maxPayload) {
        this(capacity, maxPayload, capacity - 1);
    }

    public SpscConflatingRing(int capacity, int maxPayload, int maxLag) {
        if (capacity < 2 || (capacity & (capacity - 1)) != 0) {
            throw new IllegalArgumentException(
                    "capacity must be a power of 2 and >= 2, got " + capacity);
        }
        if (maxLag < 1 || maxLag >= capacity) {
            throw new IllegalArgumentException(
                    "maxLag must be 1..capacity-1, got " + maxLag);
        }
        OffHeapRingSupport.Region region = OffHeapRingSupport.allocate(capacity, maxPayload);
        this.buffer = region.slice;
        this.rawOwner = region.raw;
        this.capacity = capacity;
        this.maxPayload = maxPayload;
        this.maxLag = maxLag;
        this.stride = OffHeapRingSupport.slotStride(maxPayload);
    }

    public int capacity() {
        return capacity;
    }

    /** Maximum payload bytes per message for this ring (instance property). */
    public int maxPayload() {
        return maxPayload;
    }

    /** Maximum readable backlog; beyond this a read reports {@link #LAPPED}. */
    public int maxLag() {
        return maxLag;
    }

    /**
     * Next sequence to publish. The newest readable message is
     * {@code producerSequence() - 1}; the loss after a {@link #LAPPED} is the
     * cursor jump to that value.
     */
    public long producerSequence() {
        return OffHeapRingSupport.getSequenceAcquire(buffer);
    }

    /** Convenience copy of a heap payload (allocation-free). */
    public void write(byte[] payload) {
        OffHeapRingSupport.checkPayloadSize(payload.length, maxPayload);
        long seq = producer.next;
        int base = OffHeapRingSupport.slotBase(seq, capacity, stride);
        OffHeapRingSupport.copyFrom(buffer, base + OFF_DATA, payload, 0, payload.length);
        // Size first, then the counter: the release store of `published` is
        // the single publication point for both payload and size.
        OffHeapRingSupport.setPackedRelease(buffer, base, 0, payload.length);
        OffHeapRingSupport.setSequenceRelease(buffer, seq + 1);
        producer.next = seq + 1;
    }

    /** Convenience copy from a {@link ByteBuffer} (consumes {@code remaining()} bytes, allocation-free). */
    public void write(ByteBuffer src) {
        int size = src.remaining();
        OffHeapRingSupport.checkPayloadSize(size, maxPayload);
        long seq = producer.next;
        int base = OffHeapRingSupport.slotBase(seq, capacity, stride);
        OffHeapRingSupport.copyFromBuffer(buffer, base + OFF_DATA, src, size);
        OffHeapRingSupport.setPackedRelease(buffer, base, 0, size);
        OffHeapRingSupport.setSequenceRelease(buffer, seq + 1);
        producer.next = seq + 1;
    }

    /**
     * Zero-copy write with a {@link DirectWriter} callback.
     *
     * <p>Pass a non-capturing lambda, method reference, or shared instance to
     * stay allocation-free; a capturing lambda allocates per call.
     */
    public void write(int size, DirectWriter writer) {
        OffHeapRingSupport.checkPayloadSize(size, maxPayload);
        long seq = producer.next;
        int base = OffHeapRingSupport.slotBase(seq, capacity, stride);
        writer.writeTo(buffer, base + OFF_DATA, size);
        OffHeapRingSupport.setPackedRelease(buffer, base, 0, size);
        OffHeapRingSupport.setSequenceRelease(buffer, seq + 1);
        producer.next = seq + 1;
    }

    /**
     * Typed write: claims the slot, wraps {@code view} over it, translates,
     * publishes. A translator failure propagates with the counter unadvanced,
     * so the half-written slot is unreachable: the cursor at {@code seq} still
     * misses ({@code published == seq}) and every older live cursor is at least
     * {@code capacity} behind, hence {@link #LAPPED}.
     */
    public <E extends Flyweight> void write(EventTranslator<E> translator, E view) {
        OffHeapRingSupport.checkTypeFits(view, maxPayload);
        long seq = producer.next;
        int base = OffHeapRingSupport.slotBase(seq, capacity, stride);
        view.wrap(buffer, base + OFF_DATA, maxPayload);
        translator.translateTo(view, seq);
        int size = OffHeapRingSupport.checkedEncodedSize(view, maxPayload);
        OffHeapRingSupport.setPackedRelease(buffer, base, 0, size);
        OffHeapRingSupport.setSequenceRelease(buffer, seq + 1);
        producer.next = seq + 1;
    }

    /**
     * Reads the slot at cursor {@code blockIndex} without advancing anything;
     * re-reading a still-live cursor is allowed.
     *
     * @return payload size, {@link #MISS} when nothing newer is published, or
     *         {@link #LAPPED} when the cursor is too far behind (jump to
     *         {@code producerSequence() - 1}). {@code dst} is unspecified on
     *         {@link #LAPPED}.
     */
    public int read(long blockIndex, byte[] dst) {
        return read(blockIndex, dst, 0);
    }

    public int read(long blockIndex, byte[] dst, int dstPos) {
        long published = OffHeapRingSupport.getSequenceAcquire(buffer);
        if (published <= blockIndex) {
            return MISS;
        }
        if (published - blockIndex > maxLag) {
            return LAPPED;
        }
        int base = OffHeapRingSupport.slotBase(blockIndex, capacity, stride);
        int size = OffHeapRingSupport.sizeOf(OffHeapRingSupport.getPackedAcquire(buffer, base));
        if (size < 0 || size > maxPayload) {
            return LAPPED;
        }
        if (dstPos < 0 || dstPos + size > dst.length) {
            throw new IllegalArgumentException("dst too small for payload of " + size);
        }
        OffHeapRingSupport.copyTo(buffer, base + OFF_DATA, dst, dstPos, size);
        // The producer may only start reusing this slot once it has published
        // `blockIndex + capacity`; if the counter has reached that, the copy
        // could have raced an overwrite.
        if (OffHeapRingSupport.getSequenceAcquire(buffer) - blockIndex >= capacity) {
            return LAPPED;
        }
        return size;
    }

    public int read(long blockIndex, ByteBuffer dst) {
        long published = OffHeapRingSupport.getSequenceAcquire(buffer);
        if (published <= blockIndex) {
            return MISS;
        }
        if (published - blockIndex > maxLag) {
            return LAPPED;
        }
        int base = OffHeapRingSupport.slotBase(blockIndex, capacity, stride);
        int size = OffHeapRingSupport.sizeOf(OffHeapRingSupport.getPackedAcquire(buffer, base));
        if (size < 0 || size > maxPayload) {
            return LAPPED;
        }
        if (dst.remaining() < size) {
            throw new IllegalArgumentException("dst too small for payload of " + size);
        }
        OffHeapRingSupport.copyToBuffer(buffer, base + OFF_DATA, dst, size);
        if (OffHeapRingSupport.getSequenceAcquire(buffer) - blockIndex >= capacity) {
            return LAPPED;
        }
        return size;
    }

    /**
     * Typed read: copies the slot payload into {@code reuse}'s own
     * {@link Flyweight#snapshotBuffer()}, revalidates the counter, then wraps
     * {@code reuse} over that frozen buffer. The snapshot cannot tear against
     * an overwriting producer, and the view does not alias a slot the ring may
     * reuse. Returns {@link #MISS} or {@link #LAPPED} with {@code reuse} left
     * untouched; the view is valid until the next typed read on the same
     * instance.
     */
    public <E extends Flyweight> int read(long blockIndex, E reuse) {
        long published = OffHeapRingSupport.getSequenceAcquire(buffer);
        if (published <= blockIndex) {
            return MISS;
        }
        if (published - blockIndex > maxLag) {
            return LAPPED;
        }
        int base = OffHeapRingSupport.slotBase(blockIndex, capacity, stride);
        int size = OffHeapRingSupport.sizeOf(OffHeapRingSupport.getPackedAcquire(buffer, base));
        if (size < 0 || size > maxPayload) {
            return LAPPED;
        }
        ByteBuffer snap = reuse.snapshotBuffer();
        if (snap.capacity() < size) {
            throw new IllegalStateException("snapshot buffer holds " + snap.capacity()
                    + " bytes, payload is " + size + " (needs maxEncodedLength())");
        }
        OffHeapRingSupport.copyToBufferStart(buffer, base + OFF_DATA, snap, size);
        if (OffHeapRingSupport.getSequenceAcquire(buffer) - blockIndex >= capacity) {
            return LAPPED;
        }
        reuse.wrap(snap, 0, size);
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
