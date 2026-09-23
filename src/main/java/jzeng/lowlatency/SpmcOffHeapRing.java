package jzeng.lowlatency;

import java.nio.ByteBuffer;

import static jzeng.lowlatency.OffHeapRingSupport.INT_HANDLE;
import static jzeng.lowlatency.OffHeapRingSupport.OFF_DATA;
import static jzeng.lowlatency.OffHeapRingSupport.OFF_SIZE;
import static jzeng.lowlatency.OffHeapRingSupport.OFF_VERSION;

/**
 * Single-producer / multiple-consumer multicast ring, fully off-heap.
 *
 * <p>One producer appends via a monotonic sequence; every consumer keeps its own
 * every consumer keeps its own {@code blockIndex} cursor and observes every message
 * (multicast — not competing consumers). Per-slot version parity: even = writing/empty,
 * odd = readable; each read adds +2 so the slot stays readable for other consumers.
 */
public final class SpmcOffHeapRing implements AutoCloseable {

    private final ByteBuffer buffer;
    private final int capacity;
    private final int maxPayload;
    private final int stride;
    private volatile boolean closed;

    public SpmcOffHeapRing(int capacity) {
        this(capacity, OffHeapRingSupport.MAX_PAYLOAD);
    }

    public SpmcOffHeapRing(int capacity, int maxPayload) {
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

    /**
     * Next producer sequence (one past the last claimed). Acquire-load of the
     * sequence counter — the backlog signal for catch-up decisions:
     * {@code backlog = producerSequence() - cursor}. Costs one acquire-load;
     * call once per poll/drain batch, not once per message in tight loops.
     */
    public long producerSequence() {
        ensureOpen();
        return (long) OffHeapRingSupport.LONG_HANDLE.getAcquire(buffer, 0);
    }

    /**
     * If {@code cursor} is more than a full ring behind the producer
     * (backlog > capacity — some slots already overwritten),
     * advance it to the oldest live sequence ({@code producerSequence() - capacity}).
     * Otherwise returns {@code cursor} unchanged, preserving every-message delivery.
     */
    public long clampToOldestAlive(long cursor) {
        ensureOpen();
        long oldestAlive = producerSequence() - capacity;
        return cursor < oldestAlive ? oldestAlive : cursor;
    }

    /**
     * Gap report: how many messages at/after {@code cursor} were overwritten
     * before they could be read. Zero when the cursor is still live
     * ({@code backlog <= capacity}). Consistent with the clamp:
     * {@code messagesLost(c) == clampToOldestAlive(c) - c}. One acquire-load;
     * call once per poll/drain batch alongside the clamp, not per message.
     */
    public long messagesLost(long cursor) {
        ensureOpen();
        return Math.max(0, producerSequence() - capacity - cursor);
    }

    /**
     * Jump to the newest settled message when {@code backlog > maxLag}.
     * Target is {@code producerSequence() - 2}: with a single producer that slot's
     * {@code write()} completed before the current claim, so it is fully published
     * and the following read hits without spinning. Returns {@code cursor}
     * unchanged when within lag, or if the target is not ahead of the cursor.
     *
     * @throws IllegalArgumentException if maxLag is negative
     */
    public long jumpToNewest(long cursor, int maxLag) {
        if (maxLag < 0) {
            throw new IllegalArgumentException("maxLag must be >= 0, got " + maxLag);
        }
        ensureOpen();
        long prod = producerSequence();
        if (prod - cursor > maxLag) {
            return Math.max(cursor, prod - 2);
        }
        return cursor;
    }

    /** Convenience copy of a heap payload (allocation-free). */
    public void write(byte[] payload) {
        ensureOpen();
        OffHeapRingSupport.checkPayloadSize(payload.length, maxPayload);
        int base = nextSlot();
        int publish = closeSlot(base);
        OffHeapRingSupport.setSizeRelease(buffer, base, payload.length);
        OffHeapRingSupport.copyFrom(buffer, base + OFF_DATA, payload, 0, payload.length);
        // Publish (odd) — readable by all consumers.
        INT_HANDLE.setRelease(buffer, base + OFF_VERSION, publish);
    }

    /** Convenience copy from a {@link ByteBuffer} (consumes {@code remaining()} bytes, allocation-free). */
    public void write(ByteBuffer src) {
        int size = src.remaining();
        OffHeapRingSupport.checkPayloadSize(size, maxPayload);
        ensureOpen();
        int base = nextSlot();
        int publish = closeSlot(base);
        OffHeapRingSupport.setSizeRelease(buffer, base, size);
        OffHeapRingSupport.copyFromBuffer(buffer, base + OFF_DATA, src, size);
        INT_HANDLE.setRelease(buffer, base + OFF_VERSION, publish);
    }

    /**
     * Zero-copy write: {@code writer} fills {@code size} bytes at {@code dataOffset}
     * while the slot is closed for readers.
     *
     * <p>Pass a non-capturing lambda, method reference, or shared instance to stay
     * allocation-free; a capturing lambda allocates per call.
     */
    public void write(int size, DirectWriter writer) {
        ensureOpen();
        OffHeapRingSupport.checkPayloadSize(size, maxPayload);
        int base = nextSlot();
        int publish = closeSlot(base);
        OffHeapRingSupport.setSizeRelease(buffer, base, size);
        writer.writeTo(buffer, base + OFF_DATA, size);
        // Publish (odd) — readable by all consumers.
        INT_HANDLE.setRelease(buffer, base + OFF_VERSION, publish);
    }

    /**
     * Typed write: {@code translator} fills the slot-bound {@code view} in place.
     * Disruptor-style publish without the on-heap event array — the slot region
     * is the preallocated event, the caller's reusable view lends it a type.
     *
     * <p>Fit is enforced per write: the type's {@code maxEncodedLength()} must fit
     * this ring, and the message's {@code encodedLength()} must fit both.
     *
     * <p>Pass a non-capturing lambda, method reference, or shared instance to stay
     * allocation-free; a capturing lambda allocates per call.
     */
    public <E extends Flyweight> void write(EventTranslator<E> translator, E view) {
        ensureOpen();
        OffHeapRingSupport.checkTypeFits(view, maxPayload);
        long seq = OffHeapRingSupport.getAndAddSequence(buffer);
        int base = OffHeapRingSupport.slotBase(seq, capacity, stride);
        int publish = closeSlot(base);
        view.wrap(buffer, base + OFF_DATA, maxPayload);
        translator.translateTo(view, seq);
        int size = OffHeapRingSupport.checkedEncodedSize(view, maxPayload);
        OffHeapRingSupport.setSizeRelease(buffer, base, size);
        // Publish (odd) — readable by all consumers.
        INT_HANDLE.setRelease(buffer, base + OFF_VERSION, publish);
    }

    /** Advances the producer sequence and returns the target slot base. */
    private int nextSlot() {
        long seq = OffHeapRingSupport.getAndAddSequence(buffer);
        return OffHeapRingSupport.slotBase(seq, capacity, stride);
    }

    /**
     * Closes a readable slot for writing (single acquire read, no double-load
     * TOCTOU); returns the version to publish with.
     */
    private int closeSlot(int base) {
        int observed = OffHeapRingSupport.getVersionAcquire(buffer, base);
        int publish = observed + 1;
        if ((observed & 1) == 1) {
            OffHeapRingSupport.setVersionRelease(buffer, base, publish);
            publish++;
        }
        return publish;
    }

    /**
     * Reads the slot at {@code blockIndex} (caller's own cursor, wraps at capacity).
     *
     * @return payload size, or -1 on miss (version even).
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
        int size = OffHeapRingSupport.getSizeAcquire(buffer, base);
        if (dstPos < 0 || size < 0 || dstPos + size > dst.length) {
            throw new IllegalArgumentException("dst too small for payload of " + size);
        }
        OffHeapRingSupport.copyTo(buffer, base + OFF_DATA, dst, dstPos, size);
        // +2: slot stays readable for the other multicast consumers.
        INT_HANDLE.setRelease(buffer, base + OFF_VERSION, version + 2);
        return size;
    }

    public int read(long blockIndex, ByteBuffer dst) {
        ensureOpen();
        int base = OffHeapRingSupport.slotBase(blockIndex, capacity, stride);
        int version = OffHeapRingSupport.getVersionAcquire(buffer, base);
        if ((version & 1) == 0) {
            return -1;
        }
        int size = OffHeapRingSupport.getSizeAcquire(buffer, base);
        if (dst.remaining() < size) {
            throw new IllegalArgumentException("dst too small for payload of " + size);
        }
        OffHeapRingSupport.copyToBuffer(buffer, base + OFF_DATA, dst, size);
        INT_HANDLE.setRelease(buffer, base + OFF_VERSION, version + 2);
        return size;
    }

    /**
     * Typed read: on hit wraps the caller-owned {@code reuse} over the slot and
     * returns the size; on miss returns -1 and leaves {@code reuse} untouched.
     * Zero-copy — no bytes moved, no objects created.
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
        int size = OffHeapRingSupport.getSizeAcquire(buffer, base);
        if (size < 0 || size > maxPayload) {
            throw new IllegalStateException("corrupt slot size " + size);
        }
        reuse.wrap(buffer, base + OFF_DATA, size);
        // +2: slot stays readable for the other multicast consumers.
        INT_HANDLE.setRelease(buffer, base + OFF_VERSION, version + 2);
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

    /** For tests: current published sequence (next write index). */
    long nextSequence() {
        return producerSequence();
    }
}
