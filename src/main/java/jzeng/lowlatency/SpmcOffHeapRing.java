package jzeng.lowlatency;

import java.nio.ByteBuffer;

import static jzeng.lowlatency.OffHeapRingSupport.INT_HANDLE;
import static jzeng.lowlatency.OffHeapRingSupport.OFF_DATA;
import static jzeng.lowlatency.OffHeapRingSupport.OFF_VERSION;

/**
 * Single-producer / multiple-consumer multicast ring, fully off-heap.
 *
 * <p>One producer appends via a monotonic sequence; every consumer keeps its own
 * every consumer keeps its own {@code blockIndex} cursor and observes every message
 * (multicast — not competing consumers). Seqlock versioning per slot: the producer
 * brackets each write with begin (odd = writing) and end (even = published)
 * stores, so the published version rises by 2 every lap. A cursor {@code k}
 * therefore expects exactly version {@code 2 * (k / capacity + 1)}: a lower
 * version means the slot isn't published for that generation yet (consumer
 * ahead — miss), a higher one means the consumer was lapped (returns
 * {@link #GAP_LAPPED}, never a silent skip — clamp with
 * {@link #clampToOldestAlive} and re-read). Reads are pure loads — no stores — and re-read the version
 * after the copy so an overlapping write degrades to a miss, never a tear.
 *
 * <p>Lifecycle is caller-owned: {@link #close()} releases the direct memory and
 * must be called exactly once; no operation may follow it. There is
 * intentionally no per-operation open check.
 */
public final class SpmcOffHeapRing implements AutoCloseable {

    /**
     * Miss signal: writer active (odd version), not yet published for this
     * generation ({@code v0 < expected}), or torn by an overlapping write.
     * Retry the same cursor (possibly parking first).
     */
    public static final int MISS = -1;

    /**
     * Gap signal: the slot holds a newer generation than {@code blockIndex}
     * expects (producer lapped the consumer). Advance via
     * {@code clampToOldestAlive} and re-read; the overlapped generation
     * is never delivered as a skip.
     */
    public static final int GAP_LAPPED = -2;

    private final ByteBuffer buffer;
    private final ByteBuffer rawOwner;
    private final int capacity;
    private final int maxPayload;
    private final int stride;
    /**
     * log2(capacity), precomputed: capacity is always a power of 2
     * (enforced by {@code allocate}), so lap arithmetic is shifts, not division.
     */
    private final int slotShift;

    public SpmcOffHeapRing(int capacity) {
        this(capacity, OffHeapRingSupport.MAX_PAYLOAD);
    }

    public SpmcOffHeapRing(int capacity, int maxPayload) {
        OffHeapRingSupport.Region region = OffHeapRingSupport.allocate(capacity, maxPayload);
        this.buffer = region.slice;
        this.rawOwner = region.raw;
        this.capacity = capacity;
        this.maxPayload = maxPayload;
        this.stride = OffHeapRingSupport.slotStride(maxPayload);
        this.slotShift = Integer.numberOfTrailingZeros(capacity);
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
        return (long) OffHeapRingSupport.LONG_HANDLE.getAcquire(buffer, 0);
    }

    /**
     * If {@code cursor} is more than a full ring behind the producer
     * (backlog > capacity — some slots already overwritten),
     * advance it to the oldest live sequence ({@code producerSequence() - capacity}).
     * Otherwise returns {@code cursor} unchanged, preserving every-message delivery.
     */
    public long clampToOldestAlive(long cursor) {
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
        long prod = producerSequence();
        if (prod - cursor > maxLag) {
            return Math.max(cursor, prod - 2);
        }
        return cursor;
    }

    /** Convenience copy of a heap payload (allocation-free). */
    public void write(byte[] payload) {
        OffHeapRingSupport.checkPayloadSize(payload.length, maxPayload);
        long seq = OffHeapRingSupport.getAndAddSequence(buffer);
        int base = OffHeapRingSupport.slotBase(seq, capacity, stride);
        int begin = beginVersion(seq);
        // Begin (odd): readers miss while the payload lands.
        INT_HANDLE.setRelease(buffer, base + OFF_VERSION, begin);
        OffHeapRingSupport.setSizeRelease(buffer, base, payload.length);
        OffHeapRingSupport.copyFrom(buffer, base + OFF_DATA, payload, 0, payload.length);
        // End (even): published to all consumers.
        INT_HANDLE.setRelease(buffer, base + OFF_VERSION, begin + 1);
    }

    /** Convenience copy from a {@link ByteBuffer} (consumes {@code remaining()} bytes, allocation-free). */
    public void write(ByteBuffer src) {
        int size = src.remaining();
        OffHeapRingSupport.checkPayloadSize(size, maxPayload);
        long seq = OffHeapRingSupport.getAndAddSequence(buffer);
        int base = OffHeapRingSupport.slotBase(seq, capacity, stride);
        int begin = beginVersion(seq);
        INT_HANDLE.setRelease(buffer, base + OFF_VERSION, begin);
        OffHeapRingSupport.setSizeRelease(buffer, base, size);
        OffHeapRingSupport.copyFromBuffer(buffer, base + OFF_DATA, src, size);
        INT_HANDLE.setRelease(buffer, base + OFF_VERSION, begin + 1);
    }

    /**
     * Zero-copy write: {@code writer} fills {@code size} bytes at {@code dataOffset}
     * while the slot is closed for readers.
     *
     * <p>Pass a non-capturing lambda, method reference, or shared instance to stay
     * allocation-free; a capturing lambda allocates per call.
     */
    public void write(int size, DirectWriter writer) {
        OffHeapRingSupport.checkPayloadSize(size, maxPayload);
        long seq = OffHeapRingSupport.getAndAddSequence(buffer);
        int base = OffHeapRingSupport.slotBase(seq, capacity, stride);
        int begin = beginVersion(seq);
        INT_HANDLE.setRelease(buffer, base + OFF_VERSION, begin);
        OffHeapRingSupport.setSizeRelease(buffer, base, size);
        writer.writeTo(buffer, base + OFF_DATA, size);
        // End (even): published to all consumers.
        INT_HANDLE.setRelease(buffer, base + OFF_VERSION, begin + 1);
    }

    /**
     * Seqlock begin version for sequence {@code seq}, computed — no version
     * load needed. Lap {@code L = seq / capacity} publishes {@code 2L+2};
     * begin parks the slot at {@code 2L+1} (odd = writing) while the payload
     * lands. Unconditional overwrite: a lapped reader detects the gap via
     * the generation check on read.
     */
    private int beginVersion(long seq) {
        return (int) (((seq >>> slotShift) << 1) + 1);
    }

    /**
     * Version cursor {@code blockIndex} expects: {@code 2 * (lap + 1)} where
     * {@code lap = blockIndex / capacity}. Version 0 (never published) is
     * always below expectation, so fresh slots miss.
     */
    private int expectedVersion(long blockIndex) {
        return (int) (((blockIndex >>> slotShift) + 1) << 1);
    }

    /**
     * Gap-vs-miss classification for an even {@code v0 != expected}.
     *
     * <p>Published versions cycle mod 2^32 (wrap after
     * {@code capacity * 2^30} messages), so a signed {@code v0 > expected}
     * misclassifies where the two straddle the wrap boundary: a lapped
     * reader would spin on MISS instead of clamping forward. The
     * subtraction is exact in two's complement and its sign is correct
     * for any realistic generation distance (serial-arithmetic comparison).
     */
    static int classifyGap(int v0, int expected) {
        return (v0 - expected) > 0 ? GAP_LAPPED : MISS;
    }

    /**
     * Reads the slot at {@code blockIndex} (caller's own cursor, wraps at capacity).
     *
     * @return payload size, {@link #MISS} on miss (writer active with odd version, not
     *         yet published for this generation with {@code v0} behind {@code expected}, or
     *         torn by an overlapping write — retry same cursor), or
     *         {@link #GAP_LAPPED} (-2) when lapped ({@code v0} ahead of
     *         {@code expected} mod 2^32).
     */
    public int read(long blockIndex, byte[] dst) {
        return read(blockIndex, dst, 0);
    }

    public int read(long blockIndex, byte[] dst, int dstPos) {
        int base = OffHeapRingSupport.slotBase(blockIndex, capacity, stride);
        int v0 = OffHeapRingSupport.getVersionAcquire(buffer, base);
        if ((v0 & 1) == 1) {
            return MISS;
        }
        int exp = expectedVersion(blockIndex);
        if (v0 != exp) {
            return classifyGap(v0, exp);
        }
        int size = OffHeapRingSupport.getSizeAcquire(buffer, base);
        if (dstPos < 0 || size < 0 || dstPos + size > dst.length) {
            throw new IllegalArgumentException("dst too small for payload of " + size);
        }
        OffHeapRingSupport.copyTo(buffer, base + OFF_DATA, dst, dstPos, size);
        if (OffHeapRingSupport.getVersionAcquire(buffer, base) != v0) {
            return MISS;
        }
        return size;
    }

    public int read(long blockIndex, ByteBuffer dst) {
        int base = OffHeapRingSupport.slotBase(blockIndex, capacity, stride);
        int v0 = OffHeapRingSupport.getVersionAcquire(buffer, base);
        if ((v0 & 1) == 1) {
            return MISS;
        }
        int exp = expectedVersion(blockIndex);
        if (v0 != exp) {
            return classifyGap(v0, exp);
        }
        int size = OffHeapRingSupport.getSizeAcquire(buffer, base);
        if (dst.remaining() < size) {
            throw new IllegalArgumentException("dst too small for payload of " + size);
        }
        OffHeapRingSupport.copyToBuffer(buffer, base + OFF_DATA, dst, size);
        if (OffHeapRingSupport.getVersionAcquire(buffer, base) != v0) {
            return MISS;
        }
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

    /** For tests: current published sequence (next write index). */
    long nextSequence() {
        return producerSequence();
    }
}
