package jzeng.lowlatency;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Shared off-heap layout and atomic accessors.
 *
 * <p>Single {@code allocateDirect} region (base 64-aligned via an aligned slice):
 * <pre>
 * HEADER (64 B): seq long @0, pad 8..63
 * SLOT s (stride B, stride % 64 == 0): base = 64 + s*stride
 *   +0  version int (even = writing/empty, odd = readable)
 *   +4  size    int (0..maxPayload)
 *   +8  reserved int (padding; formerly the SPSC unread flag)
 *   +12..63     padding
 *   +64..       payload (maxPayload B, zero-padded to a 64B multiple)
 * </pre>
 *
 * <p>The stride is a multiple of 64 so adjacent slots never share a cache
 * line: the producer writing slot {@code s+1}'s header cannot invalidate a
 * line the consumer is reading for slot {@code s} (false sharing). Offsets
 * are relative to the aligned slice, so all hot fields stay line-aligned in
 * absolute terms too.
 *
 *
 * <p>Memory-ordering map: {@code getAcquire} for acquire-loads,
 * {@code setRelease} for release-stores, {@code getAndAdd} (volatile, stronger —
 * documented) for the SPMC sequence counter. SPSC uses a producer-confined
 * sequence field plus acquire/release version parity — no RMW anywhere.
 */
final class OffHeapRingSupport {

    static final int MAX_PAYLOAD = 64;
    static final int HEADER_SIZE = 64;
    static final int SLOT_STRIDE = 128;

    static final int OFF_VERSION = 0;
    static final int OFF_SIZE = 4;
    static final int OFF_DATA = 64;

    static final VarHandle INT_HANDLE =
            MethodHandles.byteBufferViewVarHandle(int[].class, ByteOrder.nativeOrder());
    static final VarHandle LONG_HANDLE =
            MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.nativeOrder());

    private OffHeapRingSupport() {
    }

    static Region allocate(int capacity, int maxPayload) {
        if (capacity <= 0 || (capacity & (capacity - 1)) != 0) {
            throw new IllegalArgumentException("capacity must be a power of 2, got " + capacity);
        }
        if (maxPayload <= 0) {
            throw new IllegalArgumentException("maxPayload must be > 0, got " + maxPayload);
        }
        int stride = slotStride(maxPayload);
        long total = (long) HEADER_SIZE + (long) capacity * stride;
        if (total > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("ring too large: " + total + " bytes");
        }
        // Over-allocate so the working slice can start on a 64B boundary
        // (allocateDirect only guarantees ~8B alignment). One-off cost at
        // construction; the hot path touches only the slice.
        ByteBuffer raw = ByteBuffer.allocateDirect((int) total + 63);
        int pad = 0;
        try {
            pad = alignPad(addressOf(raw));
        } catch (Exception ignored) {
            // Internals inaccessible: correct but possibly unaligned.
        }
        raw.limit(pad + (int) total);
        raw.position(pad);
        return new Region(raw.slice(), raw);
    }

    /**
     * An allocated ring region. {@code slice} is the 64-aligned working view
     * (all layout offsets are relative to it); {@code raw} is the
     * over-allocated owner — retain it for the ring's lifetime so the
     * cleaner cannot free the memory out from under the slice, and free it
     * (not the slice) on close.
     */
    static final class Region {
        final ByteBuffer slice;
        final ByteBuffer raw;

        Region(ByteBuffer slice, ByteBuffer raw) {
            this.slice = slice;
            this.raw = raw;
        }
    }

    /** Bytes to skip from {@code address} to reach a 64B boundary (0..63). */
    static int alignPad(long address) {
        return (int) ((64 - (address & 63)) & 63);
    }

    /** Absolute memory address of a direct buffer (construction-time use). */
    static long addressOf(ByteBuffer buf) {
        if (ADDRESS_OFFSET == -1L || UNSAFE == null) {
            throw new IllegalStateException("direct buffer address inaccessible");
        }
        return UNSAFE.getLong(buf, ADDRESS_OFFSET);
    }

    private static final sun.misc.Unsafe UNSAFE = loadUnsafe();
    private static final long ADDRESS_OFFSET = loadAddressOffset();

    private static sun.misc.Unsafe loadUnsafe() {
        try {
            var field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            return (sun.misc.Unsafe) field.get(null);
        } catch (Exception e) {
            return null;
        }
    }

    private static long loadAddressOffset() {
        try {
            if (UNSAFE == null) {
                return -1L;
            }
            return UNSAFE.objectFieldOffset(java.nio.Buffer.class.getDeclaredField("address"));
        } catch (Exception e) {
            return -1L;
        }
    }

    /**
     * Slot stride for a max payload: 64B header region + payload, rounded up
     * to a 64B (cache-line) multiple so adjacent slots never share a line.
     */
    static int slotStride(int maxPayload) {
        return (OFF_DATA + maxPayload + 63) & ~63;
    }

    /** Absolute slot base for a (possibly wrapped) producer/consumer sequence. */
    static int slotBase(long sequence, int capacity) {
        return slotBase(sequence, capacity, SLOT_STRIDE);
    }

    static int slotBase(long sequence, int capacity, int stride) {
        // Power-of-2 capacity (enforced in allocate): bitmask == unsigned
        // remainder for all sequences, 1 cycle instead of a 64-bit division.
        long slot = sequence & (capacity - 1L);
        long base = (long) HEADER_SIZE + slot * stride;
        return (int) base;
    }

    static int getVersionAcquire(ByteBuffer buf, int slotBase) {
        return (int) INT_HANDLE.getAcquire(buf, slotBase + OFF_VERSION);
    }

    static void setVersionRelease(ByteBuffer buf, int slotBase, int version) {
        INT_HANDLE.setRelease(buf, slotBase + OFF_VERSION, version);
    }

    static int getSizeAcquire(ByteBuffer buf, int slotBase) {
        return (int) INT_HANDLE.getAcquire(buf, slotBase + OFF_SIZE);
    }

    static void setSizeRelease(ByteBuffer buf, int slotBase, int size) {
        INT_HANDLE.setRelease(buf, slotBase + OFF_SIZE, size);
    }

    static long getAndAddSequence(ByteBuffer buf) {
        return (long) LONG_HANDLE.getAndAdd(buf, 0, 1L);
    }

    static void copyFrom(ByteBuffer buf, int dataOffset, byte[] src, int srcPos, int len) {
        for (int i = 0; i < len; i++) {
            buf.put(dataOffset + i, src[srcPos + i]);
        }
    }

    static void copyTo(ByteBuffer buf, int dataOffset, byte[] dst, int dstPos, int len) {
        for (int i = 0; i < len; i++) {
            dst[dstPos + i] = buf.get(dataOffset + i);
        }
    }

    static void copyFromBuffer(ByteBuffer buf, int dataOffset, ByteBuffer src, int len) {
        for (int i = 0; i < len; i++) {
            buf.put(dataOffset + i, src.get());
        }
    }

    static void copyToBuffer(ByteBuffer buf, int dataOffset, ByteBuffer dst, int len) {
        for (int i = 0; i < len; i++) {
            dst.put(buf.get(dataOffset + i));
        }
    }

    /** Best-effort direct-buffer free; falls back to GC if internals are inaccessible. */
    static void free(ByteBuffer buf) {
        try {
            var cleanerMethod = buf.getClass().getMethod("cleaner");
            cleanerMethod.setAccessible(true);
            Object cleaner = cleanerMethod.invoke(buf);
            if (cleaner != null) {
                var cleanMethod = cleaner.getClass().getMethod("clean");
                cleanMethod.setAccessible(true);
                cleanMethod.invoke(cleaner);
            }
        } catch (Exception ignored) {
            // GC will reclaim the direct buffer eventually.
        }
    }

    static void checkPayloadSize(int size) {
        checkPayloadSize(size, MAX_PAYLOAD);
    }

    static void checkPayloadSize(int size, int maxPayload) {
        if (size < 0 || size > maxPayload) {
            throw new IllegalArgumentException("payload size must be 0.." + maxPayload + ", got " + size);
        }
    }

    /**
     * Validates a flyweight type against a ring before claiming a slot.
     */
    static void checkTypeFits(Flyweight view, int maxPayload) {
        if (view.maxEncodedLength() > maxPayload) {
            throw new IllegalArgumentException(
                    "type max " + view.maxEncodedLength() + " exceeds ring maxPayload " + maxPayload);
        }
    }

    /**
     * Validates the post-translate actual length against the type max and the
     * ring cap. Returns the size to store.
     */
    static int checkedEncodedSize(Flyweight view, int maxPayload) {
        int size = view.encodedLength();
        if (size < 0 || size > view.maxEncodedLength() || size > maxPayload) {
            throw new IllegalArgumentException(
                    "encoded size " + size + " out of range (type max "
                            + view.maxEncodedLength() + ", ring max " + maxPayload + ")");
        }
        return size;
    }
}
