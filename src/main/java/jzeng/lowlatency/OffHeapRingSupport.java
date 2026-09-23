package jzeng.lowlatency;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Shared off-heap layout and atomic accessors.
 *
 * <p>Single {@code allocateDirect} region:
 * <pre>
 * HEADER (64 B): seq long @0, pad 8..63
 * SLOT s (128 B): base = 64 + s*128
 *   +0  version int (even = writing/empty, odd = readable)
 *   +4  size    int (0..64)
 *   +8  unread  int (SPSC only, 0/1; SPMC reserved)
 *   +12..63     padding
 *   +64..127    payload (64 B)
 * </pre>
 *
 * <p>Memory-ordering map: {@code getAcquire} for acquire-loads,
 * {@code setRelease} for release-stores, {@code getAndAdd} (volatile, stronger —
 * documented) for the sequence counter, {@code compareAndSet} for claim CAS.
 */
final class OffHeapRingSupport {

    static final int MAX_PAYLOAD = 64;
    static final int HEADER_SIZE = 64;
    static final int SLOT_STRIDE = 128;

    static final int OFF_VERSION = 0;
    static final int OFF_SIZE = 4;
    static final int OFF_UNREAD = 8;
    static final int OFF_DATA = 64;

    static final VarHandle INT_HANDLE =
            MethodHandles.byteBufferViewVarHandle(int[].class, ByteOrder.nativeOrder());
    static final VarHandle LONG_HANDLE =
            MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.nativeOrder());

    private OffHeapRingSupport() {
    }

    static ByteBuffer allocate(int capacity) {
        return allocate(capacity, MAX_PAYLOAD);
    }

    static ByteBuffer allocate(int capacity, int maxPayload) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0, got " + capacity);
        }
        if (maxPayload <= 0) {
            throw new IllegalArgumentException("maxPayload must be > 0, got " + maxPayload);
        }
        long stride = slotStride(maxPayload);
        long total = (long) HEADER_SIZE + (long) capacity * stride;
        if (total > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("ring too large: " + total + " bytes");
        }
        ByteBuffer buf = ByteBuffer.allocateDirect((int) total);
        // Zero-init is guaranteed by allocateDirect.
        return buf;
    }

    /** Slot stride for a max payload: 64B header region + payload, 8-aligned. */
    static int slotStride(int maxPayload) {
        return (OFF_DATA + maxPayload + 7) & ~7;
    }

    /** Absolute slot base for a (possibly wrapped) producer/consumer sequence. */
    static int slotBase(long sequence, int capacity) {
        return slotBase(sequence, capacity, SLOT_STRIDE);
    }

    static int slotBase(long sequence, int capacity, int stride) {
        long slot = Long.remainderUnsigned(sequence, capacity);
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
