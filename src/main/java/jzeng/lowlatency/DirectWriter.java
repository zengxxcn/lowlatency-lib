package jzeng.lowlatency;

import java.nio.ByteBuffer;

/**
 * Zero-copy write callback: invoked while the target slot is closed for readers,
 * so the implementation may copy directly into the off-heap buffer at {@code dataOffset}.
 */
@FunctionalInterface
public interface DirectWriter {
    void writeTo(ByteBuffer buffer, int dataOffset, int size);
}
