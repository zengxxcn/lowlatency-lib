package jzeng.lowlatency;

import java.nio.ByteBuffer;

/**
 * Test flyweight: fixed 20B order event (long orderId @0, int qty @8,
 * long price @12). Test-scoped example of a fixed-size {@link Flyweight}.
 */
final class TestOrderEvent implements Flyweight {

    static final int ENCODED = 20;

    private ByteBuffer buffer;
    private int base;

    @Override
    public void wrap(ByteBuffer buffer, int offset, int length) {
        this.buffer = buffer;
        this.base = offset;
    }

    @Override
    public int maxEncodedLength() {
        return ENCODED;
    }

    @Override
    public int encodedLength() {
        return ENCODED;
    }

    void set(long orderId, int qty, long price) {
        buffer.putLong(base, orderId);
        buffer.putInt(base + 8, qty);
        buffer.putLong(base + 12, price);
    }

    long orderId() {
        return buffer.getLong(base);
    }

    int qty() {
        return buffer.getInt(base + 8);
    }

    long price() {
        return buffer.getLong(base + 12);
    }
}
