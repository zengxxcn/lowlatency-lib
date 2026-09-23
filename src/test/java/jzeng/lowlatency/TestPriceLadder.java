package jzeng.lowlatency;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Test flyweight: variable-depth FX price ladder. SBE-style fixed-width layout:
 * <pre>
 *   int  depth    @0
 *   char symbol[16] @4   (ASCII, zero-padded)
 *   char market[8]  @20  (ASCII, zero-padded, e.g. venue)
 *   long eventId          @28
 *   long timestampNs      @36
 *   reserved              @44..63 (pad so the worst case is exactly 256 B)
 *   entries @64: depth x 24B (long bidPx, long askPx, long qty)
 * </pre>
 * Proves the {@code maxEncodedLength()} / {@code encodedLength()} split:
 * the type's worst case sizes the ring, each message occupies its actual depth.
 */
final class TestPriceLadder implements Flyweight {

    static final int MAX_DEPTH = 8;
    static final int SYMBOL_LEN = 16;
    static final int MARKET_LEN = 8;
    static final int HEADER = 64;
    static final int ENTRY = 24;
    static final int MAX_ENCODED = HEADER + MAX_DEPTH * ENTRY;

    private ByteBuffer buffer;
    private int base;
    private int depth;

    @Override
    public void wrap(ByteBuffer buffer, int offset, int length) {
        this.buffer = buffer;
        this.base = offset;
    }

    @Override
    public int maxEncodedLength() {
        return MAX_ENCODED;
    }

    @Override
    public int encodedLength() {
        return HEADER + depth * ENTRY;
    }

    void setSymbol(String symbol) {
        putAscii(base + 4, symbol, SYMBOL_LEN);
    }

    /** Zero-alloc symbol write from pre-encoded ASCII (benchmark hot path). */
    void setSymbolBytes(byte[] ascii, int len) {
        putBytes(base + 4, ascii, len, SYMBOL_LEN);
    }

    void setMarket(String market) {
        putAscii(base + 20, market, MARKET_LEN);
    }

    /** Zero-alloc market write from pre-encoded ASCII (benchmark hot path). */
    void setMarketBytes(byte[] ascii, int len) {
        putBytes(base + 20, ascii, len, MARKET_LEN);
    }

    void setDepth(int depth) {
        if (depth < 0 || depth > MAX_DEPTH) {
            throw new IllegalArgumentException("depth 0.." + MAX_DEPTH + ", got " + depth);
        }
        this.depth = depth;
        buffer.putInt(base, depth);
    }

    void setEventId(long eventId) {
        buffer.putLong(base + 28, eventId);
    }

    void setTimestampNs(long timestampNs) {
        buffer.putLong(base + 36, timestampNs);
    }

    void setLevel(int i, long bidPx, long askPx, long qty) {
        buffer.putLong(base + HEADER + i * ENTRY, bidPx);
        buffer.putLong(base + HEADER + i * ENTRY + 8, askPx);
        buffer.putLong(base + HEADER + i * ENTRY + 16, qty);
    }

    String symbol() {
        return getAscii(base + 4, SYMBOL_LEN);
    }

    String market() {
        return getAscii(base + 20, MARKET_LEN);
    }

    int depth() {
        return buffer.getInt(base);
    }

    long eventId() {
        return buffer.getLong(base + 28);
    }

    long timestampNs() {
        return buffer.getLong(base + 36);
    }

    long levelBid(int i) {
        return buffer.getLong(base + HEADER + i * ENTRY);
    }

    long levelAsk(int i) {
        return buffer.getLong(base + HEADER + i * ENTRY + 8);
    }

    long levelQty(int i) {
        return buffer.getLong(base + HEADER + i * ENTRY + 16);
    }

    private void putAscii(int at, String s, int width) {
        byte[] bytes = s.getBytes(StandardCharsets.US_ASCII);
        putBytes(at, bytes, bytes.length, width);
    }

    private void putBytes(int at, byte[] src, int len, int width) {
        if (len > width) {
            throw new IllegalArgumentException("value exceeds " + width + " ASCII chars");
        }
        for (int i = 0; i < width; i++) {
            buffer.put(at + i, i < len ? src[i] : (byte) 0);
        }
    }

    private String getAscii(int at, int width) {
        byte[] bytes = new byte[width];
        for (int i = 0; i < width; i++) {
            bytes[i] = buffer.get(at + i);
        }
        int len = 0;
        while (len < width && bytes[len] != 0) {
            len++;
        }
        return new String(bytes, 0, len, StandardCharsets.US_ASCII);
    }
}
