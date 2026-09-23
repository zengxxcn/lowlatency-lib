package jzeng.lowlatency;

/**
 * Fills a slot-bound {@link Flyweight} view in place. Mirrors Disruptor's
 * {@code EventTranslator}: the ring lends the claimed slot through the
 * caller's reusable view, this callback populates it, the ring publishes.
 *
 * <p>Pass a non-capturing lambda, method reference, or shared instance to stay
 * allocation-free; a capturing lambda allocates per call.
 */
@FunctionalInterface
public interface EventTranslator<E extends Flyweight> {

    /** Populate {@code event} (already wrapped over the claimed slot). */
    void translateTo(E event, long sequence);
}
