package jzeng.lowlatency;

/** Write outcome: {@code SUCCESS}, or {@code ERROR} when the target slot has not been released by the consumer (ring full — backpressure, nothing was written). */
public enum SpscWriteResult {
    SUCCESS,
    ERROR
}
