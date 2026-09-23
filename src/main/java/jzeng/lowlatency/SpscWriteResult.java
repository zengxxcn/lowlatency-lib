package jzeng.lowlatency;

/** Write outcome: {@code SUCCESS}, or {@code ERROR} when a read is in progress on the target slot. */
public enum SpscWriteResult {
    SUCCESS,
    ERROR
}
