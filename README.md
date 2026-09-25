# LowLatency Lib — off-heap lock-free data structures

Very fast lock-free structures using seqlock and per-slot versioning.

## Layout (fully off-heap)

One direct allocation: `64 B header + capacity × stride`, where
`stride = roundUp64(64 + maxPayload)` — 128 B at the default 64 B max payload
(wider when the ring is sized for a larger `Flyweight` type, e.g. 320 B stride
for the 256 B worst-case price ladder). The stride is always a multiple of 64
and the region base is 64-aligned, so adjacent slots never share a cache line
(no false sharing between the producer's slot and the consumer's neighbour).

```
HEADER (64 B): producer sequence long @0, pad 8..63
SLOT s (stride B), base = 64 + s*stride:
  +0  version int   (even = writing/empty, odd = readable)
  +4  size    int   (0..maxPayload)
  +8  reserved int (padding)
  +12..63     padding (cache-line isolation)
  +64..       payload (maxPayload B)
```

## Structures & guarantees

- **SPMC (`SpmcOffHeapRing`)** — multicast: every consumer keeps its own `blockIndex`
  cursor and observes *every* message.
  - Reads add `+2` to the version so the slot stays readable for the other consumers.
  - The writer is wait-free: one fetch-add plus stores, never reads consumer state
    (which is also why it can lap readers). Reads are wait-free too — acquire loads,
    a bounded copy, one release store, no CAS at all.
  - The writer never blocks — it laps and overwrites; slow readers detect the
    gap with `messagesLost` and catch up with `clampToOldestAlive` / `jumpToNewest`.
- **SPSC (`SpscOffHeapRing`)** — exactly-once with no atomics: strict version
  parity (producer touches only even slots, consumer only odd), `write` returns
  `SUCCESS` / `ERROR`.
  - Both sides are wait-free: acquire loads plus release stores, no CAS, no
    fetch-add (the producer sequence is a confined field) — contention resolves
    as `ERROR` / `-1`, never spinning.
  - `ERROR` when the target slot is still odd, i.e. backlog == capacity: the
    write is rejected *without consuming a sequence*, so nothing is lost and
    retrying the same message is safe. Exactly-once holds with no external
    flow control needed (though keeping the producer roughly paced avoids
    wasted ERROR spins).
- **Conflated (`ConflatedValue`)** — 1 producer × N consumers last-value register
  for when the producer is faster than the consumer and only the newest value matters.
  - Depth-1 seqlock slot (even = stable, odd = writer active): `publish`
    unconditionally overwrites, `poll(cursor, dst)` copies the latest complete value iff
    newer than the cursor's last seen (each consumer sees a strictly increasing
    subsequence; gaps allowed, duplicates/stale/torn reads impossible).
  - Wait-free both sides: `publish` never blocks, `poll` is a single attempt —
    torn by a concurrent publish reads as `-1`, retried by the caller, never spun internally.
- Payload size is per-ring: `new SpmcOffHeapRing(capacity)` defaults to 64 bytes max;
  `new SpmcOffHeapRing(capacity, maxPayload)` sizes up to the message type's worst case.
  `Flyweight` types declare `maxEncodedLength()` (sizes the ring) and `encodedLength()`
  (actual bytes per message, checked per write); oversize → `IllegalArgumentException`.
  Rings are `AutoCloseable`.

## Build / test / benchmark

Requires JDK 21+ and Maven 3.9+.

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
mvn test                                   # unit + stress tests
mvn package -DskipTests                    # build jar (JMH lives in src/jmh, test scope, not shipped)
CP="target/test-classes:target/classes:$(mvn -q dependency:build-classpath -Dmdep.outputFile=/dev/stdout)"
java -cp "$CP" jzeng.lowlatency.RingBenchmarks        # ring benchmarks (raw bytes)
java -cp "$CP" jzeng.lowlatency.LadderBenchmarks      # typed-ladder benchmarks
java -cp "$CP" jzeng.lowlatency.ConflatedBenchmarks   # conflation benchmarks
```

JMH groups cover 1P×1C, 1P×3C plus SPSC against
`ArrayBlockingQueue` and `ConcurrentLinkedQueue` baselines, plus
`conflated1p1c` / `conflated1p3c` against an `AtomicReference` baseline.
The ring suite also compares against LMAX Disruptor 4.0.0 (`disruptor1p1c` /
`disruptor1p3c`): same shape as the SPMC groups (single producer, multicast
consumers, 32 B payload, capacity 1024) using a manually-driven
single-producer `RingBuffer` with per-consumer gating sequences.
`LadderBenchmarks` repeats the same comparison with a typed variable-depth FX
price ladder (`TestPriceLadder`, 88..256 B actual, 256 B worst case, capacity
1024, rings sized `(1024, MAX_ENCODED)`): SPMC/SPSC via the generic
flyweight overloads, Disruptor via pre-wrapped per-slot views, and heap
snapshots for the blocking/linked baselines.

> **Machine profile (all tables below):** bare-metal AMD Ryzen 7 5800, 8 cores /
> 16 threads, 32 MiB L3 (256 KiB L1d+L1i / 4 MiB L2 per core), 16 GiB RAM,
> Ubuntu 24.04.4 LTS, kernel 6.17.0-35-generic, OpenJDK 21.0.12 (Ubuntu),
> Maven 3.9.11.

Latest ring numbers (ops/s, group totals):

| Benchmark | Group total | Consumers (total) | Producer |
|---|---|---|---|
| `disruptor1p1c` | 131.1M | 65.6M | 65.6M |
| `spmc1p1c` | 103.0M | 51.7M | 51.4M |
| `spsc1p1c` | 44.6M | 22.3M | 22.3M |
| `agronaSpsc1p1c` (Agrona `OneToOneRingBuffer`) | 60.5M | 30.3M | 30.3M |
| `disruptor1p3c` | 135.9M | 101.9M | 34.0M |
| `spmc1p3c` | 174.1M | 131.0M | 43.2M |
| `blocking1p3c` (ArrayBlockingQueue) | 37.0M | 18.5M | 18.5M |
| `clq1p3c` (ConcurrentLinkedQueue) | 5.7M (†) | 2.6M | 3.0M |

> (†) `clq1p3c` showed high run-to-run variance in this environment; treat as
> indicative only.

Latest ladder numbers (typed variable-depth payload; same protocol, 1 fork):

| Benchmark | Group total | Consumers (total) | Producer |
|---|---|---|---|
| `spmcLadder1p1c` | 130.1M | 99.0M | 31.1M |
| `spmcLadder1p3c` | 310.9M | 284.4M | 26.5M |
| `spscLadder1p1c` | 29.7M | 14.8M | 14.8M |
| `disruptorLadder1p1c` | 65.7M | 32.8M | 32.8M |
| `disruptorLadder1p3c` | 49.9M | 37.4M | 12.5M |
| `blockingLadder1p3c` (ArrayBlockingQueue) | 12.3M | 6.1M | 6.1M |
| `clqLadder1p3c` (ConcurrentLinkedQueue) | 14.1M | 7.0M | 7.0M |

Latest conflation numbers (`ConflatedBenchmarks`, 32 B payload; same protocol, 1 fork):

| Benchmark | Group total | Consumers (total) | Producer (publish rate) |
|---|---|---|---|
| `conflated1p1c` | 173.5M | 136.8M | 36.7M |
| `atomicref1p1c` (`AtomicReference`) | 65.5M | 30.4M | 35.1M |
| `volatile1p1c` (plain volatile holder) | 84.0M | 48.9M | 35.1M |
| `conflated1p3c` | 376.1M | 365.5M | 10.6M |
| `atomicref1p3c` (`AtomicReference`) | 88.3M | 67.1M | 21.1M |
| `volatile1p3c` (plain volatile holder) | 109.4M | 86.8M | 22.6M |

> **Harness note:** all consumers (and the SPSC/Disruptor backpressure paths) carry a
> ~10 ms starvation bail — at iteration end the producer may exit while a consumer waits
> for data that will never arrive, which would hang JMH's teardown barrier. A live
> producer delivers every ~30 ns, so the bail never triggers mid-iteration. The blocking
> baseline uses `poll(1 ms)` instead of `take()` for the same reason (identical fast path
> while items are available).

> **Fairness note (same as the original):** the SPMC ring is *multicast* — each message
> is counted once *per consumer* — while the baselines are competing-consumers (counted
> once total). Compare numbers relatively, as with the original histogram.
> Conflation consumer-side numbers count polls (mostly "no new data"); the publish rate
> is the comparable figure — 1P×1C publish rates are statistically identical (~35–37M/s
> across all three), since TLAB allocation makes the baselines' per-publish garbage
> nearly free with no GC pressure. Error bars are wide (1 fork × 3 iterations); the
> durable difference is functional — zero allocation and off-heap residence — which only
> materializes under sustained load where per-publish garbage forces GC pauses.

## API sketch

Producer and consumer live on different threads; each consumer owns its cursor
(a plain `long`) and the ring holds no per-consumer state.

```java
// ---- SPMC producer thread: never blocks, may lap slow readers ----
SpmcOffHeapRing ring = new SpmcOffHeapRing(1024);
ring.write("hello".getBytes());                 // convenience copy
ring.write(5, (buf, off, n) -> { ... });        // zero-copy callback
```

```java
// ---- SPMC consumer thread: check the producer line only on miss, never per message ----
byte[] dst = new byte[64];
long cursor = 0;
for (;;) {
    int n = ring.read(cursor, dst);
    if (n < 0) {                                  // producer idle, or we got lapped —
        long prod = ring.producerSequence();      // one cross-core load decides which
        long lost = prod - ring.capacity() - cursor; // == messagesLost(cursor)
        if (lost <= 0) { park(); continue; }      // idle: wait, retry same cursor
        dropped += lost;                          // lapped: count the skip...
        cursor = prod - ring.capacity();          // ...resume at oldest live (= clampToOldestAlive)
        continue;
    }
    onMessage(dst, n);
    cursor++;
}
```

```java
// ---- SPSC producer thread: ERROR = ring full, sequence unconsumed, retry same message ----
if (ring.write(payload) != SpscWriteResult.SUCCESS) { /* back off, retry */ }
```

```java
// ---- SPSC consumer thread: exactly-once, cursor advances only on hit ----
int n = ring.read(cursor, dst);                   // size, or -1 = not yet published
if (n < 0) { /* wait/park, retry same cursor */ }
else { onMessage(dst, n); cursor++; }
```

```java
// ---- ConflatedValue publisher thread: unconditional overwrite ----
v.publish(payload);
```

```java
// ---- ConflatedValue consumer thread: newest value only, gaps allowed ----
ConflatedValue.ConflatedCursor cursor = new ConflatedValue.ConflatedCursor();
int n = v.poll(cursor, dst);                      // size, or -1 if nothing newer
```
