package com.acuver.aekyam.slotpoc.core;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/** One fixed-size semaphore (lane, connector or service) plus counters to prove it was respected. */
public final class SlotPool {

    private final Level level;
    private final String name;
    private final int capacity;
    private final Semaphore semaphore;

    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger peakInFlight = new AtomicInteger();
    private final LongAdder rejected = new LongAdder();

    public SlotPool(Level level, String name, int capacity) {
        if (capacity < 1) throw new IllegalArgumentException(level + " " + name + ": slots must be >= 1");
        this.level = level;
        this.name = name;
        this.capacity = capacity;
        this.semaphore = new Semaphore(capacity);
    }

    /** Non-blocking: take a slot, or count a rejection at this level. */
    boolean tryAcquire() {
        if (!semaphore.tryAcquire()) {
            rejected.increment();
            return false;
        }
        peakInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
        return true;
    }

    /** Give the slot back (delivery finished, or a later level rejected). */
    void release() {
        inFlight.decrementAndGet();
        semaphore.release();
    }

    public Level level() { return level; }
    public String name() { return name; }
    public int capacity() { return capacity; }
    public int available() { return semaphore.availablePermits(); }
    public int inFlight() { return inFlight.get(); }
    public int peakInFlight() { return peakInFlight.get(); }
    public long rejected() { return rejected.sum(); }

    public Stats stats() {
        return new Stats(capacity, available(), inFlight(), peakInFlight(), rejected());
    }

    public record Stats(int capacity, int available, int inFlight, int peakInFlight, long rejected) {
    }
}
