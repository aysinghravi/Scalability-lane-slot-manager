package com.acuver.aekyam.slotpoc.core;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * One semaphore (lane, connector or service) plus counters to prove it was respected.
 *
 * <p>Connector and service pools stay at their onboarded size. A lane pool is resized by the planner;
 * for a lane, {@link #capacity()} is its current planned target, and {@code inFlight} can sit above it
 * for a while after a shrink, until running deliveries finish.
 */
public final class SlotPool {

    private final Level level;
    private final String name;
    private final ResizableSemaphore semaphore;

    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger peakInFlight = new AtomicInteger();
    private final LongAdder rejected = new LongAdder();
    /** Accepted plus rejected since the planner last read it: the lane's arrivals in that window. */
    private final LongAdder attempts = new LongAdder();

    public SlotPool(Level level, String name, int capacity) {
        if (capacity < 0) throw new IllegalArgumentException(level + " " + name + ": slots must be >= 0");
        this.level = level;
        this.name = name;
        this.semaphore = new ResizableSemaphore(capacity);
    }

    /** Non-blocking: take a slot, or count a rejection at this level. */
    boolean tryAcquire() {
        attempts.increment();
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

    /** Planner only: change the slot limit. Running deliveries are never interrupted by a shrink. */
    void setCapacity(int slots) {
        semaphore.setLimit(slots);
    }

    /** Planner only: arrivals since the previous call, then reset. */
    long drainAttempts() {
        return attempts.sumThenReset();
    }

    public Level level() { return level; }
    public String name() { return name; }
    public int capacity() { return semaphore.limit(); }
    /** Free slots right now; 0 while a shrunk lane is still draining. */
    public int available() { return Math.max(0, semaphore.availablePermits()); }
    public int inFlight() { return inFlight.get(); }
    public int peakInFlight() { return peakInFlight.get(); }
    public long rejected() { return rejected.sum(); }

    public Stats stats() {
        return new Stats(capacity(), available(), inFlight(), peakInFlight(), rejected());
    }

    public record Stats(int capacity, int available, int inFlight, int peakInFlight, long rejected) {
    }
}
