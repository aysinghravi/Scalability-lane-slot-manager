package com.acuver.aekyam.slotpoc.core;

import java.util.concurrent.Semaphore;

/**
 * A {@link Semaphore} whose permit limit can change at runtime.
 *
 * <p>Growing releases the extra permits. Shrinking calls {@link #reducePermits}, which may take the
 * available count below zero: work already holding a permit is never interrupted, and no new permit is
 * handed out until enough of it has finished to bring the in-flight count under the new limit.
 */
final class ResizableSemaphore extends Semaphore {

    private volatile int limit;

    ResizableSemaphore(int limit) {
        super(limit);
        this.limit = limit;
    }

    synchronized void setLimit(int newLimit) {
        if (newLimit < 0) throw new IllegalArgumentException("limit must be >= 0");
        int delta = newLimit - limit;
        if (delta > 0) {
            release(delta);
        } else if (delta < 0) {
            reducePermits(-delta);
        }
        limit = newLimit;
    }

    int limit() {
        return limit;
    }
}
