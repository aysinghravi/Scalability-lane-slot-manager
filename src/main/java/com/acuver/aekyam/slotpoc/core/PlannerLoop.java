package com.acuver.aekyam.slotpoc.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Calls {@link SlotRegistry#rebalance()} on a fixed delay. One platform thread ({@code slot-planner}):
 * a periodic job, not request work, so it does not need a virtual thread.
 */
public final class PlannerLoop implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PlannerLoop.class);

    private final ScheduledExecutorService executor;

    public PlannerLoop(SlotRegistry registry, long intervalMs) {
        if (intervalMs < 1) throw new IllegalArgumentException("planner interval must be >= 1 ms");
        this.executor = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("slot-planner").daemon().factory());
        executor.scheduleWithFixedDelay(() -> {
            try {
                registry.rebalance();
            } catch (RuntimeException e) {
                log.error("Lane slot re-plan failed; keeping the previous targets", e);
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        log.info("Slot planner started: every {} ms", intervalMs);
    }

    @Override
    public void close() {
        executor.shutdownNow();
        log.info("Slot planner stopped");
    }
}
