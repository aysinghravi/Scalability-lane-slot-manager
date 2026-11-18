package com.acuver.aekyam.slotpoc.core;

/**
 * One lane (stage x destination): its slot policy, its resizable slot pool, and its measured demand.
 *
 * <p>Demand is estimated with Little's law, concurrency = arrival rate x latency, where arrivals are
 * every admission attempt in the window (accepted or rejected), so a lane turning work away still shows
 * the demand it is not getting. It is never below the lane's current in-flight count, and is smoothed
 * with an exponential moving average that rises fast and falls slowly: a burst is picked up within a
 * tick or two, but one quiet window does not take a busy lane's slots away.
 */
public final class Lane {

    private final String laneId;
    private final Connector connector;
    private final SlotPool pool;
    private final long latencyMs;
    private final int guaranteedSlots;
    private final int maxSlots;
    private final int weight;

    /** Planner thread only (guarded by the registry lock). */
    private double demandEwma;
    private volatile int demandSlots;
    private final java.util.concurrent.atomic.AtomicLong resizes = new java.util.concurrent.atomic.AtomicLong();

    Lane(String laneId, Connector connector, long latencyMs, int guaranteedSlots, int maxSlots, int weight) {
        this.laneId = laneId;
        this.connector = connector;
        this.latencyMs = latencyMs;
        this.guaranteedSlots = guaranteedSlots;
        this.maxSlots = maxSlots;
        this.weight = weight;
        this.pool = new SlotPool(Level.LANE, laneId, guaranteedSlots);
    }

    /**
     * Planner only: fold the last window's arrivals into the demand estimate.
     *
     * @param smoothingUp   weight of the newest window when demand is rising
     * @param smoothingDown weight of the newest window when demand is falling
     */
    int updateDemand(double windowSeconds, double smoothingUp, double smoothingDown) {
        long arrivals = pool.drainAttempts();
        double rate = windowSeconds > 0 ? arrivals / windowSeconds : 0;
        double now = Math.max(pool.inFlight(), rate * latencyMs / 1000.0);
        double alpha = now > demandEwma ? smoothingUp : smoothingDown;
        demandEwma = alpha * now + (1 - alpha) * demandEwma;
        if (demandEwma < 0.1) demandEwma = 0;
        demandSlots = (int) Math.min(Integer.MAX_VALUE, Math.ceil(demandEwma - 1e-6));
        return demandSlots;
    }

    /** Planner only: set a new slot target and count the resize. */
    void resize(int target) {
        pool.setCapacity(target);
        resizes.incrementAndGet();
    }

    public String laneId() { return laneId; }
    /** How many times the planner has changed this lane's target. */
    public long resizes() { return resizes.get(); }
    public Connector connector() { return connector; }
    public SlotPool pool() { return pool; }
    public long latencyMs() { return latencyMs; }
    public int guaranteedSlots() { return guaranteedSlots; }
    public int maxSlots() { return maxSlots; }
    public int weight() { return weight; }
    /** Slots this lane wanted at the last plan. */
    public int demandSlots() { return demandSlots; }
}
