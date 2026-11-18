package com.acuver.aekyam.slotpoc.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds the service, connector and lane semaphores and admits deliveries against all three.
 *
 * <p>{@link #admit} takes no lock: three non-blocking {@code tryAcquire} calls, most specific first
 * (lane, connector, service). If any level is full, the slots already taken are given back and the
 * request is rejected with that level.
 *
 * <p>Lane slots are shared: {@link #rebalance} (called by the planner loop every 200 ms) re-plans every
 * lane's slot target inside its connector's slots, using {@link SlotPlanner}. Connector and service
 * slots stay fixed; the connector semaphore is the hard cap while a shrunk lane drains.
 */
public final class SlotRegistry {

    private static final Logger log = LoggerFactory.getLogger(SlotRegistry.class);

    private final SlotPool service;
    private final long defaultLaneLatencyMs;
    private final double smoothingUp;
    private final double smoothingDown;
    private final double shrinkDeadband;
    private final Map<String, Connector> connectors = new ConcurrentHashMap<>();
    private final Map<String, Lane> lanes = new ConcurrentHashMap<>();
    private long lastRebalanceNanos = System.nanoTime();

    /** Default damping: demand rises at 0.5, falls at 0.15, shrinks under 10% are skipped. */
    public SlotRegistry(int serviceMaxSlots, long defaultLaneLatencyMs) {
        this(serviceMaxSlots, defaultLaneLatencyMs, 0.5, 0.15, 0.1);
    }

    /** No damping: the same smoothing both ways and every shrink applied (deterministic tests). */
    public SlotRegistry(int serviceMaxSlots, long defaultLaneLatencyMs, double demandSmoothing) {
        this(serviceMaxSlots, defaultLaneLatencyMs, demandSmoothing, demandSmoothing, 0);
    }

    /**
     * @param serviceMaxSlots      x: pod-wide slot capacity
     * @param defaultLaneLatencyMs latency used for lanes onboarded without one
     * @param smoothingUp          weight of the newest window when a lane's demand rises, in (0, 1]
     * @param smoothingDown        weight of the newest window when a lane's demand falls, in (0, 1]
     * @param shrinkDeadband       skip a shrink smaller than this fraction of the target (at least 2 slots)
     *                             while the kept slots still fit in the connector; 0 = apply every shrink
     */
    public SlotRegistry(int serviceMaxSlots, long defaultLaneLatencyMs,
                        double smoothingUp, double smoothingDown, double shrinkDeadband) {
        if (serviceMaxSlots < 1) throw new IllegalArgumentException("service slots must be >= 1");
        if (!(smoothingUp > 0 && smoothingUp <= 1) || !(smoothingDown > 0 && smoothingDown <= 1)) {
            throw new IllegalArgumentException("demand smoothing must be in (0, 1]");
        }
        if (!(shrinkDeadband >= 0 && shrinkDeadband < 1)) {
            throw new IllegalArgumentException("shrink deadband must be in [0, 1)");
        }
        this.service = new SlotPool(Level.SERVICE, "service", serviceMaxSlots);
        this.defaultLaneLatencyMs = defaultLaneLatencyMs;
        this.smoothingUp = smoothingUp;
        this.smoothingDown = smoothingDown;
        this.shrinkDeadband = shrinkDeadband;
        log.info("Slot registry created: service slots={} demandSmoothing up={} down={} shrinkDeadband={}",
                serviceMaxSlots, smoothingUp, smoothingDown, shrinkDeadband);
    }

    public Admission admit(String laneId) {
        Lane lane = lanes.get(laneId);
        if (lane == null) throw new NotFoundException("unknown lane: " + laneId);

        List<SlotPool> held = new ArrayList<>(3);
        for (SlotPool pool : List.of(lane.pool(), lane.connector().pool(), service)) {
            if (!pool.tryAcquire()) {
                SlotPermit.releaseAll(held);
                if (log.isDebugEnabled()) {
                    log.debug("Slot rejected: lane={} level={} pool={} ({}/{} in flight); rolled back {} slot(s)",
                            laneId, pool.level(), pool.name(), pool.inFlight(), pool.capacity(), held.size());
                }
                return new Admission.Rejected(laneId, pool.level(), pool.name());
            }
            held.add(pool);
        }
        if (log.isDebugEnabled()) {
            log.debug("Slot acquired: lane={} inFlight laneSlots={}/{} connectorSlots={}/{} serviceSlots={}/{}", laneId,
                    lane.pool().inFlight(), lane.pool().capacity(),
                    lane.connector().pool().inFlight(), lane.connector().pool().capacity(),
                    service.inFlight(), service.capacity());
        }
        return new Admission.Accepted(lane, new SlotPermit(held));
    }

    /**
     * Register a connector and its lanes. Each lane starts at its guaranteed slots; the planner moves
     * it between that and {@code maxSlots} as demand changes.
     */
    public synchronized Connector onboard(String connectorId, int connectorSlots, List<LaneConfig> laneConfigs) {
        if (connectorId == null || connectorId.isBlank()) throw new IllegalArgumentException("connectorId is required");
        if (connectorSlots < 1) throw new IllegalArgumentException("connector slots must be >= 1");
        if (laneConfigs == null || laneConfigs.isEmpty()) throw new IllegalArgumentException("at least one lane is required");
        if (connectors.containsKey(connectorId)) throw new IllegalStateException("connector already onboarded: " + connectorId);
        for (LaneConfig lc : laneConfigs) {
            if (lc == null || lc.laneId() == null || lc.laneId().isBlank()) throw new IllegalArgumentException("laneId is required");
            if (lanes.containsKey(lc.laneId())) throw new IllegalStateException("lane already onboarded: " + lc.laneId());
        }
        if (laneConfigs.stream().map(LaneConfig::laneId).distinct().count() != laneConfigs.size()) {
            throw new IllegalArgumentException("duplicate laneId");
        }

        Connector connector = new Connector(connectorId, connectorSlots);
        List<Lane> created = new ArrayList<>();
        int guaranteedTotal = 0;
        for (LaneConfig lc : laneConfigs) {
            String id = lc.laneId();
            if (lc.guaranteedSlots() == null) throw new IllegalArgumentException(id + ": guaranteedSlots is required");
            int guaranteed = lc.guaranteedSlots();
            int max = lc.maxSlots() == null ? connectorSlots : lc.maxSlots();
            int weight = lc.weight() == null ? 1 : lc.weight();
            long latency = lc.latencyMs() == null ? defaultLaneLatencyMs : lc.latencyMs();
            if (guaranteed < 0) throw new IllegalArgumentException(id + ": guaranteedSlots must be >= 0");
            if (max < 1) throw new IllegalArgumentException(id + ": maxSlots must be >= 1");
            if (guaranteed > max) throw new IllegalArgumentException(id + ": guaranteedSlots must be <= maxSlots");
            if (weight < 1) throw new IllegalArgumentException(id + ": weight must be >= 1");
            if (latency < 0) throw new IllegalArgumentException(id + ": latencyMs must be >= 0");
            guaranteedTotal += guaranteed;
            Lane lane = new Lane(id, connector, latency, guaranteed, max, weight);
            connector.addLane(lane);
            created.add(lane);
        }
        if (guaranteedTotal > connectorSlots) {
            throw new IllegalArgumentException("guaranteed slots " + guaranteedTotal
                    + " exceed connector slots " + connectorSlots);
        }
        connectors.put(connectorId, connector);
        created.forEach(l -> lanes.put(l.laneId(), l));

        log.info("Connector onboarded: connector={} slots={} lanes={}", connectorId, connectorSlots,
                created.stream().map(l -> l.laneId() + "(guaranteed=" + l.guaranteedSlots() + ", max=" + l.maxSlots()
                        + ", weight=" + l.weight() + ", latencyMs=" + l.latencyMs() + ")").toList());
        return connector;
    }

    /** Re-plan every lane's slots from the demand seen since the previous call. Called by the planner loop. */
    public synchronized void rebalance() {
        long now = System.nanoTime();
        double windowSeconds = (now - lastRebalanceNanos) / 1e9;
        lastRebalanceNanos = now;
        rebalance(windowSeconds);
    }

    /** Re-plan as if {@code windowSeconds} had passed since the previous call (tests drive this directly). */
    synchronized void rebalance(double windowSeconds) {
        for (Connector connector : connectors.values()) {
            List<Lane> connectorLanes = connector.lanes();
            List<SlotPlanner.LaneInput> inputs = new ArrayList<>(connectorLanes.size());
            for (Lane lane : connectorLanes) {
                int demand = lane.updateDemand(windowSeconds, smoothingUp, smoothingDown);
                inputs.add(new SlotPlanner.LaneInput(lane.guaranteedSlots(), lane.maxSlots(), lane.weight(), demand));
            }
            int capacity = connector.pool().capacity();
            int[] targets = damp(connectorLanes, SlotPlanner.plan(capacity, inputs), capacity);

            List<String> changes = new ArrayList<>();
            for (int i = 0; i < targets.length; i++) {
                Lane lane = connectorLanes.get(i);
                int old = lane.pool().capacity();
                if (old != targets[i]) {
                    lane.resize(targets[i]);
                    changes.add(lane.laneId() + " " + old + "->" + targets[i]
                            + " (demand=" + lane.demandSlots() + ", inFlight=" + lane.pool().inFlight() + ")");
                }
            }
            if (!changes.isEmpty()) {
                log.debug("Lane slots re-planned: connector={} slots={} changes={}",
                        connector.connectorId(), connector.pool().capacity(), changes);
            }
        }
    }

    /**
     * Shrink deadband: keep a lane's current target instead of a small shrink, so noise in the demand
     * estimate does not resize it every tick. A shrink is kept back only when it is smaller than the band,
     * it does not end at the lane's guarantee (an idle lane always returns there), and the kept slots
     * still fit inside the connector next to every other lane's planned target. Growth is never delayed.
     */
    private int[] damp(List<Lane> connectorLanes, int[] planned, int capacity) {
        if (shrinkDeadband <= 0) return planned;
        int[] out = planned.clone();
        int total = 0;
        for (int t : out) total += t;
        for (int i = 0; i < out.length; i++) {
            Lane lane = connectorLanes.get(i);
            int current = lane.pool().capacity();
            int shrink = current - out[i];
            if (shrink <= 0 || out[i] == lane.guaranteedSlots()) continue;
            int band = Math.max(2, (int) Math.ceil(current * shrinkDeadband));
            if (shrink < band && total + shrink <= capacity) {
                out[i] = current;
                total += shrink;
            }
        }
        return out;
    }

    public Connector connector(String connectorId) {
        Connector c = connectors.get(connectorId);
        if (c == null) throw new NotFoundException("unknown connector: " + connectorId);
        return c;
    }

    public Lane lane(String laneId) {
        Lane lane = lanes.get(laneId);
        if (lane == null) throw new NotFoundException("unknown lane: " + laneId);
        return lane;
    }

    public SlotPool servicePool() {
        return service;
    }

    public List<Connector> connectors() {
        return connectors.values().stream().sorted(Comparator.comparing(Connector::connectorId)).toList();
    }

    /**
     * @param laneId          stage x destination
     * @param guaranteedSlots slots reserved even when the lane is idle (required, >= 0)
     * @param maxSlots        the most the lane may be given; null = the connector's slots
     * @param weight          share of spare slots relative to sibling lanes; null = 1
     * @param latencyMs       simulated delivery time, also used to turn arrivals into demand; null = registry default
     */
    public record LaneConfig(String laneId, Integer guaranteedSlots, Integer maxSlots, Integer weight, Long latencyMs) {
    }
}
