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
 */
public final class SlotRegistry {

    private static final Logger log = LoggerFactory.getLogger(SlotRegistry.class);

    private final SlotPool service;
    private final long defaultLaneLatencyMs;
    private final Map<String, Connector> connectors = new ConcurrentHashMap<>();
    private final Map<String, Lane> lanes = new ConcurrentHashMap<>();

    /**
     * @param serviceMaxSlots      x: pod-wide slot capacity
     * @param defaultLaneLatencyMs latency used for lanes onboarded without one
     */
    public SlotRegistry(int serviceMaxSlots, long defaultLaneLatencyMs) {
        this.service = new SlotPool(Level.SERVICE, "service", serviceMaxSlots);
        this.defaultLaneLatencyMs = defaultLaneLatencyMs;
        log.info("Slot registry created: service slots={}", serviceMaxSlots);
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

    /** Register a connector and its lanes with fixed slot counts. */
    public synchronized Connector onboard(String connectorId, int connectorSlots, List<LaneConfig> laneConfigs) {
        if (connectorId == null || connectorId.isBlank()) throw new IllegalArgumentException("connectorId is required");
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
        for (LaneConfig lc : laneConfigs) {
            long latency = lc.latencyMs() == null ? defaultLaneLatencyMs : lc.latencyMs();
            if (latency < 0) throw new IllegalArgumentException("latencyMs must be >= 0");
            Lane lane = new Lane(lc.laneId(), connector, new SlotPool(Level.LANE, lc.laneId(), lc.slots()), latency);
            connector.addLane(lane);
            created.add(lane);
        }
        connectors.put(connectorId, connector);
        created.forEach(l -> lanes.put(l.laneId(), l));

        log.info("Connector onboarded: connector={} slots={} lanes={}", connectorId, connectorSlots,
                created.stream().map(l -> l.laneId() + "(slots=" + l.pool().capacity() + ", latencyMs=" + l.latencyMs() + ")")
                        .toList());
        int laneTotal = created.stream().mapToInt(l -> l.pool().capacity()).sum();
        if (laneTotal > connectorSlots) {
            log.info("Connector {} is over-subscribed: lane slots {} > connector slots {} (connector cap applies)",
                    connectorId, laneTotal, connectorSlots);
        }
        return connector;
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

    /** @param latencyMs optional simulated delivery time; null = registry default */
    public record LaneConfig(String laneId, int slots, Long latencyMs) {
    }
}
