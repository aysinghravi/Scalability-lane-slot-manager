package com.acuver.aekyam.slotpoc.core;

import java.util.ArrayList;
import java.util.List;

/** One connector instance. Its slots are shared by all of its lanes. */
public final class Connector {

    private final String connectorId;
    private final SlotPool pool;
    private final List<Lane> lanes = new ArrayList<>();

    Connector(String connectorId, int slots) {
        this.connectorId = connectorId;
        this.pool = new SlotPool(Level.CONNECTOR, connectorId, slots);
    }

    void addLane(Lane lane) {
        lanes.add(lane);
    }

    public String connectorId() { return connectorId; }
    public SlotPool pool() { return pool; }
    public List<Lane> lanes() { return List.copyOf(lanes); }
}
