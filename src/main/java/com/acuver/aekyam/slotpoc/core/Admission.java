package com.acuver.aekyam.slotpoc.core;

/** Result of asking for a slot at all three levels. */
public sealed interface Admission {

    String laneId();

    /** All three permits are held; the caller must close {@code permit} when the delivery ends. */
    record Accepted(Lane lane, SlotPermit permit) implements Admission {
        @Override
        public String laneId() {
            return lane.laneId();
        }
    }

    /** No permit at {@code rejectedBy}; any permits taken at earlier levels were already given back. */
    record Rejected(String laneId, Level rejectedBy, String poolName) implements Admission {
    }
}
