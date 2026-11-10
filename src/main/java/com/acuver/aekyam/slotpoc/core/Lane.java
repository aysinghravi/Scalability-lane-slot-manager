package com.acuver.aekyam.slotpoc.core;

/**
 * One lane (stage x destination) served by one connector.
 *
 * @param latencyMs simulated delivery time for this lane's destination
 */
public record Lane(String laneId, Connector connector, SlotPool pool, long latencyMs) {
}
