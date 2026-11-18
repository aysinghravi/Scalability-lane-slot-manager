package com.acuver.aekyam.slotpoc.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * All tunables of the POC, bound from {@code slot-poc.*} in application.yml.
 * The fallbacks below apply only when a property is missing from application.yml.
 *
 * @param serviceMaxSlots            x: total slots one pod may have in flight (whiteboard: 1000)
 * @param defaultLaneLatencyMs       simulated delivery time for a lane onboarded without {@code latencyMs}
 * @param retryAfterSeconds          value of the {@code Retry-After} header on a 429
 * @param deliveryShutdownWaitSeconds how long shutdown waits for in-flight deliveries to finish
 * @param planner                    shared lane slots: how often and how smoothly lane targets are re-planned
 */
@ConfigurationProperties(prefix = "slot-poc")
public record SlotPocProperties(Integer serviceMaxSlots,
                                Long defaultLaneLatencyMs,
                                Integer retryAfterSeconds,
                                Integer deliveryShutdownWaitSeconds,
                                Planner planner) {

    public SlotPocProperties {
        if (serviceMaxSlots == null) serviceMaxSlots = 1000;
        if (defaultLaneLatencyMs == null) defaultLaneLatencyMs = 100L;
        if (retryAfterSeconds == null) retryAfterSeconds = 1;
        if (deliveryShutdownWaitSeconds == null) deliveryShutdownWaitSeconds = 30;
        if (planner == null) planner = new Planner(null, null, null, null, null);
    }

    /**
     * @param enabled             false = lanes stay fixed at their guaranteed slots
     * @param intervalMs          how often lane slots are re-planned (lane scheduler uses 200 ms)
     * @param demandSmoothingUp   weight of the newest window when demand rises, in (0, 1]: react fast to bursts
     * @param demandSmoothingDown weight of the newest window when demand falls, in (0, 1]: give slots back slowly
     * @param shrinkDeadband      skip a shrink smaller than this fraction of the target (min 2 slots), in [0, 1)
     */
    public record Planner(Boolean enabled, Long intervalMs, Double demandSmoothingUp,
                          Double demandSmoothingDown, Double shrinkDeadband) {
        public Planner {
            if (enabled == null) enabled = true;
            if (intervalMs == null) intervalMs = 200L;
            if (demandSmoothingUp == null) demandSmoothingUp = 0.5;
            if (demandSmoothingDown == null) demandSmoothingDown = 0.15;
            if (shrinkDeadband == null) shrinkDeadband = 0.1;
        }
    }
}
