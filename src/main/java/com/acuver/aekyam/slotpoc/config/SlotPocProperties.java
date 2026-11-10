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
 */
@ConfigurationProperties(prefix = "slot-poc")
public record SlotPocProperties(Integer serviceMaxSlots,
                                Long defaultLaneLatencyMs,
                                Integer retryAfterSeconds,
                                Integer deliveryShutdownWaitSeconds) {

    public SlotPocProperties {
        if (serviceMaxSlots == null) serviceMaxSlots = 1000;
        if (defaultLaneLatencyMs == null) defaultLaneLatencyMs = 100L;
        if (retryAfterSeconds == null) retryAfterSeconds = 1;
        if (deliveryShutdownWaitSeconds == null) deliveryShutdownWaitSeconds = 30;
    }
}
