package com.acuver.aekyam.slotpoc.config;

import com.acuver.aekyam.slotpoc.core.PlannerLoop;
import com.acuver.aekyam.slotpoc.core.SlotRegistry;
import com.acuver.aekyam.slotpoc.delivery.DeliveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SlotPocConfig {

    private static final Logger log = LoggerFactory.getLogger(SlotPocConfig.class);

    @Bean
    public SlotRegistry slotRegistry(SlotPocProperties props) {
        SlotPocProperties.Planner planner = props.planner();
        log.info("Slot POC config: serviceMaxSlots={}, defaultLaneLatencyMs={}, retryAfterSeconds={}, "
                        + "deliveryShutdownWaitSeconds={}, planner.enabled={}, planner.intervalMs={}, "
                        + "planner.demandSmoothingUp={}, planner.demandSmoothingDown={}, planner.shrinkDeadband={}",
                props.serviceMaxSlots(), props.defaultLaneLatencyMs(), props.retryAfterSeconds(),
                props.deliveryShutdownWaitSeconds(), planner.enabled(), planner.intervalMs(),
                planner.demandSmoothingUp(), planner.demandSmoothingDown(), planner.shrinkDeadband());
        if (!planner.enabled()) log.info("Slot planner disabled: every lane stays at its guaranteed slots");
        return new SlotRegistry(props.serviceMaxSlots(), props.defaultLaneLatencyMs(),
                planner.demandSmoothingUp(), planner.demandSmoothingDown(), planner.shrinkDeadband());
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "slot-poc.planner", name = "enabled", havingValue = "true", matchIfMissing = true)
    public PlannerLoop plannerLoop(SlotRegistry registry, SlotPocProperties props) {
        return new PlannerLoop(registry, props.planner().intervalMs());
    }

    @Bean(destroyMethod = "close")
    public DeliveryService deliveryService(SlotPocProperties props) {
        return new DeliveryService(props.deliveryShutdownWaitSeconds());
    }
}
