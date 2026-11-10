package com.acuver.aekyam.slotpoc.config;

import com.acuver.aekyam.slotpoc.core.SlotRegistry;
import com.acuver.aekyam.slotpoc.delivery.DeliveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SlotPocConfig {

    private static final Logger log = LoggerFactory.getLogger(SlotPocConfig.class);

    @Bean
    public SlotRegistry slotRegistry(SlotPocProperties props) {
        log.info("Slot POC config: serviceMaxSlots={}, defaultLaneLatencyMs={}, retryAfterSeconds={}, "
                        + "deliveryShutdownWaitSeconds={}",
                props.serviceMaxSlots(), props.defaultLaneLatencyMs(), props.retryAfterSeconds(),
                props.deliveryShutdownWaitSeconds());
        return new SlotRegistry(props.serviceMaxSlots(), props.defaultLaneLatencyMs());
    }

    @Bean(destroyMethod = "close")
    public DeliveryService deliveryService(SlotPocProperties props) {
        return new DeliveryService(props.deliveryShutdownWaitSeconds());
    }
}
