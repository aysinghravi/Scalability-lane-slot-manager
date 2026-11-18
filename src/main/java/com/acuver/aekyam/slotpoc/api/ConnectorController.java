package com.acuver.aekyam.slotpoc.api;

import com.acuver.aekyam.slotpoc.core.Connector;
import com.acuver.aekyam.slotpoc.core.SlotPool;
import com.acuver.aekyam.slotpoc.core.SlotRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Onboard connectors and their lanes, and read slot stats. */
@RestController
@RequestMapping("/api/v1")
public class ConnectorController {

    private static final Logger log = LoggerFactory.getLogger(ConnectorController.class);

    private final SlotRegistry registry;

    public ConnectorController(SlotRegistry registry) {
        this.registry = registry;
    }

    @PostMapping("/connectors")
    @ResponseStatus(HttpStatus.CREATED)
    public ConnectorView onboard(@RequestBody OnboardRequest req) {
        if (req == null || req.slots() == null) throw new IllegalArgumentException("slots is required");
        log.info("Onboard request: connector={} slots={} lanes={}", req.connectorId(), req.slots(),
                req.lanes() == null ? 0 : req.lanes().size());
        return ConnectorView.of(registry.onboard(req.connectorId(), req.slots(), req.lanes()));
    }

    @GetMapping("/connectors/{connectorId}")
    public ConnectorView get(@PathVariable String connectorId) {
        log.debug("Get connector: {}", connectorId);
        return ConnectorView.of(registry.connector(connectorId));
    }

    /** Slots at every level: capacity, available, in flight, peak, rejected. */
    @GetMapping("/stats")
    public StatsView stats() {
        log.debug("Get stats");
        return new StatsView(registry.servicePool().stats(),
                registry.connectors().stream().map(ConnectorView::of).toList());
    }

    public record OnboardRequest(String connectorId, Integer slots, List<SlotRegistry.LaneConfig> lanes) {
    }

    /** {@code slots.capacity} is the lane's current planned target; {@code demand} is what it wanted at the last plan. */
    public record LaneView(String laneId, long latencyMs, int guaranteedSlots, int maxSlots, int weight,
                           int demand, long resizes, SlotPool.Stats slots) {
    }

    public record ConnectorView(String connectorId, SlotPool.Stats slots, List<LaneView> lanes) {
        static ConnectorView of(Connector c) {
            return new ConnectorView(c.connectorId(), c.pool().stats(), c.lanes().stream()
                    .map(l -> new LaneView(l.laneId(), l.latencyMs(), l.guaranteedSlots(), l.maxSlots(),
                            l.weight(), l.demandSlots(), l.resizes(), l.pool().stats())).toList());
        }
    }

    public record StatsView(SlotPool.Stats service, List<ConnectorView> connectors) {
    }
}
