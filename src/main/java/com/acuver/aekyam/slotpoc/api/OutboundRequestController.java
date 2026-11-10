package com.acuver.aekyam.slotpoc.api;

import com.acuver.aekyam.slotpoc.config.SlotPocProperties;
import com.acuver.aekyam.slotpoc.core.Admission;
import com.acuver.aekyam.slotpoc.core.Level;
import com.acuver.aekyam.slotpoc.core.SlotRegistry;
import com.acuver.aekyam.slotpoc.delivery.DeliveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/** Accepts (202) or rejects (429) one outbound request against lane, connector and service slots. */
@RestController
@RequestMapping("/api/v1/requests")
public class OutboundRequestController {

    private static final Logger log = LoggerFactory.getLogger(OutboundRequestController.class);

    private final SlotRegistry registry;
    private final DeliveryService delivery;
    private final String retryAfter;

    public OutboundRequestController(SlotRegistry registry, DeliveryService delivery, SlotPocProperties props) {
        this.registry = registry;
        this.delivery = delivery;
        this.retryAfter = String.valueOf(props.retryAfterSeconds());
    }

    @PostMapping
    public ResponseEntity<Object> submit(@RequestBody OutboundRequest request) {
        if (request == null || request.laneId() == null || request.laneId().isBlank()) {
            throw new IllegalArgumentException("laneId is required");
        }
        if (request.latencyMs() != null && request.latencyMs() < 0) {
            throw new IllegalArgumentException("latencyMs must be >= 0");
        }
        String requestId = UUID.randomUUID().toString();

        return switch (registry.admit(request.laneId())) {
            case Admission.Accepted ok -> {
                delivery.dispatch(requestId, ok, request.latencyMs());
                log.info("Request accepted: requestId={} lane={} -> 202", requestId, ok.laneId());
                yield ResponseEntity.status(HttpStatus.ACCEPTED)
                        .body(new Accepted("ACCEPTED", requestId, ok.laneId()));
            }
            case Admission.Rejected r -> {
                log.info("Request rejected: requestId={} lane={} rejectedBy={} pool={} -> 429",
                        requestId, r.laneId(), r.rejectedBy(), r.poolName());
                yield ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                        .header("Retry-After", retryAfter)
                        .body(new Rejected("REJECTED", requestId, r.laneId(), r.rejectedBy(), r.poolName()));
            }
        };
    }

    /** @param latencyMs optional: overrides the lane's simulated delivery time */
    public record OutboundRequest(String laneId, Long latencyMs) {
    }

    public record Accepted(String status, String requestId, String laneId) {
    }

    public record Rejected(String status, String requestId, String laneId, Level rejectedBy, String slotPool) {
    }
}
