package com.acuver.aekyam.slotpoc.delivery;

import com.acuver.aekyam.slotpoc.core.Admission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Runs each accepted delivery on its own virtual thread (named {@code delivery-N}). The downstream
 * call is simulated with a sleep. The slots are released in {@code finally} (try-with-resources),
 * so they can never leak.
 */
public final class DeliveryService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DeliveryService.class);

    private final ExecutorService executor =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("delivery-", 0).factory());
    private final int shutdownWaitSeconds;

    public DeliveryService(int shutdownWaitSeconds) {
        this.shutdownWaitSeconds = shutdownWaitSeconds;
    }

    /** @param latencyOverrideMs null = use the lane's latency */
    public void dispatch(String requestId, Admission.Accepted accepted, Long latencyOverrideMs) {
        long latency = latencyOverrideMs != null ? latencyOverrideMs : accepted.lane().latencyMs();
        try {
            executor.execute(() -> {
                long start = System.nanoTime();
                log.debug("Delivery started: requestId={} lane={} latencyMs={}", requestId, accepted.laneId(), latency);
                try (var permit = accepted.permit()) {
                    if (latency > 0) Thread.sleep(latency);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Delivery interrupted: requestId={} lane={}", requestId, accepted.laneId());
                    return;
                } catch (RuntimeException e) {
                    log.error("Delivery failed: requestId={} lane={}", requestId, accepted.laneId(), e);
                    return;
                }
                log.debug("Delivery finished, slots released: requestId={} lane={} tookMs={}",
                        requestId, accepted.laneId(), (System.nanoTime() - start) / 1_000_000);
            });
        } catch (RuntimeException e) {   // executor shut down
            accepted.permit().close();
            log.error("Delivery not started (executor shut down), slots released: requestId={} lane={}",
                    requestId, accepted.laneId());
            throw e;
        }
    }

    @Override
    public void close() {
        log.info("Delivery service shutting down, waiting up to {}s for in-flight deliveries", shutdownWaitSeconds);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(shutdownWaitSeconds, TimeUnit.SECONDS)) {
                log.warn("In-flight deliveries did not finish in {}s, interrupting them", shutdownWaitSeconds);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
