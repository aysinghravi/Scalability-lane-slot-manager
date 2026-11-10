package com.acuver.aekyam.slotpoc.core;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SlotRegistryTest {

    private static SlotRegistry.LaneConfig lane(String id, int slots) {
        return new SlotRegistry.LaneConfig(id, slots, null);
    }

    private static Admission.Accepted accept(SlotRegistry r, String laneId) {
        Admission a = r.admit(laneId);
        assertThat(a).as("admission for " + laneId).isInstanceOf(Admission.Accepted.class);
        return (Admission.Accepted) a;
    }

    private static Level rejectedBy(SlotRegistry r, String laneId) {
        Admission a = r.admit(laneId);
        assertThat(a).as("admission for " + laneId).isInstanceOf(Admission.Rejected.class);
        return ((Admission.Rejected) a).rejectedBy();
    }

    @Test
    @DisplayName("Lane full -> rejected by LANE; releasing a slot lets the next one in")
    void laneLimit() {
        SlotRegistry r = new SlotRegistry(100, 100);
        r.onboard("c1", 10, List.of(lane("a", 2)));
        accept(r, "a");
        SlotPermit second = accept(r, "a").permit();
        assertThat(rejectedBy(r, "a")).isEqualTo(Level.LANE);

        second.close();
        accept(r, "a");
    }

    @Test
    @DisplayName("Connector full -> rejected by CONNECTOR; the lane slot taken first is given back")
    void connectorLimit() {
        SlotRegistry r = new SlotRegistry(100, 100);
        r.onboard("c1", 3, List.of(lane("a", 2), lane("b", 2)));
        accept(r, "a");
        accept(r, "a");
        accept(r, "b");
        assertThat(rejectedBy(r, "b")).isEqualTo(Level.CONNECTOR);
        assertThat(r.lane("b").pool().inFlight()).isEqualTo(1);
        assertThat(r.lane("b").pool().available()).isEqualTo(1);
    }

    @Test
    @DisplayName("Service full -> rejected by SERVICE; lane and connector slots are given back")
    void serviceLimit() {
        SlotRegistry r = new SlotRegistry(2, 100);
        r.onboard("c1", 10, List.of(lane("a", 10)));
        accept(r, "a");
        accept(r, "a");
        assertThat(rejectedBy(r, "a")).isEqualTo(Level.SERVICE);
        assertThat(r.lane("a").pool().inFlight()).isEqualTo(2);
        assertThat(r.connector("c1").pool().inFlight()).isEqualTo(2);
    }

    @Test
    @DisplayName("Closing a permit twice releases its slots only once")
    void permitReleasedOnce() {
        SlotRegistry r = new SlotRegistry(10, 100);
        r.onboard("c1", 10, List.of(lane("a", 1)));
        SlotPermit p = accept(r, "a").permit();
        p.close();
        p.close();
        assertThat(r.lane("a").pool().available()).isEqualTo(1);
        assertThat(r.servicePool().available()).isEqualTo(10);
    }

    @Test
    @DisplayName("Under 20k concurrent requests on virtual threads no cap is ever exceeded")
    void capsNeverExceededUnderLoad() {
        SlotRegistry r = new SlotRegistry(50, 100);
        r.onboard("c1", 30, List.of(lane("a", 20), lane("b", 20)));
        r.onboard("c2", 25, List.of(lane("c", 25)));
        String[] laneIds = {"a", "b", "c"};

        int total = 20_000;
        AtomicLong accepted = new AtomicLong();
        AtomicLong rejected = new AtomicLong();
        try (ExecutorService clients = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < total; i++) {
                String laneId = laneIds[i % laneIds.length];
                clients.submit(() -> {
                    switch (r.admit(laneId)) {
                        case Admission.Accepted ok -> {
                            accepted.incrementAndGet();
                            try (var p = ok.permit()) {
                                Thread.sleep(ThreadLocalRandom.current().nextInt(0, 3));
                            }
                        }
                        case Admission.Rejected rej -> rejected.incrementAndGet();
                    }
                    return null;
                });
            }
        }

        List<SlotPool> pools = List.of(r.servicePool(), r.connector("c1").pool(), r.connector("c2").pool(),
                r.lane("a").pool(), r.lane("b").pool(), r.lane("c").pool());
        assertThat(accepted.get() + rejected.get()).isEqualTo(total);
        assertThat(accepted.get()).isPositive();
        for (SlotPool p : pools) {
            System.out.printf("%-9s %-7s cap=%3d peak=%3d rejected=%6d%n",
                    p.level(), p.name(), p.capacity(), p.peakInFlight(), p.rejected());
            assertThat(p.peakInFlight()).as(p.name() + " peak").isLessThanOrEqualTo(p.capacity());
            assertThat(p.inFlight()).as(p.name() + " in flight at end").isZero();
            assertThat(p.available()).as(p.name() + " slots returned").isEqualTo(p.capacity());
        }
        assertThat(pools.stream().mapToLong(SlotPool::rejected).sum())
                .as("each rejection is counted at exactly one level").isEqualTo(rejected.get());
    }

    @Test
    @DisplayName("Onboarding validates input; unknown lanes are not found")
    void onboardingAndLookup() {
        SlotRegistry r = new SlotRegistry(10, 100);
        r.onboard("c1", 5, List.of(lane("a", 1)));
        assertThatThrownBy(() -> r.onboard("c1", 5, List.of(lane("x", 1)))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> r.onboard("c2", 5, List.of(lane("a", 1)))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> r.onboard("c3", 5, List.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> r.onboard("c4", 0, List.of(lane("y", 1)))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> r.admit("nope")).isInstanceOf(NotFoundException.class);
    }
}
