package com.acuver.aekyam.slotpoc.core;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SlotRegistryTest {

    /** A lane that never moves: guaranteed == max. */
    private static SlotRegistry.LaneConfig fixed(String id, int slots) {
        return new SlotRegistry.LaneConfig(id, slots, slots, 1, null);
    }

    private static SlotRegistry.LaneConfig shared(String id, int guaranteed, Integer max, Long latencyMs) {
        return new SlotRegistry.LaneConfig(id, guaranteed, max, 1, latencyMs);
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

    private static List<SlotPermit> acceptN(SlotRegistry r, String laneId, int n) {
        List<SlotPermit> held = new ArrayList<>();
        for (int i = 0; i < n; i++) held.add(accept(r, laneId).permit());
        return held;
    }

    @Test
    @DisplayName("Lane full -> rejected by LANE; releasing a slot lets the next one in")
    void laneLimit() {
        SlotRegistry r = new SlotRegistry(100, 100);
        r.onboard("c1", 10, List.of(fixed("a", 2)));
        accept(r, "a");
        SlotPermit second = accept(r, "a").permit();
        assertThat(rejectedBy(r, "a")).isEqualTo(Level.LANE);

        second.close();
        accept(r, "a");
    }

    @Test
    @DisplayName("Service full -> rejected by SERVICE; lane and connector slots are given back")
    void serviceLimit() {
        SlotRegistry r = new SlotRegistry(2, 100);
        r.onboard("c1", 10, List.of(fixed("a", 10)));
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
        r.onboard("c1", 10, List.of(fixed("a", 1)));
        SlotPermit p = accept(r, "a").permit();
        p.close();
        p.close();
        assertThat(r.lane("a").pool().available()).isEqualTo(1);
        assertThat(r.servicePool().available()).isEqualTo(10);
    }

    @Test
    @DisplayName("Shared slots: bucket1 borrows from idle bucket2, gives back when bucket2 gets busy, "
            + "drains without interruption; the connector cap holds during the drain")
    void sharedSlots() {
        // smoothing 1.0 = no averaging, so each re-plan reflects exactly the last window
        SlotRegistry r = new SlotRegistry(1000, 100, 1.0);
        r.onboard("S3_connector_1", 130, List.of(
                shared("bucket1", 18, null, 600L),
                shared("bucket2", 30, null, 300L)));
        SlotPool b1 = r.lane("bucket1").pool();
        SlotPool b2 = r.lane("bucket2").pool();
        assertThat(b1.capacity()).isEqualTo(18);
        assertThat(b2.capacity()).isEqualTo(30);

        // 1. bucket1 gets 30 requests in 0.2 s (150/s x 0.6 s = 90 slots wanted): 18 in, 12 rejected
        List<SlotPermit> b1Held = acceptN(r, "bucket1", 18);
        for (int i = 0; i < 12; i++) assertThat(rejectedBy(r, "bucket1")).isEqualTo(Level.LANE);
        r.rebalance(0.2);
        assertThat(b1.capacity()).as("bucket1 borrows 72 spare").isEqualTo(90);
        assertThat(b2.capacity()).as("idle bucket2 keeps its guarantee").isEqualTo(30);

        // 2. bucket1 fills its new 90, then a quiet window: demand = in flight = 90
        b1Held.addAll(acceptN(r, "bucket1", 72));
        assertThat(rejectedBy(r, "bucket1")).isEqualTo(Level.LANE);
        r.rebalance(0.2); // counts the 73 arrivals above
        r.rebalance(0.2); // quiet window
        assertThat(b1.capacity()).isEqualTo(90);

        // 3. bucket2 gets 40 requests in 0.2 s (200/s x 0.3 s = 60 wanted): 30 in, 10 rejected
        List<SlotPermit> b2Held = acceptN(r, "bucket2", 30);
        for (int i = 0; i < 10; i++) assertThat(rejectedBy(r, "bucket2")).isEqualTo(Level.LANE);
        r.rebalance(0.2);
        // spare 82: bucket2 needs 30 more (fits its half), bucket1 gets the other 52
        assertThat(b2.capacity()).isEqualTo(60);
        assertThat(b1.capacity()).isEqualTo(70);
        assertThat(b1.inFlight()).as("shrinking never interrupts running work").isEqualTo(90);

        // 4. bucket1 is over its target, so it rejects until it drains
        assertThat(rejectedBy(r, "bucket1")).isEqualTo(Level.LANE);
        // bucket2 has lane room (30/60) but the connector has only 130 - 120 = 10 left
        b2Held.addAll(acceptN(r, "bucket2", 10));
        assertThat(rejectedBy(r, "bucket2")).isEqualTo(Level.CONNECTOR);
        assertThat(b2.inFlight()).as("lane slot rolled back").isEqualTo(40);
        assertThat(r.connector("S3_connector_1").pool().peakInFlight()).isLessThanOrEqualTo(130);

        // 5. 20 bucket1 deliveries finish: 70/70, still full; one more finishes and it admits again
        for (int i = 0; i < 20; i++) b1Held.remove(0).close();
        assertThat(rejectedBy(r, "bucket1")).isEqualTo(Level.LANE);
        b1Held.remove(0).close();
        accept(r, "bucket1");
    }

    @Test
    @DisplayName("Idle lane shrinks back to its guarantee once its demand fades")
    void shrinksBackWhenIdle() {
        SlotRegistry r = new SlotRegistry(1000, 100, 1.0);
        r.onboard("c1", 100, List.of(shared("a", 5, null, 1000L)));
        for (int i = 0; i < 20; i++) accept(r, "a").permit().close(); // 100/s x 1 s = 100 wanted
        r.rebalance(0.2);
        assertThat(r.lane("a").pool().capacity()).isEqualTo(100);
        r.rebalance(0.2);
        assertThat(r.lane("a").pool().capacity()).isEqualTo(5);
    }

    /** Noisy load: windows alternate 100/s and 60/s on a 1 s latency lane. Returns target per tick. */
    private static List<Integer> noisyTargets(SlotRegistry r, int ticks) {
        r.onboard("c1", 100, List.of(shared("a", 5, null, 1000L)));
        List<Integer> targets = new ArrayList<>();
        for (int tick = 0; tick < ticks; tick++) {
            int arrivals = tick % 2 == 0 ? 20 : 12;
            for (int i = 0; i < arrivals; i++) {
                if (r.admit("a") instanceof Admission.Accepted ok) ok.permit().close();
            }
            r.rebalance(0.2);
            targets.add(r.lane("a").pool().capacity());
        }
        return targets;
    }

    private static long changes(List<Integer> targets, int from) {
        long n = 0;
        for (int i = Math.max(1, from); i < targets.size(); i++) if (!targets.get(i).equals(targets.get(i - 1))) n++;
        return n;
    }

    @Test
    @DisplayName("Damping: noisy demand no longer resizes the lane every tick, and an idle lane still returns to its guarantee")
    void dampingStopsFlapping() {
        List<Integer> undamped = noisyTargets(new SlotRegistry(1000, 100, 0.5), 60);
        SlotRegistry damped = new SlotRegistry(1000, 100, 0.5, 0.15, 0.1);
        List<Integer> dampedTargets = noisyTargets(damped, 60);

        long undampedChanges = changes(undamped, 20);
        long dampedChanges = changes(dampedTargets, 20);
        System.out.printf("noisy load, last 40 ticks: undamped %d resizes (%s..), damped %d resizes (%s..)%n",
                undampedChanges, undamped.subList(20, 26), dampedChanges, dampedTargets.subList(20, 26));
        assertThat(undampedChanges).isGreaterThanOrEqualTo(30);
        assertThat(dampedChanges).isLessThanOrEqualTo(2);
        assertThat(dampedTargets.get(59)).as("damped lane holds the high side of the noise").isBetween(90, 100);

        for (int tick = 0; tick < 30; tick++) damped.rebalance(0.2); // traffic stops
        assertThat(damped.lane("a").pool().capacity()).as("idle lane back at its guarantee").isEqualTo(5);
        assertThat(damped.lane("a").resizes()).as("resizes are counted").isPositive();
    }

    @Test
    @DisplayName("Damping never lets kept slots push the lane targets past the connector")
    void dampingRespectsConnector() {
        SlotRegistry r = new SlotRegistry(1000, 100, 1.0, 1.0, 0.5);
        r.onboard("c1", 100, List.of(shared("a", 10, null, 1000L), shared("b", 10, null, 1000L)));
        for (int i = 0; i < 20; i++) r.admit("a"); // a wants 100
        r.rebalance(0.2);
        assertThat(r.lane("a").pool().capacity()).isEqualTo(90);
        for (int i = 0; i < 12; i++) r.admit("a"); // a wants 60: a shrink of 30, under the 50% band
        for (int i = 0; i < 8; i++) r.admit("b");  // b wants 40: needs the slots a would keep
        r.rebalance(0.2);
        int a = r.lane("a").pool().capacity(), b = r.lane("b").pool().capacity();
        assertThat(a + b).isLessThanOrEqualTo(100);
        assertThat(b).isEqualTo(40);
        assertThat(a).isEqualTo(60);
    }

    @Test
    @DisplayName("Under 20k concurrent requests on virtual threads, with re-planning every 5 ms, no cap is exceeded")
    void capsNeverExceededUnderLoad() throws Exception {
        SlotRegistry r = new SlotRegistry(50, 100);
        r.onboard("c1", 30, List.of(shared("a", 10, 20, 2L), shared("b", 10, 20, 2L)));
        r.onboard("c2", 25, List.of(shared("c", 10, 25, 2L)));
        String[] laneIds = {"a", "b", "c"};

        int total = 20_000;
        AtomicLong accepted = new AtomicLong();
        AtomicLong rejected = new AtomicLong();
        ScheduledExecutorService planner = Executors.newSingleThreadScheduledExecutor();
        planner.scheduleWithFixedDelay(r::rebalance, 5, 5, TimeUnit.MILLISECONDS);
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
        planner.shutdown();
        assertThat(planner.awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        List<SlotPool> fixedPools = List.of(r.servicePool(), r.connector("c1").pool(), r.connector("c2").pool());
        List<Lane> lanes = List.of(r.lane("a"), r.lane("b"), r.lane("c"));
        assertThat(accepted.get() + rejected.get()).isEqualTo(total);
        assertThat(accepted.get()).isPositive();
        for (SlotPool p : fixedPools) {
            System.out.printf("%-9s %-7s cap=%3d peak=%3d rejected=%6d%n",
                    p.level(), p.name(), p.capacity(), p.peakInFlight(), p.rejected());
            assertThat(p.peakInFlight()).as(p.name() + " peak").isLessThanOrEqualTo(p.capacity());
            assertThat(p.inFlight()).as(p.name() + " in flight at end").isZero();
            assertThat(p.available()).as(p.name() + " slots returned").isEqualTo(p.capacity());
        }
        for (Lane l : lanes) {
            SlotPool p = l.pool();
            System.out.printf("%-9s %-7s target=%3d max=%3d peak=%3d rejected=%6d%n",
                    p.level(), p.name(), p.capacity(), l.maxSlots(), p.peakInFlight(), p.rejected());
            assertThat(p.peakInFlight()).as(p.name() + " peak").isLessThanOrEqualTo(l.maxSlots());
            assertThat(p.inFlight()).as(p.name() + " in flight at end").isZero();
            assertThat(p.available()).as(p.name() + " slots returned").isEqualTo(p.capacity());
        }
        long rejectionsCounted = fixedPools.stream().mapToLong(SlotPool::rejected).sum()
                + lanes.stream().mapToLong(l -> l.pool().rejected()).sum();
        assertThat(rejectionsCounted).as("each rejection is counted at exactly one level").isEqualTo(rejected.get());
    }

    @Test
    @DisplayName("Onboarding validates input; unknown lanes are not found")
    void onboardingAndLookup() {
        SlotRegistry r = new SlotRegistry(10, 100);
        r.onboard("c1", 5, List.of(fixed("a", 1)));
        assertThatThrownBy(() -> r.onboard("c1", 5, List.of(fixed("x", 1)))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> r.onboard("c2", 5, List.of(fixed("a", 1)))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> r.onboard("c3", 5, List.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> r.onboard("c4", 0, List.of(fixed("y", 1)))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> r.onboard("c5", 5, List.of(shared("p", 3, null, null), shared("q", 3, null, null))))
                .as("guarantees above connector slots").isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> r.onboard("c6", 5, List.of(shared("z", 4, 2, null))))
                .as("guaranteed above max").isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> r.onboard("c7", 5, List.of(new SlotRegistry.LaneConfig("w", null, null, null, null))))
                .as("guaranteedSlots required").isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> r.admit("nope")).isInstanceOf(NotFoundException.class);
    }
}
