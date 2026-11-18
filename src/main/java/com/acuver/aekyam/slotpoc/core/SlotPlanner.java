package com.acuver.aekyam.slotpoc.core;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Splits one connector's slots across its lanes. A pure function: no threads, no state.
 *
 * <ol>
 *   <li>Every lane gets its guaranteed slots, even when idle.</li>
 *   <li>A lane with demand but no guarantee gets 1 slot, while slots remain.</li>
 *   <li>The spare slots go to lanes whose demand is still unmet, by weighted max-min fairness
 *       (water-filling): each gets a share proportional to its weight, never more than it needs, and
 *       what a lane can't use is shared again among the rest.</li>
 *   <li>Fractional shares are rounded by largest remainder, so the total never exceeds the capacity.</li>
 * </ol>
 *
 * <p>A lane's need is {@code min(demand, maxSlots)}. Spare slots nobody needs stay unassigned.
 */
public final class SlotPlanner {

    private SlotPlanner() {
    }

    /**
     * @param guaranteed slots reserved for the lane even when idle
     * @param max        the most the lane may be given
     * @param weight     share of the spare slots, relative to the other lanes
     * @param demand     slots the lane currently wants
     */
    public record LaneInput(int guaranteed, int max, int weight, int demand) {
    }

    /** @return one target per lane, in input order; their sum is never above {@code capacity} */
    public static int[] plan(int capacity, List<LaneInput> lanes) {
        int n = lanes.size();
        int[] alloc = new int[n];
        int[] need = new int[n];
        int spare = capacity;

        for (int i = 0; i < n; i++) {
            alloc[i] = lanes.get(i).guaranteed();
            spare -= alloc[i];
        }
        if (spare < 0) throw new IllegalArgumentException("guaranteed slots exceed capacity " + capacity);

        for (int i = 0; i < n && spare > 0; i++) {
            LaneInput l = lanes.get(i);
            if (l.demand() > 0 && alloc[i] == 0 && l.max() > 0) {
                alloc[i] = 1;
                spare--;
            }
        }
        for (int i = 0; i < n; i++) {
            LaneInput l = lanes.get(i);
            need[i] = Math.max(0, Math.min(l.demand(), l.max()) - alloc[i]);
        }

        while (spare > 0) {
            List<Integer> active = new ArrayList<>();
            long totalWeight = 0;
            for (int i = 0; i < n; i++) {
                if (need[i] > 0) {
                    active.add(i);
                    totalWeight += lanes.get(i).weight();
                }
            }
            if (active.isEmpty()) break;

            // Lanes whose whole need fits inside their weighted share are filled; the water level then rises.
            boolean filledAny = false;
            int roundSpare = spare;
            for (int i : active) {
                if ((long) need[i] * totalWeight <= (long) roundSpare * lanes.get(i).weight()) {
                    alloc[i] += need[i];
                    spare -= need[i];
                    need[i] = 0;
                    filledAny = true;
                }
            }
            if (filledAny) continue;

            // Nobody fits: split what is left by weight, then round by largest remainder.
            double[] remainder = new double[n];
            int given = 0;
            for (int i : active) {
                double exact = (double) spare * lanes.get(i).weight() / totalWeight;
                int whole = (int) Math.floor(exact);
                alloc[i] += whole;
                need[i] -= whole;
                given += whole;
                remainder[i] = exact - whole;
            }
            int left = spare - given;
            active.sort(Comparator.<Integer>comparingDouble(i -> remainder[i]).reversed()
                    .thenComparing(Comparator.naturalOrder()));
            for (int k = 0; k < left; k++) {
                int i = active.get(k);
                alloc[i]++;
                need[i]--;
            }
            spare = 0;
        }
        return alloc;
    }
}
