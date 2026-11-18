package com.acuver.aekyam.slotpoc.core;

import com.acuver.aekyam.slotpoc.core.SlotPlanner.LaneInput;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SlotPlannerTest {

    private static LaneInput lane(int guaranteed, int max, int weight, int demand) {
        return new LaneInput(guaranteed, max, weight, demand);
    }

    @Test
    @DisplayName("Worked example: S3_connector_1 (130), bucket1 borrows while bucket2 is idle, then gives back")
    void workedExample() {
        // bucket2 idle: bucket1 gets its 18 plus 72 spare; bucket2 keeps its guaranteed 30
        assertThat(SlotPlanner.plan(130, List.of(lane(18, 130, 1, 90), lane(30, 130, 1, 0))))
                .containsExactly(90, 30);
        // bucket2 wants 50: spare 82 split evenly is 41 each; bucket2 needs only 20, the other 21 go to bucket1
        assertThat(SlotPlanner.plan(130, List.of(lane(18, 130, 1, 90), lane(30, 130, 1, 50))))
                .containsExactly(80, 50);
    }

    @Test
    @DisplayName("Idle lanes keep their guaranteed slots; spare is left unassigned")
    void idleKeepsGuarantee() {
        assertThat(SlotPlanner.plan(100, List.of(lane(10, 100, 1, 0), lane(20, 100, 1, 0))))
                .containsExactly(10, 20);
    }

    @Test
    @DisplayName("maxSlots caps a lane even when spare slots remain")
    void maxRespected() {
        assertThat(SlotPlanner.plan(100, List.of(lane(5, 25, 1, 80), lane(5, 100, 1, 10))))
                .containsExactly(25, 10);
    }

    @Test
    @DisplayName("Spare slots split by weight when every lane wants more than its share")
    void weighted() {
        assertThat(SlotPlanner.plan(90, List.of(lane(0, 90, 2, 1000), lane(0, 90, 1, 1000))))
                .containsExactly(60, 30);
    }

    @Test
    @DisplayName("A lane with demand but no guarantee still gets a slot")
    void demandGetsOne() {
        assertThat(SlotPlanner.plan(3, List.of(lane(3, 3, 1, 50), lane(0, 3, 1, 1))))
                .containsExactly(3, 0); // no spare left: guarantees come first
        assertThat(SlotPlanner.plan(4, List.of(lane(3, 4, 1, 50), lane(0, 4, 1, 1))))
                .containsExactly(3, 1);
    }

    @Test
    @DisplayName("Rounding hands leftovers out by largest remainder; total equals capacity")
    void rounding() {
        int[] t = SlotPlanner.plan(10, List.of(lane(0, 10, 1, 100), lane(0, 10, 1, 100), lane(0, 10, 1, 100)));
        assertThat(Arrays.stream(t).sum()).isEqualTo(10);
        assertThat(t).containsExactly(4, 3, 3);
    }

    @Test
    @DisplayName("Same input, same plan")
    void stable() {
        List<LaneInput> in = List.of(lane(18, 130, 1, 90), lane(30, 130, 2, 70), lane(0, 40, 1, 40));
        assertThat(SlotPlanner.plan(130, in)).containsExactly(SlotPlanner.plan(130, in));
    }

    @Test
    @DisplayName("Guarantees above capacity are rejected")
    void guaranteesAboveCapacity() {
        assertThatThrownBy(() -> SlotPlanner.plan(10, List.of(lane(6, 10, 1, 0), lane(5, 10, 1, 0))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Random inputs: guarantees met, max respected, never above demand-or-guarantee, sum <= capacity")
    void randomInvariants() {
        Random rnd = new Random(42);
        for (int run = 0; run < 5_000; run++) {
            int capacity = 1 + rnd.nextInt(300);
            int lanes = 1 + rnd.nextInt(8);
            int guaranteeBudget = capacity;
            List<LaneInput> in = new ArrayList<>();
            for (int i = 0; i < lanes; i++) {
                int g = rnd.nextInt(Math.max(1, guaranteeBudget / lanes + 1));
                guaranteeBudget -= g;
                int max = g + 1 + rnd.nextInt(capacity);
                in.add(lane(g, max, 1 + rnd.nextInt(4), rnd.nextInt(capacity * 2)));
            }
            int[] t = SlotPlanner.plan(capacity, in);
            assertThat(Arrays.stream(t).sum()).isLessThanOrEqualTo(capacity);
            for (int i = 0; i < lanes; i++) {
                LaneInput l = in.get(i);
                assertThat(t[i]).isGreaterThanOrEqualTo(l.guaranteed());
                assertThat(t[i]).isLessThanOrEqualTo(Math.max(l.guaranteed(), Math.min(l.max(), Math.max(l.demand(), 1))));
            }
        }
    }
}
