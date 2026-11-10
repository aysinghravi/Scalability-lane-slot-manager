package com.acuver.aekyam.slotpoc.core;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** The slots held by one accepted delivery. Closing releases them once, in reverse order. */
public final class SlotPermit implements AutoCloseable {

    private final List<SlotPool> held;
    private final AtomicBoolean released = new AtomicBoolean();

    SlotPermit(List<SlotPool> held) {
        this.held = List.copyOf(held);
    }

    @Override
    public void close() {
        if (released.compareAndSet(false, true)) releaseAll(held);
    }

    static void releaseAll(List<SlotPool> pools) {
        for (int i = pools.size() - 1; i >= 0; i--) pools.get(i).release();
    }
}
