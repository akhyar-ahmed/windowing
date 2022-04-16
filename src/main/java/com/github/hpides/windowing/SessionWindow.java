package com.github.hpides.windowing;

/**
 * This class represents a time-based session window, defined by the attribute `gap`.
 * See the documentation in Window for more detail on time and count semantics in this assignment.
 */
public class SessionWindow extends Window {

    private final long gap;

    public SessionWindow(final long gap) {
        this.gap = gap;
    }

    public long getGap() {
        return this.gap;
    }
}
