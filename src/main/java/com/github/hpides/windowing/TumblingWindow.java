package com.github.hpides.windowing;

/**
 * This class represents a time-based tumbling window, defined by the attribute `length`.
 * See the documentation in Window for more detail on time and count semantics in this assignment.
 */
public class TumblingWindow extends Window {

    private final long length;

    public TumblingWindow(final long length) {
        this.length = length;
    }

    public long getLength() {
        return this.length;
    }
}
