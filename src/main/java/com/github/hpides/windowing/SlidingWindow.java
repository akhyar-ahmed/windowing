package com.github.hpides.windowing;

/**
 * This class represents a time-based sliding window, defined by the attributes `length` and `slide`.
 * See the documentation in Window for more detail on time and count semantics in this assignment.
 */
public class SlidingWindow extends Window {

    private final long length;
    private final long slide;

    public SlidingWindow(final long length, final long slide) {
        this.length = length;
        this.slide = slide;
    }

    public long getLength() {
        return this.length;
    }

    public long getSlide() {
        return this.slide;
    }
}
