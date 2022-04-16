package com.github.hpides.windowing;

/**
 * This simple class represents an event in our stream. It has a simple numeric value and an event-time timestamp.
 */
public class Event {
    private final long value;
    private final long timestamp;

    public Event(final long timestamp, final long value) {
        this.value = value;
        this.timestamp = timestamp;
    }

    public long getValue() {
        return this.value;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "value=" + value +
                ", timestamp=" + timestamp +
                '}';
    }
}
