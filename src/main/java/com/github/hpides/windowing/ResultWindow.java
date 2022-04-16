package com.github.hpides.windowing;

import java.util.Objects;

/**
 * This class represents an aggregated result for a given window. It has the window's start and end timestamp, as well
 * as the aggregate value. Note here that the `aggregate` is an object and not a primitive because it can be null. A
 * window with no events in it will cause a null aggregate value.
 *
 * The `startTime` and `endTime` have different meanings for the different window types.
 *
 * For TumblingWindows and SlidingWindows, they represent the exact bounds, e.g., the first TumblingWindow with length
 * 10 will have a `startTime` of 0 and an `endTime` of 10. The next window will have 10 and 20, then 20 and 30, and so on.
 *
 * For SessionWindows, the `startTime` represents the event timestamp of the first event in the session and the
 * `endTime` represents the end of the session including the gap. So a session window with gap 5 and events at times 2
 * and 4 will lead to a startTime of 2 and an endTime of 9.
 *
 * For TumblingCountWindows, the startTime and endTime represent the count positions of the events. A tumbling count
 * window with a length of 3 will result in the start and end times: 1-3, 4-6, 7-9, ...
 *
 * Examples of the expected times can be seen in our test cases.
 *
 */
public class ResultWindow {
    private final long startTime;
    private final long endTime;
    private final Long aggregate;

    public ResultWindow(final long startTime, final long endTime, final Long aggregate) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.aggregate = aggregate;
    }

    public long getStartTime() {
        return this.startTime;
    }

    public long getEndTime() {
        return this.endTime;
    }

    public Long getAggregate() {
        return this.aggregate;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        final ResultWindow that = (ResultWindow) o;
        return this.startTime == that.startTime &&
                this.endTime == that.endTime &&
                Objects.equals(this.aggregate, that.aggregate);
    }

    @Override
    public String toString() {
        return "ResultWindow{" +
                "startTime=" + startTime +
                ", endTime=" + endTime +
                ", aggregate=" + aggregate +
                '}';
    }
}
