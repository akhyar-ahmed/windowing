package com.github.hpides.windowing;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A simple median aggregation. For an uneven number of events, it returns the middle one. For an even number, it
 * returns the lower of the two middle events.
 */
public class MedianAggregateFunction implements AggregateFunction {
    @Override
    public Long aggregate(final List<Long> values) {
        if (values.isEmpty()) {
            return null;
        }

        final List<Long> sortedValues = values.stream().sorted().collect(Collectors.toList());
        return sortedValues.get(sortedValues.size() / 2);
    }
}
