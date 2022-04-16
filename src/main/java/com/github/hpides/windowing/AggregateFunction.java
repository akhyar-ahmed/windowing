package com.github.hpides.windowing;

import java.util.List;

/**
 * A simple interface for aggregation functions.
 * If the values provided are empty, the `aggregate` method should return null. This is the case for empty windows.
 */
public interface AggregateFunction {
    Long aggregate(List<Long> values);
}
