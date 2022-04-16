package com.github.hpides.windowing;

import java.util.List;

/**
 * A simple sum aggregation.
 */
public class SumAggregateFunction implements AggregateFunction {

    @Override
    public Long aggregate(final List<Long> values) {
        return values.stream().reduce(Long::sum).orElse(null);
    }
}
