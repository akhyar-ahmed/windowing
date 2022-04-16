package com.github.hpides.windowing;

import java.util.List;

/**
 * A simple integer-based average aggregation. To keep the interfaces simple, the average returns a long and not a
 * double so we lose a bit of precision here which is not relevant in this assignment.
 */
public class AvgAggregateFunction implements AggregateFunction {
    @Override
    public Long aggregate(final List<Long> values) {
        final Long sum = new SumAggregateFunction().aggregate(values);
        if (sum == null) {
            return null;
        }

        return sum / values.size();
    }
}
