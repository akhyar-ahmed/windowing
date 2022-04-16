package com.github.hpides.windowing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.Test;

public class InOrderStreamTest {

    @Test
    public void simpleTumblingSumInOrder() {
        final Window tumblingWindow = new TumblingWindow(10);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(tumblingWindow, sumFn);

        windowOp.processEvent(new Event(1, 1));
        windowOp.processEvent(new Event(2, 2));
        windowOp.processEvent(new Event(3, 3));
        windowOp.processEvent(new Event(4, 4));
        windowOp.processEvent(new Event(5, 5));

        final List<ResultWindow> results = windowOp.processWatermark(10);

        assertThat(results).containsExactly(new ResultWindow(0, 10, 15L));
    }

    @Test
    public void simpleTumblingAvgInOrder() {
        final Window tumblingWindow = new TumblingWindow(10);
        final AggregateFunction avgFn = new AvgAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(tumblingWindow, avgFn);

        windowOp.processEvent(new Event(1, 1));
        windowOp.processEvent(new Event(2, 2));
        windowOp.processEvent(new Event(3, 3));
        windowOp.processEvent(new Event(4, 4));
        windowOp.processEvent(new Event(5, 5));

        final List<ResultWindow> results = windowOp.processWatermark(10);

        assertThat(results).containsExactly(new ResultWindow(0, 10, 3L));
    }

    @Test
    public void simpleTumblingMedianInOrder() {
        final Window tumblingWindow = new TumblingWindow(10);
        final AggregateFunction medianFn = new MedianAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(tumblingWindow, medianFn);

        windowOp.processEvent(new Event(1, 1));
        windowOp.processEvent(new Event(3, 3));
        windowOp.processEvent(new Event(5, 5));
        windowOp.processEvent(new Event(7, 7));
        windowOp.processEvent(new Event(9, 9));

        final List<ResultWindow> results = windowOp.processWatermark(10);

        assertThat(results).containsExactly(new ResultWindow(0, 10, 5L));
    }

    @Test
    public void simpleSlidingSumInOrder() {
        final Window slidingWindow = new SlidingWindow(10, 5);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(slidingWindow, sumFn);

        windowOp.processEvent(new Event( 1,  10));
        windowOp.processEvent(new Event( 3,  30));
        windowOp.processEvent(new Event( 5,  50));
        windowOp.processEvent(new Event( 7,  70));
        windowOp.processEvent(new Event( 9,  90));
        windowOp.processEvent(new Event(11, 110));
        windowOp.processEvent(new Event(17, 170));

        final List<ResultWindow> results = windowOp.processWatermark(15);

        assertThat(results).containsExactly(new ResultWindow(0, 10, 250L), new ResultWindow(5, 15, 320L));
    }

    @Test
    public void simpleSessionSumInOrder() {
        final Window sessionWindow = new SessionWindow(4);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(sessionWindow, sumFn);

        windowOp.processEvent(new Event( 1,  10));
        windowOp.processEvent(new Event( 3,  30));
        windowOp.processEvent(new Event( 5,  50));
        windowOp.processEvent(new Event( 7,  70));
        windowOp.processEvent(new Event(12, 120));
        windowOp.processEvent(new Event(13, 130));
        windowOp.processEvent(new Event(16, 160));
        windowOp.processEvent(new Event(24, 240));

        final List<ResultWindow> results = windowOp.processWatermark(25);

        assertThat(results).containsExactly(new ResultWindow(1, 11, 160L), new ResultWindow(12, 20, 410L));
    }

    @Test
    public void simpleCountSumInOrder() {
        final Window tumblingCountWindow = new TumblingCountWindow(3);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(tumblingCountWindow, sumFn);

        windowOp.processEvent(new Event( 1,  10));
        windowOp.processEvent(new Event( 3,  30));
        windowOp.processEvent(new Event( 5,  50));
        windowOp.processEvent(new Event( 7,  70));
        windowOp.processEvent(new Event(12, 120));
        windowOp.processEvent(new Event(13, 130));
        windowOp.processEvent(new Event(16, 160));
        windowOp.processEvent(new Event(24, 240));

        final List<ResultWindow> results = windowOp.processWatermark(25);

        assertThat(results).containsExactly(new ResultWindow(1, 3, 90L), new ResultWindow(4, 6, 320L));
    }
}
