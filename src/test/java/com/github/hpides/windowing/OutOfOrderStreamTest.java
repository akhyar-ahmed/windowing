package com.github.hpides.windowing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Test;


public class OutOfOrderStreamTest {

    @Test
    public void simpleTumblingSumOutOfOrder() {
        final Window tumblingWindow = new TumblingWindow(10);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(tumblingWindow, sumFn);

        windowOp.processEvent(new Event(1, 1));
        windowOp.processEvent(new Event(2, 2));
        windowOp.processEvent(new Event(3, 3));
        windowOp.processEvent(new Event(4, 4));
        windowOp.processEvent(new Event(12, 12));
        windowOp.processEvent(new Event(5, 5));
        windowOp.processEvent(new Event(15, 15));
        windowOp.processEvent(new Event(8, 8));

        final List<ResultWindow> results1 = windowOp.processWatermark(13);
        final List<ResultWindow> results2 = windowOp.processWatermark(22);

        assertThat(results1).containsExactly(new ResultWindow(0, 10, 23L));
        assertThat(results2).containsExactly(new ResultWindow(10, 20, 27L));
    }

    @Test
    public void simpleSlidingSumOutOfOrder() {
        final Window slidingWindow = new SlidingWindow(10, 5);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(slidingWindow, sumFn);

        windowOp.processEvent(new Event( 3,  30));
        windowOp.processEvent(new Event( 1,  10));
        windowOp.processEvent(new Event(11, 110));
        windowOp.processEvent(new Event( 9,  90));
        windowOp.processEvent(new Event( 7,  70));
        windowOp.processEvent(new Event( 5,  50));
        windowOp.processEvent(new Event(17, 170));

        final List<ResultWindow> results1 = windowOp.processWatermark(15);
        final List<ResultWindow> results2 = windowOp.processWatermark(22);

        assertThat(results1).containsExactly(new ResultWindow(0, 10, 250L), new ResultWindow(5, 15, 320L));
        assertThat(results2).containsExactly(new ResultWindow(10, 20, 280L));
    }

    @Test
    public void simpleSessionSumOutOfOrder() {
        final Window sessionWindow = new SessionWindow(6);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(sessionWindow, sumFn);

        windowOp.processEvent(new Event( 1,  10));
        windowOp.processEvent(new Event( 5,  70));
        windowOp.processEvent(new Event( 3,  30));
        windowOp.processEvent(new Event( 6,  50));
        windowOp.processEvent(new Event(13, 130));
        windowOp.processEvent(new Event(16, 120));
        windowOp.processEvent(new Event(14, 160));
        windowOp.processEvent(new Event(24, 240));

        final List<ResultWindow> results = windowOp.processWatermark(25);

        assertThat(results).containsExactly(new ResultWindow(1, 12, 160L), new ResultWindow(13, 22, 410L));
    }

    @Test
    public void simpleCountSumOutOfOrder() {
        final Window tumblingCountWindow = new TumblingCountWindow(3);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(tumblingCountWindow, sumFn);

        windowOp.processEvent(new Event( 1,  10));
        windowOp.processEvent(new Event( 3,  30));
        windowOp.processEvent(new Event( 7,  70));
        
        windowOp.processEvent(new Event( 5,  50));
        windowOp.processEvent(new Event(13, 130));
        windowOp.processEvent(new Event(12, 120));
        
        windowOp.processEvent(new Event(16, 160));
        windowOp.processEvent(new Event(24, 240));

        final List<ResultWindow> results = windowOp.processWatermark(25);

        assertThat(results).containsExactly(new ResultWindow(1, 3, 110L), new ResultWindow(4, 6, 300L));
    }

}
