package com.github.hpides.windowing;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.hpides.windowing.AggregateFunction;
import com.github.hpides.windowing.Event;
import com.github.hpides.windowing.ResultWindow;
import com.github.hpides.windowing.SlidingWindow;
import com.github.hpides.windowing.SumAggregateFunction;
import com.github.hpides.windowing.TumblingWindow;
import com.github.hpides.windowing.Window;
import com.github.hpides.windowing.WindowAggregationOperator;
import java.util.List;
import org.junit.Test;

public class TumblingSlidingTest {

    @Test
    public void emptyTumblingWindow() {
        final Window tumblingWindow = new TumblingWindow(1000);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(tumblingWindow, sumFn);

        windowOp.processEvent(new Event(  10, 100));
        windowOp.processEvent(new Event(2010, 200));

        final List<ResultWindow> results = windowOp.processWatermark(3000);
        assertThat(results).containsExactly(
                new ResultWindow(   0, 1000, 100L),
                new ResultWindow(1000, 2000, null),
                new ResultWindow(2000, 3000, 200L)
        );
    }

    @Test
    public void emptySlidingWindow() {
        final Window slidingWindow = new SlidingWindow(1000, 500);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(slidingWindow, sumFn);

        windowOp.processEvent(new Event(  10, 100));
        windowOp.processEvent(new Event(1610, 200));

        final List<ResultWindow> results = windowOp.processWatermark(2000);
        assertThat(results).containsExactly(
                new ResultWindow(   0, 1000, 100L),
                new ResultWindow( 500, 1500, null),
                new ResultWindow(1000, 2000, 200L)
        );
    }

    @Test
    public void createPreviousWindow() {
        final Window tumblingWindow = new TumblingWindow(1000);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(tumblingWindow, sumFn);

        windowOp.processEvent(new Event(1020, 100));
        windowOp.processEvent(new Event( 980, 200));

        final List<ResultWindow> results = windowOp.processWatermark(2000);
        assertThat(results).containsExactly(
                new ResultWindow(   0, 1000, 200L),
                new ResultWindow(1000, 2000, 100L)
        );
    }

    @Test
    public void manySlidingWindows() {
        final Window slidingWindow = new SlidingWindow(1000, 500);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(slidingWindow, sumFn);

        windowOp.processEvent(new Event( 100, 1));
        windowOp.processEvent(new Event( 600, 2));
        windowOp.processEvent(new Event(1100, 3));
        windowOp.processEvent(new Event(1600, 4));
        windowOp.processEvent(new Event(2100, 5));
        windowOp.processEvent(new Event(2600, 6));
        windowOp.processEvent(new Event(3100, 7));
        windowOp.processEvent(new Event(3600, 8));
        windowOp.processEvent(new Event(4100, 9));

        final List<ResultWindow> results = windowOp.processWatermark(5000);
        assertThat(results).containsExactly(
                new ResultWindow(   0, 1000,  3L),
                new ResultWindow( 500, 1500,  5L),
                new ResultWindow(1000, 2000,  7L),
                new ResultWindow(1500, 2500,  9L),
                new ResultWindow(2000, 3000, 11L),
                new ResultWindow(2500, 3500, 13L),
                new ResultWindow(3000, 4000, 15L),
                new ResultWindow(3500, 4500, 17L),
                new ResultWindow(4000, 5000,  9L)
        );
    }
}
