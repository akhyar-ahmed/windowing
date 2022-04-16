package com.github.hpides.windowing;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.hpides.windowing.AggregateFunction;
import com.github.hpides.windowing.Event;
import com.github.hpides.windowing.ResultWindow;
import com.github.hpides.windowing.SessionWindow;
import com.github.hpides.windowing.SlidingWindow;
import com.github.hpides.windowing.SumAggregateFunction;
import com.github.hpides.windowing.TumblingCountWindow;
import com.github.hpides.windowing.TumblingWindow;
import com.github.hpides.windowing.Window;
import com.github.hpides.windowing.WindowAggregationOperator;
import java.util.List;
import org.junit.Test;

public class OutOfOrderFirstEventTest {
    @Test
    public void tumblingWindow() {
        final Window tumblingWindow = new TumblingWindow(1000);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(tumblingWindow, sumFn);

        windowOp.processEvent(new Event(200, 2));
        windowOp.processEvent(new Event(100, 1));

        windowOp.processEvent(new Event(1200, 3));
        windowOp.processEvent(new Event(1100, 4));

        final List<ResultWindow> results = windowOp.processWatermark(2000);
        assertThat(results).containsExactly(
                new ResultWindow(   0, 1000, 3L),
                new ResultWindow(1000, 2000, 7L)
        );
    }

    @Test
    public void slidingWindow() {
        final Window slidingWindow = new SlidingWindow(1000, 500);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(slidingWindow, sumFn);

        windowOp.processEvent(new Event(200, 2));
        windowOp.processEvent(new Event(100, 1));

        windowOp.processEvent(new Event(1200, 3));
        windowOp.processEvent(new Event(1100, 4));

        final List<ResultWindow> results = windowOp.processWatermark(2000);
        assertThat(results).containsExactly(
                new ResultWindow(   0, 1000, 3L),
                new ResultWindow( 500, 1500, 7L),
                new ResultWindow(1000, 2000, 7L)
        );
    }

    @Test
    public void sessionWindow() {
        final Window sessionWindow = new SessionWindow(500);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(sessionWindow, sumFn);

        windowOp.processEvent(new Event(200, 2));
        windowOp.processEvent(new Event(100, 1));

        windowOp.processEvent(new Event(1200, 3));
        windowOp.processEvent(new Event(1100, 4));

        final List<ResultWindow> results = windowOp.processWatermark(2000);
        assertThat(results).containsExactly(
                new ResultWindow( 100,  700, 3L),
                new ResultWindow(1100, 1700, 7L)
        );
    }

    @Test
    public void countWindow() {
        final Window countWindow = new TumblingCountWindow(3);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(countWindow, sumFn);

        windowOp.processEvent(new Event(200, 2));
        windowOp.processEvent(new Event(100, 1));
        windowOp.processEvent(new Event(1200, 3));
        
        windowOp.processEvent(new Event(1100, 4));

        final List<ResultWindow> results = windowOp.processWatermark(2000);
        
        assertThat(results).containsExactly(new ResultWindow(1, 3, 6L));
        
    }
}
