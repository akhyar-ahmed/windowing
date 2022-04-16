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

public class TooLateStreamTest {
    @Test
    public void ignoreLateTumblingEvent() {
        final Window tumblingWindow = new TumblingWindow(1000);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(tumblingWindow, sumFn);

        windowOp.processEvent(new Event( 10, 1));
        windowOp.processEvent(new Event(100, 2));
        assertThat(windowOp.processWatermark(50)).isEmpty();

        // This should be ignored because it is too late
        windowOp.processEvent(new Event(20, 1000));

        windowOp.processEvent(new Event(200, 5));

        final List<ResultWindow> results = windowOp.processWatermark(1000);
        assertThat(results).containsExactly(new ResultWindow(0, 1000, 8L));
    }

    @Test
    public void ignoreLateSlidingEvent() {
        final Window sliding = new SlidingWindow(1000, 500);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(sliding, sumFn);

        windowOp.processEvent(new Event( 10, 1));
        windowOp.processEvent(new Event(100, 2));
        assertThat(windowOp.processWatermark(50)).isEmpty();

        // This should be ignored because it is too late
        windowOp.processEvent(new Event(20, 1000));

        windowOp.processEvent(new Event(200, 5));

        final List<ResultWindow> results = windowOp.processWatermark(1000);
        assertThat(results).containsExactly(new ResultWindow(0, 1000, 8L));
    }

    @Test
    public void ignoreLateSessionEvent() {
        final Window sessionWindow = new SessionWindow(300);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(sessionWindow, sumFn);

        windowOp.processEvent(new Event( 10, 1));
        windowOp.processEvent(new Event(100, 2));
        assertThat(windowOp.processWatermark(50)).isEmpty();

        // This should be ignored because it is too late
        windowOp.processEvent(new Event(20, 1000));

        windowOp.processEvent(new Event(200, 5));

        final List<ResultWindow> results = windowOp.processWatermark(1000);
        assertThat(results).containsExactly(new ResultWindow(10, 500, 8L));
    }

    @Test
    public void ignoreLateCountEvent() {
        final Window countWindow = new TumblingCountWindow(3);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(countWindow, sumFn);

        windowOp.processEvent(new Event( 10, 1));
        windowOp.processEvent(new Event(100, 2));
        assertThat(windowOp.processWatermark(50)).isEmpty();
        windowOp.processEvent(new Event(20, 1000));

        windowOp.processEvent(new Event(200, 5));

        final List<ResultWindow> results = windowOp.processWatermark(1000);

        assertThat(results).containsExactly(new ResultWindow(1, 3, 1003L));        
    }
}
