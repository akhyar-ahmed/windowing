package com.github.hpides.windowing;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.hpides.windowing.AggregateFunction;
import com.github.hpides.windowing.Event;
import com.github.hpides.windowing.ResultWindow;
import com.github.hpides.windowing.SessionWindow;
import com.github.hpides.windowing.SumAggregateFunction;
import com.github.hpides.windowing.TumblingCountWindow;
import com.github.hpides.windowing.Window;
import com.github.hpides.windowing.WindowAggregationOperator;
import java.util.List;
import org.junit.Test;

public class WindowShiftTest {
    @Test
    public void sessionWindowMerge() {
        final Window sessionWindow = new SessionWindow(4);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(sessionWindow, sumFn);

        windowOp.processEvent(new Event( 1,  10));
        windowOp.processEvent(new Event( 7,  70));
        windowOp.processEvent(new Event( 3,  30));
        windowOp.processEvent(new Event( 5,  50));
        windowOp.processEvent(new Event(13, 130));
        windowOp.processEvent(new Event(16, 160));
        windowOp.processEvent(new Event(12, 120));
        windowOp.processEvent(new Event(24, 240));

        final List<ResultWindow> results = windowOp.processWatermark(25);
        assertThat(results).containsExactly(new ResultWindow(1, 11, 160L), new ResultWindow(12, 20, 410L));
    }

    @Test
    public void windowBounds() {
        final Window sessionWindow = new SessionWindow(4);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(sessionWindow, sumFn);

        windowOp.processEvent(new Event(1, 10));
        windowOp.processEvent(new Event(3, 30));
        windowOp.processEvent(new Event(7, 70));
        windowOp.processEvent(new Event(9, 90));

        final List<ResultWindow> results = windowOp.processWatermark(15);
        assertThat(results).containsExactly(new ResultWindow(1, 7, 40L), new ResultWindow(7, 13, 160L));
    }

    @Test
    public void countWindowCascade() {
        final Window countWindow = new TumblingCountWindow(3);
        final AggregateFunction sumFn = new SumAggregateFunction();
        final WindowAggregationOperator windowOp = new WindowAggregationOperator(countWindow, sumFn);

        windowOp.processEvent(new Event(200, 2));
        windowOp.processEvent(new Event(300, 3));
        windowOp.processEvent(new Event(400, 4));
        
        windowOp.processEvent(new Event(500, 5));
        windowOp.processEvent(new Event(600, 6));
        windowOp.processEvent(new Event(700, 7));
        
        windowOp.processEvent(new Event(800, 8));
        windowOp.processEvent(new Event(900, 9));
        windowOp.processEvent(new Event(100, 1));

        final List<ResultWindow> results = windowOp.processWatermark(2000);
        assertThat(results).containsExactly(
                new ResultWindow(1, 3,  9L),
                new ResultWindow(4, 6, 18L),
                new ResultWindow(7, 9, 18L)
        );
    }
}
