package com.github.hpides.windowing;

import java.nio.file.SecureDirectoryStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;


// import org.junit.Test;


/**
 * This is the main class for the exercise. The job of the WindowAggregationOperator is to take in a stream of events,
 * group them into windows and perform aggregations on the data in those windows.
 *
 * The window types that need to be supported are:
 *   - TumblingWindow (time-basesd tumbling window)
 *   - SlidingWindow (time-based sliding window)
 *   - SessionWindow (time-based session window)
 *   - TumblingCountWindow (count-based tumbling window)
 *
 * The aggregation functions that need to be supported are:
 *   - SumAggregateFunction (sum aggregation)
 *   - AvgAggregateFunction (average aggregation)
 *   - MedianAggregateFunction (median aggregation)
 *
 * Read the individual documentation for the classes to understand them better. You should not have to change any code
 * in the aggregate or window classes.
 *
 * This class has two methods that need to be implemented. See the documentation below for more details on them.
 */
public class WindowAggregationOperator {

    private final Window window;
    private final AggregateFunction aggregateFunction;
    private String windowName;
    private Integer sessionStart;
    private Boolean sessionStartFlag;
    private List<Long> tumbling = new ArrayList<Long>();
    private Map<Integer, List<Long>> windows = new HashMap<Integer, List<Long>>();
    private Map<Integer, List<Integer>> intervals =new HashMap<Integer, List<Integer>>();
    private Map<Integer, Integer> windows_status = new HashMap<Integer, Integer>();
    private Map<Integer, Long> tumblingCount = new HashMap<Integer, Long>();
    private List <Long> tumblingCountValue;
    private long waterMarkValue;
    /**
     * This constructor is called to create a new WindowAggregationOperator with a given window type and aggregation
     * function. You will possibly need to extend this to initialize some variables but do not change the signature.
     *
     * @param window The window type that should be used in the windowed aggregation.
     * @param aggregateFunction The aggregation function that should be used in the windowed aggregation.
     */
    public WindowAggregationOperator(final Window window, final AggregateFunction aggregateFunction) {
        this.window = window;
        this.aggregateFunction = aggregateFunction;

        this.tumbling = new ArrayList<Long>();
        this.windows = new HashMap<Integer, List<Long>>();
        this.intervals =new HashMap<Integer, List<Integer>>();
        this.windows_status = new HashMap<Integer, Integer>();
        this.tumblingCount = new HashMap<Integer, Long>();
   
        this.windowName =  this.window.getClass().getName();

        this.tumbling.clear();
        for(int i = 0; i<100000;i++){
            this.tumbling.add(i,(long)-1);
        }
        this.windows.clear();
        this.intervals.clear();
        this.windows_status.clear();

        for(int i = 0; i<1000;i++){
            this.windows_status.put(i,1);
        }
        this.sessionStartFlag = true;
        this.tumblingCount.clear();
        this.tumblingCountValue = new ArrayList<Long>();
        this.waterMarkValue = 0;

        

    }

    /**
     * This method is a key method for the streaming operator. It takes in an Event and performs the necessary
     * computation for a windowed aggregation. You should implement the logic here to support the window types and
     * aggregation functions mentioned above. The order of the events based on their timestamps is not guaranteed,
     * i.e., they can arrive out-of-order. You should account for this in the more advanced test cases
     * (see OutOfOrderStreamTest) and our hidden test cases.
     *
     * @param event The event that should be processed.
     */
    public void processEvent(final Event event) {
        // YOUR CODE HERE
        //
        // YOUR CODE HERE END
        // System.out.println(this.windowName);
        String w1 = new String("com.github.hpides.windowing.TumblingWindow");
        String w2 = new String("com.github.hpides.windowing.SlidingWindow");
        String w3 = new String("com.github.hpides.windowing.SessionWindow");
        String w4 = new String("com.github.hpides.windowing.TumblingCountWindow");

        if(this.windowName.equals(w1)){
            long val = event.getValue();
            int time = (int)event.getTimestamp();

            if(this.waterMarkValue<(long)time){
                this.tumbling.set(time, val);
            }
        }
        else if(this.windowName.equals(w2)){
            long val = event.getValue();
            long time = event.getTimestamp();
            if(time>this.waterMarkValue){
                
                this.tumbling.set((int)time, val);
            }

        }
        else if(this.windowName.equals(w3)){            
            long val = event.getValue();
            int time = (int)event.getTimestamp();

            if(this.sessionStartFlag || time<this.sessionStart  ){
                this.sessionStartFlag = false;
                this.sessionStart = time;

            }
            if(this.waterMarkValue<(long)time){
                this.tumbling.set(time, val);
            }
        }
        else if(this.windowName.equals(w4)){
            long val = event.getValue();
            int time = (int)event.getTimestamp();
            // System.out.println(time);
            // System.out.println(val+1);
            this.tumblingCount.put(time, val);
            if(this.sessionStartFlag || time<this.sessionStart  ){
                this.sessionStartFlag = false;
                this.sessionStart = time;

            }
            this.tumblingCountValue.add(val);
        }

    }

    /**
     * This method triggers the complete windows up to the watermark. Remember, a watermark is a special event that
     * tells the operators that no events with a lower timestamp that the watermark will arrive in the future. If an
     * older event arrives, it should be ignored because the result has already been emitted.
     *
     * This method is responsible for producing the aggregated output values for the specified windows. As a watermark
     * can trigger multiple previous windows, the results are returned as a list. Read the ResultWindow documentation
     * for more details on the fields that it has and what they mean for different window types. The order of the
     * ResultWindows is determined by the endTime, earliest first.
     *
     * In this assignment, we will not use watermarks that are far in the future and cause many "empty" windows. A
     * watermark will only complete a window i) that has events in it or ii) that is empty but newer events have created
     * a newer window.
     *
     * As the endTime of a window is excluded from the window's events, a watermark at time 15 can cause a window ending
     * at 15 to be triggered.
     *
     * @param watermarkTimestamp The event-time timestamp of the watermark.
     * @return List of ResultWindows for all the windows that are complete based on the knowledge of the watermark.
     */
    public List<ResultWindow> processWatermark(final long watermarkTimestamp) {
        // this.result.clear();
        List<ResultWindow> result = new ArrayList<ResultWindow>();
        // System.out.println(this.result);
        int waterMark = (int) watermarkTimestamp;
        this.waterMarkValue = watermarkTimestamp;
        // System.out.println(waterMarkValue);
        String w1 = new String("com.github.hpides.windowing.TumblingWindow");
        String w2 = new String("com.github.hpides.windowing.SlidingWindow");
        String w3 = new String("com.github.hpides.windowing.SessionWindow");
        String w4 = new String("com.github.hpides.windowing.TumblingCountWindow");

        if(this.windowName.equals(w1)){
            TumblingWindow tw = (TumblingWindow) this.window;
            int windowSize = (int) tw.getLength();
            int count = 1;
            int windowNum = 0;
            int fromIndex = 0;
            int toIndex = 0;
            for(int i = 0;i<this.tumbling.size();i++){
                // System.out.println(this.windows.get(0));
                if(count == windowSize && i<waterMark){
                    toIndex = i+1;
                    List <Long> temp = this.tumbling.subList(fromIndex, toIndex);
                    this.windows.put(windowNum, temp);

                    List<Integer> interval = new ArrayList<>();
                    interval.add(fromIndex);
                    interval.add(toIndex);
                    this.intervals.put(windowNum, interval);
                    fromIndex = toIndex;
                    windowNum++;
                    count = 0;
                }
                else if(i == waterMark){

                    for (int l = 0;l<this.windows.size(); l++){
                        
                        List <Long> aggList = new ArrayList<Long>();
                        for (int k =0;k<this.windows.get(l).size();k++){
                            long a = -1;
                            if(this.windows.get(l).get(k) > a){
                                aggList.add(this.windows.get(l).get(k));
                            }
                        }


                        if(aggList == null){
                            if(l==0){
                                continue;
                            }
                            int curr_endValue = this.intervals.get(l).get(1);
                            int prev_startValue = this.intervals.get(l-1).get(0);
                            List <Integer> val1 = new ArrayList<>();
                            val1.add(prev_startValue);
                            val1.add(curr_endValue);
                            this.intervals.put(l-1, val1);
                            System.out.println(intervals);

                        }
                        
                        // System.out.print(this.windows_status.get(l));
                        if(this.windows_status.get(l)==1){
                            // System.out.println("Hi");
                            ResultWindow res;
                            res = new ResultWindow(this.intervals.get(l).get(0), this.intervals.get(l).get(1), this.aggregateFunction.aggregate(aggList));
                            result.add(res);
                            this.windows_status.put(l,0);
                        }
                    }
                    if((i == waterMark && tumbling.get(i) != -1) || (i == waterMark && tumbling.get(i) == -1 && tumbling.get(i+1)!= -1)){
                        toIndex = i+1;
                        List <Long> temp = this.tumbling.subList(fromIndex, toIndex);
                        this.windows.put(windowNum, temp);
                        List<Integer> interval1 = new ArrayList<>();
                        interval1.add(fromIndex);
                        interval1.add(toIndex);
                        this.intervals.put(windowNum, interval1);
                        this.windows_status.put(windowNum, 0);
                        fromIndex = toIndex;
                        windowNum++;
                        count = 0;
                    }  

                    
                        
                }
                    
                count++;
                if(i==waterMark){
                    break;
                }

            }
            
            return result;
        }
        else if(this.windowName.equals(w2)){
            SlidingWindow sw = (SlidingWindow) this.window;
            int windowSize = (int) sw.getLength();
            int slideSize = (int) sw.getSlide();
            int count = 1;
            int windowNum = 0;
            int fromIndex = 0;
            int toIndex = 0;
            int prev_i = 0;
            for(int i = 0;i<this.tumbling.size();i++){
                if(count == windowSize && i<waterMark){

                    toIndex = i+1;
                    List <Long> temp = this.tumbling.subList(fromIndex, toIndex);
                    this.windows.put(windowNum, temp);

                    List<Integer> interval = new ArrayList<>();
                    interval.add(fromIndex);
                    interval.add(toIndex);
                    this.intervals.put(windowNum, interval);
                    fromIndex = prev_i+slideSize;
                    windowNum++;
                    count = 0;
                    i = (prev_i+slideSize)-1;
                    prev_i = fromIndex;   

                }
                else if(i == waterMark){
                    
                    for (int l = 0;l<this.windows.size(); l++){
                        
                        List <Long> aggList = new ArrayList<Long>();
                        for (int k =0;k<this.windows.get(l).size();k++){
                            long a = -1;
                            if(this.windows.get(l).get(k) > a){
                                aggList.add(this.windows.get(l).get(k));
                            }
                        }


                        if(aggList == null){
                            if(l==0){
                                continue;
                            }
                            int curr_endValue = this.intervals.get(l).get(1);
                            int prev_startValue = this.intervals.get(l-1).get(0);
                            List <Integer> val1 = new ArrayList<>();
                            val1.add(prev_startValue);
                            val1.add(curr_endValue);
                            this.intervals.put(l-1, val1);
                            System.out.println(intervals);

                        }
                        if(this.windows_status.get(l)==1){
                            ResultWindow res;
                            res = new ResultWindow(this.intervals.get(l).get(0), this.intervals.get(l).get(1), this.aggregateFunction.aggregate(aggList));
                            result.add(res);
                            this.windows_status.put(l,0);
                        }

                    }
                    if((i == waterMark && tumbling.get(i) != -1) || (i == waterMark && tumbling.get(i) == -1 && tumbling.get(i+1)!= -1)){
                        // System.out.println("i equals watermark");
                        toIndex = i+1;

                        List <Long> temp = this.tumbling.subList(fromIndex, toIndex);
                        this.windows.put(windowNum, temp);
                        List<Integer> interval1 = new ArrayList<>();
                        interval1.add(fromIndex);
                        interval1.add(toIndex);
                        this.intervals.put(windowNum, interval1);
                        this.windows_status.put(windowNum, 0);
                        fromIndex = prev_i+slideSize;
                        windowNum++;
                        count = 1;
                        i = (prev_i+slideSize)-1;
                        prev_i = fromIndex;
                    }  

                    
                        
                }
                    
                count++;
                if(i==waterMark){
                    break;
                }

            }
            
            return result;
        }
        else if(this.windowName.equals(w3)){
            SessionWindow ssw = (SessionWindow) this.window;
            int gap = (int) ssw.getGap();
            int count = 1;
            int windowNum = 0;
            int fromIndex = this.sessionStart;
            int toIndex = 0;
            int count_gap = 0;
            int start_gap = this.sessionStart;
            for(int i = this.sessionStart;i<this.tumbling.size();i++){
                // System.out.println(this.sessionStart);
                // System.out.println(this.tumbling.get(i));
                // System.out.println(this.windows);
                if(this.tumbling.get(i) == (long)-1 && i < waterMark){
                    start_gap = i;
                    while(this.tumbling.get(i) == (long)-1 && i+1 < waterMark){
                        // System.out.println(this.tumbling.get(i));
                        
                        count_gap++;
                        i++;

                    }
                }
                // System.out.println(count_gap);
                // System.out.println(i);
                if(count_gap >=gap && i<waterMark){
                    
                    if(count_gap == gap){
                        // System.out.println("Hi");
                        toIndex = i;
                    }
                    else{
                        toIndex = (start_gap+gap)-1;
                    }
                    List <Long> temp = this.tumbling.subList(fromIndex, toIndex);
                    this.windows.put(windowNum, temp);

                    List<Integer> interval = new ArrayList<>();
                    interval.add(fromIndex);
                    interval.add(toIndex);
                    this.intervals.put(windowNum, interval);
                    // this.windows_status.put(windowNum, 0);
                    if(count_gap==gap){
                        fromIndex = toIndex;
                    }
                    else{
                        fromIndex = i;
                    }
                    
                    windowNum++;
                    count_gap = 0;
                }
                else if(i == waterMark){
                    
                    for (int l = 0;l<this.windows.size(); l++){
                        
                        List <Long> aggList = new ArrayList<Long>();
                        for (int k =0;k<this.windows.get(l).size();k++){
                            long a = -1;
                            if(this.windows.get(l).get(k) > a){
                                aggList.add(this.windows.get(l).get(k));
                            }
                        }
                        ///

                        if(aggList == null){
                            if(l==0){
                                continue;
                            }
                            // System.out.println(aggList);
                            int curr_endValue = this.intervals.get(l).get(1);
                            int prev_startValue = this.intervals.get(l-1).get(0);
                            List <Integer> val1 = new ArrayList<>();
                            val1.add(prev_startValue);
                            val1.add(curr_endValue);
                            this.intervals.put(l-1, val1);
                            System.out.println(intervals);

                        }
                        
                        // System.out.print(this.windows_status.get(l));
                        if(this.windows_status.get(l)==1){
                            // System.out.println("Hi");
                            ResultWindow res;
                            res = new ResultWindow(this.intervals.get(l).get(0), this.intervals.get(l).get(1), this.aggregateFunction.aggregate(aggList));
                            result.add(res);
                            this.windows_status.put(l,0);
                        }

                        
                        // System.out.println(this.result.size());
                        // System.out.println(this.windows_status.get(l));
                    }
                    if((i == waterMark && tumbling.get(i) != -1) || (i == waterMark && tumbling.get(i) == -1 && tumbling.get(i+1)!= -1)){
                        toIndex = i+1;
                        List <Long> temp = this.tumbling.subList(fromIndex, toIndex);
                        this.windows.put(windowNum, temp);
                        List<Integer> interval1 = new ArrayList<>();
                        interval1.add(fromIndex);
                        interval1.add(toIndex-1);
                        this.intervals.put(windowNum, interval1);
                        this.windows_status.put(windowNum, 0);
                        fromIndex = toIndex;
                        windowNum++;
                        count_gap = 0;
                    }  

                    
                        
                }
                    
                count++;
                if(i==waterMark){
                    break;
                }

            }
            
            return result;
        }
        else if(this.windowName.equals(w4)){
            TumblingCountWindow tcw = (TumblingCountWindow) this.window;
            int count_length = (int) tcw.getLength();
            int count = 0;
            int windowNum = 0;
            int fromIndex =1 ;
            int toIndex = 1;
            int start = 1;
            int end = 0;
            // System.out.println(this.tumblingCount.size());
            for(int i = 0;i<=this.tumblingCount.size();i++){
                
                if(count == count_length && i<waterMark){
                    toIndex = i+1;

                    // List <Long> temp = this.tumblingCount. subList(fromIndex, toIndex);
                    List<Long> temp = new ArrayList<Long>();
                    
                    for(int k = i-count_length;k<toIndex-1;k++){
                        long j = this.tumblingCountValue.get(k);
                        temp.add(j);
                    }
                    // System.out.println(temp);
                    this.windows.put(windowNum, temp);
                    // System.out.println(windows);
                    List<Integer> interval = new ArrayList<>();
                    end +=(count_length);
                    interval.add(start);
                    interval.add(end);
                    this.intervals.put(windowNum, interval);
                    fromIndex = toIndex;
                    windowNum++;
                    count = 0;
                    start += count_length;

                    for (int l = 0;l<this.windows.size(); l++){
                        
                        List <Long> aggList = new ArrayList<Long>();
                        for (int k =0;k<this.windows.get(l).size();k++){
                            long a = -1;
                            if(this.windows.get(l).get(k) > a){
                                aggList.add(this.windows.get(l).get(k));
                            }
                        }
                        ///

                        if(aggList == null){
                            if(l==0){
                                continue;
                            }
                            // System.out.println(aggList);
                            int curr_endValue = this.intervals.get(l).get(1);
                            int prev_startValue = this.intervals.get(l-1).get(0);
                            List <Integer> val1 = new ArrayList<>();
                            val1.add(prev_startValue);
                            val1.add(curr_endValue);
                            this.intervals.put(l-1, val1);
                            System.out.println(intervals);

                        }
                        
                        // System.out.print(this.windows_status.get(l));
                        if(this.windows_status.get(l)==1){
                            // System.out.println("Hi");
                            ResultWindow res;
                            res = new ResultWindow(this.intervals.get(l).get(0), this.intervals.get(l).get(1), this.aggregateFunction.aggregate(aggList));
                            result.add(res);
                            this.windows_status.put(l,0);
                        }

                    }
                    if((i == waterMark && tumbling.get(i) != -1) || (i == waterMark && tumbling.get(i) == -1 && tumbling.get(i+1)!= -1)){
                        toIndex = i+1;
                        List <Long> temp1 = this.tumbling.subList(fromIndex, toIndex);
                        this.windows.put(windowNum, temp1);

                        List<Integer> interval1 = new ArrayList<>();
                        end +=count_length;
                        interval1.add(start);
                        interval1.add(end);
                        this.intervals.put(windowNum, interval1);
                        // this.windows_status.put(windowNum, 0);
                        fromIndex = toIndex;
                        windowNum++;
                        count = 0;
                        start += count_length; 
                    }  
                }
                else if(i == waterMark){
                    
                    for (int l = 0;l<this.windows.size(); l++){
                        
                        List <Long> aggList = new ArrayList<Long>();
                        for (int k =0;k<this.windows.get(l).size();k++){
                            long a = -1;
                            if(this.windows.get(l).get(k) > a){
                                aggList.add(this.windows.get(l).get(k));
                            }
                        }
                        ///

                        if(aggList == null){
                            if(l==0){
                                continue;
                            }
                            // System.out.println(aggList);
                            int curr_endValue = this.intervals.get(l).get(1);
                            int prev_startValue = this.intervals.get(l-1).get(0);
                            List <Integer> val1 = new ArrayList<>();
                            val1.add(prev_startValue);
                            val1.add(curr_endValue);
                            this.intervals.put(l-1, val1);
                            System.out.println(intervals);

                        }
                        
                        // System.out.print(this.windows_status.get(l));
                        if(this.windows_status.get(l)==1){
                            // System.out.println("Hi");
                            ResultWindow res;
                            res = new ResultWindow(this.intervals.get(l).get(0), this.intervals.get(l).get(1), this.aggregateFunction.aggregate(aggList));
                            result.add(res);
                            this.windows_status.put(l,0);
                        }
                    }
                    if((i == waterMark && tumbling.get(i) != -1) || (i == waterMark && tumbling.get(i) == -1 && tumbling.get(i+1)!= -1)){
                        toIndex = i+1;
                        List <Long> temp = this.tumbling.subList(fromIndex, toIndex);
                        this.windows.put(windowNum, temp);

                        List<Integer> interval = new ArrayList<>();
                        end +=count_length;
                        interval.add(start);
                        interval.add(end);
                        this.intervals.put(windowNum, interval);
                        // this.windows_status.put(windowNum, 0);
                        fromIndex = toIndex;
                        windowNum++;
                        count = 0;
                        start += count_length; 
                    }  
                        
                }
                    
                
                if(i==waterMark){
                    break;
                }
                count++;

            }
            
            return result;
        }
        return null;
        // YOUR CODE HERE END
    }
}
