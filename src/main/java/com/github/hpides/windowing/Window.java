package com.github.hpides.windowing;

/**
 * Parent class for all window types. This does nothing except let us pass the windows in via the same type.
 *
 * In this assignment, time starts at 0.
 * In all time-based windows, the intervals they cover are defined as: [startTime, endTime), i.e., they include the
 * startTime but exclude the endTime. A tumbling window from 10-20 will include events with a timestamp of 10, 12, 17,
 * 19, but not 20. 20 is then part of the next window. The same is applicable to sliding windows and session windows.
 * For session windows, this also means that an event at time 9 does not(!) belong to a window with a gap of 5 and an
 * element with a timestamp of 4. In that case, 9 would be the endTime of the session window, as the endTime is an open
 * interval and excluded, as well as the start time of a new session window.
 *
 * Count-based windows have a slightly different notion of start and end times. To avoid to many fields, we use the
 * `startTime` and `endTime` fields for both time and counts. Counting starts at 1 and represents a closed interval on
 * both sides, e.g. a tumbling count window with length 3 will start at 1 and end at 3 (as it has events 1, 2, and 3 in
 * it). The next window starts at 4 and ends at 6, and so on. 
 */
public class Window {}
