// Source: Day03-Flink四大基石 (cam5acsuc1c9pehx) snippet #7
package com.bigdata.snippets;

class cam5acsuc1c9pehxSnippet007 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        /*
         * Caused by: java.lang.RuntimeException: Record has Long.MIN_VALUE timestamp (= no timestamp marker).
         * Is the time characteristic set to 'ProcessingTime', or did you forget to call
         * 'DataStream.assignTimestampsAndWatermarks(...)'?
         *   at org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows.assignWindows(TumblingEventTimeWindows.java:83)
         *   at org.apache.flink.streaming.runtime.operators.windowing.WindowOperator.processElement(WindowOperator.java:302)
         */
    }
}
