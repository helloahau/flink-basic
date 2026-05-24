// Source: Day03-Flink四大基石 (cam5acsuc1c9pehx) snippet #6
package com.bigdata.snippets;

class cam5acsuc1c9pehxSnippet006 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        /*
         * Exception in thread "main" org.apache.flink.api.common.typeutils.CompositeType$InvalidFieldReferenceException:
         * Cannot reference field by field expression on GenericType<com.bigdata.day05.OrderInfo2>
         * Field expressions are only supported on POJO types, tuples, and case classes.
         * (See the Flink documentation on what is considered a POJO.)
         *   at org.apache.flink.streaming.util.typeutils.FieldAccessorFactory.getAccessor(FieldAccessorFactory.java:224)
         *   at org.apache.flink.streaming.api.functions.aggregation.SumAggregator.<init>(SumAggregator.java:53)
         *   at org.apache.flink.streaming.api.datastream.WindowedStream.sum(WindowedStream.java:688)
         *   at com.bigdata.day05._01WatermarkDemo.main(_01WatermarkDemo.java:103)
         */
    }
}
