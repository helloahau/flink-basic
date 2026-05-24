// Source: Day02-Flink-普通API的使用 (ves1cik0ui24swqt) snippet #14
package com.bigdata.snippets;

class ves1cik0ui24swqtSnippet014 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        /*
         *
         * Exception in thread "main" org.apache.flink.api.common.typeutils.CompositeType$InvalidFieldReferenceException: Cannot reference field by position on PojoType<com.bigdata.day02.Ball, fields = [name: String, num: Integer]>Referencing a field by position is supported on tuples, case classes, and arrays. Additionally, you can select the 0th field of a primitive/basic type (e.g. int).
         * 	at org.apache.flink.streaming.util.typeutils.FieldAccessorFactory.getAccessor(FieldAccessorFactory.java:113)
         * 	at org.apache.flink.streaming.api.functions.aggregation.SumAggregator.<init>(SumAggregator.java:41)
         * 	at org.apache.flink.streaming.api.datastream.KeyedStream.sum(KeyedStream.java:797)
         * 	at com.bigdata.day02.Demo12.main(Demo12.java:35)
         *
         */
    }
}
