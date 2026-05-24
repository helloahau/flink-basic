// Source: Day05-FlinkSQL (pb9wqs8sv708mmpl) snippet #20
package com.bigdata.snippets;

public class pb9wqs8sv708mmplSnippet020 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        Caused by: org.apache.flink.table.api.ValidationException: Could not find any factory for identifier 'json' that implements 'org.apache.flink.table.factories.DeserializationFormatFactory' in the classpath.

        Available factory identifiers are:

        raw
        	at org.apache.flink.table.factories.FactoryUtil.discoverFactory(FactoryUtil.java:319)
        	at org.apache.flink.table.factories.FactoryUtil$TableFactoryHelper.discoverOptionalFormatFactory(FactoryUtil.java:751)
    }
}
