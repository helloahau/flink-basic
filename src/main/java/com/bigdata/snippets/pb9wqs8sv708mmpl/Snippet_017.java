// Source: Day05-FlinkSQL (pb9wqs8sv708mmpl) snippet #17
package com.bigdata.snippets;

public class pb9wqs8sv708mmplSnippet017 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        Exception in thread "main" org.apache.flink.table.api.ValidationException: Too many fields referenced from an atomic type.
        	at org.apache.flink.table.typeutils.FieldInfoUtils.extractFieldInfoFromAtomicType(FieldInfoUtils.java:473)

        假如遇到以上错误，请检查wc实体类中，是否有  无参构造方法。
    }
}
