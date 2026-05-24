// Source: Day05-FlinkSQL (pb9wqs8sv708mmpl) snippet #8
package com.bigdata.snippets;

public class pb9wqs8sv708mmplSnippet008 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        // 将timestamp字段重命名为ts
        Table sensorTable = tableEnv.fromDataStream(sensorDS, $("id").as("sid"), $("vc"));
    }
}
