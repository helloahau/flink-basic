// Source: Day05-FlinkSQL (pb9wqs8sv708mmpl) snippet #7
package com.bigdata.snippets;

public class pb9wqs8sv708mmplSnippet007 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        // 提取Event中的timestamp和url作为表中的列
        Table sensorTable = tableEnv.fromDataStream(sensorDS, $("id"), $("vc"));
    }
}
