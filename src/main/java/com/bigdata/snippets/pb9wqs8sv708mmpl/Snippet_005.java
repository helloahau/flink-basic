// Source: Day05-FlinkSQL (pb9wqs8sv708mmpl) snippet #5
package com.bigdata.snippets;

public class pb9wqs8sv708mmplSnippet005 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        // 注册表，用于输出数据到外部系统
        tableEnv.executeSql("CREATE TABLE OutputTable ... WITH ( 'connector' = ... )");

        // 经过查询转换，得到结果表
        Table result = ...

        // 将结果表写入已注册的输出表中
        result.executeInsert("OutputTable");
    }
}
