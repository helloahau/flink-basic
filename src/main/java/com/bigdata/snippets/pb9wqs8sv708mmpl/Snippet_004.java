// Source: Day05-FlinkSQL (pb9wqs8sv708mmpl) snippet #4
package com.bigdata.snippets;

public class pb9wqs8sv708mmplSnippet004 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        //TableResult tableResult = tableEnv.executeSql("select * from table1");
        //tableResult.print();
         Table table = tableEnv.sqlQuery("select * from table1");
    }
}
