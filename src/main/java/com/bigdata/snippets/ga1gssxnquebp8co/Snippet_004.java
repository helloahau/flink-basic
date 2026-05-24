// Source: Day04-Flink四大基石2 (ga1gssxnquebp8co) snippet #4
package com.bigdata.snippets;

public class ga1gssxnquebp8coSnippet004 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        出现这个问题：
        [root@bigdata01 app]# flink run -c com.bigdata.day05.Demo02 -s hdfs://bigdata01:9820/flink/checkpoint/e6042dae1a29a23c0ffdb0dcc8820b0e/chk-259 FlinkDemo-1.0-SNAPSHOT.jar 
        java.lang.NoSuchMethodError: org.apache.commons.cli.CommandLine.hasOption(Lorg/apache/commons/cli/Option;)Z
    }
}
