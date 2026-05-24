// Source: 大数据技术之Flink优化 (wiq1t6e6i5on9bgi) snippet #2
package com.bigdata.snippets;

public class wiq1t6e6i5on9bgiSnippet002 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        // API 指定
        tableEnv.getConfig().setIdleStateRetention(Duration.ofHours(1));
        // 参数指定
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.state.ttl", "1 h");
    }
}
