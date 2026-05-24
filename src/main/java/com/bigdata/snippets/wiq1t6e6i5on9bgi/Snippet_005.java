// Source: 大数据技术之Flink优化 (wiq1t6e6i5on9bgi) snippet #5
package com.bigdata.snippets;

class wiq1t6e6i5on9bgiSnippet005 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        /*
         *
         * // 初始化 table environment
         * TableEnvironment tEnv =...
         *
         * // 获取 tableEnv 的配置对象
         * Configuration configuration = tEnv.getConfig().getConfiguration();
         *
         * // 设置参数：(要结合 minibatch 一起使用)
         * // 开启 Split Distinct
         * configuration.setString("table.optimizer.distinct-agg.split.enabled", "true");
         * // 第一层打散的 bucket 数目
         * configuration.setString("table.optimizer.distinct-agg.split.bucket-num", "1024");
         *
         */
    }
}
