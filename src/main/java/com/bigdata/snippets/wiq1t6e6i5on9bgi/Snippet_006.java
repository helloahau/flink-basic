// Source: 大数据技术之Flink优化 (wiq1t6e6i5on9bgi) snippet #6
package com.bigdata.snippets;

class wiq1t6e6i5on9bgiSnippet006 {
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
         * // 设置参数：
         * // 开启 miniBatch
         * configuration.setString("table.exec.mini-batch.enabled", "true");
         * // 批量输出的间隔时间
         * configuration.setString("table.exec.mini-batch.allow-latency", "5 s");
         * // 防止 OOM 设置每个批次最多缓存数据的条数，可以设为 2 万条
         * configuration.setString("table.exec.mini-batch.size", "20000");
         * // 开启 LocalGlobal
         * configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
         * // 开启 Split Distinct
         * configuration.setString("table.optimizer.distinct-agg.split.enabled", "true");
         * // 第一层打散的 bucket 数目
         * configuration.setString("table.optimizer.distinct-agg.split.bucket-num", "1024");
         * // 指定时区
         * configuration.setString("table.local-time-zone", "Asia/Shanghai");
         *
         *
         * tEnv.executeSql("
         *          xxxxsql            
         *     ");
         *
         */
    }
}
