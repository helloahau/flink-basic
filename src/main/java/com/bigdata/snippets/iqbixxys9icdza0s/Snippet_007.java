// Source: Day06-Flink-高级部分 (iqbixxys9icdza0s) snippet #7
package com.bigdata.snippets;

class iqbixxys9icdza0sSnippet007 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        /*
         * 第一波，正常数据:
         * {"userId":"1","type":"create","ts":"2023-07-18 10:10:10"}
         * {"userId":"1","type":"create","ts":"2023-07-18 10:14:10"}
         * {"userId":"1","type":"pay","ts":"2023-07-18 10:14:11"}
         * {"userId":"1","type":"pay","ts":"2023-07-18 10:14:11"}
         * {"userId":"1","type":"xxx","ts":"2023-07-18 10:14:12"}
         * {"userId":"1","type":"xxx","ts":"2023-07-18 10:24:12"}
         *
         * 第二波：非正常数据
         * {"userId":"1","type":"create","ts":"2023-07-18 10:24:13"}
         * {"userId":"1","type":"pay","ts":"2023-07-18 10:34:15"}
         * {"userId":"1","type":"pay","ts":"2023-07-18 10:44:15"}
         */
    }
}
