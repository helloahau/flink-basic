// Source: Day03-Flink四大基石 (cam5acsuc1c9pehx) snippet #5
package com.bigdata.snippets;

class cam5acsuc1c9pehxSnippet005 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        /*
         * 假如明天出去玩，09:00集合，最多允许迟到10分钟。
         * 08:50 张三来了    08:50 - 10 = 08:40
         * 09:05 李四来了    09:05 - 10 = 08:55
         * 09:35 王五来了    watermark = 09:35 - 10 = 09:25
         * 能否上到车上的条件是：watermark <= 时间点
         */
    }
}
