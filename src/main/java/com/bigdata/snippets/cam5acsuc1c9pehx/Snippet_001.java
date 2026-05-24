// Source: Day03-Flink四大基石 (cam5acsuc1c9pehx) snippet #1
package com.bigdata.snippets;

class cam5acsuc1c9pehxSnippet001 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        /*
         * Keyed Window
         * stream
         *         .keyBy(...)              <-  按照一个Key进行分组
         *         .window(...)            <-  将数据流中的元素分配到相应的窗口中
         *         [.trigger(...)]            <-  指定触发器Trigger（可选）
         *         [.evictor(...)]            <-  指定清除器Evictor(可选)
         *         .reduce/aggregate/process/apply()      <-  窗口处理函数Window Function
         */
    }
}
