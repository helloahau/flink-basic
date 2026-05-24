// Source: Day03-Flink四大基石 (cam5acsuc1c9pehx) snippet #2
package com.bigdata.snippets;

public class cam5acsuc1c9pehxSnippet002 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        // Non-Keyed Window
        stream
                .windowAll(...)         <-  不分组，将数据流中的所有元素分配到相应的窗口中
                [.trigger(...)]            <-  指定触发器Trigger（可选）
                [.evictor(...)]            <-  指定清除器Evictor(可选)
                .reduce/aggregate/process()      <-  窗口处理函数Window Function
    }
}
