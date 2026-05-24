// Source: Day03-Flink四大基石 (cam5acsuc1c9pehx) snippet #9
package com.bigdata.snippets;

public class cam5acsuc1c9pehxSnippet009 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        用法:
        DataStream<Tuple2<String, Long>> sideOutput = result.getSideOutput(outputTag);
        // 对得到的保存超级迟到数据的DataStream进行处理
        sideOutput.print("late>>>");
    }
}
