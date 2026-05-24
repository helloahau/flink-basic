// Source: Day06-Flink-高级部分 (iqbixxys9icdza0s) snippet #4
package com.bigdata.snippets;

public class iqbixxys9icdza0sSnippet004 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        //指定循环触发4次
        start.times(4);
        //可以执行触发次数范围,让循环执行次数在该范围之内
        start.times(2, 4);
    }
}
