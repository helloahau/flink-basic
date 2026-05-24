// Source: Day03-Flink四大基石 (cam5acsuc1c9pehx) snippet #3
package com.bigdata.snippets;

public class cam5acsuc1c9pehxSnippet003 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        实现方法（常见的增量聚合函数如下）：
        reduce(reduceFunction)
        aggregate(aggregateFunction)
        sum()
        min()
        max()

        reduce接受两个相同类型的输入，生成一个同类型输出，所以泛型就一个 <T>
        maxBy、minBy、sum这3个底层都是由reduce实现的
        aggregate的输入值、中间结果值、输出值它们3个类型可以各不相同，泛型有<T, ACC, R>
    }
}
