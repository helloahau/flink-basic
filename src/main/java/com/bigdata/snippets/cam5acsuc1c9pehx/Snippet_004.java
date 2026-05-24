// Source: Day03-Flink四大基石 (cam5acsuc1c9pehx) snippet #4
package com.bigdata.snippets;

public class cam5acsuc1c9pehxSnippet004 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        实现方法
        apply(windowFunction）
        process(processWindowFunction)

        全量聚合: 窗口需要维护全部原始数据，窗口触发进行全量聚合。
        ProcessWindowFunction一次性迭代整个窗口里的所有元素，比较重要的一个对象是Context，可以获取到事件和状态信息，这样我们就可以实现更加灵活的控制，该算子会浪费很多性能，主要原因是不增量计算，要缓存整个窗口然后再去处理，所以要设计好内存。
    }
}
