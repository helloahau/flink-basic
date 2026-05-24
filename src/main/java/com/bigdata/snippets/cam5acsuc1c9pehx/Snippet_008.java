// Source: Day03-Flink四大基石 (cam5acsuc1c9pehx) snippet #8
package com.bigdata.snippets;

class cam5acsuc1c9pehxSnippet008 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        /*
         * 使用方式:
         * 先定义OutputTag对象(注意，必须new一个匿名内部类形式的OutputTag对象的实例)
         * 然后调用sideOutputLateData方法
         * // side output   OutputTag对象必须是匿名内部类的形式创建出来, 本质上得到的是OutputTag对象的一个匿名子类
         * OutputTag<Tuple2<String, Long>> outputTag = new OutputTag<Tuple2<String, Long>>("side output"){};
         * WindowedStream<Tuple2<String, Long>, Tuple, TimeWindow> sideOutputLateData =
         *         allowedLateness.sideOutputLateData(outputTag);
         */
    }
}
