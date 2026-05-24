// Source: Day02-Flink-普通API的使用 (ves1cik0ui24swqt) snippet #2
package com.bigdata.snippets;

class ves1cik0ui24swqtSnippet002 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        /*
         *
         * Rich 类型的Source可以比非Rich的多出有：
         *     - open方法，实例化的时候会执行一次，多个并行度会执行多次的哦（因为是多个实例了）
         *     - close方法，销毁实例的时候会执行一次，多个并行度会执行多次的哦
         *     - getRuntimeContext 方法可以获得当前的Runtime对象（底层API）
         *
         */
    }
}
