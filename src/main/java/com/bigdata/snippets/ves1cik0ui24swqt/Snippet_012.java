// Source: Day02-Flink-普通API的使用 (ves1cik0ui24swqt) snippet #12
package com.bigdata.snippets;

public class ves1cik0ui24swqtSnippet012 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        假如有如下数据：
        env.fromElements(
                        Tuple2.of("篮球", 1),
                        Tuple2.of("篮球", 2),
                        Tuple2.of("篮球", 3),
                        Tuple2.of("足球", 3),
                        Tuple2.of("足球", 2),
                        Tuple2.of("足球", 3)
                );
        求：篮球多少个，足球多少个？
    }
}
