// Source: Day02-Flink-普通API的使用 (ves1cik0ui24swqt) snippet #8
package com.bigdata.snippets;

public class ves1cik0ui24swqtSnippet008 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        //用字段位置
        wordAndOne.keyBy(0, 1);

        //用KeySelector
        wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> getKey(Tuple2<String, Integer> value) throws Exception {
                return Tuple2.of(value.f0, value.f1);
            }
        });
    }
}
