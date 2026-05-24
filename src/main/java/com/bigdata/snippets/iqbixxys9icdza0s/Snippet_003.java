// Source: Day06-Flink-高级部分 (iqbixxys9icdza0s) snippet #3
package com.bigdata.snippets;

public class iqbixxys9icdza0sSnippet003 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        Pattern.<LoginEvent>begin("first").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return value.getType().equals("fail");
            }
        })
    }
}
