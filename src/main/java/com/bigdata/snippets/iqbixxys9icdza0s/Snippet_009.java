// Source: Day06-Flink-高级部分 (iqbixxys9icdza0s) snippet #9
package com.bigdata.snippets;

public class iqbixxys9icdza0sSnippet009 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        create table user_info
        (
            id   int auto_increment,
            name varchar(255) null,
            age  int          null,
            constraint user_info_pk
                primary key (id)
        );
    }
}
