// Source: Day05-FlinkSQL (pb9wqs8sv708mmpl) snippet #22
package com.bigdata.snippets;

public class pb9wqs8sv708mmplSnippet022 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        create table t_success
        (
            id      int auto_increment,
            user_id int         null,
            page_id int         null,
            status  varchar(20) null,
            constraint t_success_pk
                primary key (id)
        );
    }
}
