// Source: Day05-FlinkSQL (pb9wqs8sv708mmpl) snippet #23
package com.bigdata.snippets;

public class pb9wqs8sv708mmplSnippet023 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        CREATE TABLE MyUserTable (
          id BIGINT,
          name STRING,
          age INT,
          status BOOLEAN,
          PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
           'connector' = 'jdbc',
           'url' = 'jdbc:mysql://localhost:3306/spark_demo',
           'table-name' = 't_success',
            'username' = 'root',
            'password' = '123456'

        );
    }
}
