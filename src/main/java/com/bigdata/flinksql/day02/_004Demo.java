package com.bigdata.flinksql.day02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @基本功能:
 * @program:FlinkDemo2
 * @author: 闫哥
 * @create:2025-04-20 09:49:50
 *
 *  flinksql 读取mysql的数据
 **/
public class _004Demo {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取tableEnv对象
        // 通过env 获取一个table 环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("CREATE TABLE table1 (\n" +
                "  `user_id` int,\n" +
                "  `page_id` int,\n" +
                "  `status` STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://node01:3306/gongcheng?useUnicode=true&characterEncoding=utf8',\n" +
                "    'table-name' = 't_success', \n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456'\n" +
                ")");

        // 第一种打印的方式
        //TableResult tableResult = tEnv.executeSql("select * from table1 where status='success'");
        //tableResult.print();

        // 第二种打印的方式
        tEnv.executeSql("CREATE TABLE table2 (\n" +
                "    user_id INT, \n" +
                "  `status` STRING\n" +
                ") WITH (\n" +
                "'connector' = 'print'\n" +
                ");\n");
        tEnv.executeSql("insert into table2 select user_id,status from table1 where status='success'");

    }
}
