package com.bigdata.flinksql.day02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @基本功能:
 * @program:FlinkDemo2
 * @author: 闫哥
 * @create:2025-04-20 09:49:50
 **/
public class _003Demo {

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
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic1',\n" +
                "  'properties.bootstrap.servers' = 'node01:9092',\n" +
                "  'properties.group.id' = 'g1',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE table2 (\n" +
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

        tEnv.executeSql(" insert into table2 select * from table1 where status='success'");

    }
}
