package com.bigdata.flinksql.day02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @基本功能:
 * @program:FlinkDemo2
 * @create:2025-04-20 10:59:41
 **/
public class _006Demo {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("CREATE TABLE table1 (\n" +
                "  `username` string,\n" +
                "  `price` int,\n" +
                "  `event_time` TIMESTAMP(3),\n" +
                "  watermark for event_time as event_time - interval '3' second\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic1',\n" +
                "  'properties.bootstrap.servers' = 'node01:9092',\n" +
                "  'properties.group.id' = 'g1',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        // 编写sql语句

        tEnv.executeSql("select \n" +
                "   window_start,\n" +
                "   window_end,\n" +
                "   username,\n" +
                "   count(1) zongNum,\n" +
                "   sum(price) totalMoney \n" +
                "   from table(HOP(TABLE table1, DESCRIPTOR(event_time), INTERVAL '10' second,INTERVAL '60' second))\n" +
                "group by window_start,window_end,username").print();





    }
}
