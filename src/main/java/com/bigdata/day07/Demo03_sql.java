package com.bigdata.day07;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @create:2025-12-03 11:12:28
 **/
public class Demo03_sql {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取tableEnv对象
        // 通过env 获取一个table 环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE table1 (\n" +
                "  `ts`             timestamp(3),\n" +
                "  `cardid`         STRING,\n" +
                "  `location`       STRING,\n" +
                "  `action`         STRING,\n" +
                "   WATERMARK FOR ts as ts - INTERVAL '0' SECOND"  +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'first',\n" +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'g1',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ");");

        //tableEnv.executeSql("select * from table1").print();
        tableEnv.executeSql("select cardid\n" +
                "from table1\n" +
                "match_recognize(\n" +
                "  partition by cardid\n" +
                "  order by ts\n" +
                "  measures\n" +
                "     e1.ts as `start_ts`,\n" +
                "     last(e2.ts) as `end_ts`,\n" +
                "     e1.action as `event`\n" +
                "  one row per match\n" +
                "  AFTER MATCH SKIP PAST LAST ROW\n" +    //AFTER MATCH SKIP TO NEXT ROW
                "  pattern(e1 B* e2) within interval '10' minute \n" +
                "  define\n" +
                "     e1 as e1.action = 'Consumption',\n" +
                "     B  as B.action <> 'Consumption',\n" +
                "     e2 as e2.action = 'Consumption' and e1.location <> e2.location\n" +
                ")").print();


    }
}
