package com.bigdata.smart;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class _05_智慧交通超速车辆sql版本 {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取tableEnv
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 获取kafka中的数据
        // {"action_time":1682219447,"monitor_id":"0001","camera_id":"1","car":"豫A12345","speed":34.5,"road_id":"01","area_id":"20"}
        tableEnv.executeSql("CREATE TABLE table1 (\n" +
                "  `action_time` BIGINT,\n" +
                "  `monitor_id` string,\n" +
                "  `camera_id` string,\n" +
                "  `car` string,\n" +
                "  `speed` double,\n" +
                "  `road_id` string,\n" +
                "  `area_id` string,\n" +
                "  `event_time`   as  TO_TIMESTAMP(FROM_UNIXTIME( action_time, 'yyyy-MM-dd HH:mm:ss')), \n"  +
                "   watermark for event_time as event_time - interval '0' second"  +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic-car',\n" +
                "  'properties.bootstrap.servers' = 'node01:9092',\n" +
                "  'properties.group.id' = 'g1232',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
        // 因为需要将违章车辆插入违章表，所以创建一个mysql的表
        tableEnv.executeSql("CREATE TABLE t_speeding_info (\n" +
                " id BIGINT primary key NOT ENFORCED, \n" +
                "  `car` STRING,\n" +
                "  `monitor_id` STRING,\n" +
                "  `road_id` string,\n" +
                "  `real_speed` DOUBLE,\n" +
                "  `limit_speed` int,\n" +
                "  `action_time` bigint\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://node01:3306/smart_transportation?useUnicode=true&characterEncoding=utf8',\n" +
                "    'table-name' = 't_speeding_info', \n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456'\n" +
                ")");

        // 超速指的是当前车速 >= 限速表中的车速的1.2倍
        tableEnv.executeSql("CREATE TABLE monitor (\n" +
                "  `monitor_id` STRING,\n" +
                "  `road_id` STRING,\n" +
                "  `speed_limit` int,\n" +
                "  `area_id` STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://node01:3306/smart_transportation?useUnicode=true&characterEncoding=utf8',\n" +
                "    'table-name' = 't_monitor_info', \n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456'\n" +
                ")");




        // 统计涉嫌套牌车辆  先根据车牌分组和窗口分组，并且是不同的monitor_id，然后统计次数大于等于2的
        tableEnv.executeSql(
                "insert into t_speeding_info " +
                        " select " +
                        //  " null ,  \n" +
                        " CAST (UNIX_TIMESTAMP() AS BIGINT) AS id , \n" +
                        "car, \n" +
                        "t1.monitor_id, \n" +
                        "t1.road_id, \n" +
                        "t1.speed as real_speed, \n" +
                        "m.speed_limit as limit_speed,\n" +
                        "t1.action_time from \n" +
                       "  table1 t1 left join monitor m on t1.monitor_id = m.monitor_id where t1.speed >= ( ifnull(m.speed_limit,60) *1.1)  ");

    }
}
