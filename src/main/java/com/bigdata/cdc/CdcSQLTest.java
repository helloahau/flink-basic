package com.bigdata.cdc;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @基本功能:
 * @program:FlinkProject
 * @author: 闫哥
 * @create:2025-06-13 11:01:11
 **/
public class CdcSQLTest {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        // 获取tableEnv对象
        // 通过env 获取一个table 环境
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        //2. 创建表对象
        //3. 编写sql语句
        //4. 将Table变为stream流
        tenv.executeSql("CREATE TABLE user_info2 (\n" +
                " id INT NOT NULL primary key,\n" +
                " name STRING,\n" +
                " age int\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = 'bigdata01',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '123456',\n" +
                " 'scan.startup.mode' = 'latest-offset', " +
                " 'database-name' = 'cdc_test',\n" +
                " 'table-name' = 'user_info'\n" +
                ")");

        tenv.executeSql("select * from user_info2").print();

        Table table = tenv.sqlQuery("select * from user_info2");
        DataStream<Tuple2<Boolean, Row>> retractStream = tenv.toRetractStream(table, Row.class);
        retractStream.print();


        //5. execute-执行
        env.execute();
    }
}
