package com.bigdata.day06;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @create:2025-12-02 10:11:04
 **/
public class Demo01 {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8888);
        tableEnv.createTemporaryView("table1",dataStreamSource);

        /*TableResult tableResult = tableEnv.executeSql("select * from table1");
        tableResult.print();*/
        Table table = tableEnv.sqlQuery("select * from table1");
        table.printSchema();// 打印表结构
        // table 中的数据是无法直接打印的，要想得到里面的数据，需要将table对象变为流对象
        // toAppendStream 已经淘汰啦
        //DataStream<Row> appendStream = tableEnv.toAppendStream(table, Row.class);
        // 新的api 和toAppendStream 作用一样
        DataStream<Row> dataStream = tableEnv.toDataStream(table, Row.class);

        dataStream.print();


        env.execute();
    }
}
