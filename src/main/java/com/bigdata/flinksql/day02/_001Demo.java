package com.bigdata.flinksql.day02;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @基本功能:
 * @program:FlinkDemo2
 * @create:2025-04-20 09:29:01
 *
 * 该案例主要用于测试flink中的connector 的用法，无特殊的实战意义
 *
 **/
public class _001Demo {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // 获取tableEnv对象
        // 通过env 获取一个table 环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //2. 创建表对象
        tEnv.executeSql("CREATE TABLE source ( \n" +
                "    id INT, \n" +
                "    ts BIGINT, \n" +
                "    vc INT\n" +
                ") WITH ( \n" +
                "    'connector' = 'datagen', \n" +
                "    'rows-per-second'='1', \n" +
                "    'fields.id.kind'='random', \n" +
                "    'fields.id.min'='1', \n" +
                "    'fields.id.max'='10', \n" +
                "    'fields.ts.kind'='sequence', \n" +
                "    'fields.ts.start'='1', \n" +
                "    'fields.ts.end'='1000000', \n" +
                "    'fields.vc.kind'='random', \n" +
                "    'fields.vc.min'='1', \n" +
                "    'fields.vc.max'='100'\n" +
                ");\n");
        //3. 在创建一个表
        tEnv.executeSql("CREATE TABLE sink (\n" +
                "    id INT, \n" +
                "    sumVC INT \n" +
                ") WITH (\n" +
                "'connector' = 'print'\n" +
                ");\n");
        //4. 将Table变为stream流
        tEnv.executeSql("insert into sink select id,sum(vc) sumVC from source group by id");

        // 本身想着将数据写入到了sink 中，需要查询查看结果，但是此处的sink 是print，所以会直接打印到控制台，所以如果你书写如下代码，会报错
        // Connector 'print' can only be used as a sink. It cannot be used as a source.
        /*TableResult tableResult = tEnv.executeSql("select * from sink");
        tableResult.print();*/


        //5. execute-执行
        // No operators defined in streaming topology. Cannot execute.
        // env.execute();
    }
}
