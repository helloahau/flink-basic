package com.bigdata.day06;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @基本功能:
 * @program:FlinkDemo
 * @create:2025-11-24 09:18:30
 **/
public class _01CheckPointDemo {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        System.setProperty("HADOOP_USER_NAME", "root");
        env.enableCheckpointing(1000);
        // Flink 2.x: state backend configured via configuration, not setStateBackend
        // env.setStateBackend(new HashMapStateBackend());
        // Flink 2.x: enableExternalizedCheckpoints replaced by setExternalizedCheckpointRetention
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        //2. source-加载数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = dataStreamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] arr = s.split(",");
                return Tuple2.of(arr[0], Integer.valueOf(arr[1]));
            }
        });
        // Flink 2.x: keyBy(int) removed; use lambda
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = mapStream.keyBy(t -> t.f0).sum(1);

        result.print();


        //5. execute-执行
        env.execute();
    }
}
