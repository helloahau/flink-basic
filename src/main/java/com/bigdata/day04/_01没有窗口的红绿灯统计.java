package com.bigdata.day04;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @基本功能:
 * @program:FlinkDemo2
 * @author: 闫哥
 * @create:2025-04-18 09:14:52
 **/
public class _01没有窗口的红绿灯统计 {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);

        //2. source-加载数据
        //3. transformation-数据处理转换
        dataStreamSource.map(new MapFunction<String, Tuple2<Integer,Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(String line) throws Exception {
                String[] arr = line.split(",");
                return Tuple2.of(Integer.valueOf(arr[0]),Integer.valueOf(arr[1]));
            }
        }).keyBy(new KeySelector<Tuple2<Integer, Integer>, Integer>() {

            @Override
            public Integer getKey(Tuple2<Integer, Integer> t2) throws Exception {
                return t2.f0;
            }
        }).sum(1).print();
        //4. sink-数据输出


        //5. execute-执行
        env.execute();
    }
}
