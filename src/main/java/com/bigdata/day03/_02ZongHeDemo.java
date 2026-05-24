package com.bigdata.day03;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class _02ZongHeDemo {

    // 边输入内容边过滤，过滤掉骂人的话
    public static void main(String[] args) throws Exception {
        //1. 创建一个执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 创建一个socket
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8888);

        String[] zanghua = {"傻逼","tmd","你妈的"};
        List<String> list = Arrays.asList(zanghua);
        dataStreamSource.print();

        dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                System.out.println(line);
                String[] arr = line.split(",");
                for (String word : arr) {
                    collector.collect(Tuple2.of(word,1));
                }
            }
        }).filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> tuple2) throws Exception {
                String word = tuple2.f0;

                return !list.contains(word);
            }
        }).keyBy(t -> t.f0).reduce((t1,t2)->Tuple2.of(t1.f0,t1.f1+t2.f1)).print();


        //5. execute-执行
        env.execute();
    }
}
