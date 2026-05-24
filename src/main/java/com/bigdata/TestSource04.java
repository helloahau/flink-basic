package com.bigdata;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class TestSource04 {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.out.println(env.getParallelism()); // 16

        // 监听windows上的socket输入
        //DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8888);
        // 监听linux上的socket输入
        DataStreamSource<String> dataStreamSource = env.socketTextStream("bigdata01", 8888);
        dataStreamSource.print();

        System.out.println(dataStreamSource.getParallelism()); // 1


        env.execute();
    }
}
