package com.bigdata.day02;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Demo03 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<String> list = Arrays.asList("hello", "word");
        DataStreamSource<String> dataStreamSource = env.fromCollection(list);
        dataStreamSource.print();
        DataStreamSource<String> dataStreamSource1 = env.fromElements("hello", "spark");
        dataStreamSource1.print();
        DataStreamSource<Long> streamSource = env.fromSequence(1, 100);
        streamSource.print();
        env.execute();
    }
}
