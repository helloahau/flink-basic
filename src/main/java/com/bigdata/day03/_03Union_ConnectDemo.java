package com.bigdata.day03;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @基本功能:
 * @program:FlinkDemo2
 * @author: 闫哥
 * @create:2025-04-17 09:33:03
 **/
public class _03Union_ConnectDemo {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2. source-加载数据
        DataStreamSource<String> ds1 = env.fromElements("大数据", "JavaWeb", "鸿蒙开发");
        DataStreamSource<String> ds2 = env.fromElements("Bigdata", "SpringBoot", "JS");
        DataStream<String> unionStream = ds1.union(ds2);
        unionStream.print();

        //3. 不同类型的流
        DataStream<Long> ds3 = env.fromSequence(1, 10);
        ConnectedStreams<String, Long> connectStream = ds1.connect(ds3);
        //4. sink-数据输出
        SingleOutputStreamOperator<String> mapStream = connectStream.map(new CoMapFunction<String, Long, String>() {
            @Override
            public String map1(String value) throws Exception {
                return value;
            }

            @Override
            public String map2(Long value) throws Exception {
                return String.valueOf(value);
            }
        });
        mapStream.print();


        //5. execute-执行
        env.execute();
    }
}
