package com.bigdata.day03;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @基本功能:
 * @program:FlinkDemo2
 * @author: 闫哥
 * @create:2025-04-17 09:05:29
 **/
public class _01_ReduceDemo {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2. source-加载数据
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(), new Path("datas/a.log")
        ).build();
        DataStream<String> dataStream = env.fromSource(
                fileSource,
                WatermarkStrategy.noWatermarks(),
                "map-input"
        );
        //3. transformation-数据处理转换
        dataStream.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                String[] arr = line.split(" ");
                String ip = arr[0];
                return Tuple2.of(ip,1);
            }
        }).keyBy(t -> t.f0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return Tuple2.of(t1.f0,t1.f1+t2.f1);
            }
        }).print();
        //4. sink-数据输出


        //5. execute-执行
        env.execute();
    }
}
