package com.bigdata;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @基本功能:
 * @program:FlinkDemo2
 * @author: 闫哥
 * @create:2025-04-16 16:16:00
 **/
public class _02FlatMapDemo {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2. source-加载数据
        //2. source-加载数据
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(), new Path("datas/flatmap.log")
        ).build();
        DataStream<String> dataStream = env.fromSource(
                fileSource,
                WatermarkStrategy.noWatermarks(),
                "flatmap-input"
        );
        //3. transformation-数据处理转换
        SingleOutputStreamOperator<String> flattedMap = dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] arr = line.split(",");
                String name = arr[0];
                for (int i = 1; i < arr.length; i++) {
                    collector.collect(name + "有" + arr[i]);
                }

            }
        });
        //4. sink-数据输出
        flattedMap.print();

        //5. execute-执行
        env.execute();
    }
}
