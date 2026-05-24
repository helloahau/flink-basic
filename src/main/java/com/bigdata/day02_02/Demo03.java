package com.bigdata.day02_02;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: 闫哥
 * @create:2025-08-13 16:50:27
 **/
public class Demo03 {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.fromElements("hello", "world");
        //2. source-加载数据
        //3. transformation-数据处理转换
        //4. sink-数据输出
        FileSink<String> fieSink = FileSink
                // 输出行式存储的文件，指定路径、指定编码
                .<String>forRowFormat(new Path("./datas"), new SimpleStringEncoder<>("UTF-8"))
                .build();

        dataStreamSource.sinkTo(fieSink);

        //5. execute-执行
        env.execute();
    }
}
