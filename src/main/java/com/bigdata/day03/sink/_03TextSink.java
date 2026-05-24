package com.bigdata.day03.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @基本功能:
 * @program:FlinkDemo2
 * @author: 闫哥
 * @create:2025-04-17 14:32:46
 **/
public class _03TextSink {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2. source-加载数据
        //3. transformation-数据处理转换
        //4. sink-数据输出
        DataStreamSource<String> dataStreamSource = env.fromElements("hello world", "hello flink", "hello java", "hello scala");

        dataStreamSource.writeAsText("datas/aaa.txt", FileSystem.WriteMode.OVERWRITE);


        //5. execute-执行
        env.execute();
    }
}
