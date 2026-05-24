package com.bigdata;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

public class TestSource02 {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.readTextFile("datas/wc.txt");
        dataStreamSource.print();
        
        String filePath = "datas/wc.txt";
        TextInputFormat inputFormat = new TextInputFormat(new Path(filePath));

        // 使用 readFile 方法读取文件（PROCESS_ONCE 表示只读取一次）
        DataStream<String> dataStream = env.readFile(
                inputFormat,
                filePath // 监控间隔（毫秒，PROCESS_ONCE 模式下此参数无效）
        );

        dataStream.print();

        env.execute();
    }
}
