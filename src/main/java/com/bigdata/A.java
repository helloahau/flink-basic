package com.bigdata;



import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;

import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO
 *
 * @version 1.0
 */
public class A {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // TODO 从文件读： 新Source架构
        // default to datas/wc.txt when no argument is provided
        String filePath = (args.length > 0) ? args[0] : "datas/wc.txt";
        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path(filePath)
                )
                .build();

        env
                .fromSource(fileSource, WatermarkStrategy.noWatermarks(), "filesource")
                .print();
        env.execute();
    }
}
/**
 * 新的Source写法：
 * env.fromSource(Source的实现类，Watermark，名字)
 */
