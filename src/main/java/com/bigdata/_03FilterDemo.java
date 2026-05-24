package com.bigdata;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;

/**
 * @基本功能:
 * @program:FlinkDemo2
 * @create:2025-04-16 17:02:54
 **/
public class _03FilterDemo {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2. source-加载数据
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
        //3. transformation-数据处理转换
        dataStream.map(new MapFunction<String, LogBean>() {
            @Override
            public LogBean map(String line) throws Exception {
                String[] arr = line.split(" ");
                String timeStr = arr[2];// 17/05/2015:10:05:30
                Date date = DateUtils.parseDate(timeStr, "dd/MM/yyyy:HH:mm:ss");
                long time = date.getTime();

                LogBean logBean = new LogBean(arr[0], Integer.parseInt(arr[1]),time , arr[3], arr[4]);
                return logBean;
            }
        }).filter(new FilterFunction<LogBean>() {
            @Override
            public boolean filter(LogBean logBean) throws Exception {

                // 符合条件的数据留下来，不符合的过滤掉
                return logBean.getIp().equals("83.149.9.216");
            }
        }).print();
        //4. sink-数据输出


        //5. execute-执行
        env.execute();
    }
}
