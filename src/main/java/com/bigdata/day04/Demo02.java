package com.bigdata.day04;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import java.time.Duration;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @create:2025-08-14 16:14:55
 **/
public class Demo02 {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //2. source-加载数据
        KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers("bigdata01:9092")
        .setTopics("first")
        .setGroupId("my-group3")
        .setStartingOffsets(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .build();

        DataStreamSource<String> dataStreamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        //3. transformation-数据处理转换
        dataStreamSource.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word,1);
            }
        }).keyBy(tupel->tupel.f0)
        .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
        .process(new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
            @Override
            public void process(String key, ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
                int sum = 0;
                for (Tuple2<String, Integer> tuple2 : elements) {
                    sum += tuple2.f1;
                }

                TimeWindow window = context.window();

                long start = window.getStart();
                long end = window.getEnd();
                                String startTime = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
                                String endTime = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");

                                StringBuffer sb = new StringBuffer();
                                sb.append(startTime).append(":").append(endTime).append(",").append(key).append(",").append(sum);

                                out.collect(sb.toString());
                            }
                        }).print();
        //4. sink-数据输出

        //5. execute-执行
        env.execute();
    }
}
