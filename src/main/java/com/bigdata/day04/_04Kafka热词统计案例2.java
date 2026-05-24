package com.bigdata.day04;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.time.Duration;

/**
 * @基本功能:
 * @program:FlinkDemo2
 * @create:2025-04-18 10:15:46
 **/
public class _04Kafka热词统计案例2 {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 并行度设置为1 效果更好
        env.setParallelism(1);

        //2. source-加载数据
        // 通过flink 获取kafka数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("node01:9092,node02:9092,node03:9092")
                .setTopics("first")
                .setGroupId("my-group2")
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
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

            @Override
            public String getKey(Tuple2<String, Integer> t2) throws Exception {
                return t2.f0;
            }
        }).window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .apply(
                        // 第一个泛型是输入数据的类型，第二个泛型是返回值类型   第三个是key 的类型， 第四个是窗口对象
                new WindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                    @Override
                    public void apply(
                            String key   // key值
                            , TimeWindow window,  // window对象
                            Iterable<Tuple2<String, Integer>> input, // 每个组内所有的数据
                            Collector<String> out // 用于向外输出的
                    ) throws Exception {

                        int sum = 0;
                        for (Tuple2<String, Integer> tuple2 : input) {
                            sum += tuple2.f1;
                        }
                        long start = window.getStart();// 窗口开始时间
                        long end = window.getEnd();// 窗口结束时间
                        String startStr = DateFormatUtils.format(start,"yyyy-MM-dd HH:mm:ss");
                        String endStr = DateFormatUtils.format(end,"yyyy-MM-dd HH:mm:ss");
                        out.collect("开始时间："+startStr+"结束时间："+endStr+"，热词="+key+",出现次数："+sum);

                    }
                })
        .print();
        //4. sink-数据输出


        //5. execute-执行
        env.execute();
    }
}
