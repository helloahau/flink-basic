package com.bigdata.day04;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: 闫哥
 * @create:2025-11-28 09:30:26
 **/


public class TestAllowedLateness {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> streamSource = KafkaSource.<String>builder().setBootstrapServers("bigdata01:9092").setTopics("first").setStartingOffsets(OffsetsInitializer.latest()).setValueOnlyDeserializer(new SimpleStringSchema()).build();

        DataStreamSource<String> dataStreamSource = env.fromSource(streamSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
        //dataStreamSource.print();

        SingleOutputStreamOperator<OrderInfo> streamOperator = dataStreamSource.map(new MapFunction<String, OrderInfo>() {
            @Override
            public OrderInfo map(String json) throws Exception {
                return JSON.parseObject(json, OrderInfo.class);
            }
        });


        // 此处添加水印
        SingleOutputStreamOperator<String> result = streamOperator.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(
                        new SerializableTimestampAssigner<OrderInfo>() {
                            // long 是时间戳吗？是秒值还是毫秒呢？年月日时分秒的的字段怎么办呢？
                            @Override
                            public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
                                // 这个方法的返回值是毫秒，所有的数据只要不是这个毫秒值，都需要转换为毫秒
                                return orderInfo.getTimeStamp();
                            }
                        }
                )).keyBy(order -> order.getUid()).window(TumblingEventTimeWindows.of(Time.seconds(5)))


                .allowedLateness(Time.seconds(10))

                .apply(new WindowFunction<OrderInfo, String, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer key, TimeWindow window, Iterable<OrderInfo> input, Collector<String> out) throws Exception {
                        // 返回   时间开始--> 时间结束  用户id   金额
                        long start = window.getStart();
                        long end = window.getEnd();
                        String startTime = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
                        String endTime = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");
                        int sumMoney = 0;
                        for (OrderInfo orderInfo : input) {

                            sumMoney += orderInfo.getMoney();
                        }

                        out.collect(startTime + "->" + endTime + "->" + key + "-->" + sumMoney);

                    }
                });

        result.print();

        env.execute();
    }
}
