package com.bigdata.day07;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.*;
import java.util.Date;
/**
 * @基本功能:
 * @program:FlinkDemo
 * @create:2025-12-03 10:01:33
 **/
public class Demo02_cep {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("first")
                .setGroupId("g1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<PayEvent> ds1 = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")
                .map(new MapFunction<String, PayEvent>() {
                    @Override
                    public PayEvent map(String value) throws Exception {
                        return JSON.parseObject(value, PayEvent.class);
                    }
                });


        SingleOutputStreamOperator<PayEvent> ds2 = ds1.assignTimestampsAndWatermarks(
                WatermarkStrategy.<PayEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<PayEvent>() {
                            @Override
                            public long extractTimestamp(PayEvent element, long recordTimestamp) {
                                try {
                                    Date date = DateUtils.parseDate(element.getTs(), "yyyy-MM-dd HH:mm:ss");
                                    // 返回值是毫秒
                                    return date.getTime();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        })
        );

        KeyedStream<PayEvent, Integer> ds3 = ds2.keyBy(payEvent -> payEvent.getUserId());
        // 统计连续登录失败三次的用户信息
        // 定义模式 Pattern
        Pattern<PayEvent, PayEvent> pattern = Pattern.<PayEvent>begin("first", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new IterativeCondition<PayEvent>() {
                    @Override
                    public boolean filter(PayEvent payEvent, Context<PayEvent> context) throws Exception {
                        return payEvent.getType().equals("create");
                    }
                }).followedBy("second").where(new IterativeCondition<PayEvent>() {
                    @Override
                    public boolean filter(PayEvent payEvent, Context<PayEvent> context) throws Exception {
                        return payEvent.getType().equals("pay");
                    }
                }).within(Duration.ofMinutes(10));
        // 将我们的规则和流挂钩
        PatternStream<PayEvent> patternStream = CEP.pattern(ds3, pattern);

        SingleOutputStreamOperator<Set<Integer>> select = patternStream.select(new PatternSelectFunction<PayEvent, Set<Integer>>() {
            @Override
            public Set<Integer> select(Map<String, List<PayEvent>> map) throws Exception {

                HashSet<Integer> set = new HashSet<>();

                Collection<List<PayEvent>> values = map.values();
                for (List<PayEvent> value : values) {
                    for (PayEvent loginEvent : value) {
                        // System.out.println(loginEvent.getId());
                        set.add(loginEvent.getUserId());
                    }
                }

                return set;
            }
        });

        select.print("第一个是create,第二个操作是pay的用户是:");


        env.execute();
    }
}
