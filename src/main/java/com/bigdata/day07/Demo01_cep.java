package com.bigdata.day07;

import org.apache.commons.lang3.time.DateUtils;
import java.util.Date;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.*;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @create:2025-12-03 10:01:33
 **/
public class Demo01_cep {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<LoginEvent> ds1 = env.fromElements(
                new LoginEvent("1", "fail", "2023-07-18 10:10:20"),
                new LoginEvent("1", "success", "2023-07-18 10:10:21"),
                new LoginEvent("1", "fail", "2023-07-18 10:10:22"),
                new LoginEvent("1", "fail", "2023-07-18 10:13:25"),
                new LoginEvent("1", "fail", "2023-07-18 10:18:30"),
                new LoginEvent("1", "fail", "2023-07-18 10:18:30"),
                new LoginEvent("2", "fail", "2023-07-18 10:10:21"),
                new LoginEvent("2", "fail", "2023-07-18 10:10:22"),
                new LoginEvent("2", "success", "2023-07-18 10:10:23"),
                new LoginEvent("2", "fail", "2023-07-18 10:10:24")
        );

        SingleOutputStreamOperator<LoginEvent> ds2 = ds1.assignTimestampsAndWatermarks(
                WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                try {
                                    Date date = DateUtils.parseDate(element.getLoginTime(), "yyyy-MM-dd HH:mm:ss");
                                    // 返回值是毫秒
                                    return date.getTime();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        })
        );

        KeyedStream<LoginEvent, String> ds3 = ds2.keyBy(loginEvent -> loginEvent.getId());
        // 统计连续登录失败三次的用户信息
        // 定义模式 Pattern
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("first", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.getStatus().equals("fail");
                    }
                }).times(3).consecutive().within(Duration.ofMinutes(10));
        // 将我们的规则和流挂钩
        PatternStream<LoginEvent> patternStream = CEP.pattern(ds3, pattern);

        SingleOutputStreamOperator<Set<String>> select = patternStream.select(new PatternSelectFunction<LoginEvent, Set<String>>() {
            @Override
            public Set<String> select(Map<String, List<LoginEvent>> map) throws Exception {

                System.out.println(map.values());
                Set<String> keySet = map.keySet();
                System.out.println(keySet);

                HashSet<String> set = new HashSet<>();

                Collection<List<LoginEvent>> values = map.values();
                for (List<LoginEvent> value : values) {
                    for (LoginEvent loginEvent : value) {
                       // System.out.println(loginEvent.getId());
                        set.add(loginEvent.getId());
                    }
                }

                return set;
            }
        });

        select.print("哪些id 连续登录失败三次:");


        env.execute();
    }
}
