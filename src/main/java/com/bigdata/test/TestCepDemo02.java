package com.bigdata.test;

import com.alibaba.fastjson2.JSON;
import com.bigdata.bean.PayEvent;
import com.bigdata.schema.JSONDeserializationSchema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @基本功能:
 * @program:FlinkProject
 * @author: 闫哥
 * @create:2025-12-02 09:10:49
 **/


//{"userId":"1","type":"create","ts":"2023-07-18 10:10:10"}
//{"userId":"1","type":"create","ts":"2023-07-18 10:14:10"}
//{"userId":"1","type":"pay","ts":"2023-07-18 10:14:11"}
//{"userId":"1","type":"pay","ts":"2023-07-18 10:14:11"}
//{"userId":"1","type":"xxx","ts":"2023-07-18 10:14:12"}
public class TestCepDemo02 {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","bigdata01:9092");
        properties.setProperty("group.id","g1");
        //FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer("topic1",new SimpleStringSchema(), properties);
        FlinkKafkaConsumer<PayEvent> consumer = new FlinkKafkaConsumer("topic1",new JSONDeserializationSchema<PayEvent>(PayEvent.class), properties);
        DataStreamSource<PayEvent> ds1 = env.addSource(consumer);
        ds1.print("ds1:");
        /*
        // 我们写了一个map算子就是为了将json字符串转换为实体，太不划算了。
        ds1.map(new MapFunction<String, PayEvent>() {
            @Override
            public PayEvent map(String s) throws Exception {
                return JSON.parseObject(s, PayEvent.class);
            }
        }).print();*/
        SingleOutputStreamOperator<PayEvent> ds2 = ds1.assignTimestampsAndWatermarks(WatermarkStrategy.<PayEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(
                new SerializableTimestampAssigner<PayEvent>() {
                    @SneakyThrows
                    @Override
                    public long extractTimestamp(PayEvent element, long recordTimestamp) {
                        long time = DateUtils.parseDate(element.getTs(), "yyyy-MM-dd HH:mm:ss").getTime();
                        return time;
                    }
                }
        ));

        ds2.print("ds2:");
        // 定义Pattern
        Pattern<PayEvent, PayEvent> pattern = Pattern.<PayEvent>begin("first", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<PayEvent>() {
                    @Override
                    public boolean filter(PayEvent value) throws Exception {
                        return value.getType().equals("create");
                    }
                })
                .followedBy("second").where(new SimpleCondition<PayEvent>() {
                    @Override
                    public boolean filter(PayEvent value) throws Exception {
                        return value.getType().equals("pay");
                    }
                })
                .within(Time.minutes(10));// 此处的10分钟，一定数据要触发该10分钟的时候才会有结果

        // 使用cep
        //在数据流上用模式匹配
        PatternStream<PayEvent> patternStream = CEP.pattern(ds2.keyBy(v -> v.getUserId()), pattern);

        OutputTag<String> outputTag = new OutputTag<String>("x1"){};

        // 输出数据
        //选择输出数据
        SingleOutputStreamOperator<String> ds3 = patternStream.select(outputTag, new PatternTimeoutFunction<PayEvent, String>() {
            @Override
            public String timeout(Map<String, List<PayEvent>> map, long l) throws Exception {
                return map.get("first").get(0).getUserId();
            }
        }, new PatternSelectFunction<PayEvent, String>() {
            @Override
            public String select(Map<String, List<PayEvent>> map) throws Exception {
                // String userId = map.get("first").get(0).getUserId();
                // return userId;
                System.out.println("select.......");
                System.out.println(map);
                return map.get("first").get(0).getUserId();
            }
        });
        ds3.print("正常的ID(先creat,后pay的用户)：");

        DataStream<String> ds4 = ds3.getSideOutput(outputTag);
        ds4.print("超时的用户：");



        env.execute();
    }
}
