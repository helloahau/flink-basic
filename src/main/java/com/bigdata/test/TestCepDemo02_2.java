package com.bigdata.test;

import com.alibaba.fastjson.JSON;
import com.bigdata.day07.PayEvent;
import com.bigdata.schema.JSONDeserializationSchema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @基本功能:
 * @program:FlinkProject
 * @author: 闫哥
 * @create:2025-12-20 09:10:49
 **/


//{"userId":"1","type":"create","ts":"2023-07-18 10:10:10"}
//{"userId":"1","type":"create","ts":"2023-07-18 10:14:10"}
//{"userId":"1","type":"pay","ts":"2023-07-18 10:14:11"}
//{"userId":"1","type":"pay","ts":"2023-07-18 10:14:11"}
//{"userId":"1","type":"xxx","ts":"2023-07-18 10:14:12"}
class TestCepDemo02_2 {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","bigdata01:9092");
        properties.setProperty("group.id","g1");
        //FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer("topic1",new SimpleStringSchema(), properties);
        FlinkKafkaConsumer<PayEvent> consumer = new FlinkKafkaConsumer("topic1",new JSONDeserializationSchema<PayEvent>(PayEvent.class), properties);
        DataStreamSource<PayEvent> ds1 = env.addSource(consumer);
        ds1.print();
        /*
        // 我们写了一个map算子就是为了将json字符串转换为实体，太不划算了。
        ds1.map(new MapFunction<String, PayEvent>() {
            @Override
            public PayEvent map(String s) throws Exception {
                return JSON.parseObject(s, PayEvent.class);
            }
        }).print();*/


        env.execute();
    }
}
