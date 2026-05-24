package com.bigdata.test;

import com.bigdata.day07.PayEvent;
import com.bigdata.schema.JSONDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @基本功能:
 * @program:FlinkProject
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

        // Flink 2.x: FlinkKafkaConsumer removed; use KafkaSource
        KafkaSource<PayEvent> source = KafkaSource.<PayEvent>builder()
                .setBootstrapServers("bigdata01:9092")
                .setTopics("topic1")
                .setGroupId("g1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new JSONDeserializationSchema<PayEvent>(PayEvent.class)))
                .build();
        DataStreamSource<PayEvent> ds1 = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSource");
        ds1.print();


        env.execute();
    }
}
