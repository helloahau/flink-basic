package com.bigdata.twophase;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class KafkaExactlyOnceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String topic = "topic1";
        // Flink 2.x: FlinkKafkaConsumer removed; use KafkaSource
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("bigdata01:9092")
                .setTopics(topic)
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> KafkaDs = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSource");
        KafkaDs.print("测试kafka一致性结果数据:");
        env.execute();
    }
}
