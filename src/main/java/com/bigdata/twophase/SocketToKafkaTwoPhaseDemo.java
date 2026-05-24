package com.bigdata.twophase;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class SocketToKafkaTwoPhaseDemo {
    public static void main(String[] args) throws Exception {
        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Flink 2.x: FsStateBackend removed; checkpoint storage set via configuration
        // env.setStateBackend(new FsStateBackend("hdfs://bigdata01:9820/flink/checkpoint"));
        //开启快照，每隔10s保存一次快照
        env.enableCheckpointing(100000);
        //设置ck执行语义，精确一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置两次ck之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //设置ck超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // Flink 2.x: setFailOnCheckpointingErrors removed; use setTolerableCheckpointFailureNumber
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
        // Flink 2.x: enableExternalizedCheckpoints replaced by setExternalizedCheckpointRetention
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        System.setProperty("HADOOP_USER_NAME", "root");
        DataStream<String> socketDs = env.socketTextStream("bigdata01", 8888);
        socketDs.print();
        String topic = "topic1";
        // Flink 2.x: FlinkKafkaProducer removed; use KafkaSink
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("bigdata01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setTransactionalIdPrefix("socket-to-kafka")
                .setKafkaProducerConfig(new java.util.Properties() {{
                    put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, String.valueOf(60000 * 15));
                }})
                .build();
        socketDs.sinkTo(sink);
        env.execute();
    }
}
