package com.bigdata.twophase;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class KafkaExactlyOnceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String topic = "topic1";
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"bigdata01:9092");
        //设置消费者的隔离级别，读未提交
        prop.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_uncommitted");
        DataStreamSource<String> KafkaDs = env.addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), prop));
        KafkaDs.print("测试kafka一致性结果数据:");
        env.execute();
    }
}
