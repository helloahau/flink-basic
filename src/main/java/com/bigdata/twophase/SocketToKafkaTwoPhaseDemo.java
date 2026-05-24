package com.bigdata.twophase;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class SocketToKafkaTwoPhaseDemo {
    public static void main(String[] args) throws Exception {
        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置快照保存的位置
        env.setStateBackend(new FsStateBackend("hdfs://bigdata01:9820/flink/checkpoint"));
        //开启快照，每隔10s保存一次快照
        env.enableCheckpointing(100000);
        //设置ck执行语义，精确一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置两次ck之间的最小时间间隔，两次ck之间最少差500ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //设置ck超时时间，如果超时则默认本次ck失败，继续下一次ck即可，超时60秒
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //设置如果ck出现问题是否让程序报错，true报错，false不报错进行下一次ck
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        //设置任务取消时是否保存检查点，retain保存检查点
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 在windows运行，将数据提交hdfs,会出现权限问题，使用这个语句解决。
        System.setProperty("HADOOP_USER_NAME", "root");
        DataStream<String> socketDs = env.socketTextStream("bigdata01", 8888);
        socketDs.print();
        String topic = "topic1";
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "bigdata01:9092");
        prop.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, String.valueOf(60000*15));
        socketDs.addSink(new FlinkKafkaProducer<String>(topic,new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),prop,FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
        env.execute();
    }
}
