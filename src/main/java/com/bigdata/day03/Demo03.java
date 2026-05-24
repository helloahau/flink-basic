package com.bigdata.day03;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @create:2025-08-14 09:49:26
 **/
public class Demo03 {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 通过黑窗口向topic1中发送消息，含有success字样的消息，会出现在topic2中。

        // 通过source读取数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("bigdata01:9092")
                .setTopics("first1")
                .setGroupId("my-group1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        SingleOutputStreamOperator<String> filter = dataStreamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String msg) throws Exception {
                return msg.contains("success");
            }
        });




        //2. source-加载数据
        //3. transformation-数据处理转换
        //4. sink-数据输出
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                // 指定 kafka 的地址和端口
                .setBootstrapServers("bigdata01:9092")
                // 指定序列化器：指定Topic名称、具体的序列化
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("second1")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();
        filter.sinkTo(kafkaSink);

        //5. execute-执行
        env.execute();
    }
}
