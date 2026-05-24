package com.bigdata.day04;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * @基本功能:
 * @program:FlinkDemo
 **/
public class kafkaSendDemo {

    public static void main(String[] args) throws InterruptedException {
        // Properties 它是map的一种
        Properties properties = new Properties();
        // 设置连接kafka集群的ip和端口
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"node01:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        // 创建了一个消息生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        String[] arr = {"美国关税","黄金继续突破前高","哪吒闹海","恒大足球队","郑州烂尾楼"};
        Random random = new Random();

        for (int i = 0; i < 50000; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first",arr[random.nextInt(arr.length)]);
            kafkaProducer.send(producerRecord);
            Thread.sleep(50);
        }

        kafkaProducer.close();
    }

}
