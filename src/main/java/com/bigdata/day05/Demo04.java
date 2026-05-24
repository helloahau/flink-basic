package com.bigdata.day05;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @create:2025-12-01 17:01:23
 **/
public class Demo04 {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("first")
                .setGroupId("donghu111")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        // 张三,1001
        dataStreamSource.map(new RichMapFunction<String, String>() {

            Connection conn = null;
            PreparedStatement ps = null;
            ResultSet resultSet =null;


            Map<String,String> map =new HashMap<String,String>();

            @Override
            public void open(OpenContext context) throws Exception {
                // jdbc 纯代码
                // 这个里面编写连接数据库的代码
                Class.forName("com.mysql.jdbc.Driver");
                conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/demo", "root", "123456");
                ps = conn.prepareStatement("select * from city ");
                resultSet = ps.executeQuery();
                while(resultSet.next()){
                   String cityId= resultSet.getString("city_id");
                   String cityName= resultSet.getString("city_name");
                    map.put(cityId,cityName);

                }



            }

            @Override
            public void close() throws Exception {
                resultSet.close();
                ps.close();
                conn.close();
            }

            @Override
            public String map(String value) throws Exception {

                String[] arr = value.split(",");
                String id= arr[1];

                String city = map.get(id);

                return value+","+city;
            }
        }).print();


        env.execute();
    }
}
