package com.bigdata.day05;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import com.google.common.cache.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @create:2025-12-01 17:01:23
 **/
public class Demo06 {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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

            // 定义一个Cache
            LoadingCache<String, String> cache;
            @Override
            public void open(OpenContext context) throws Exception {
                // jdbc 纯代码
                // 这个里面编写连接数据库的代码
                Class.forName("com.mysql.jdbc.Driver");
                conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/demo", "root", "123456");
                ps = conn.prepareStatement("select * from city where city_id=?");

                cache = CacheBuilder.newBuilder()
                        //最多缓存个数，超过了就根据最近最少使用算法来移除缓存 LRU
                        .maximumSize(1000)
                        //在更新后的指定时间后就回收
                        // 不会自动调用，而是当过期后，又用到了过期的key值数据才会触发的。
                        .expireAfterWrite(100, TimeUnit.SECONDS)
                        .build(//指定加载缓存的逻辑
                                new CacheLoader<String, String>() {
                                    // 假如缓存中没有数据，会触发该方法的执行，并将结果自动保存到缓存中
                                    @Override
                                    public String load(String cityId) throws Exception {
                                        System.out.println("进入数据库查询啦。。。。。。。");
                                        ps.setString(1,cityId);
                                        ResultSet resultSet = ps.executeQuery();
                                        String cityName = null;
                                        if(resultSet.next()){
                                            System.out.println("进入到了if中.....");
                                            cityName = resultSet.getString("city_name");
                                        }
                                        return cityName;
                                    }
                                });

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

                String cityName = cache.get(id);

                return value+","+cityName;
            }
        }).print();


        env.execute();
    }
}
