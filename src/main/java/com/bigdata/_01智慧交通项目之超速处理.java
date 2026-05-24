package com.bigdata;

import com.alibaba.fastjson.JSON;
import com.bigdata.pojo.CarInfo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.*;

/**
 * @基本功能:
 * @program:SmartTraffic
 * @author: 闫哥
 * @create:2025-08-14 11:41:33
 **/
public class _01智慧交通项目之超速处理 {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         *  1、读取kafka数据
         *  2、将json数据转变成JavaBean
         *  3、应该过滤 filter 算子，算子中连接数据库
         *  4、将结果保存到mysql中
         */
        // 读取kafka数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("bigdata01:9092")
                .setTopics("topic-car")
                .setGroupId("my-car")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        // 第二步：将json --> bean
        SingleOutputStreamOperator<CarInfo> mapStreamSource = dataStreamSource.map(new RichMapFunction<String, CarInfo>() {

            Connection connection = null;
            PreparedStatement statement = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                String url = "jdbc:mysql://localhost:3306/smart_transportation?useSSL=false&serverTimezone=UTC";
                String username = "root";  // 通常是 root
                String password = "123456";
                connection = DriverManager.getConnection(url, username, password);
                statement = connection.prepareStatement("select * from t_monitor_info where monitor_id=?");
            }

            @Override
            public void close() throws Exception {

                statement.close();
                connection.close();

            }

            @Override
            public CarInfo map(String line) throws Exception {
                CarInfo carInfo = JSON.parseObject(line, CarInfo.class);
                String monitorId = carInfo.getMonitorId();

                statement.setString(1, monitorId);
                ResultSet resultSet = statement.executeQuery();
                if (resultSet.next()) {
                    int speedLimit = resultSet.getInt("speed_limit");
                    carInfo.setLimitSpeed(speedLimit);
                } else {
                    carInfo.setLimitSpeed(60);
                }

                return carInfo;
            }
        });

        SingleOutputStreamOperator<CarInfo> filterSource = mapStreamSource.filter(new FilterFunction<CarInfo>() {
            @Override
            public boolean filter(CarInfo carInfo) throws Exception {
                return carInfo.getSpeed() >= carInfo.getLimitSpeed() * 1.1;
            }
        });



        JdbcConnectionOptions.JdbcConnectionOptionsBuilder jdbcConnectionOptionsBuilder = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder();
        JdbcConnectionOptions jdbcConnectionOptions = jdbcConnectionOptionsBuilder.
                withUrl("jdbc:mysql://localhost:3306/smart_transportation").
                withUsername("root").
                withPassword("123456").
                withDriverName("com.mysql.cj.jdbc.Driver").build();
        SinkFunction<CarInfo> sink = JdbcSink.sink(
                "insert into t_speeding_info values(null,?,?,?,?,?,?)",
                new JdbcStatementBuilder<CarInfo>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, CarInfo carInfo) throws SQLException {
                        preparedStatement.setString(1, carInfo.getCar());
                        preparedStatement.setString(2, carInfo.getMonitorId());
                        preparedStatement.setString(3, carInfo.getRoadId());
                        preparedStatement.setDouble(4, carInfo.getSpeed());
                        preparedStatement.setInt(5, carInfo.getLimitSpeed());
                        preparedStatement.setLong(6, carInfo.getActionTime());

                    }
                }, JdbcExecutionOptions.builder().withBatchSize(1).build(),
                jdbcConnectionOptions
        );

        // filterSource 保存数据库
        filterSource.addSink(sink);


        //5. execute-执行
        env.execute();
    }
}
