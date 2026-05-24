package com.bigdata.smart;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
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
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.*;

/**
 * @基本功能:
 * @program:FlinkDemo2
 * @author: 闫哥
 * @create:2025-04-18 15:55:28
 **/
public class _02_智慧交通中卡口拥堵情况统计 {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2. source-加载数据
        // 从kafka的topic-car 中获取数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("node01:9092,node02:9092,node03:9092")
                .setTopics("topic-car")
                .setGroupId("smart_jiaotong")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        SingleOutputStreamOperator<CarInfo> mapStream = dataStreamSource.map(new RichMapFunction<String, CarInfo>() {


            @Override
            public CarInfo map(String jsonStr) throws Exception {

                CarInfo carInfo = JSON.parseObject(jsonStr, CarInfo.class);

                return carInfo;
            }
        });

        SingleOutputStreamOperator<AverageSpeed> resultStream = mapStream.keyBy(new KeySelector<CarInfo, String>() {

            @Override
            public String getKey(CarInfo carInfo) throws Exception {
                return carInfo.getMonitorId();
            }
        }).window(SlidingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(30))).apply(new WindowFunction<CarInfo, AverageSpeed, String, TimeWindow>() {
            @Override
            public void apply(String monitorId, TimeWindow window, Iterable<CarInfo> input, Collector<AverageSpeed> out) throws Exception {

                AverageSpeed averageSpeed = new AverageSpeed();
                averageSpeed.setMonitorId(monitorId);
                averageSpeed.setStartTime(window.getStart());
                averageSpeed.setEndTime(window.getEnd());

                double sumSpeed = 0;
                int carCount = 0;
                for (CarInfo carInfo : input) {
                    sumSpeed += carInfo.getSpeed();
                    carCount++;
                }
                averageSpeed.setCarCount(carCount);
                averageSpeed.setAvgSpeed(sumSpeed / carCount);
                out.collect(averageSpeed);
            }
        });

        resultStream.print();

        //3. transformation-数据处理转换
        //4. sink-数据输出
        // 将过滤后的数据写入数据库
        JdbcConnectionOptions jdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUrl("jdbc:mysql://node01:3306/smart_transportation")
                .withUsername("root").withPassword("123456").build();
        // 将数据写入到mysql中
        resultStream.addSink(JdbcSink.sink(
                "insert into t_average_speed values(null,?,?,?,?,?)", new JdbcStatementBuilder<AverageSpeed>() {
                    @Override
                    public void accept(PreparedStatement stat, AverageSpeed averageSpeed) throws SQLException {
                        stat.setLong(1,averageSpeed.getStartTime());
                        stat.setLong(2,averageSpeed.getEndTime());
                        stat.setString(3,averageSpeed.getMonitorId());
                        stat.setDouble(4,averageSpeed.getAvgSpeed());
                        stat.setInt(5,averageSpeed.getCarCount());
                    }
                }, JdbcExecutionOptions.builder().withBatchSize(1).build(), jdbcConnectionOptions

        ));


        //5. execute-执行
        env.execute();
    }
}
