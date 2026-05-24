package com.bigdata;

import com.alibaba.fastjson.JSON;
import com.bigdata.smart.AverageSpeed;
import com.bigdata.smart.CarInfo;
import com.bigdata.smart.Violation;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @基本功能:
 * @program:Smart_Tran
 * @author: 闫哥
 * @create:2025-11-28 11:46:03
 **/
public class _03FinkCore_taopai {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder().setBootstrapServers("bigdata01:9092").setTopics("topic-car").setStartingOffsets(OffsetsInitializer.latest()).setValueOnlyDeserializer(new SimpleStringSchema()).build();

        DataStreamSource<String> dataStreamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
        //dataStreamSource.print();

        SingleOutputStreamOperator<CarInfo> mapStream = dataStreamSource.map(new RichMapFunction<String, CarInfo>() {
            

            @Override
            public CarInfo map(String json) throws Exception {

                CarInfo carInfo = JSON.parseObject(json, CarInfo.class);
                
                return carInfo;
            }

        });

        // 两辆汽车在规定的时间内同时出现在不同的卡口，涉嫌套牌
        SingleOutputStreamOperator<Violation> resultStream = mapStream.keyBy(new KeySelector<CarInfo, String>() {
            @Override
            public String getKey(CarInfo value) throws Exception {
                return value.getCar();
            }
        }).filter(new RichFilterFunction<CarInfo>() {


            ListState<CarInfo> preCarInfo = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ListStateDescriptor<CarInfo> stateDescriptor = new ListStateDescriptor<CarInfo>("listState", CarInfo.class);
                preCarInfo = getRuntimeContext().getListState(stateDescriptor);
            }

            @Override
            public boolean filter(CarInfo carInfo) throws Exception {
                // 什么样的数据需要保留： 卡口不一样，间隔时间短 5
                Iterable<CarInfo> carInfos = preCarInfo.get();
                ArrayList<CarInfo> list = new ArrayList<>();
                for (CarInfo car : carInfos) {
                    list.add(car);
                }
                list.sort(new Comparator<CarInfo>() {
                    @Override
                    public int compare(CarInfo car1, CarInfo car2) {
                        return (int) (car1.getActionTime() - car2.getActionTime());
                    }
                });
                System.out.println(list);

                // 豫C5879G
                boolean isTao = false;

                for (int i = 0; i < list.size(); i++) {
                    String monitorId = carInfo.getMonitorId();
                    long actionTime = carInfo.getActionTime();

                    CarInfo historyCar = list.get(i);

                    if (monitorId != historyCar.getMonitorId() && Math.abs(actionTime - historyCar.getActionTime()) <= 5) {
                        isTao = true;
                        break;
                    }

                }

                preCarInfo.add(carInfo);

                return isTao;
            }
        }).map(new MapFunction<CarInfo, Violation>() {
            @Override
            public Violation map(CarInfo carInfo) throws Exception {
                return new Violation(0, carInfo.getCar(), "涉嫌套牌", System.currentTimeMillis());
            }
        });

        // 将结果保存到mysql

        JdbcConnectionOptions jdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://localhost:3306/smart_transportation")
                .withDriverName("com.mysql.cj.jdbc.Driver")
                .withUsername("root").withPassword("123456").build();

        // 如何把结果保存到 mysql
        resultStream.addSink(JdbcSink.sink(
                "insert into t_violation_list values (null,?,?,?)",
                new JdbcStatementBuilder<Violation>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Violation violation) throws SQLException {
                        preparedStatement.setString(1,violation.getCar());
                        preparedStatement.setString(2, violation.getViolation());
                        preparedStatement.setLong(3,violation.getCreateTime());
                    }
                }, JdbcExecutionOptions.builder().withBatchSize(1).build(),jdbcConnectionOptions

        ));


        env.execute();
    }
}
