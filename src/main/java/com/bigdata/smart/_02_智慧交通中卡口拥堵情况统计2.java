package com.bigdata.smart;

import com.alibaba.fastjson.JSON;
import com.bigdata.day04._07WaterMarkDemo03;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;

/**
 * @基本功能:
 * @program:FlinkDemo2
 * @create:2025-04-18 15:55:28
 **/
public class _02_智慧交通中卡口拥堵情况统计2 {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

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

        // 添加水印
        SingleOutputStreamOperator<CarInfo> outputStreamOperator = mapStream.assignTimestampsAndWatermarks(WatermarkStrategy.<CarInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(
                new SerializableTimestampAssigner<CarInfo>() {
                    // long 是时间戳吗？是秒值还是毫秒呢？年月日时分秒的的字段怎么办呢？
                    @Override
                    public long extractTimestamp(CarInfo carInfo, long recordTimestamp) {
                        // 这个方法的返回值是毫秒，所有的数据只要不是这个毫秒值，都需要转换为毫秒
                        return carInfo.getActionTime()*1000;
                    }
                }
        ));

        SingleOutputStreamOperator<AverageSpeed> resultStream = outputStreamOperator.keyBy(new KeySelector<CarInfo, String>() {

            @Override
            public String getKey(CarInfo carInfo) throws Exception {
                return carInfo.getMonitorId();
            }
        }).window(SlidingEventTimeWindows.of(Duration.ofMinutes(1), Duration.ofSeconds(30))).apply(new WindowFunction<CarInfo, AverageSpeed, String, TimeWindow>() {
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

        // Flink 2.x: JdbcSink.sink() returns 1.x SinkFunction incompatible with addSink(); use inline RichSinkFunction
        resultStream.addSink(new RichSinkFunction<AverageSpeed>() {
            private transient java.sql.Connection conn;
            private transient java.sql.PreparedStatement ps;

            @Override
            public void open(OpenContext context) throws Exception {
                conn = java.sql.DriverManager.getConnection(
                        "jdbc:mysql://node01:3306/smart_transportation?useSSL=false&serverTimezone=UTC",
                        "root", "123456");
                ps = conn.prepareStatement("insert into t_average_speed values(null,?,?,?,?,?)");
            }

            @Override
            public void invoke(AverageSpeed averageSpeed, Context context) throws Exception {
                ps.setLong(1, averageSpeed.getStartTime());
                ps.setLong(2, averageSpeed.getEndTime());
                ps.setString(3, averageSpeed.getMonitorId());
                ps.setDouble(4, averageSpeed.getAvgSpeed());
                ps.setInt(5, averageSpeed.getCarCount());
                ps.executeUpdate();
            }

            @Override
            public void close() throws Exception {
                if (ps != null) ps.close();
                if (conn != null) conn.close();
            }
        });

        //5. execute-执行
        env.execute();
    }
}
