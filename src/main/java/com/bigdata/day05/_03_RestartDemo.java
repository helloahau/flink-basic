package com.bigdata.day05;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @基本功能:
 * @program:FlinkDemo2
 * @create:2025-04-19 11:00:07
 **/
public class _03_RestartDemo {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // Flink 2.x: restart strategy via Configuration (RestartStrategies removed)
        // 无重启: RestartStrategyOptions.RESTART_STRATEGY -> "none"
        // 固定延迟重启3次/10s: restart-strategy=fixed-delay, attempts=3
        // 2分钟内重启3次，间隔5s
        Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "failure-rate");
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL, 3);
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL, Duration.ofMinutes(2));
        config.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY, Duration.ofSeconds(5));
        env.configure(config);

        //2. source-加载数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8877);
        //3. transformation-数据处理转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = dataStreamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] arr = s.split(",");
                if(arr[0].equals("bug")){
                    throw new Exception("程序中出现了bug,看是否可以重启....");
                }
                return Tuple2.of(arr[0], Integer.valueOf(arr[1]));
            }
        });
        //3. transformation-数据处理转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = mapStream.keyBy(t -> t.f0).sum(1);
        //4. sink-数据输出
        result.print();

        //5. execute-执行
        env.execute();
    }
}
