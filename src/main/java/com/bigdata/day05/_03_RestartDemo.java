package com.bigdata.day05;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @基本功能:
 * @program:FlinkDemo2
 * @author: 闫哥
 * @create:2025-04-19 11:00:07
 **/
public class _03_RestartDemo {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //开启重启策略，默认不开启
        // 在 Flink 1.12 及更高版本中，默认的重启策略有所变化。从 Flink 1.12 开始，默认的重启策略是“无重启”（No Restart），这意味着如果作业失败，它将不会自动重启。为了启用自动重启，你需要显式地设置一个重启策略
        //env.setRestartStrategy(RestartStrategies.noRestart());

        //重启3次，重启时间间隔是10s
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));

        //2分钟内重启3次，重启时间间隔是5s
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(3,
                        Time.of(2, TimeUnit.MINUTES),
                        Time.of(5,TimeUnit.SECONDS))
        );



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
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = mapStream.keyBy(0).sum(1);
        //4. sink-数据输出
        result.print();


        //5. execute-执行
        env.execute();
    }
}
