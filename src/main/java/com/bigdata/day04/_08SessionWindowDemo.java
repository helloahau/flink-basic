package com.bigdata.day04;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;

import java.time.Duration;

/**
 * 会话窗口（Session Window）演示
 *
 * 会话窗口的特点:
 * - 没有固定大小，由会话间隔（session gap）定义
 * - 当两条数据之间的时间间隔超过 session gap，上一个窗口关闭，下一个窗口开始
 * - 不同于滚动/滑动窗口，会话窗口大小是动态的
 *
 * 本示例同时演示：
 * 1. 基于 ProcessingTime 的会话窗口
 * 2. 基于 EventTime 的会话窗口（要求有 Watermark）
 */
public class _08SessionWindowDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ---------------------------------------------------------------
        // Part 1: ProcessingTime session window
        // ---------------------------------------------------------------
        DataStream<Tuple2<String, Integer>> stream = env.fromData(
                Tuple2.of("sensor_1", 10),
                Tuple2.of("sensor_1", 20),
                Tuple2.of("sensor_2", 30),
                Tuple2.of("sensor_1", 40),
                Tuple2.of("sensor_2", 50)
        );

        // ProcessingTime 会话窗口：gap = 5 秒内不活跃则关闭窗口
        stream.keyBy(t -> t.f0)
                .window(ProcessingTimeSessionWindows.withGap(Duration.ofSeconds(5)))
                .sum(1)
                .print("ProcessingTime Session");

        // ---------------------------------------------------------------
        // Part 2: EventTime session window (需要 watermark)
        // ---------------------------------------------------------------
        DataStream<Tuple2<String, Integer>> eventStream = env.fromData(
                Tuple2.of("sensor_1", 10),
                Tuple2.of("sensor_1", 20),
                Tuple2.of("sensor_2", 30),
                Tuple2.of("sensor_1", 40),
                Tuple2.of("sensor_2", 50)
        );

        // EventTime 会话窗口：gap = 3 秒
        eventStream.keyBy(t -> t.f0)
                .window(EventTimeSessionWindows.withGap(Duration.ofSeconds(3)))
                .sum(1)
                .print("EventTime Session");

        env.execute("Session Window Demo");
    }
}
