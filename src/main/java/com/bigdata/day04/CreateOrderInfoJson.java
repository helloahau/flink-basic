package com.bigdata.day04;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import java.time.Duration;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @create:2025-11-28 09:30:26
 **/


public class CreateOrderInfoJson {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<OrderInfo> streamSource = env.addSource(new MySource(), "自动创建订单");

        streamSource.map(new MapFunction<OrderInfo, String>() {

            @Override
            public String map(OrderInfo orderInfo) throws Exception {
                return JSON.toJSONString(orderInfo);
            }
        }).print();

        env.execute();
    }
}
