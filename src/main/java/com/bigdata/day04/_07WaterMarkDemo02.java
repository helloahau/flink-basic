package com.bigdata.day04;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Random;
import java.util.UUID;

/**
 * @基本功能:
 * @program:FlinkDemo2
 * @author: 闫哥
 * @create:2025-04-18 14:27:41
 **/
public class _07WaterMarkDemo02 {

    @Data  // set get toString
    @AllArgsConstructor
    @NoArgsConstructor
    public static class OrderInfo{
        private String orderId;
        private int uid;
        private int money;
        private long timeStamp;

        // explicit setters/getters — Lombok fallback for Maven batch compilation
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public void setUid(int uid) { this.uid = uid; }
        public void setMoney(int money) { this.money = money; }
        public void setTimeStamp(long timeStamp) { this.timeStamp = timeStamp; }
        public int getUid() { return uid; }
        public int getMoney() { return money; }
        public long getTimeStamp() { return timeStamp; }
        public String getOrderId() { return orderId; }
    }

    // 自定义source , 每隔1秒钟生成一个订单
    public static class MySource implements SourceFunction<OrderInfo> {

        private boolean flag = true;
        @Override
        public void run(SourceContext<OrderInfo> ctx) throws Exception {
            // 源源不断的产生数据
            Random random = new Random();
            while(flag){
                OrderInfo orderInfo = new OrderInfo();
                orderInfo.setOrderId(UUID.randomUUID().toString());
                orderInfo.setUid(random.nextInt(3));
                orderInfo.setMoney(random.nextInt(101));
                orderInfo.setTimeStamp(System.currentTimeMillis());
                ctx.collect(orderInfo);
                Thread.sleep(1000);// 间隔1s
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        //2. source-加载数据
        DataStreamSource<OrderInfo> dataStreamSource = env.addSource(new MySource());

        dataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(
                new SerializableTimestampAssigner<OrderInfo>() {
                    // long 是时间戳吗？是秒值还是毫秒呢？年月日时分秒的的字段怎么办呢？
                    @Override
                    public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
                        // 这个方法的返回值是毫秒，所有的数据只要不是这个毫秒值，都需要转换为毫秒
                        return orderInfo.getTimeStamp();
                    }
                }
        )).keyBy(orderInfo -> orderInfo.getUid()).window(TumblingEventTimeWindows.of(Time.seconds(5))).apply(new WindowFunction<OrderInfo, String, Integer, TimeWindow>() {
            @Override
            public void apply(Integer integer, TimeWindow window, Iterable<OrderInfo> input, Collector<String> out) throws Exception {
                long start = window.getStart();
                long end = window.getEnd();
                String startStr = DateFormatUtils.format(start,"yyyy-MM-dd HH:mm:ss");
                String endStr = DateFormatUtils.format(end,"yyyy-MM-dd HH:mm:ss");
                int sumMoney = 0;
                for (OrderInfo orderInfo : input) {
                    sumMoney += orderInfo.getMoney();
                }
                out.collect("开始时间："+startStr+"结束时间："+endStr+"，用户id="+integer+",订单总额："+sumMoney);
            }
        }).print();
        //4. sink-数据输出


        //5. execute-执行
        env.execute();
    }
}
