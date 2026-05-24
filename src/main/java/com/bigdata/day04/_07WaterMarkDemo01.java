package com.bigdata.day04;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import java.util.Random;
import java.util.UUID;
import java.time.Duration;

/**
 * @基本功能:
 * @program:FlinkDemo2
 * @create:2025-04-18 14:27:41
 **/
public class _07WaterMarkDemo01 {

    @Data  // set get toString
    @AllArgsConstructor
    @NoArgsConstructor
    public static class OrderInfo{
        private String orderId;
        private int uid;
        private int money;
        private long timeStamp;

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

        //3. transformation-数据处理转换
        // 每隔5秒计算5秒内的每一个用户的订单总额
        dataStreamSource.keyBy(orderInfo -> orderInfo.getUid()).window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5))).sum("money").print();
        //4. sink-数据输出


        //5. execute-执行
        env.execute();
    }
}
