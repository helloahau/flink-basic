package com.bigdata.day02;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.UUID;

/**
 * 需求: 每隔1秒随机生成一条订单信息(订单ID、用户ID、订单金额、时间戳)
 * 要求:
 * - 随机生成订单ID(UUID)
 * - 随机生成用户ID(0-2)
 * - 随机生成订单金额(0-100)
 * - 时间戳为当前系统时间
 */

@Data  // set get toString
@AllArgsConstructor
@NoArgsConstructor
class OrderInfo{
    private String orderId;
    private int uid;
    private int money;
    private long timeStamp;
}
// class MySource extends RichSourceFunction<OrderInfo> {
//class MySource extends RichParallelSourceFunction<OrderInfo> {
class MySource implements SourceFunction<OrderInfo> {
    boolean flag = true;

    @Override
    public void run(SourceContext ctx) throws Exception {
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

    // source 停止之前需要干点啥
    @Override
    public void cancel() {
        flag = false;
    }
}
public class CustomSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);
        // 将自定义的数据源放入到env中
        DataStreamSource dataStreamSource = env.addSource(new MySource())/*.setParallelism(1)*/;
        System.out.println(dataStreamSource.getParallelism());
        dataStreamSource.print();
        env.execute();
    }


}
