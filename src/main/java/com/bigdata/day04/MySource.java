package com.bigdata.day04;

import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;

import java.util.Random;
import java.util.UUID;

/**
 * Custom source that generates random OrderInfo events every second.
 * Used by CreateOrderInfoJson.
 */
public class MySource implements SourceFunction<OrderInfo> {

    private boolean flag = true;

    @Override
    public void run(SourceContext<OrderInfo> ctx) throws Exception {
        Random random = new Random();
        while (flag) {
            OrderInfo orderInfo = new OrderInfo();
            orderInfo.setOrderId(UUID.randomUUID().toString());
            orderInfo.setUid(random.nextInt(3));
            orderInfo.setMoney(random.nextInt(101));
            orderInfo.setTimeStamp(System.currentTimeMillis());
            ctx.collect(orderInfo);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}

