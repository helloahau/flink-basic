package com.bigdata.day04;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * OrderInfo POJO for day04 demos (CreateOrderInfoJson, TestAllowedLateness).
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderInfo implements Serializable {
    private String orderId;
    private int uid;
    private int money;
    private long timeStamp;

}

