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

    // explicit getters/setters — Lombok fallback for Maven batch compilation
    public String getOrderId() { return orderId; }
    public int getUid() { return uid; }
    public int getMoney() { return money; }
    public long getTimeStamp() { return timeStamp; }
    public void setOrderId(String orderId) { this.orderId = orderId; }
    public void setUid(int uid) { this.uid = uid; }
    public void setMoney(int money) { this.money = money; }
    public void setTimeStamp(long timeStamp) { this.timeStamp = timeStamp; }
}

