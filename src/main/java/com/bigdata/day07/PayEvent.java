package com.bigdata.day07;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PayEvent {

    private int userId;
    private String type;
    private String ts;

    // explicit constructor + getters — Lombok fallback for Maven batch compilation
    public PayEvent(int userId, String type, String ts) {
        this.userId = userId;
        this.type = type;
        this.ts = ts;
    }
    public int getUserId() { return userId; }
    public String getType() { return type; }
    public String getTs() { return ts; }
}
