package com.bigdata.smart;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Violation{
    private int id;
    private String car;
    private String violation;
    private Long createTime;

    // explicit constructor + getters — Lombok fallback for Maven batch compilation
    public Violation(int id, String car, String violation, Long createTime) {
        this.id = id;
        this.car = car;
        this.violation = violation;
        this.createTime = createTime;
    }
    public String getCar() { return car; }
    public String getViolation() { return violation; }
    public Long getCreateTime() { return createTime; }
}
