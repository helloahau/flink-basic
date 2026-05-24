package com.bigdata;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Ball {
    private String name;
    private int num;

    // explicit constructor — Lombok fallback for Maven batch compilation
    public Ball(String name, int num) {
        this.name = name;
        this.num = num;
    }
}
