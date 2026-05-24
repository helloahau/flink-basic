package com.bigdata.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WC{
    private String word;
    private int num;

    // explicit constructor — Lombok fallback for Maven batch compilation
    public WC(String word, int num) {
        this.word = word;
        this.num = num;
    }
}
