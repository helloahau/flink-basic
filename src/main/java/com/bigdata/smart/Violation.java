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
}
