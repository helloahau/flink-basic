package com.bigdata.smart;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AverageSpeed {

    private Long startTime;
    private Long endTime;
    private String monitorId;
    private Double avgSpeed;
    private Integer carCount;

}
