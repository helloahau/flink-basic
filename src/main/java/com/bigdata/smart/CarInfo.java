package com.bigdata.smart;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CarInfo {

    private Long actionTime;
    private String monitorId;
    private String cameraId;
    private String car;
    private Double speed;
    private String roadId;
    private String areaId;

    // 这个属性不属于这里，但是可以使用
    private double limitSpeed;

}
