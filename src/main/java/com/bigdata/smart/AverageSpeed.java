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

    // explicit getters/setters — Lombok fallback for Maven batch compilation
    public Long getStartTime() { return startTime; }
    public Long getEndTime() { return endTime; }
    public String getMonitorId() { return monitorId; }
    public Double getAvgSpeed() { return avgSpeed; }
    public Integer getCarCount() { return carCount; }
    public void setStartTime(Long startTime) { this.startTime = startTime; }
    public void setEndTime(Long endTime) { this.endTime = endTime; }
    public void setMonitorId(String monitorId) { this.monitorId = monitorId; }
    public void setAvgSpeed(Double avgSpeed) { this.avgSpeed = avgSpeed; }
    public void setCarCount(Integer carCount) { this.carCount = carCount; }
}
