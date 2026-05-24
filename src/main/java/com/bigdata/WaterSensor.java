package com.bigdata;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {

    private String id;
    private Long ts;
    private int vc;

    // explicit constructor — Lombok fallback for Maven batch compilation
    public WaterSensor(String id, Long ts, int vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }
    public String getId() { return id; }
    public Long getTs() { return ts; }
    public int getVc() { return vc; }
    public void setId(String id) { this.id = id; }
    public void setTs(Long ts) { this.ts = ts; }
    public void setVc(int vc) { this.vc = vc; }
}
