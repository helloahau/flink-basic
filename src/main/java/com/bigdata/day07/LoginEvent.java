package com.bigdata.day07;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginEvent {

    private String id;
    private String status;
    private String loginTime;

    // explicit constructor + getters — Lombok fallback for Maven batch compilation
    public LoginEvent(String id, String status, String loginTime) {
        this.id = id;
        this.status = status;
        this.loginTime = loginTime;
    }
    public String getId() { return id; }
    public String getStatus() { return status; }
    public String getLoginTime() { return loginTime; }
}
