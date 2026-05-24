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


}
