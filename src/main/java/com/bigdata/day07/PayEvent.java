package com.bigdata.day07;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PayEvent {

    private int userId;
    private String type;
    private String ts;
}
