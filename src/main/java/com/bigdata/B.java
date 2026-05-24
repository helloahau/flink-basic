package com.bigdata;

import org.apache.flink.api.java.utils.ParameterTool;

// 测试传参时，带有--input 字样
public class B {

    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String inputPath = null;
        if(parameterTool.has("input")){
            inputPath = parameterTool.get("input");
        }else{
            inputPath = "C:/Abc/";
        }

        System.out.println(inputPath);
    }
}
