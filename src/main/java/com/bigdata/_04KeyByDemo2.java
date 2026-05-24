package com.bigdata;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @基本功能:
 * @program:FlinkDemo2
 * @author: 闫哥
 * @create:2025-04-16 17:08:34
 **/
public class _04KeyByDemo2 {


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class Ball {
        private String name;
        private int num;
     }

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2. source-加载数据
        DataStreamSource<Ball> dataStreamSource = env.fromElements(new Ball("篮球", 1),
                new Ball("篮球", 2),
                new Ball("篮球", 3),
                new Ball("足球", 3),
                new Ball("足球", 2),
                new Ball("足球", 3));
        //3. transformation-数据处理转换
        /*dataStreamSource.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        }).sum(1).print();*/
        // 简化版本
        dataStreamSource.keyBy(v->v.name).sum("num").print();

        //4. sink-数据输出


        //5. execute-执行
        env.execute();
    }
}
