package com.bigdata.day04;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: 闫哥
 * @create:2025-08-15 10:28:39
 **/
public class Demo05_reduce {


    public static final Tuple3[] ENGLISH = new Tuple3[] {
            Tuple3.of("class1", "张三", 100L),
            Tuple3.of("class1", "李四", 40L),
            Tuple3.of("class1", "王五", 60L),
            Tuple3.of("class2", "赵六", 20L),
            Tuple3.of("class2", "小七", 30L),
            Tuple3.of("class2", "小八", 50L)
    };

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //2. source-加载数据
        DataStreamSource<Tuple3<String,String,Long>> dataStreamSource = env.fromElements(ENGLISH);

        //3. transformation-数据处理转换
        // 需求：统计每个班级的平均分（reduce做不到），可以求每个班级的总分
        /**
         *  如果没有窗口，可以使用的函数有： sum reduce process
         *  使用了窗口：apply process reduce sum agg 等
         */
        dataStreamSource.keyBy(tuple3->tuple3.f0).countWindow(3)
                .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1, Tuple3<String, String, Long> value2) throws Exception {

                        return Tuple3.of(value1.f0,null, value1.f2+value2.f2);
                    }
                }).print();
        //4. sink-数据输出

        //5. execute-执行
        env.execute();
    }
}
