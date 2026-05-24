package com.bigdata.day03;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @create:2025-11-27 16:01:10
 **/
public class Demo08 {

    // 窗口函数 agg
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

        DataStreamSource<Tuple3<String,String,Long>> dataStreamSource = env.fromElements(ENGLISH);


        WindowedStream<Tuple3<String, String, Long>, String, GlobalWindow> stream = dataStreamSource.keyBy(new KeySelector<Tuple3<String, String, Long>, String>() {
            @Override
            public String getKey(Tuple3<String, String, Long> value) throws Exception {
                return value.f0;
            }
        }).countWindow(3);

        stream.process(new ProcessWindowFunction<Tuple3<String, String, Long>, Tuple2<String,Double>, String, GlobalWindow>() {
            @Override
            public void process(String key, ProcessWindowFunction<Tuple3<String, String, Long>, Tuple2<String, Double>, String, GlobalWindow>.Context context, Iterable<Tuple3<String, String, Long>> elements, Collector<Tuple2<String, Double>> out) throws Exception {
                int personNum=0;
                long sumScore=0;
                for (Tuple3<String, String, Long> tuple3 : elements) {
                    personNum+=1;
                    sumScore+=tuple3.f2;
                }
                double avgScore=sumScore*1.0/personNum;
                out.collect(Tuple2.of(key,avgScore));
            }
        }).print();


        env.execute();
    }
}
