package com.bigdata.day04;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class _06ZengLiangDemo {

    public static final Tuple3[] ENGLISH = new Tuple3[] {
            Tuple3.of("class1", "张三", 100L),
            Tuple3.of("class1", "李四", 40L),
            Tuple3.of("class1", "王五", 60L),
            Tuple3.of("class2", "赵六", 20L),
            Tuple3.of("class2", "小七", 30L),
            Tuple3.of("class2", "小八", 50L)
    };

    // 需求：统计每个班级的平均分，使用全量函数

    public static void main(String[] args) throws Exception {
        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 为什么我们测试窗口的时候，每次都把并行度设置为1 ,因为窗口的计算，是按照并行度进行计算的，哪个窗口达到了触发条件，就运行哪个窗口
        env.setParallelism(1);

        //2. source-加载数据
        DataStreamSource<Tuple3<String,String,Long>> dataStreamSource = env.fromElements(ENGLISH);

        KeyedStream<Tuple3<String,String,Long>, String> keyedStream = dataStreamSource.keyBy(new KeySelector<Tuple3<String,String,Long>, String>() {

            @Override
            public String getKey(Tuple3<String,String,Long> tuple3) throws Exception {
                return tuple3.f0;
            }
        });

        // 三个参数:输入类型(IN)、累加器类型(ACC)和输出类型(OUT)
        keyedStream.countWindow(3).apply(new WindowFunction<Tuple3<String, String, Long>, Tuple2<String,Double>, String, GlobalWindow>() {
            @Override
            public void apply(String clazz, GlobalWindow window, Iterable<Tuple3<String, String, Long>> input, Collector<Tuple2<String, Double>> out) throws Exception {

                Long sum = 0L;
                int personNum = 0;
                for (Tuple3<String, String, Long> tuple3 : input) {
                    sum = tuple3.f2 + sum;
                    personNum += 1;
                }
                out.collect(Tuple2.of(clazz,sum*1.0/personNum));
            }
        }).print();

        env.execute();


    }
}
