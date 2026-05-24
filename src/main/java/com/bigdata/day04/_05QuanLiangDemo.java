package com.bigdata.day04;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _05QuanLiangDemo {

    public static final Tuple3[] ENGLISH = new Tuple3[] {
            Tuple3.of("class1", "张三", 100L),
            Tuple3.of("class1", "李四", 40L),
            Tuple3.of("class1", "王五", 60L),
            Tuple3.of("class2", "赵六", 20L),
            Tuple3.of("class2", "小七", 30L),
            Tuple3.of("class2", "小八", 50L)
    };

    // 需求：统计每个班级的平均分，使用增量函数

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
        keyedStream.countWindow(3).aggregate(new AggregateFunction<Tuple3<String, String, Long>, Tuple3<String,Long,Integer>, Tuple2<String,Double>>() {
            // 初始化一个中间变量
            // 累加器类型(ACC) 第一个是班级的名称，第二个是班级的总分，第三个是班级的人数
            Tuple3<String,Long,Integer> tuple3 = Tuple3.of(null,0L,0);
            @Override
            public Tuple3<String, Long, Integer> createAccumulator() {
                return tuple3;
            }

            @Override
            public Tuple3<String, Long, Integer> add(Tuple3<String, String, Long> t31, Tuple3<String, Long, Integer> t32) {
                // t31 = (class1,zhangsan,100)  t32 = (class1,321,3)
                return Tuple3.of(t31.f0,t31.f2+t32.f1,t32.f2+1);
            }

            @Override
            public Tuple2<String, Double> getResult(Tuple3<String, Long, Integer> tuple3) {
                return Tuple2.of(tuple3.f0,((double)tuple3.f1)/tuple3.f2);
            }

            @Override
            public Tuple3<String, Long, Integer> merge(Tuple3<String, Long, Integer> acc1, Tuple3<String, Long, Integer> acc2) {
                return Tuple3.of(acc1.f0,acc1.f1+acc2.f1,acc1.f2+acc2.f2);
            }

        }).print();

        env.execute();


    }
}
