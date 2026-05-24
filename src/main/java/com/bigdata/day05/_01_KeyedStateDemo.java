package com.bigdata.day05;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @基本功能:
 * @program:FlinkDemo2
 * @create:2025-04-19 09:12:30
 **/
public class _01_KeyedStateDemo {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //2. source-加载数据
        //2. source-加载数据
        DataStream<Tuple2<String, Long>> tupleDS = env.fromElements(
                Tuple2.of("北京", 1L),
                Tuple2.of("上海", 2L),
                Tuple2.of("北京", 6L),
                Tuple2.of("上海", 8L),
                Tuple2.of("北京", 3L),
                Tuple2.of("上海", 4L),
                Tuple2.of("北京", 7L)
        );

        //3. transformation-数据处理转换
        tupleDS.keyBy(v -> v.f0).map(new RichMapFunction<Tuple2<String, Long>, Tuple2<String ,Long>>() {

            // map 只能获取当前的流中的数据，无法获取历史数据，所以需要使用状态来保存历史数据
            // 状态采用ValueState  用于保存历史上的最大值
            ValueState<Long> maxValueState = null;
            @Override
            public void open(OpenContext context) throws Exception {
                ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<Long>("valueState",Long.class);
                maxValueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void close() throws Exception {
                maxValueState.clear();
            }

            @Override
            public Tuple2<String ,Long> map(Tuple2<String, Long> tuple2) throws Exception {
                Long maxValue = maxValueState.value();
                Long currentValue = tuple2.f1;
                if(maxValue == null || currentValue > maxValue){
                    maxValueState.update(currentValue);
                }
                return Tuple2.of(tuple2.f0,maxValue);
            }
        }).print();
        //4. sink-数据输出


        //5. execute-执行
        env.execute();
    }
}
