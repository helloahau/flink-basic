package com.bigdata.day02;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @create:2025-11-26 14:21:56
 **/
public class Demo09_cedao {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> streamSource = env.fromSequence(1, 100);

        OutputTag<Long> odd = new OutputTag<>("奇数", TypeInformation.of(Long.class));
        OutputTag<Long> even = new OutputTag<>("偶数", TypeInformation.of(Long.class));

        SingleOutputStreamOperator<Long> streamOperator = streamSource.process(new ProcessFunction<Long, Long>() {

            @Override
            public void processElement(Long value, ProcessFunction<Long, Long>.Context ctx, Collector<Long> out) throws Exception {
                if (value % 2 == 0) {
                    ctx.output(even, value);
                } else {
                    ctx.output(odd, value);
                }

                out.collect(value);
            }
        });

        streamOperator.print("主干道:");

        SideOutputDataStream<Long> oddStream = streamOperator.getSideOutput(odd);
        oddStream.print("奇数");


        SideOutputDataStream<Long> evenStream = streamOperator.getSideOutput(even);
        evenStream.print("偶数");



        env.execute();
    }
}
