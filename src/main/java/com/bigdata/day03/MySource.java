package com.bigdata.day03;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.UUID;

class MySource implements ParallelSourceFunction<String>{

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        ctx.collect(UUID.randomUUID().toString());
    }

    @Override
    public void cancel() {

    }
}
public class _06自定义并列的Source {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(6);
        // 将自定义的数据源放入到env中
        DataStreamSource<String> orderInfoDataStreamSource = env.addSource(new MySource());
        orderInfoDataStreamSource.print();

        env.execute();
    }

}
