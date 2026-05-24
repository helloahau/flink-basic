package com.bigdata.day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.UUID;

class MySource2 extends RichParallelSourceFunction<String> {

    // ctrl + o 可以提示都有哪些方法可以被重写
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("这个方法可以做初始化");
    }

    @Override
    public void close() throws Exception {
        System.out.println("这个方法就是一般做关闭操作");
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        ctx.collect(UUID.randomUUID().toString());
    }

    @Override
    public void cancel() {

    }
}
class _07自定义数据源之Rich {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(6);
        // 将自定义的数据源放入到env中
        DataStreamSource<String> orderInfoDataStreamSource = env.addSource(new MySource2());
        orderInfoDataStreamSource.print();

        env.execute();
    }
}
