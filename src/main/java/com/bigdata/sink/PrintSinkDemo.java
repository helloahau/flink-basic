package com.bigdata.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


class PrintSinkDemo extends RichSinkFunction<Long> {

    private String msg;

    public PrintSinkDemo(String msg) {
        this.msg = msg;
    }

    public PrintSinkDemo() {

    }

    @Override
    public void invoke(Long value, Context context) throws Exception {
        // 开始定义需要输出的格式
        int partitionNum = getRuntimeContext().getIndexOfThisSubtask() + 1;
        if(msg == null){
            System.out.println(partitionNum+"> "+value);
        }else{
            System.out.println(msg+":"+partitionNum+"> "+value);
        }

    }
}
class _01_SinkDemo {



    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<Long> streamSource = env.fromSequence(1, 10);

        // 第一种输出方式  print
        streamSource.print();
        streamSource.print("我是字符串:");

        //假如你想模拟实现print打印，你需要学习自定义sink
        streamSource.addSink(new PrintSinkDemo());
        streamSource.addSink(new PrintSinkDemo("我是字符串:"));




        //5. execute-执行
        env.execute();
    }
}
