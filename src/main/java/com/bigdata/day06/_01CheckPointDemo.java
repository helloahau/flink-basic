package com.bigdata.day06;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: 闫哥
 * @create:2025-11-24 09:18:30
 **/
public class _01CheckPointDemo {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 在windows运行，将数据提交hdfs,会出现权限问题，使用这个语句解决。
        System.setProperty("HADOOP_USER_NAME", "root");
        // 在这个基础之上，添加快照
        // 第一句：开启快照，每隔1s保存一次快照
        env.enableCheckpointing(1000);
        // 第二句：设置快照保存的位置
        env.setStateBackend(new FsStateBackend("hdfs://bigdata01:9820/flink/checkpoint"));
        // 第三句： 通过webui的cancel按钮，取消flink的job时，不删除HDFS的checkpoint目录
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //2. source-加载数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = dataStreamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] arr = s.split(",");
                return Tuple2.of(arr[0], Integer.valueOf(arr[1]));
            }
        });
        //3. transformation-数据处理转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = mapStream.keyBy(0).sum(1);


        result.print();
        //4. sink-数据输出


        //5. execute-执行
        env.execute();
    }
}
