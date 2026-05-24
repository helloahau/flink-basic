package com.bigdata;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount03 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource  = env.fromElements("spark flink kafka", "spark sqoop flink", "kakfa hadoop flink");

        // 复制小括号，写死右箭头，复制大括号
        /*dataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] arr = line.split(" ");
                for (String word : arr) {
                    collector.collect(word);
                }
            }
        });*/

        dataStreamSource.flatMap((String line, Collector<String> collector) ->{
                String[] arr = line.split(" ");
                for (String word : arr) {
                    collector.collect(word);
                }
        }).returns(Types.STRING).map((String s) ->Tuple2.of(s,1)).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy((Tuple2<String, Integer> tuple2) -> tuple2.f0).sum(1).print();

        env.execute();
    }
}
