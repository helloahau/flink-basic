package com.bigdata;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * Desc 演示Flink-DataStream-流批一体API完成批处理WordCount
 * 使用Java8的lambda表示完成函数式风格的WordCount
 */
class WordCount02_3 {
    public static void main(String[] args) throws Exception {
        //TODO 1.env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setRuntimeMode(RuntimeExecutionMode.STREAMING);//指定计算模式为流
        //env.setRuntimeMode(RuntimeExecutionMode.BATCH);//指定计算模式为批
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);//自动
        //不设置的话默认是流模式defaultValue(RuntimeExecutionMode.STREAMING)

        //TODO 2.source-加载数据
        DataStream<String> dataStream = env.fromElements("flink hadoop spark", "flink hadoop spark", "flink hadoop", "flink");

        //TODO 3.transformation-数据转换处理
        //3.1对每一行数据进行分割并压扁
        /*
        public interface FlatMapFunction<T, O> extends Function, Serializable {
            void flatMap(T value, Collector<O> out) throws Exception;
        }
        */
        /*DataStream<String> wordsDS = dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });*/

        //注意:Java8的函数的语法/lambda表达式的语法: (参数)->{函数体}
        DataStream<String> wordsDS = dataStream.flatMap(
            (String value, Collector<String> out) -> {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        ).returns(Types.STRING);


        //3.2 每个单词记为<单词,1>
        /*
        public interface MapFunction<T, O> extends Function, Serializable {
            O map(T value) throws Exception;
         }
         */
        /*DataStream<Tuple2<String, Integer>> wordAndOneDS = wordsDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });*/
        DataStream<Tuple2<String, Integer>> wordAndOneDS = wordsDS.map(
            (String value) -> Tuple2.of(value, 1)
        ).returns(Types.TUPLE(Types.STRING, Types.INT));

        //3.3分组
        //注意:DataSet中分组用groupBy,DataStream中分组用keyBy
        //KeyedStream<Tuple2<String, Integer>, Tuple> keyedDS = wordAndOneDS.keyBy(0);
        /*
        public interface KeySelector<IN, KEY> extends Function, Serializable {
            KEY getKey(IN value) throws Exception;
        }
         */
        /*KeyedStream<Tuple2<String, Integer>, String> keyedDS = wordAndOneDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });*/
        KeyedStream<Tuple2<String, Integer>, String> keyedDS = wordAndOneDS.keyBy((Tuple2<String, Integer> value) -> value.f0);

        //3.4聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedDS.sum(1);

        //TODO 4.sink-数据输出
        result.print();

        //TODO 5.execute-执行
        env.execute();
    }
}
