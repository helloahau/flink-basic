package com.bigdata.flinksql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @基本功能:
 * @program:FlinkDemo2
 * @author: 闫哥
 * @create:2025-04-19 17:06:52
 **/
public class _003FlinkSQLWordCount {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取tableEnv对象
        // 通过env 获取一个table 环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //2. 创建表对象
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8899);
        // hello hello word abc hello
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapStream = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] arr = line.split("\\s+");
                for (String word : arr) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });
        tEnv.createTemporaryView("wordcount", flatMapStream,$("word"),$("num"));

        Table table = tEnv.sqlQuery("select word,sum(num) as cnt from wordcount group by word");
        //3. 编写sql语句
        //4. 将Table变为stream流
        DataStream<Row> changelogStream = tEnv.toChangelogStream(table);
        changelogStream.print();

        //tEnv.toDataStream(table).print();


        //5. execute-执行
        env.execute();
    }
}
