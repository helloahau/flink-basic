package com.bigdata.day06;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import com.bigdata.sql.WC;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @基本功能:
 * @program:FlinkDemo
 * @author: 闫哥
 * @create:2025-12-02 11:02:46
 **/
public class Demo04 {

    public static void main(String[] args) throws Exception {

        //1. env-准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取tableEnv对象
        // 通过env 获取一个table 环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8888);
        // hello abc hello spark
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] arr = line.split("\\s+");
                for (String word : arr) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        Table table = tableEnv.fromDataStream(flatMap,$("word"),$("num"));
        // DSL的语法
        Table resultTable = table.groupBy($("word")).select($("word"), $("num").sum().as("sumNum"));
        // resultTable.printSchema();


        // 创建一个schema ，发现返回值依然是 Row对象，所以没什么特别大的效果，以后不写了
        /*Schema schema = Schema.newBuilder()
                .column("word", DataTypes.STRING())
                .column("sumNum", DataTypes.INT())
                .build();*/

        //DataStream<Row> changelogStream = tableEnv.toChangelogStream(resultTable,schema);
        DataStream<Row> changelogStream = tableEnv.toChangelogStream(resultTable);
        //changelogStream.print();

        changelogStream.map(new MapFunction<Row, WC>() {
            @Override
            public WC map(Row row) throws Exception {

                String word = (String) row.getField("word");
                int sumNum = (int) row.getField("sumNum");

                return new WC(word,sumNum);
            }
        }).print();


        env.execute();
    }
}
