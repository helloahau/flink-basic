package com.bigdata;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount02 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource =null;
        if(args.length == 0){
            dataStreamSource = env.fromElements("spark flink kafka", "spark sqoop flink", "kakfa hadoop flink");

        }else{
            FileSource<String> fileSource = FileSource
                    .forRecordStreamFormat(
                            new TextLineInputFormat(),
                            new Path(args[0])
                    )
                    .build();

            dataStreamSource = env
                    .fromSource(fileSource, WatermarkStrategy.noWatermarks(), "filesource");

        }


        // 明确目标  进行单词统计
        SingleOutputStreamOperator<String> flattedMap = dataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                //System.out.println(line);
                String[] arr = line.split(" ");

                for (String word : arr) {
                    collector.collect(word);
                }
            }
        });

        System.out.println(dataStreamSource.getParallelism());
        flattedMap.setParallelism(2);
        System.out.println(flattedMap.getParallelism());
        //flattedMap.print();
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = flattedMap.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });
        //mapStream.print();

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = mapStream.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);

        sum.print();


        env.execute();
    }
}
