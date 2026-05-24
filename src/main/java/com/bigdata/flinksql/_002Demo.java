package com.bigdata.flinksql;

import com.bigdata.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class _002Demo {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取tableEnv
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 2L, 2),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3),
                new WaterSensor("s3", 4L, 4)
        );
        // 第一个需求，ts > 2 的用户数据
        tableEnv.createTemporaryView("sensor", sensorDS);
        Table table = tableEnv.sqlQuery("select id,ts,vc from sensor where ts>2");
        DataStream<WaterSensor> dataStream = tableEnv.toDataStream(table, WaterSensor.class);
        dataStream.print();
        // 统计每一个用户的 总的浏览量
        // 也可以使用别的方法，将一个流变为一个表
        Table sensorTable = tableEnv.fromDataStream(sensorDS);
        tableEnv.createTemporaryView("sensor2", sensorTable);

        Table table2 = tableEnv.sqlQuery("select id,sum(vc) from sensor2 group by id");
        DataStream<Row> changelogStream = tableEnv.toChangelogStream(table2);
        changelogStream.print();

        env.execute();
    }
}
