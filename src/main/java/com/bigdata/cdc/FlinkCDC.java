package com.bigdata.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @基本功能:
 * @program:FlinkProject
 * @create:2025-12-22 09:54:52
 *
 * Flink 2.x: uses CDC 3.x MySqlSource which implements the FLIP-27 Source interface.
 * env.fromSource() replaces the old env.addSource(DebeziumSourceFunction).
 **/
public class FlinkCDC {

    public static void main(String[] args) throws Exception {
        //1.获取Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 开启CK (optional, commented out for simplicity)
        //        env.enableCheckpointing(5000);
        //        env.getCheckpointConfig().setCheckpointTimeout(10000);
        //        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        //2.通过 CDC 3.x MySqlSource 构建 Source (FLIP-27 新接口，取代 DebeziumSourceFunction)
        // 此处经常使用的是 initial 还有 latest 这两个经常使用
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("bigdata01")
                .port(3306)
                .username("root")
                .password("123456")
                // 监听的数据库有哪些
                .databaseList("cdc_test")
                // 监听的表有哪些，表前面要写库
                .tableList("cdc_test.user_info")
                // binlog 将来以何种面貌示人
                .deserializer(new StringDebeziumDeserializationSchema())
                // 从哪里开始进行抽取
                .startupOptions(StartupOptions.initial())
                .build();

        //3.数据打印
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Binlog Source")
                .print();

        //4.启动任务
        env.execute("FlinkCDC");
    }

}
