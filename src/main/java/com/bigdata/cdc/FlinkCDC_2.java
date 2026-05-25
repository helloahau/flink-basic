package com.bigdata.cdc;

import com.bigdata.schema.CustomerDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @基本功能:
 * @program:FlinkProject
 * @create:2023-12-22 09:54:52
 *
 * Flink 2.x: uses CDC 3.x MySqlSource (FLIP-27 Source) + fromSource().
 * Checkpoint storage configured via Configuration (setCheckpointStorage removed in Flink 2.x).
 **/
class FlinkCDC_2 {

    public static void main(String[] args) throws Exception {
        //1.获取Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 开启CK
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // Flink 2.x: setCheckpointStorage(String) removed; use Configuration instead
        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "hdfs://bigdata01:9820/cdc-test/ck");
        env.configure(config);
        System.setProperty("HADOOP_USER_NAME", "root");

        //2.通过 CDC 3.x MySqlSource 构建 Source (FLIP-27 新接口，取代 DebeziumSourceFunction)
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
                .deserializer(new CustomerDeserializationSchema())
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
