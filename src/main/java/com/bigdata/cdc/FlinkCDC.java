package com.bigdata.cdc;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @基本功能:
 * @program:FlinkProject
 * @author: 闫哥
 * @create:2025-12-22 09:54:52
 **/
public class FlinkCDC {

    public static void main(String[] args) throws Exception {
        //1.获取Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 开启CK
        //        env.enableCheckpointing(5000);
        //        env.getCheckpointConfig().setCheckpointTimeout(10000);
        //        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //
        //        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/cdc-test/ck"));

        //2.通过FlinkCDC构建SourceFunction
        // 此处经常使用的是initial  还有lastet 这两个经常使用
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
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
        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        //3.数据打印
        dataStreamSource.print();

        //4.启动任务
        env.execute("FlinkCDC");
    }

}
