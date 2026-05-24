// Source: Day05-FlinkSQL (pb9wqs8sv708mmpl) snippet #6
package com.bigdata.snippets;

public class pb9wqs8sv708mmplSnippet006 {
    // Tutorial snippet — may require surrounding context to compile.
    public static void demo() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 读取数据源
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.fromSource(...)

        // 将数据流转换成表
        Table sensorTable = tableEnv.fromDataStream(sensorDS);
    }
}
