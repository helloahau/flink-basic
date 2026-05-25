package com.bigdata.iceberg;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;

import java.security.PrivilegedExceptionAction;

/**
 * @基本功能: Iceberg 批量写入示例
 *
 * 场景说明:
 *   模拟水位传感器数据 (WaterSensor) 批量写入到 Iceberg 格式的数据仓库。
 *   使用 HadoopCatalog + 本地文件系统，无需外部服务即可运行。
 *
 * Catalog 选项:
 *   A) 默认 – HadoopCatalog, 本地路径 /tmp/flink-iceberg-warehouse  ← 本文件使用
 *   B) 注释块 – Nessie REST Catalog + MinIO (lakehouse-nessie:19120 + minio:9000)
 *
 * 前提依赖 (pom.xml 已添加):
 *   org.apache.iceberg:iceberg-flink-runtime-2.1:1.11.0
 *   org.apache.hadoop:hadoop-common:3.3.6
 *
 * 运行后数据位置 (Medallion Architecture — Bronze 层):
 *   ./flink-iceberg-warehouse/bronze/flink_demo/water_sensors/
 *
 * Flink UI (本地批任务不启动 Web UI；若提交到集群可访问 http://localhost:8082)
 **/
public class _01_IcebergBatchWrite {

    /** Medallion Architecture: 原始数据写入 Bronze 层 */
    private static final String WAREHOUSE = "./flink-iceberg-warehouse/bronze";

    public static void main(String[] args) throws Exception {

        // Java 17.0.13 fix: init Hadoop UGI in simple mode, then wrap all work
        // in Subject.doAs() so Subject.getSubject(AccessControlContext) works.
        HadoopCompat.initSimpleAuth();
        HadoopCompat.runWithSubject((PrivilegedExceptionAction<Void>) () -> {
            run();
            return null;
        });
    }

    private static void run() throws Exception {

        // ── 1. 环境准备 ──────────────────────────────────────────────
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);   // 批处理模式
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ── 2. 注册 Iceberg Catalog ───────────────────────────────────
        // ▶ 选项 A: HadoopCatalog + 本地文件系统（推荐本地测试）
        tableEnv.executeSql(
            "CREATE CATALOG IF NOT EXISTS iceberg_local WITH ("
            + "'type'             = 'iceberg',"
            + "'catalog-type'     = 'hadoop',"
            + "'warehouse'        = '" + WAREHOUSE + "'"
            + ")"
        );

        // ▶ 选项 B: Nessie REST Catalog + MinIO（取消注释后屏蔽选项 A）
        // tableEnv.executeSql(
        //     "CREATE CATALOG IF NOT EXISTS iceberg_local WITH ("
        //     + "'type'                   = 'iceberg',"
        //     + "'catalog-impl'           = 'org.apache.iceberg.rest.RESTCatalog',"
        //     + "'uri'                    = 'http://localhost:19120/iceberg',"
        //     + "'warehouse'              = 's3a://gold/warehouse',"
        //     + "'io-impl'                = 'org.apache.iceberg.aws.s3.S3FileIO',"
        //     + "'s3.endpoint'            = 'http://localhost:9000',"
        //     + "'s3.access-key-id'       = 'minioadmin',"
        //     + "'s3.secret-access-key'   = 'minioadmin123',"
        //     + "'s3.path-style-access'   = 'true'"
        //     + ")"
        // );

        tableEnv.useCatalog("iceberg_local");

        // ── 3. 创建 Database ──────────────────────────────────────────
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS flink_demo");
        tableEnv.useDatabase("flink_demo");

        // ── 4. 创建 Iceberg 目标表 ────────────────────────────────────
        //   Iceberg 表属性:
        //     format-version = 2  → 支持 Row-level delete (MERGE INTO / DELETE FROM)
        //     write.format.default = parquet (Iceberg 默认)
        tableEnv.executeSql(
            "CREATE TABLE IF NOT EXISTS water_sensors ("
            + "  id     STRING   COMMENT '传感器ID',"
            + "  ts     BIGINT   COMMENT '事件时间戳(ms)',"
            + "  vc     INT      COMMENT '水位值(cm)'"
            + ") WITH ("
            + "  'format-version' = '2'"
            + ")"
        );

        // ── 5. 批量写入数据（方式一：VALUES 直接写入）─────────────────
        System.out.println(">>> 开始写入传感器数据到 Iceberg 表...");

        tableEnv.executeSql(
            "INSERT INTO water_sensors VALUES "
            + "('s1', 1000,  10),"  // 正常水位
            + "('s1', 2000,  13),"
            + "('s1', 3000, 100),"  // 异常高水位
            + "('s2', 1000,  20),"
            + "('s2', 2000,  22),"
            + "('s2', 3000,   5),"  // 正常低水位
            + "('s3', 1000,  30),"
            + "('s3', 2000,   8),"
            + "('s3', 3000,  40),"
            + "('s4', 1000,   3),"
            + "('s4', 2000, 150),"  // 极值
            + "('s4', 3000,   7)"
        ).await();   // ← ensure data is committed before verification reads

        // ── 6. 验证写入结果 (使用 Iceberg Java API，无 mini-cluster 竞争条件)
        System.out.println(">>> 写入完成，验证 Iceberg 表内容：");
        try (HadoopCatalog cat = new HadoopCatalog(new Configuration(false), WAREHOUSE)) {
            Table table = cat.loadTable(TableIdentifier.of("flink_demo", "water_sensors"));
            String records = table.currentSnapshot().summary().get("added-records");
            System.out.println("  total rows in snapshot: " + records);

            System.out.println(">>> 各传感器记录数 & 最大水位：");
            java.util.Map<String, long[]> agg = new java.util.TreeMap<>();   // [count, maxVc]
            try (CloseableIterable<Record> recs = IcebergGenerics.read(table).build()) {
                for (Record r : recs) {
                    String id = (String) r.get(0);
                    int vc = (Integer) r.get(2);
                    long[] v = agg.computeIfAbsent(id, k -> new long[]{0, Long.MIN_VALUE});
                    v[0]++;
                    v[1] = Math.max(v[1], vc);
                }
            }
            System.out.printf("  %-5s %-12s %-8s%n", "id", "record_count", "max_vc");
            agg.forEach((id, v) ->
                System.out.printf("  %-5s %-12d %-8d%n", id, v[0], v[1]));
        }

        System.out.println(">>> Iceberg 数据写入完成！数据目录: " + WAREHOUSE + "/flink_demo/water_sensors  [Bronze 层]");
    }
}

